#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Aggregate the suspend/resume phase-breakdown log records into a report.

Reads the atelet and worker pod logs, either as kubectl logs dumps (JSON
lines; unrelated lines are skipped) or as a Cloud Logging export
(`gcloud logging read --format json`: a JSON array of entries with the record
under jsonPayload), and aggregates the two joinable record kinds the node
side emits, each written by both layers:

  - "Restore timing breakdown"     atelet (ate.actor.restore.duration.*)
                                   and ateom (ateom.actor.restore.duration.*)
  - "Checkpoint timing breakdown"  atelet (ate.actor.checkpoint.duration.*)
                                   and ateom (ateom.actor.checkpoint.duration.*)

The report answers "where does the SuspendActor / ResumeActor time go":
per-phase percentiles at each layer, and the slowest operations as nested
waterfalls (the ateom record joined under the atelet record of the same
actor).

Usage:
    phase_report.py run-logs/*.log [--csv DEST_DIR] [--slowest N]

Feed it one source per run: the kubectl dumps, or the Cloud Logging export,
not both, or every record counts twice.

The records are developer-facing logs, not metric API; this reader is the
consumer that makes them percentiles. See benchmarking/analysis/README.md.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import sys
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# The duration-key prefixes. Source of truth: cmd/atelet/metrics.go
# (restoreDurationMetric, checkpointDurationMetric) and
# cmd/ateom-microvm/phaselog.go.
BREAKDOWN_PREFIXES = {
    ("atelet", "restore"): "ate.actor.restore.duration.",
    ("atelet", "checkpoint"): "ate.actor.checkpoint.duration.",
    ("ateom", "restore"): "ateom.actor.restore.duration.",
    ("ateom", "checkpoint"): "ateom.actor.checkpoint.duration.",
}
BREAKDOWN_MSGS = {"Restore timing breakdown", "Checkpoint timing breakdown"}

ACTOR_UID_KEY = "ate.actor.uid"
SCOPE_KEY = "ate.snapshot.scope"
PHASE_KEY = "ate.snapshot.phase"
KIND_KEY = "ate.snapshot.kind"
SANDBOX_CLASS_KEY = "ate.sandbox.class"
TEMPLATE_KEY = "ate.template.name"
ERROR_TYPE_KEY = "error.type"

# The ateom records carry no sandbox class or snapshot kind of their own;
# both ateoms write the same two messages, and the atelet record of the same
# operation says which runtime it was. join_layers copies them across.
MICROVM = "microvm"
GVISOR = "gvisor"

# The phase every record reports, and the one the report derives: the part of
# the total no logged phase accounts for.
TOTAL = "total"
UNATTRIBUTED = "unattributed"

# Sequential order of the known phases, for display. Phases absent from a
# record simply don't print; unknown phases print after, in input order.
PHASE_ORDER = [
    # atelet restore
    "volume_mount", "manifest_fetch", "sandbox_assets", "download",
    "oci_unpack", "ateom_restore",
    # ateom restore: micro-VM (cmd/ateom-microvm/phaselog.go) and gVisor
    # (cmd/ateom-gvisor/phaselog.go), each in its own sequential order
    "prep", "egress_prepare", "net_setup", "bundles", "upper_join", "lowers",
    "pause_rootfs", "pause_create", "pause_restore",
    "app_rootfs", "app_create", "app_restore",
    "tap", "vmm_launch", "vm_restore", "resume", "wakeup_probe", "activate",
    # atelet + ateom checkpoint
    "pause", "checkpoint", "snapshot", "durable_dir", "rootfs_upper",
    "ateom_checkpoint", "teardown", "persist",
    UNATTRIBUTED, TOTAL,
]
_PHASE_RANK = {name: i for i, name in enumerate(PHASE_ORDER)}

# Which atelet phase wraps the ateom record.
INNER_PHASE = {"restore": "ateom_restore", "checkpoint": "ateom_checkpoint"}

# The micro-VM checkpoint captures that run concurrently on the paused guest:
# the paused window costs their max, not their sum. gVisor's checkpoint phases
# are sequential.
CONCURRENT_CAPTURES = ("snapshot", "durable_dir", "rootfs_upper")

# Slack on the window in which the ateom record of an operation must land:
# atelet does a little work after the ateom call returns before it writes its
# own record (unmounting, resetting the actor dirs), and the two clocks are
# the same node's but not the same goroutine's.
JOIN_SLACK_S = 1.0


@dataclass
class Breakdown:
    """One parsed timing-breakdown record."""
    source: str  # atelet | ateom
    op: str  # restore | checkpoint
    time: str
    ts: float | None  # epoch seconds parsed from time, None when unparseable
    actor_uid: str
    template: str
    sandbox_class: str
    scope: str
    kind: str
    phases: dict[str, float]  # phase name -> seconds
    failed: bool


@dataclass
class Parsed:
    breakdowns: list[Breakdown] = field(default_factory=list)
    lines_seen: int = 0
    lines_matched: int = 0
    bad_values: int = 0  # duration keys whose value was not a number


_TIME_RE = re.compile(r"^(.*T\d\d:\d\d:\d\d)(\.\d+)?(Z|[+-]\d\d:?\d\d)?$")


def parse_time(s: str) -> float | None:
    """RFC 3339 -> epoch seconds. slog writes nanoseconds, which fromisoformat
    rejects, so the fraction is trimmed to microseconds. None when unparseable
    (a record then still counts, it just cannot be joined by time)."""
    m = _TIME_RE.match(s.strip()) if s else None
    if not m:
        return None
    base, frac, tz = m.groups()
    frac = (frac or "")[:7]
    tz = "+00:00" if tz in (None, "Z") else tz
    try:
        return datetime.fromisoformat(f"{base}{frac}{tz}").timestamp()
    except ValueError:
        return None


def unattributed(source: str, op: str, sandbox_class: str, phases: dict[str, float]) -> float | None:
    """The total minus what the logged phases account for, where that is
    well-defined: the atelet checkpoint phases are sequential; the micro-VM
    ateom checkpoint is prep, pause, the concurrent captures (max), then
    teardown; the gVisor ateom checkpoint phases are all sequential. The
    atelet restore phases overlap by design (download runs alongside the
    asset fetch and OCI unpack) and both ateoms' restore phases partition
    their total by construction, so neither gets a residual; nor does an
    ateom checkpoint whose runtime is unknown (its atelet record is missing).
    The phases are sub-intervals of the total, so a negative result is a bug
    in the emitter or in this formula; it is not clamped, so that it shows."""
    total = phases.get(TOTAL)
    if total is None:
        return None
    if (source, op) == ("atelet", "checkpoint"):
        spent = sum(phases.get(p, 0.0) for p in ("sandbox_assets", "ateom_checkpoint", "persist"))
    elif (source, op, sandbox_class) == ("ateom", "checkpoint", MICROVM):
        spent = (phases.get("prep", 0.0) + phases.get("pause", 0.0)
                 + max(phases.get(p, 0.0) for p in CONCURRENT_CAPTURES)
                 + phases.get("teardown", 0.0))
    elif (source, op, sandbox_class) == ("ateom", "checkpoint", GVISOR):
        spent = sum(v for k, v in phases.items() if k != TOTAL)
    else:
        return None
    return total - spent


def parse_line(obj: dict, out: Parsed) -> None:
    # A Cloud Logging entry nests the record under jsonPayload and moves
    # slog's time onto the entry's timestamp; a kubectl dump is the record.
    payload = obj.get("jsonPayload")
    if isinstance(payload, dict):
        entry, obj = obj, dict(payload)
        obj.setdefault("time", entry.get("timestamp", ""))
    msg = obj.get("msg", "")
    if msg in BREAKDOWN_MSGS:
        for (source, op), prefix in BREAKDOWN_PREFIXES.items():
            phases: dict[str, float] = {}
            for k, v in obj.items():
                if not k.startswith(prefix):
                    continue
                try:
                    phases[k[len(prefix):]] = float(v)
                except (TypeError, ValueError):
                    # One bad value costs that phase, not the run's report.
                    out.bad_values += 1
            if not phases:
                continue
            time_s = obj.get("time", "")
            out.breakdowns.append(Breakdown(
                source=source,
                op=op,
                time=time_s,
                ts=parse_time(time_s),
                actor_uid=obj.get(ACTOR_UID_KEY, ""),
                template=obj.get(TEMPLATE_KEY, ""),
                sandbox_class=obj.get(SANDBOX_CLASS_KEY, ""),
                scope=obj.get(SCOPE_KEY, ""),
                kind=obj.get(KIND_KEY, ""),
                phases=phases,
                failed=ERROR_TYPE_KEY in obj,
            ))
            out.lines_matched += 1
            return


def parse_lines(lines: Iterable[str], out: Parsed | None = None) -> Parsed:
    """Parse one pod log, line by line, into out (a new Parsed by default).
    This is also the entry point for a caller that already holds the log
    text, such as the locust runner reading pods through the Kubernetes API."""
    out = Parsed() if out is None else out
    for line in lines:
        # kubectl log dumps may prefix each line (pod name, timestamp);
        # recover the JSON object from the first brace.
        brace = line.find("{")
        if brace < 0:
            continue
        out.lines_seen += 1
        try:
            obj = json.loads(line[brace:])
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            parse_line(obj, out)
    return out


def parse_files(paths: list[str]) -> Parsed:
    out = Parsed()
    for path in paths:
        f = sys.stdin if path == "-" else open(path, encoding="utf-8", errors="replace")
        with f:
            text = f.read()
        if text.lstrip().startswith("["):
            # A Cloud Logging export: one JSON array of entries, pretty-printed
            # across lines, so it cannot be read line by line.
            try:
                entries = json.loads(text)
            except json.JSONDecodeError as e:
                # Not an export after all: a dump whose lines carry a bracket
                # prefix, or a truncated export. Say so, then read it line by
                # line like any other dump rather than drop the whole file.
                print(f"warn: {path}: starts with '[' but is not a JSON array ({e}); "
                      f"reading it line by line", file=sys.stderr)
            else:
                if isinstance(entries, list):
                    for obj in entries:
                        if isinstance(obj, dict):
                            out.lines_seen += 1
                            parse_line(obj, out)
                    continue
        parse_lines(text.splitlines(), out)
    return out


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100, method="inclusive")[int(q) - 1]


def phase_sort_key(name: str) -> tuple[int, str]:
    return (_PHASE_RANK.get(name, len(PHASE_ORDER)), name)


def fmt_s(seconds: float) -> str:
    return f"{seconds * 1000:8.1f}"


def report_phases(breakdowns: list[Breakdown], writer) -> list[dict]:
    """Per (source, op, sandbox class, kind, scope, phase) percentiles. A
    golden restore (which downloads the golden image) and a latest restore,
    or a gVisor and a micro-VM checkpoint, are different distributions and
    must not be pooled. Returns the rows for CSV."""
    groups: dict[tuple, list[float]] = defaultdict(list)
    for b in breakdowns:
        if b.failed:
            continue
        for name, seconds in b.phases.items():
            groups[(b.source, b.op, b.sandbox_class or "-", b.kind or "-", b.scope or "-", name)].append(seconds)

    rows = []
    writer("== Phase percentiles (ms) ==")
    writer(f"{'layer':7} {'op':11} {'class':8} {'kind':7} {'scope':15} {'phase':16} {'n':>5} "
           f"{'p50':>8} {'p90':>8} {'p95':>8} {'max':>8}")
    for key in sorted(groups, key=lambda k: (k[:5], phase_sort_key(k[5]))):
        vals = sorted(groups[key])
        source, op, sandbox_class, kind, scope, name = key
        row = {
            "layer": source, "op": op, "class": sandbox_class, "kind": kind,
            "scope": scope, "phase": name,
            "count": len(vals),
            "p50_ms": percentile(vals, 50) * 1000,
            "p90_ms": percentile(vals, 90) * 1000,
            "p95_ms": percentile(vals, 95) * 1000,
            "max_ms": max(vals) * 1000,
        }
        rows.append(row)
        writer(f"{source:7} {op:11} {sandbox_class:8} {kind:7} {scope:15} {name:16} {len(vals):5d} "
               f"{fmt_s(percentile(vals, 50))} {fmt_s(percentile(vals, 90))} "
               f"{fmt_s(percentile(vals, 95))} {fmt_s(max(vals))}")
    failed = sum(1 for b in breakdowns if b.failed)
    if failed:
        writer(f"(excluded {failed} failed operation records)")
    return rows


def ateom_window(op: Breakdown) -> tuple[float, float] | None:
    """When, relative to the atelet record's time, the ateom record of the
    same operation was written. ateom writes it as its RPC returns, i.e. at
    the end of the atelet ateom_* phase: for a checkpoint that is before
    persist runs, for a restore it is the last phase. An operation that never
    reached the ateom call has no window and nothing to pair with."""
    total = op.phases.get(TOTAL)
    if op.ts is None or total is None or INNER_PHASE[op.op] not in op.phases:
        return None
    if op.op == "checkpoint":
        return (op.ts - total - JOIN_SLACK_S,
                op.ts - op.phases.get("persist", 0.0) + JOIN_SLACK_S)
    return (op.ts - op.phases[INNER_PHASE[op.op]] - JOIN_SLACK_S, op.ts + JOIN_SLACK_S)


def pair_records(breakdowns: list[Breakdown]) -> dict[int, Breakdown]:
    """Match each atelet record to the ateom record of the same operation:
    same actor and op, written inside the atelet ateom_* phase's window, and
    not already claimed by another operation. Returns id(atelet) -> ateom.

    One cycle emits exactly one of each. An ateom record outside the window
    belongs to another cycle whose partner is missing (pod gone, log rotated,
    window cut); claiming it would print a gap that never happened, so an
    operation without a match prints without one. Matching in time order
    with a consumed set keeps rapid cycles of one actor from sharing or
    swapping records. A paired ateom record also inherits the snapshot kind
    and sandbox class its layer does not log, so the two layers' percentiles
    split alike and the residual knows which runtime's rule applies."""
    by_actor: dict[tuple, list[Breakdown]] = defaultdict(list)
    for b in breakdowns:
        if b.source == "ateom" and b.ts is not None:
            by_actor[(b.actor_uid, b.op)].append(b)
    for v in by_actor.values():
        v.sort(key=lambda b: b.ts)

    pairs: dict[int, Breakdown] = {}
    consumed: set[int] = set()
    atelet = sorted((b for b in breakdowns if b.source == "atelet" and b.ts is not None),
                    key=lambda b: b.ts)
    for op in atelet:
        window = ateom_window(op)
        if window is None:
            continue
        lo, hi = window
        candidates = [a for a in by_actor.get((op.actor_uid, op.op), [])
                      if id(a) not in consumed and lo <= a.ts <= hi]
        if not candidates:
            continue
        inner = candidates[-1]
        consumed.add(id(inner))
        pairs[id(op)] = inner
        if not inner.kind:
            inner.kind = op.kind
        if not inner.sandbox_class:
            inner.sandbox_class = op.sandbox_class
    return pairs


def join_layers(breakdowns: list[Breakdown]) -> dict[int, Breakdown]:
    """Pair the layers' records, then derive each record's unattributed
    residual, which for an ateom checkpoint depends on the runtime the pairing
    just identified. Returns the pairs, as pair_records does."""
    pairs = pair_records(breakdowns)
    for b in breakdowns:
        residual = unattributed(b.source, b.op, b.sandbox_class, b.phases)
        if residual is not None:
            b.phases[UNATTRIBUTED] = residual
    return pairs


def report_waterfalls(breakdowns: list[Breakdown], writer, slowest: int,
                      pairs: dict[int, Breakdown] | None = None) -> None:
    """The slowest operations: the ateom record nested under the atelet
    ateom_* phase, and the gap between that phase and the ateom total (RPC
    and queueing between the layers)."""
    if pairs is None:
        pairs = join_layers(breakdowns)
    atelet = [b for b in breakdowns if b.source == "atelet" and not b.failed]
    atelet.sort(key=lambda b: b.phases.get(TOTAL, 0), reverse=True)

    writer("")
    writer(f"== Slowest {slowest} operations (waterfall, ms) ==")
    for b in atelet[:slowest]:
        inner = pairs.get(id(b))
        total = b.phases.get(TOTAL, 0)
        writer(f"{b.op} actor={b.actor_uid} template={b.template} "
               f"class={b.sandbox_class or '-'} scope={b.scope or '-'} kind={b.kind or '-'} "
               f"total={total * 1000:.1f}")
        for name in sorted(b.phases, key=phase_sort_key):
            if name == TOTAL:
                continue
            writer(f"  atelet {name:15} {fmt_s(b.phases[name])}")
            if name == INNER_PHASE.get(b.op) and inner:
                for iname in sorted(inner.phases, key=phase_sort_key):
                    if iname == TOTAL:
                        continue
                    writer(f"    ateom  {iname:13} {fmt_s(inner.phases[iname])}")
                if TOTAL in inner.phases:
                    gap = b.phases[name] - inner.phases[TOTAL]
                    writer(f"    (gap)  {'rpc/queueing':13} {fmt_s(gap)}")


def stats_rows(rows: list[dict], timestamp: str, tag: str, test_name: str) -> list[dict]:
    """report_phases() rows in the shape of the benchmark runner's stats.jsonl
    (benchmarking/locust/runner.py): one object per metric with the run's
    timestamp, tag and test name, and a flat, string-valued measurements map,
    as the runner's other writers emit. The metric name carries the row's
    dimensions so a dashboard can select a phase the way it selects a locust
    request type; the dimensions are also fields of the map, since phase
    names contain underscores and the name cannot be split back apart."""
    out = []
    for r in rows:
        dims = {k: (r[k] if r[k] and r[k] != "-" else "none")
                for k in ("layer", "op", "class", "kind", "scope", "phase")}
        metric = "_".join(("phase", *dims.values()))
        measurements = {**dims, **{k: r[k] for k in ("count", "p50_ms", "p90_ms", "p95_ms", "max_ms")}}
        out.append({
            "timestamp": timestamp,
            "tag": tag,
            "test_name": test_name,
            "metric": re.sub(r"[^a-z0-9_]+", "_", metric.lower()),
            "measurements": {k: str(v) for k, v in measurements.items()},
        })
    return out


def write_csv(dest: Path, name: str, rows: list[dict]) -> None:
    if not rows:
        return
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / name
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("logs", nargs="+",
                    help="kubectl logs dumps (JSON lines) or Cloud Logging exports "
                         "(JSON array, as gcloud logging read --format json writes); "
                         "'-' for stdin")
    ap.add_argument("--csv", type=Path, default=None,
                    help="also write phase_percentiles.csv here")
    ap.add_argument("--slowest", type=int, default=3,
                    help="number of slowest operations to print as waterfalls")
    args = ap.parse_args()

    parsed = parse_files(args.logs)
    print(f"parsed {parsed.lines_matched} timing breakdown records "
          f"out of {parsed.lines_seen} JSON log entries")
    if parsed.bad_values:
        print(f"(skipped {parsed.bad_values} non-numeric duration values)")
    if not parsed.breakdowns:
        print("no matching records; are these the atelet and worker pod logs?", file=sys.stderr)
        return 1

    # Pair before the percentiles: a paired ateom record takes its kind and
    # sandbox class from the atelet record, so both layers' rows split the
    # same way and each checkpoint's residual uses its runtime's rule.
    pairs = join_layers(parsed.breakdowns)
    print()
    phase_rows = report_phases(parsed.breakdowns, print)
    report_waterfalls(parsed.breakdowns, print, args.slowest, pairs)

    if args.csv:
        write_csv(args.csv, "phase_percentiles.csv", phase_rows)
        print(f"\nCSV written to {args.csv}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
