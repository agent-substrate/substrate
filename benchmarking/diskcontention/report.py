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
"""Builds the disk-contention report from run_arm.sh output directories.

For every run it computes probe resume and suspend (or pause) percentiles
from traces.txt inside the measurement window (warm-up excluded), joins the
node evidence from collect.json, then compares each arm with the probes-only
baseline (A) of the same runtime and lifecycle mode and prints a markdown
report with quality checks.

Standard library only.
"""

import argparse
import datetime
import glob
import json
import os
import random
import re
import statistics
import sys

PROBE_OPS = ("ProbeResumeActor", "ProbeSuspendActor", "ProbePauseActor")
# The probe op that hibernates an actor, by --lifecycle-mode.
HIBERNATE_OPS = {"suspend": "ProbeSuspendActor", "pause": "ProbePauseActor"}
SHUTDOWN_TAIL_S = 20


def arm_sort_key(arm: str) -> tuple[int, float, str]:
    """Orders A -> B<pct> -> Bmax -> B<pct>nf -> Bmaxnf -> C / C<n> -> other."""
    if arm == "A":
        return (0, 0.0, arm)
    m = re.fullmatch(r"B(\d+|max)(nf)?", arm)
    if m:
        pct_val = 999.0 if m.group(1) == "max" else float(m.group(1))
        group = 2 if m.group(2) else 1
        return (group, pct_val, arm)
    m = re.fullmatch(r"C(\d*)", arm)
    if m:
        n_val = float(m.group(1)) if m.group(1) else 8.0
        return (3, n_val, arm)
    return (99, 0.0, arm)


def pct(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    k = (len(s) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def parse_ts(s: str) -> float | None:
    """Parses an RFC 3339 timestamp, as the runner and kubectl print them.

    Before Python 3.11, fromisoformat takes neither "Z" nor fractional
    seconds other than 3 or 6 digits, and RFC 3339 allows up to nine (with
    trailing zeros trimmed), so the fraction is cut or padded to six.
    """
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    m = re.fullmatch(r"([^.]+)\.(\d+)(.*)", s)
    if m:
        head, frac, tz = m.groups()
        s = f"{head}.{frac[:6]:0<6}{tz}"
    try:
        return datetime.datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


def flag_value(flags: str, name: str, default=None):
    toks = flags.split()
    for i, t in enumerate(toks):
        if t == name and i + 1 < len(toks):
            return toks[i + 1]
    return default


def lifecycle_mode(meta: dict) -> str:
    """How the run's probes hibernate their actors: suspend (the default) or pause."""
    return flag_value(meta.get("flags", ""), "--lifecycle-mode", "suspend")


def probe_ops(meta: dict) -> tuple[str, ...]:
    """The probe ops a run records: resume, and suspend or pause."""
    op = HIBERNATE_OPS.get(lifecycle_mode(meta))
    return ("ProbeResumeActor",) + ((op,) if op else ())


def section(meta: dict) -> str:
    """The report section of a run: its runtime, plus its lifecycle mode unless suspend.

    Arms are compared with the A baseline of their own section, so pause runs
    are never compared with suspend runs.
    """
    mode = lifecycle_mode(meta)
    return meta["runtime"] if mode == "suspend" else f"{meta['runtime']} ({mode})"


def load_run(d: str) -> dict | None:
    meta_p = os.path.join(d, "meta.json")
    if not os.path.exists(meta_p):
        return None
    meta = json.load(open(meta_p))
    ws = meta["start"] + meta["warmup"]
    # Locust's stop cancels in-flight calls (Canceled / Aborted rows); drop the tail.
    we = meta["end"] - SHUTDOWN_TAIL_S
    run = {"dir": d, "meta": meta, "window_s": we - ws, "ops": {}, "noise": {}}

    traces = glob.glob(os.path.join(d, "*traces.txt"))[:1]
    samples: dict[str, list[float]] = {}
    fails: dict[str, int] = {}
    errs: dict[str, list[str]] = {}
    total_traces = 0
    for path in traces:
        with open(path) as f:
            next(f, None)
            for line in f:
                cols = line.rstrip("\n").split("\t")
                if len(cols) < 6:
                    continue
                t = parse_ts(cols[0])
                if t is None or t < ws or t > we:
                    continue
                total_traces += 1
                name = cols[1]
                if cols[5]:
                    fails[name] = fails.get(name, 0) + 1
                    errs.setdefault(name, []).append(cols[5][-160:])
                    continue
                try:
                    samples.setdefault(name, []).append(float(cols[2]))
                except ValueError:
                    pass
    for op in PROBE_OPS:
        xs = samples.get(op, [])
        run["ops"][op] = {
            "n": len(xs),
            "fail": fails.get(op, 0),
            "p50": pct(xs, 0.5),
            "p95": pct(xs, 0.95),
            "p99": pct(xs, 0.99),
            "samples": xs,
            "errors": errs.get(op, []),
        }
    write_n = len(samples.get("NoiseWriteDisk", []))
    wsize = int(flag_value(meta.get("flags", ""), "--dc-noise-write-size-bytes", 64 << 20))
    run["noise"] = {
        "writes": write_n,
        "write_fail": fails.get("NoiseWriteDisk", 0),
        "app_MBps": write_n * wsize / 1e6 / run["window_s"] if run["window_s"] > 0 else None,
        "sweperf_ops": sum(len(v) for k, v in samples.items()
                           if not k.startswith(("Probe", "Noise", "Glutton"))),
    }
    run["trace_rows_in_window"] = total_traces

    # Whole-run locust rows (warm-up included) for cross-checking trace coverage.
    stats_p = os.path.join(d, "stats.jsonl")
    run["stats_counts"] = {}
    if os.path.exists(stats_p):
        for line in open(stats_p):
            try:
                o = json.loads(line)
            except ValueError:
                continue
            m = o.get("metric", "")
            for op in PROBE_OPS:
                if m == f"grpc_{op}":
                    run["stats_counts"][op] = o.get("measurements", {}).get("request_count")
    col_p = os.path.join(d, "collect.json")
    run["collect"] = json.load(open(col_p)) if os.path.exists(col_p) else {}
    return run


def bootstrap_ratio(a: list[float], b: list[float], q: float, iters: int = 2000) -> tuple:
    """95% bootstrap CI of pct(b, q) / pct(a, q)."""
    if len(a) < 10 or len(b) < 10:
        return (None, None)
    rng = random.Random(1)
    rs = []
    for _ in range(iters):
        ra = pct([rng.choice(a) for _ in a], q)
        rb = pct([rng.choice(b) for _ in b], q)
        if ra:
            rs.append(rb / ra)
    if not rs:
        return (None, None)
    rs.sort()
    return (rs[int(0.025 * len(rs))], rs[int(0.975 * len(rs)) - 1])


def fmt(x, nd=0, suffix=""):
    if x is None:
        return "–"
    return f"{x:.{nd}f}{suffix}"


def pct_fmt(x, nd=0):
    if x is None:
        return "–"
    return fmt(x * 100, nd, "%")


def g(d: dict, *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def mean_of(runs: list[dict], *keys) -> float | None:
    """Mean of the numeric values at keys over runs, None if there are none."""
    vals = [g(r, *keys) for r in runs]
    vals = [v for v in vals if isinstance(v, (int, float))]
    return statistics.mean(vals) if vals else None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("out_dir")
    p.add_argument("--md", help="write markdown here (default stdout)")
    p.add_argument("--json", help="write per-run summary JSON here")
    a = p.parse_args()

    runs = [r for r in (load_run(os.path.dirname(m)) for m in
                        glob.glob(os.path.join(a.out_dir, "**", "meta.json"), recursive=True)) if r]
    if not runs:
        print("no runs found", file=sys.stderr)
        return 1
    by = {}
    for r in runs:
        by.setdefault((section(r["meta"]), r["meta"]["arm"]), []).append(r)

    L = []
    L.append("# Disk contention repro: results\n")
    L.append(f"Runs: {len(runs)}. Generated {datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d %H:%M} UTC.\n")
    checks = []
    for sec in sorted({k[0] for k in by}):
        arms = sorted({k[1] for k in by if k[0] == sec}, key=arm_sort_key)
        base = by.get((sec, "A"), [])
        base_pool = {op: sum((r["ops"][op]["samples"] for r in base), []) for op in PROBE_OPS}
        L.append(f"\n## {sec}\n")
        L.append("### Probe latency (ms, pooled over reps, warm-up excluded)\n")
        L.append("| Arm | Reps | Op | n | p50 | p95 | p99 | p95 / A (95% CI) | p95 per rep |")
        L.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for arm in arms:
            rs = by[(sec, arm)]
            ops = sorted({op for r in rs for op in probe_ops(r["meta"])}, key=PROBE_OPS.index)
            for op in ops:
                pool = sum((r["ops"][op]["samples"] for r in rs), [])
                p95 = pct(pool, 0.95)
                ratio = ci = ""
                if arm != "A" and base_pool[op] and pool:
                    bp = pct(base_pool[op], 0.95)
                    lo, hi = bootstrap_ratio(base_pool[op], pool, 0.95)
                    ratio = f"{p95 / bp:.2f}" if bp else "–"
                    ci = f" ({fmt(lo, 2)}–{fmt(hi, 2)})" if lo else ""
                per_rep = ", ".join(fmt(r["ops"][op]["p95"]) for r in rs)
                L.append(f"| {arm} | {len(rs)} | {op.replace('Probe', '')} | {len(pool)} | "
                         f"{fmt(pct(pool, .5))} | {fmt(p95)} | {fmt(pct(pool, .99))} | "
                         f"{ratio}{ci} | {per_rep} |")

        L.append("\n### Node disk and pressure (mean over reps)\n")
        L.append("| Arm | Noise app MB/s | Disk write MB/s (avg / p95-30s) | Disk util | "
                 "W await ms | R await ms | Queue | PSI io some / full | PSI cpu some | CPU busy | "
                 "Net tx / rx MB/s | GCE non-Healthy (worst disk) | Dirty MB |")
        L.append("| --- " * 13 + "|")
        for arm in arms:
            rs = by[(sec, arm)]
            def m(*keys, rs=rs):
                return mean_of(rs, *keys)
            nonh = []
            for r in rs:
                st = g(r, "collect", "gce_disk", "performance_status_frac")
                if isinstance(st, dict):
                    rep_nonh = []
                    for dev, fr in st.items():
                        if "pvc" not in dev and isinstance(fr, dict):
                            rep_nonh.append(
                                min(1.0, sum(v for k, v in fr.items() if k != "Healthy" and isinstance(v, (int, float))))
                            )
                    if rep_nonh:
                        nonh.append(max(rep_nonh))
            L.append(
                f"| {arm} | {fmt(m('noise', 'app_MBps'), 1)} | "
                f"{fmt(m('collect', 'node', 'write_MBps_avg'), 1)} / "
                f"{fmt(m('collect', 'node', 'write_MBps_p95_30s'), 1)} | "
                f"{pct_fmt(m('collect', 'node', 'util_frac'), 0)} | "
                f"{fmt(m('collect', 'node', 'write_await_ms'), 1)} | "
                f"{fmt(m('collect', 'node', 'read_await_ms'), 1)} | "
                f"{fmt(m('collect', 'node', 'queue_depth_avg'), 1)} | "
                f"{pct_fmt(m('collect', 'node', 'psi_io_some_frac'), 1)} / "
                f"{pct_fmt(m('collect', 'node', 'psi_io_full_frac'), 1)} | "
                f"{pct_fmt(m('collect', 'node', 'psi_cpu_some_frac'), 1)} | "
                f"{pct_fmt(m('collect', 'node', 'cpu_busy_frac'), 0)} | "
                f"{fmt(m('collect', 'node', 'net_tx_MBps_avg'), 0)} / "
                f"{fmt(m('collect', 'node', 'net_rx_MBps_avg'), 0)} | "
                f"{pct_fmt(statistics.mean(nonh) if nonh else None, 0)} | "
                f"{fmt(m('collect', 'node', 'dirty_MB_avg'), 0)} |")

        # Runs with /var/lib/ate/snapshots on a disk of its own.
        snap_arms = [arm for arm in arms if any(g(r, "collect", "snapshots_node") for r in by[(sec, arm)])]
        if snap_arms:
            L.append("\n### Snapshot disk, /var/lib/ate/snapshots (mean over reps)\n")
            L.append("| Arm | Device | Write MB/s (avg / p95-30s) | Read MB/s | Util | W await ms | Queue |")
            L.append("| --- " * 7 + "|")
            for arm in snap_arms:
                rs = by[(sec, arm)]
                devices = sorted({g(r, "collect", "snapshots_node", "device") for r in rs} - {None})
                L.append(
                    f"| {arm} | {', '.join(devices)} | "
                    f"{fmt(mean_of(rs, 'collect', 'snapshots_node', 'write_MBps_avg'), 1)} / "
                    f"{fmt(mean_of(rs, 'collect', 'snapshots_node', 'write_MBps_p95_30s'), 1)} | "
                    f"{fmt(mean_of(rs, 'collect', 'snapshots_node', 'read_MBps_avg'), 1)} | "
                    f"{pct_fmt(mean_of(rs, 'collect', 'snapshots_node', 'util_frac'), 0)} | "
                    f"{fmt(mean_of(rs, 'collect', 'snapshots_node', 'write_await_ms'), 1)} | "
                    f"{fmt(mean_of(rs, 'collect', 'snapshots_node', 'queue_depth_avg'), 1)} |")

        L.append("\n### Server-side phase p95 (s, mean over reps)\n")
        phases = [
            ("checkpoint_phases", "worker_checkpoint"),
            ("checkpoint_phases", "persist"),
            ("checkpoint_phases", "total"),
            ("restore_phases", "download"),
            ("restore_phases", "oci_unpack"),
            ("restore_phases", "worker_restore"),
            ("restore_phases", "total"),
        ]
        L.append("| Arm | " + " | ".join(f"{k.split('_')[0]}.{ph}" for k, ph in phases) + " |")
        L.append("| --- " * (len(phases) + 1) + "|")
        for arm in arms:
            rs = by[(sec, arm)]
            cells = []
            for k, ph in phases:
                vals = [g(r, "collect", k, ph, "p95_s") for r in rs]
                vals = [v for v in vals if isinstance(v, (int, float))]
                cells.append(fmt(statistics.mean(vals), 2) if vals else "–")
            L.append(f"| {arm} | " + " | ".join(cells) + " |")

        # Quality checks per run.
        for arm in arms:
            for r in by[(sec, arm)]:
                meta = r["meta"]
                tag = f"{sec}/{arm}/{meta['tag']}"
                for op in probe_ops(meta):
                    o = r["ops"][op]
                    if o["n"] < 30:
                        checks.append(f"{tag}: only {o['n']} {op} samples in window (<30)")
                    tot = o["n"] + o["fail"]
                    if tot and o["fail"] / tot > 0.01:
                        checks.append(f"{tag}: {op} failure rate {o['fail'] / tot:.1%}: "
                                      + "; ".join(sorted(set(o["errors"]))[:3]))
                    sc = r["stats_counts"].get(op)
                    if sc and o["n"] + o["fail"] > float(sc):
                        checks.append(f"{tag}: {op} traces ({o['n']}) exceed locust count ({sc})")
                if meta["warmup"] < 120:
                    checks.append(f"{tag}: warm-up only {meta['warmup']}s")
                if str(meta.get("active_actors_before", "0")) not in ("0", "0.0", "?"):
                    checks.append(f"{tag}: {meta['active_actors_before']} actors active at start")
                rate = flag_value(meta.get("flags", ""), "--dc-noise-rate-mbps")
                users = meta.get("users", 0)
                probes = int(flag_value(meta.get("flags", ""), "--dc-probe-users", 0) or 0)
                mode = flag_value(meta.get("flags", ""), "--dc-noise-mode", "none")
                if mode == "disk" and rate and float(rate) > 0 and users > probes:
                    target = float(rate) * (users - probes)
                    got = r["noise"]["app_MBps"] or 0
                    if abs(got - target) / target > 0.2:
                        checks.append(f"{tag}: noise {got:.0f} MB/s vs target {target:.0f} MB/s")
                if mode == "disk" and r["noise"]["write_fail"]:
                    checks.append(f"{tag}: {r['noise']['write_fail']} noise writes failed")
                if mode == "sweperf" and not r["noise"]["sweperf_ops"]:
                    checks.append(f"{tag}: no sweperf operations recorded in window")
                if not r["collect"]:
                    checks.append(f"{tag}: collect.json missing")
                free = g(r, "collect", "node", "rootfs_free_frac_min")
                if isinstance(free, (int, float)) and free < 0.15:
                    checks.append(f"{tag}: boot disk free fell to {free:.0%} "
                                  "(kubelet evicts at ~10%; results may include evictions)")

        # Rep-to-rep spread of the baseline.
        for op in PROBE_OPS:
            p95s = [r["ops"][op]["p95"] for r in base if r["ops"][op]["p95"]]
            if len(p95s) >= 2:
                cv = statistics.pstdev(p95s) / statistics.mean(p95s)
                L.append(f"\nBaseline {op} p95 across reps: CV {cv:.0%} "
                         f"({', '.join(fmt(x) for x in p95s)} ms)")
                if cv > 0.25:
                    checks.append(f"{sec}: baseline {op} p95 CV {cv:.0%} > 25%")

    L.append("\n## Quality checks\n")
    L.extend([f"- ⚠️ {c}" for c in checks] or ["- ✅ all checks passed"])
    md = "\n".join(L) + "\n"
    if a.md:
        open(a.md, "w").write(md)
    print(md)
    if a.json:
        slim = []
        for r in runs:
            s = {k: v for k, v in r.items() if k not in ("collect",)}
            s["ops"] = {op: {k: v for k, v in o.items() if k != "samples"}
                        for op, o in r["ops"].items()}
            s["node"] = g(r, "collect", "node")
            slim.append(s)
        json.dump(slim, open(a.json, "w"), indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
