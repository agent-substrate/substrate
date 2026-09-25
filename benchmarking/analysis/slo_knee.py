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

"""Compute throughput-at-SLO-knee from a concurrency sweep of locust runs.

Each run in a sweep is a `stats.jsonl` file produced by runner.py: one JSON
object per gRPC op (Type_Name), carrying `requests_per_s` throughput and the
`p50..p100` response-time percentiles (milliseconds). A sweep is the set of
runs sharing a build `tag`, taken at increasing offered load (distinct
`test_name`s, e.g. glutton_baseline_1_user .. glutton_oversubscribe_15_users).

The SLO-knee throughput for an op is the *maximum sustained RPS achieved while
a chosen latency percentile stays under a ceiling*. Concretely, among the
sweep points whose p95 (or p99, ...) response time is <= the ceiling AND whose
failure ratio is within tolerance, the knee is the point with the highest RPS.
Past the knee, offered load keeps climbing but latency has blown past the SLO,
so that extra "throughput" is not usable capacity.

This maps directly onto the spec-doc substrate-row axes: throughput @<1s and
@<5s are `--ceiling-ms 1000` and `--ceiling-ms 5000`; p50/p95 are read straight
off each point.

Offered concurrency (the `-u` user count) is NOT present in stats.jsonl -- it
lives in tests.yaml / the runner argv. The knee metric does not need it (it is
computed from (rps, latency) pairs), but supplying a name->users map via
--users-by-name or --tests-yaml labels each point with its concurrency, which
is what makes the knee's *location* interpretable.

Pure stdlib: no locust, pandas, or numpy dependency, so it runs offline over
saved run artifacts and is unit-testable with synthetic fixtures.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Iterable

# Default latency ceilings (ms) == the spec-doc substrate-row throughput axes
# (@<1s and @<5s).
DEFAULT_CEILINGS_MS = (1000.0, 5000.0)

# A sweep point whose failure ratio exceeds this is not a valid throughput
# measurement -- a run that "achieves" high RPS by fast-failing requests is
# not delivering usable capacity, so it must not be eligible to be the knee.
DEFAULT_MAX_FAILURE_RATIO = 0.01


def normalize_percentile_key(p: str) -> str:
    """Map a user-facing percentile spec onto the measurements dict key.

    runner.py encodes locust's CSV percentile columns as p<pct> with dots
    turned into underscores: "50%"->"p50", "95%"->"p95", "99.9%"->"p99_9".
    Accept any of "p95", "95", "95%", "p99.9", "99.9" and return "p95"/"p99_9".
    """
    s = p.strip().lower().lstrip("p").rstrip("%")
    return "p" + s.replace(".", "_")


def _to_float(v: object) -> float | None:
    """Coerce a measurements value (always a string from csv.DictReader) to
    float, treating locust's empty / "N/A" placeholders as missing."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.upper() == "N/A":
        return None
    try:
        return float(s)
    except ValueError:
        return None


@dataclass
class Point:
    """One sweep point: one op (metric) from one run (test_name/tag)."""

    tag: str
    test_name: str
    metric: str
    rps: float | None
    request_count: float | None
    failure_count: float | None
    latencies_ms: dict[str, float]  # normalized key -> ms
    users: int | None = None

    @property
    def failure_ratio(self) -> float | None:
        if self.request_count is None or self.request_count <= 0:
            return None
        fc = self.failure_count or 0.0
        return fc / self.request_count

    def latency(self, pct_key: str) -> float | None:
        return self.latencies_ms.get(pct_key)


def record_to_point(record: dict, users_map: dict[str, int] | None = None) -> Point:
    m = record.get("measurements", {}) or {}
    latencies = {}
    for k, v in m.items():
        if k.startswith("p"):
            fv = _to_float(v)
            if fv is not None:
                latencies[k] = fv
    test_name = record.get("test_name", "") or ""
    users = None
    if users_map:
        users = users_map.get(test_name)
    return Point(
        tag=record.get("tag", "") or "",
        test_name=test_name,
        metric=record.get("metric", "") or "",
        rps=_to_float(m.get("requests_per_s")),
        request_count=_to_float(m.get("request_count")),
        failure_count=_to_float(m.get("failure_count")),
        latencies_ms=latencies,
        users=users,
    )


def iter_jsonl_records(paths: Iterable[str]) -> Iterable[dict]:
    for path in paths:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def expand_inputs(inputs: Iterable[str]) -> list[str]:
    """Resolve each input (a stats.jsonl file, a glob, or a directory that is
    walked for stats.jsonl / *.jsonl) into a flat list of jsonl file paths."""
    paths: list[str] = []
    for item in inputs:
        if os.path.isdir(item):
            found = sorted(glob.glob(os.path.join(item, "**", "*.jsonl"), recursive=True))
            paths.extend(found)
        elif any(ch in item for ch in "*?["):
            paths.extend(sorted(glob.glob(item, recursive=True)))
        else:
            paths.append(item)
    # De-dup while preserving order.
    seen = set()
    out = []
    for p in paths:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


@dataclass
class KneeResult:
    tag: str
    metric: str
    percentile: str  # normalized key, e.g. "p95"
    ceiling_ms: float
    knee_rps: float | None
    knee_users: int | None
    knee_test_name: str | None
    knee_latency_ms: float | None
    slo_ever_met: bool  # any point under ceiling (before failure filter)
    n_points: int
    n_under_ceiling: int
    n_valid: int  # under ceiling AND within failure tolerance
    points: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "tag": self.tag,
            "metric": self.metric,
            "percentile": self.percentile,
            "ceiling_ms": self.ceiling_ms,
            "knee_rps": self.knee_rps,
            "knee_users": self.knee_users,
            "knee_test_name": self.knee_test_name,
            "knee_latency_ms": self.knee_latency_ms,
            "slo_ever_met": self.slo_ever_met,
            "n_points": self.n_points,
            "n_under_ceiling": self.n_under_ceiling,
            "n_valid": self.n_valid,
            "points": self.points,
        }


def compute_knee(
    points: list[Point],
    pct_key: str,
    ceiling_ms: float,
    max_failure_ratio: float,
) -> KneeResult:
    """Knee = max-RPS point among those with latency<=ceiling and an
    acceptable failure ratio. `points` must already be a single (tag, metric)
    group."""
    tag = points[0].tag if points else ""
    metric = points[0].metric if points else ""

    under_ceiling = 0
    valid: list[Point] = []
    point_rows = []
    for p in points:
        lat = p.latency(pct_key)
        fr = p.failure_ratio
        latency_ok = lat is not None and lat <= ceiling_ms
        failure_ok = fr is None or fr <= max_failure_ratio
        if latency_ok:
            under_ceiling += 1
        # A point is knee-eligible only if it has an RPS reading, is under the
        # latency ceiling, and did not fast-fail its way there.
        if latency_ok and failure_ok and p.rps is not None:
            valid.append(p)
        point_rows.append(
            {
                "test_name": p.test_name,
                "users": p.users,
                "rps": p.rps,
                pct_key: lat,
                "failure_ratio": fr,
                "latency_ok": latency_ok,
                "failure_ok": failure_ok,
                "knee_eligible": latency_ok and failure_ok and p.rps is not None,
            }
        )

    knee = max(valid, key=lambda p: p.rps) if valid else None
    return KneeResult(
        tag=tag,
        metric=metric,
        percentile=pct_key,
        ceiling_ms=ceiling_ms,
        knee_rps=(knee.rps if knee else None),
        knee_users=(knee.users if knee else None),
        knee_test_name=(knee.test_name if knee else None),
        knee_latency_ms=(knee.latency(pct_key) if knee else None),
        slo_ever_met=under_ceiling > 0,
        n_points=len(points),
        n_under_ceiling=under_ceiling,
        n_valid=len(valid),
        points=point_rows,
    )


def group_points(points: list[Point]) -> dict[tuple[str, str], list[Point]]:
    groups: dict[tuple[str, str], list[Point]] = {}
    for p in points:
        groups.setdefault((p.tag, p.metric), []).append(p)
    return groups


def analyze(
    records: Iterable[dict],
    percentiles: list[str],
    ceilings_ms: list[float],
    users_map: dict[str, int] | None = None,
    max_failure_ratio: float = DEFAULT_MAX_FAILURE_RATIO,
    metric_filter: str | None = None,
) -> list[KneeResult]:
    points = [record_to_point(r, users_map) for r in records]
    if metric_filter:
        points = [p for p in points if metric_filter in p.metric]
    groups = group_points(points)
    results: list[KneeResult] = []
    for (_tag, _metric), grp in sorted(groups.items()):
        # Order a group's points by offered load when known, else by RPS, so
        # the emitted `points` table reads as a load ladder.
        grp_sorted = sorted(
            grp,
            key=lambda p: (p.users if p.users is not None else -1, p.rps or 0.0),
        )
        for pct in percentiles:
            pct_key = normalize_percentile_key(pct)
            for ceiling in ceilings_ms:
                results.append(
                    compute_knee(grp_sorted, pct_key, ceiling, max_failure_ratio)
                )
    return results


def parse_users_by_name(spec: str | None) -> dict[str, int]:
    """Parse "name1=1,name2=5,name3=10" into {name: users}."""
    out: dict[str, int] = {}
    if not spec:
        return out
    for chunk in spec.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        name, _, val = chunk.partition("=")
        if not _:
            raise ValueError(f"bad --users-by-name entry (want name=users): {chunk!r}")
        out[name.strip()] = int(val.strip())
    return out


def users_map_from_tests_yaml(path: str) -> dict[str, int]:
    """Read tests.yaml -> {test.name: test.users}. Lazy-imports PyYAML so the
    module has no hard YAML dependency for the common (jsonl-only) path."""
    import yaml  # lazy: only needed when --tests-yaml is used

    with open(path) as f:
        doc = yaml.safe_load(f) or {}
    out: dict[str, int] = {}
    for t in doc.get("tests", []) or []:
        name = t.get("name")
        users = t.get("users")
        if name is not None and users is not None:
            out[name] = int(users)
    return out


def format_table(results: list[KneeResult]) -> str:
    lines = []
    for r in results:
        header = (
            f"[{r.metric}]  {r.percentile} <= {r.ceiling_ms:.0f}ms  tag={r.tag or '(none)'}"
        )
        lines.append(header)
        if r.knee_rps is None:
            if not r.slo_ever_met:
                lines.append(
                    f"  SLO NEVER MET: no point kept {r.percentile} <= {r.ceiling_ms:.0f}ms "
                    f"across {r.n_points} point(s)"
                )
            else:
                lines.append(
                    f"  no valid knee: {r.n_under_ceiling} point(s) under ceiling but all "
                    f"exceeded the failure tolerance"
                )
        else:
            loc = (
                f"@ {r.knee_users} users"
                if r.knee_users is not None
                else f"@ {r.knee_test_name}"
            )
            lines.append(
                f"  knee = {r.knee_rps:.2f} rps {loc} "
                f"({r.percentile}={r.knee_latency_ms:.0f}ms; "
                f"{r.n_valid}/{r.n_points} points valid)"
            )
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "inputs",
        nargs="+",
        help="stats.jsonl file(s), glob(s), or run director(y/ies) to walk for *.jsonl",
    )
    p.add_argument(
        "--percentile",
        "-p",
        action="append",
        default=None,
        help="latency percentile to gate on (repeatable). Default: p95. "
        "Accepts p95 / 95 / 95%% / p99_9 / 99.9",
    )
    p.add_argument(
        "--ceiling-ms",
        type=float,
        action="append",
        default=None,
        help=f"latency ceiling in ms (repeatable). Default: {list(DEFAULT_CEILINGS_MS)}",
    )
    p.add_argument(
        "--max-failure-ratio",
        type=float,
        default=DEFAULT_MAX_FAILURE_RATIO,
        help=f"exclude sweep points whose failure ratio exceeds this "
        f"(default {DEFAULT_MAX_FAILURE_RATIO})",
    )
    p.add_argument(
        "--users-by-name",
        default=None,
        help='label points with offered concurrency, e.g. "t1=1,t5=5,t10=10"',
    )
    p.add_argument(
        "--tests-yaml",
        default=None,
        help="tests.yaml to read test.name->test.users from (needs PyYAML)",
    )
    p.add_argument(
        "--metric-filter",
        default=None,
        help="only analyze metrics containing this substring (e.g. ResumeActor)",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="emit JSON instead of a human-readable table",
    )
    args = p.parse_args(argv)

    percentiles = args.percentile or ["p95"]
    ceilings = args.ceiling_ms or list(DEFAULT_CEILINGS_MS)

    users_map: dict[str, int] = {}
    if args.tests_yaml:
        users_map.update(users_map_from_tests_yaml(args.tests_yaml))
    if args.users_by_name:
        users_map.update(parse_users_by_name(args.users_by_name))

    paths = expand_inputs(args.inputs)
    if not paths:
        print("no input jsonl files resolved", file=sys.stderr)
        return 2
    records = list(iter_jsonl_records(paths))
    results = analyze(
        records,
        percentiles=percentiles,
        ceilings_ms=ceilings,
        users_map=users_map or None,
        max_failure_ratio=args.max_failure_ratio,
        metric_filter=args.metric_filter,
    )

    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2))
    else:
        print(format_table(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
