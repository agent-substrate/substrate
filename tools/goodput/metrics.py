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

"""Goodput numbers for one window: counts, rates and ratios."""

from typing import Any, Optional

import classify


def percentile(values: list[float], p: float) -> Optional[float]:
    """Linear-interpolated percentile (p in 0..100), None for no values."""
    if not values:
        return None
    xs = sorted(values)
    if len(xs) == 1:
        return round(xs[0], 3)
    k = (len(xs) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return round(xs[lo] + (xs[hi] - xs[lo]) * (k - lo), 3)


def in_window(t: Optional[float], window: tuple[float, float]) -> bool:
    return t is not None and window[0] <= t <= window[1]


def window_metrics(rows: list[dict[str, Any]], startup_ts: list[float], episode_ts: list[float],
                   window: tuple[float, float], capacity: dict[str, Optional[float]],
                   refs: dict[str, float], ratio_fields: list[str]) -> dict[str, Any]:
    """Counts and rates for tasks that belong to the window.

    rows are classified tasks ({"status", "reasons", "end", "first_failure_t",
    "wall_ms", "values"}, values being the record's check fields). A finished
    task belongs to the window it ended in, and an unfinished task (always bad)
    to the window of its first failure. Startup failures are reported as
    attempts and episodes and stay out of the ratio.

    For each field in ratio_fields (the applied baseline_ratio checks), the
    p50/p90/p99 of value / refs[field] over finished tasks with a value.
    """
    length = max(0.0, window[1] - window[0])
    finished = [r for r in rows if in_window(r["end"], window)]
    unfinished_bad = [r for r in rows if r["end"] is None and in_window(r["first_failure_t"], window)]
    good = [r for r in finished if r["status"] == classify.GOOD]
    bad = [r for r in finished if r["status"] == classify.BAD]
    reasons: dict[str, int] = {}
    for r in bad + unfinished_bad:
        for reason in r["reasons"]:
            reasons[reason] = reasons.get(reason, 0) + 1
    hours = length / 3600.0
    per_hr = len(good) / hours if hours else None
    denom = len(finished) + len(unfinished_bad)
    walls = [r["wall_ms"] for r in good]
    m: dict[str, Any] = {
        "window_start_ts": window[0], "window_end_ts": window[1], "window_s": round(length, 3),
        "tasks_done": len(finished), "tasks_good": len(good), "tasks_bad": len(bad),
        "tasks_unfinished_bad": len(unfinished_bad), "bad_reasons": dict(sorted(reasons.items())),
        "startup_failure_attempts": sum(1 for t in startup_ts if in_window(t, window)),
        "startup_failure_episodes": sum(1 for t in episode_ts if in_window(t, window)),
        "good_tasks_per_hr": round(per_hr, 3) if per_hr is not None else None,
        "goodput_ratio_pct": round(100.0 * len(good) / denom, 2) if denom else None,
        "good_wall_ms_p50": percentile(walls, 50), "good_wall_ms_p90": percentile(walls, 90),
        "good_wall_ms_p99": percentile(walls, 99),
    }
    for key, cap in (("node", capacity.get("node_count")), ("vcpu", capacity.get("allocatable_cores")),
                     ("gb_ram", capacity.get("allocatable_ram_gb"))):
        m[f"good_tasks_per_{key}_hr"] = round(per_hr / cap, 4) if per_hr is not None and cap else None
    for f in ratio_fields:
        ratios = [r["values"][f] / refs[f] for r in finished if r["values"][f] is not None]
        for p in (50, 90, 99):
            m[f"{f}_ratio_p{p}"] = percentile(ratios, p)
    cel_ref = refs.get("cel_ms")
    if (cel_ref or 0) > 0 and walls:
        m["eta_infra"] = round(float(cel_ref) / m["good_wall_ms_p50"], 4)
    return m
