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


def _ref(row: dict[str, Any], baseline: Optional[dict[str, Any]], key: str) -> Optional[float]:
    """The row's own baseline value for key if it carries the key, else baseline's; None if unset."""
    v = row[key] if key in row else (baseline or {}).get(key)
    return float(v) if v else None


def window_metrics(rows: list[dict[str, Any]], startup_ts: list[float], window: tuple[float, float],
                   capacity: dict[str, Optional[float]], baseline: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Counts, rates and ratios for tasks that belong to the window.

    rows are classified tasks ({"status", "reasons", "end", "first_failure_t", "wall_ms"}, plus
    "fail_step", "cel_ms", "resume_max_ms" and "ttfe_max_ms" from a task record). A row's own
    "wall_ref_ms" and "task_cel_ref_ms" (from the baseline for its template) win over baseline's.
    A finished task belongs to the window it ended in; an unfinished bad task to
    the window of its first failure; unfinished tasks without a failure are not
    counted. Startup failures are reported as attempts only and stay out of the ratio.
    """
    length = max(0.0, window[1] - window[0])
    finished = [r for r in rows if in_window(r["end"], window)]
    unfinished_bad = [r for r in rows if r["end"] is None and r["status"] == classify.BAD
                      and in_window(r["first_failure_t"], window)]
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
        "startup_failure_episodes": None,  # not computed yet; only newer startup failure lines name the user
        "done_tasks_per_hr": round(len(finished) / hours, 3) if hours else None,
        "good_tasks_per_hr": round(per_hr, 3) if per_hr is not None else None,
        "goodput_ratio_pct": round(100.0 * len(good) / denom, 2) if denom else None,
        "good_wall_ms_p50": percentile(walls, 50), "good_wall_ms_p90": percentile(walls, 90),
        "good_wall_ms_p99": percentile(walls, 99),
    }
    for key, cap in (("node", capacity.get("node_count")), ("vcpu", capacity.get("allocatable_cores")),
                     ("gb_ram", capacity.get("allocatable_ram_gb"))):
        m[f"good_tasks_per_{key}_hr"] = round(per_hr / cap, 4) if per_hr is not None and cap else None
    # Ratios to the baseline. The wall ratio and eta_infra cover every finished
    # task; the CEL slowdown only those that passed G1, as a failed execute adds no CEL.
    done_walls, wall_ratios, slowdowns, cel_refs = [], [], [], set()
    for r in finished:
        wall_ref, cel_ref = _ref(r, baseline, "wall_ref_ms"), _ref(r, baseline, "task_cel_ref_ms")
        if r["wall_ms"]:
            done_walls.append(r["wall_ms"])
            cel_refs.add(cel_ref)
            if wall_ref:
                wall_ratios.append(r["wall_ms"] / wall_ref)
        if cel_ref and r.get("cel_ms") is not None and not r.get("fail_step"):
            slowdowns.append(r["cel_ms"] / cel_ref)
    # Each task record's slowest resume (0 when every resume was a no-op) and TTFE.
    resumes = [r["resume_max_ms"] for r in finished if r.get("resume_max_ms")]
    ttfes = [r["ttfe_max_ms"] for r in finished if r.get("ttfe_max_ms") is not None]
    for name, xs in (("wall_ratio", wall_ratios), ("cel_slowdown", slowdowns), ("resume_max_ms", resumes),
                     ("ttfe_max_ms", ttfes)):
        for p in (50, 90, 99):
            m[f"{name}_p{p}"] = percentile(xs, p)
    # Reference CEL / median wall of the finished tasks, when they share one reference.
    cel_ref = cel_refs.pop() if len(cel_refs) == 1 else None
    m["eta_infra"] = round(cel_ref / percentile(done_walls, 50), 4) if cel_ref else None
    return m
