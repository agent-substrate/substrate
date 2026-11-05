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

"""Values of the SWE-Perf tasks of one window: counts, percentiles and ratios against the baseline."""

from typing import Any, Optional

from .tasks import Task

# The values window_values gives, besides the ratios.
VALUES = {"window_start_ts", "window_end_ts", "window_s", "unfinished", "startup_failure_attempts",
          "startup_failure_episodes", "good_wall_ms_p50", "good_wall_ms_p90", "good_wall_ms_p99"}


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


def window_values(window_tasks: list[Task], good: list[bool], startup_ts: list[float], episode_ts: list[float],
                  window: tuple[float, float], refs: dict[str, float], ratio_fields: list[str]) -> dict[str, Any]:
    """Values of the tasks of a window, good saying which of them were good.

    The window's tasks are the finished tasks that ended in it and the
    unfinished ones (always bad) that first failed in it. Startup failures are
    reported as attempts and episodes and stay out of the ratio. For each field
    in ratio_fields, the p50/p90/p99 of value / refs[field] over finished tasks
    with a value; None when refs[field] is 0.
    """
    length = max(0.0, window[1] - window[0])
    finished = [t.record for t in window_tasks if t.finished]
    walls = [t.record.wall_ms for t, g in zip(window_tasks, good) if g]
    m: dict[str, Any] = {
        "window_start_ts": window[0], "window_end_ts": window[1], "window_s": round(length, 3),
        "unfinished": len(window_tasks) - len(finished),
        "startup_failure_attempts": sum(1 for t in startup_ts if in_window(t, window)),
        "startup_failure_episodes": sum(1 for t in episode_ts if in_window(t, window)),
        "good_wall_ms_p50": percentile(walls, 50), "good_wall_ms_p90": percentile(walls, 90),
        "good_wall_ms_p99": percentile(walls, 99),
    }
    for f in ratio_fields:
        # A baseline of 0 has no ratios.
        ratios = [getattr(r, f) / refs[f] for r in finished if getattr(r, f) is not None] if refs[f] else []
        for p in (50, 90, 99):
            m[f"{f}_ratio_p{p}"] = percentile(ratios, p)
    return m
