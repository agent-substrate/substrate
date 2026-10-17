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

"""Builds tasks from task log events and classifies them good or bad.

A task is one pass over the trajectory. An actor may serve task after task, so
each actor's lines are split into task segments at every "Actor finished all
cycles" line, and a cycle's latest "Starting" is paired with its "Completed" only
inside the current segment.
"""

from dataclasses import dataclass, field
from typing import Any, Optional

import parse

GOOD = "good"
BAD = "bad"
UNFINISHED = "unfinished"

REASON_RESUME = "resume_failed"
REASON_EXECUTE = "execute_failed"
REASON_SUSPEND = "suspend_failed"
REASON_SLOW = "slow"

FAIL_KIND = {parse.MSG_RESUME_FAIL: REASON_RESUME, parse.MSG_EXEC_FAIL: REASON_EXECUTE,
             parse.MSG_SUSPEND_FAIL: REASON_SUSPEND}


@dataclass
class Failure:
    reason: str
    t: float
    cycle: Optional[int]


@dataclass
class Task:
    """One trajectory pass of one actor."""
    actor: str
    index: int  # 0-based position among this actor's tasks
    start: Optional[float] = None  # first "Starting" of the segment
    end: Optional[float] = None  # "Actor finished all cycles" time, if it finished
    cycle_ms: dict[int, float] = field(default_factory=dict)  # cycle -> latest Starting..Completed
    starts: dict[int, int] = field(default_factory=dict)  # cycle -> number of Starting lines
    failures: list[Failure] = field(default_factory=list)
    _open: dict[int, float] = field(default_factory=dict, repr=False)

    @property
    def finished(self) -> bool:
        return self.end is not None

    @property
    def wall_ms(self) -> float:
        """Sum of the cycles' Starting..Completed times (think time between cycles excluded)."""
        return round(sum(self.cycle_ms.values()), 3)

    @property
    def retries(self) -> int:
        return sum(n - 1 for n in self.starts.values())

    @property
    def first_failure(self) -> Optional[Failure]:
        return self.failures[0] if self.failures else None

    def empty(self) -> bool:
        return self.start is None and not self.failures


@dataclass
class StartupFailure:
    t: float
    actor: str  # from the error text; empty if it names none


def cutoff(events: list[parse.Event], run_end: Optional[float],
           end_ts: Optional[float]) -> tuple[Optional[float], str]:
    """When the load stopped. Failures after it are teardown, not the user's task.

    Order: the benchmark's shutdown marker, boomer's quit line, trial_summary
    run_end, server_summary end_ts.
    """
    for msg, source in ((parse.MSG_SHUTDOWN, "shutdown_marker"), (parse.MSG_BOOMER_QUIT, "boomer_quit")):
        ts = [ev.t for ev in events if ev.msg == msg]
        if ts:
            return min(ts), source
    if run_end:
        return run_end, "run_end"
    if end_ts:
        return end_ts, "end_ts"
    return None, "none"


def build_tasks(events: list[parse.Event], cut: Optional[float]) -> tuple[list[Task], list[StartupFailure]]:
    """Tasks of every actor (finished and unfinished) and startup failures before the cut-off."""
    limit = cut if cut is not None else float("inf")
    tasks: list[Task] = []
    current: dict[str, Task] = {}
    count: dict[str, int] = {}
    startup: list[StartupFailure] = []

    def seg(actor: str) -> Task:
        if actor not in current:
            current[actor] = Task(actor=actor, index=count.get(actor, 0))
            count[actor] = count.get(actor, 0) + 1
        return current[actor]

    for ev in events:
        if ev.t > limit:
            continue
        if ev.msg.startswith(parse.MSG_ONSTART_FAIL_PREFIX):
            startup.append(StartupFailure(t=ev.t, actor=ev.actor))
            continue
        if not ev.actor:
            continue
        if ev.msg == parse.MSG_START and ev.cycle is not None:
            task = seg(ev.actor)
            task.start = ev.t if task.start is None else task.start
            task._open[ev.cycle] = ev.t  # a retried cycle replaces the earlier Starting
            task.starts[ev.cycle] = task.starts.get(ev.cycle, 0) + 1
        elif ev.msg == parse.MSG_DONE and ev.cycle is not None:
            task = seg(ev.actor)
            if ev.cycle in task._open:
                task.cycle_ms[ev.cycle] = round(1000 * (ev.t - task._open.pop(ev.cycle)), 3)
        elif ev.msg in FAIL_KIND:
            seg(ev.actor).failures.append(Failure(reason=FAIL_KIND[ev.msg], t=ev.t, cycle=ev.cycle))
        elif ev.msg.startswith(parse.MSG_FINISHED_PREFIX):
            task = seg(ev.actor)
            task.end = ev.t
            tasks.append(task)
            del current[ev.actor]
    tasks.extend(t for t in current.values() if not t.empty())
    tasks.sort(key=lambda t: (_first_time(t), t.actor, t.index))
    return tasks, startup


def _first_time(t: Task) -> float:
    if t.start is not None:
        return t.start
    if t.failures:
        return t.failures[0].t
    return t.end if t.end is not None else 0.0


def match_baseline(baselines: list[dict[str, Any]], runtime: str, machine_type: str, template: str,
                   log_format: str) -> Optional[dict[str, Any]]:
    """The first baseline for this runtime, machine type and task log format, and
    template when both the baseline and the run name one."""
    for b in baselines:
        if (b.get("runtime"), b.get("machine_type"), b.get("format")) != (runtime, machine_type, log_format):
            continue
        if b.get("template") and template and b["template"] != template:
            continue
        return b
    return None


def classify(task: Task, mode: dict[str, Any], baseline: Optional[dict[str, Any]]) -> tuple[str, list[str]]:
    """good / bad / unfinished, with the reasons a task is bad.

    G1: any failure before the cut-off, including a resume that failed and was
    retried, makes the task bad (the same rule as the benchmark's loopFailed).
    G2 (TTFE / resume latency) needs a per-task record line, which the logs don't
    have yet, so it is not applied. G3 on wall time, only with a baseline and only
    for a task with no failure: wall_ms / wall_ref_ms above s_max is slow. An
    unfinished task is bad only if it failed before the cut-off.
    """
    reasons = []
    for f in task.failures:
        if f.reason not in reasons:
            reasons.append(f.reason)
    if not task.finished:
        return (BAD, reasons) if reasons else (UNFINISHED, [])
    if not reasons and baseline and baseline.get("wall_ref_ms"):
        if task.wall_ms / float(baseline["wall_ref_ms"]) > float(mode["s_max"]):
            reasons.append(REASON_SLOW)
    return (BAD, reasons) if reasons else (GOOD, [])
