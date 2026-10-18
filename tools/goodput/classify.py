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

A task is one pass over the trajectory. In a run with task records, each record
logged before the cut-off is a finished task, and an actor's failure lines with
no record after them are its unfinished task; cycle lines are not used.

In a run without task records, an actor may serve task after task, so each
actor's lines are split into task segments at every "Actor finished all cycles"
line, and a cycle's latest "Starting" is paired with its "Completed" only inside
the current segment.
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
REASON_FAILED = "failed"  # a task record that failed without naming the step
REASON_SLOW_RESUME = "slow_resume"
REASON_SLOW_TTFE = "slow_ttfe"
REASON_SLOW = "slow"
REASON_SLOW_WALL = "slow_wall"  # the optional wall check (g3_wall) on a task with a record

FAIL_KIND = {parse.MSG_RESUME_FAIL: REASON_RESUME, parse.MSG_EXEC_FAIL: REASON_EXECUTE,
             parse.MSG_SUSPEND_FAIL: REASON_SUSPEND}
RECORD_FAIL_STEP = {"resume": REASON_RESUME, "execute": REASON_EXECUTE, "suspend": REASON_SUSPEND}

LSSD_SUFFIX = "-lssd"  # machine types with local SSD, which get G2's tighter resume bound
MAX_STARTUP_FAILURES_PER_TASK = 10  # above this many per finished task, a run is invalid
NOTE_NO_TASK_FINISHED = "no task finished within the run"


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
    start: Optional[float] = None  # first "Starting" of the segment, or the record's task_start
    end: Optional[float] = None  # when it finished: the task-end line or the record's end
    cycle_ms: dict[int, float] = field(default_factory=dict)  # cycle -> latest Starting..Completed
    starts: dict[int, int] = field(default_factory=dict)  # cycle -> number of Starting lines
    failures: list[Failure] = field(default_factory=list)
    record: Optional[parse.TaskRecord] = None  # the finished task's record, in a run with task records
    _open: dict[int, float] = field(default_factory=dict, repr=False)

    @property
    def finished(self) -> bool:
        return self.end is not None

    @property
    def wall_ms(self) -> float:
        """The record's wall_ms; without one, the sum of the cycles' Starting..Completed times
        (think time between cycles excluded)."""
        if self.record is not None:
            return self.record.wall_ms
        return round(sum(self.cycle_ms.values()), 3)

    @property
    def retries(self) -> int:
        if self.record is not None:
            return self.record.retries
        return sum(n - 1 for n in self.starts.values())

    @property
    def first_failure(self) -> Optional[Failure]:
        return self.failures[0] if self.failures else None

    def g1_reasons(self) -> list[str]:
        """Why the task failed G1: the record's first failed step, else each failure kind in log order."""
        if self.record is not None:
            return [RECORD_FAIL_STEP.get(self.record.fail_step, REASON_FAILED)] if self.record.failed else []
        reasons = []
        for f in self.failures:
            if f.reason not in reasons:
                reasons.append(f.reason)
        return reasons

    def empty(self) -> bool:
        return self.start is None and not self.failures and self.record is None


@dataclass
class StartupFailure:
    t: float
    actor: str  # empty if the line names none
    user: Optional[int] = None  # on newer startup failure lines


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
            startup.append(StartupFailure(t=ev.t, actor=ev.actor, user=ev.user))
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


def build_record_tasks(events: list[parse.Event],
                       cut: Optional[float]) -> tuple[list[Task], list[StartupFailure]]:
    """Tasks of a run with task records, and startup failures before the cut-off.

    Each record logged before the cut-off is a finished task; the record says
    whether and where it failed. An actor whose failure lines before the cut-off
    have no record after them has an unfinished task with those failures. An
    unfinished task without a failure line leaves no trace and isn't counted.
    """
    limit = cut if cut is not None else float("inf")
    tasks: list[Task] = []
    current: dict[str, Task] = {}  # actor -> its task that has failure lines and no record yet
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
            startup.append(StartupFailure(t=ev.t, actor=ev.actor, user=ev.user))
        elif ev.actor and ev.record is not None:
            task = seg(ev.actor)
            task.record, task.start, task.end = ev.record, ev.record.task_start, ev.record.end
            tasks.append(task)
            del current[ev.actor]
        elif ev.actor and ev.msg in FAIL_KIND:
            seg(ev.actor).failures.append(Failure(reason=FAIL_KIND[ev.msg], t=ev.t, cycle=ev.cycle))
    tasks.extend(current.values())
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
    """The baseline for this runtime, machine type and task log format: the first
    one for the same template, else the first one that names no template."""
    same = [b for b in baselines
            if (b.get("runtime"), b.get("machine_type"), b.get("format")) == (runtime, machine_type, log_format)]
    for b in same:
        if (b.get("template") or "") == template:
            return b
    return next((b for b in same if not b.get("template")), None)


def resolve_mode(mode: dict[str, Any], machine_type: str) -> dict[str, Any]:
    """The mode's rules for a run on machine_type: on a local SSD machine type
    (-lssd), G2's resume bound is resume_max_lssd_s instead of resume_max_s."""
    if machine_type.endswith(LSSD_SUFFIX) and mode.get("resume_max_lssd_s") is not None:
        return dict(mode, resume_max_s=mode["resume_max_lssd_s"])
    return mode


def g2_reasons(task: Task, mode: dict[str, Any]) -> list[str]:
    """G2 (latency), for a task with a record when the mode turns G2 on: its slowest
    resume within resume_max_s and its slowest TTFE within ttfe_max_s. A record
    without a TTFE is not checked for it."""
    rec = task.record
    if rec is None or not mode.get("g2"):
        return []
    reasons = []
    if rec.resume_max_ms > 1000.0 * float(mode["resume_max_s"]):
        reasons.append(REASON_SLOW_RESUME)
    if rec.ttfe_max_ms is not None and rec.ttfe_max_ms > 1000.0 * float(mode["ttfe_max_s"]):
        reasons.append(REASON_SLOW_TTFE)
    return reasons


def g3_basis(has_record: bool, baseline: Optional[dict[str, Any]]) -> str:
    """What G3 compares with the baseline: a task record's cel_ms ("cel"), the
    cycle-line wall time in a run without records ("wall"), or nothing ("skipped")."""
    key, basis = ("task_cel_ref_ms", "cel") if has_record else ("wall_ref_ms", "wall")
    return basis if baseline and baseline.get(key) else "skipped"


def slowdown(task: Task, baseline: Optional[dict[str, Any]]) -> Optional[float]:
    """G3's ratio: cel_ms / task_cel_ref_ms for a task with a record, else
    wall_ms / wall_ref_ms; None without a baseline value for it."""
    basis = g3_basis(task.record is not None, baseline)
    if basis == "cel":
        return task.record.cel_ms / float(baseline["task_cel_ref_ms"])
    if basis == "wall":
        return task.wall_ms / float(baseline["wall_ref_ms"])
    return None


def g3_wall_basis(mode: dict[str, Any], has_record: bool, baseline: Optional[dict[str, Any]]) -> str:
    """Whether the mode's optional wall check (g3_wall) applies: "off", "on", "skipped"
    (no wall_ref_ms), or "n/a" in a run without task records, whose G3 compares wall time already."""
    if not mode.get("g3_wall"):
        return "off"
    if not has_record:
        return "n/a"
    return "on" if baseline and baseline.get("wall_ref_ms") else "skipped"


def classify(task: Task, mode: dict[str, Any], baseline: Optional[dict[str, Any]]) -> tuple[str, list[str]]:
    """good / bad / unfinished, with the reasons a task is bad.

    G1: any failure before the cut-off, including a resume that failed and was
    retried, makes the task bad (the same rule as the benchmark's loopFailed).
    With a record that is its failed flag, with fail_step as the reason; without
    one, every kind of failure line the task logged.
    G2 (resume / TTFE latency) needs a record; see g2_reasons.
    G3, only with a baseline: a slowdown (see slowdown) above s_max is slow. With
    g3_wall, a task with a record is also slow_wall when wall_ms / wall_ref_ms is
    above s_max.
    G2 and G3 apply only to a finished task that passed G1, and a task can fail
    both. An unfinished task is bad only if it failed before the cut-off.
    """
    reasons = task.g1_reasons()
    if not task.finished:
        return (BAD, reasons) if reasons else (UNFINISHED, [])
    if not reasons:
        reasons = g2_reasons(task, mode)
        s = slowdown(task, baseline)
        if s is not None and s > float(mode["s_max"]):
            reasons.append(REASON_SLOW)
        if g3_wall_basis(mode, task.record is not None, baseline) == "on":
            if task.wall_ms / float(baseline["wall_ref_ms"]) > float(mode["s_max"]):
                reasons.append(REASON_SLOW_WALL)
    return (BAD, reasons) if reasons else (GOOD, [])


def run_validity(tasks_finished: int, startup_failure_attempts: int,
                 max_startup_failures_per_task: float = MAX_STARTUP_FAILURES_PER_TASK,
                 sessions_started: int = 0) -> tuple[bool, str]:
    """Whether a run measured the platform at all, and why not: it is invalid with
    no finished task, or with more than max_startup_failures_per_task startup
    failures per finished task (the actors mostly never started).

    A run in which sessions_started sessions started, without that many startup
    failures per session, but no task finished before the cut-off stays valid with
    no task done: its tasks took longer than the run. Pass sessions_started only for
    runs that log task records, where a missing record means an unfinished task.
    """
    if tasks_finished == 0:
        if sessions_started and startup_failure_attempts <= max_startup_failures_per_task * sessions_started:
            return True, ""
        return False, "no finished tasks"
    if startup_failure_attempts > max_startup_failures_per_task * tasks_finished:
        return False, f"{startup_failure_attempts} startup failures for {tasks_finished} finished tasks"
    return True, ""


def sessions_started(events: list[parse.Event], cut: Optional[float]) -> int:
    """Actors that started a cycle by the cut-off."""
    limit = cut if cut is not None else float("inf")
    return len({ev.actor for ev in events if ev.msg == parse.MSG_START and ev.actor and ev.t <= limit})
