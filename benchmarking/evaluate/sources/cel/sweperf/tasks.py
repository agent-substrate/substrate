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

"""Builds tasks from task records and failure lines, with the failure each one had.

A finished task is one task record. Each task runs on a fresh actor, so an
actor that logged a failure before the cut-off but no record is one task that
never finished. A task that failed is bad (G1), named by its first failure.
"""

from dataclasses import dataclass, field
from typing import Optional

from . import parse

REASON_RESUME = "resume_failed"
REASON_EXECUTE = "execute_failed"
REASON_SUSPEND = "suspend_failed"
REASON_FAILED = "failed"  # a failed record without a known fail_step

STEP_REASON = {"resume": REASON_RESUME, "execute": REASON_EXECUTE, "suspend": REASON_SUSPEND}
FAILURE_REASON_ORDER = (*STEP_REASON.values(), REASON_FAILED)
LINE_STEP = {parse.MSG_RESUME_FAIL: "resume", parse.MSG_EXEC_FAIL: "execute", parse.MSG_SUSPEND_FAIL: "suspend",
             parse.MSG_SUSPEND_ASYNC_FAIL: "suspend"}

# Task record measurements a rule (and a baseline) can use. Not timestamps,
# ids or fail_cycle, which is a cycle index rather than a measurement.
FIELDS = ("wall_ms", "cel_ms", "resume_max_ms", "ttfe_max_ms", "startup_ms", "think_ms",
          "retries", "cycles", "steps")


@dataclass
class Task:
    """A finished task (its record) or an unfinished one (its failure lines)."""
    actor: str
    record: Optional[parse.TaskRecord] = None
    failures: list[parse.Event] = field(default_factory=list)
    end: Optional[float] = None  # the record's end, clamped to the cut-off

    @property
    def finished(self) -> bool:
        return self.record is not None

    @property
    def fail_step(self) -> str:
        if self.record:
            return self.record.fail_step
        return LINE_STEP[self.failures[0].msg]

    @property
    def fail_cycle(self) -> Optional[int]:
        if self.record:
            return self.record.fail_cycle
        return self.failures[0].cycle

    @property
    def first_failure_t(self) -> Optional[float]:
        """When an unfinished task first failed; records don't say when a failure happened."""
        return self.failures[0].t if self.failures else None


def cutoff(events: list[parse.Event], run_end: Optional[float],
           end_ts: Optional[float]) -> tuple[Optional[float], str]:
    """When the load stopped. Failures after it belong to the teardown.

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


def build_tasks(run: parse.RunLog, cut: Optional[float]) -> tuple[list[Task], list[parse.Event]]:
    """Tasks (finished and unfinished) and startup failures before the cut-off.

    Every record is a task, even one logged just after the cut-off: interrupted
    tasks log none, so its last cycle finished before the load stopped.
    """
    limit = cut if cut is not None else float("inf")
    tasks = [Task(actor=r.actor, record=r, end=min(r.end, limit)) for r in run.records]
    finished = {r.actor for r in run.records}
    unfinished: dict[str, Task] = {}
    startup = []
    for ev in run.events:
        if ev.t > limit:
            continue
        if ev.msg.startswith(parse.MSG_ONSTART_FAIL_PREFIX):
            startup.append(ev)
        elif ev.msg in parse.FAILURE_MSGS and ev.actor and ev.actor not in finished:
            unfinished.setdefault(ev.actor, Task(actor=ev.actor)).failures.append(ev)
    tasks += unfinished.values()
    tasks.sort(key=lambda t: (t.end if t.finished else t.first_failure_t, t.actor))
    return tasks, startup


def startup_episodes(startup: list[parse.Event], records: list[parse.TaskRecord]) -> list[float]:
    """Start times of startup failure episodes: a user's consecutive startup
    failures until its next task record. A failure without a user is its own episode."""
    marks = sorted([(e.t, e.user, False) for e in startup] + [(r.t, r.user, True) for r in records],
                   key=lambda m: m[0])
    failing: set[int] = set()
    starts = []
    for t, user, is_record in marks:
        if is_record:
            failing.discard(user)
        elif user is None or user not in failing:
            starts.append(t)
            if user is not None:
                failing.add(user)
    return starts


def failure(task: Task) -> str:
    """The reason task is bad whatever the rules say: its first failure (G1). "" when it didn't fail."""
    if not task.finished:
        return STEP_REASON[task.fail_step]
    if task.record.failed:
        return STEP_REASON.get(task.record.fail_step, REASON_FAILED)
    return ""
