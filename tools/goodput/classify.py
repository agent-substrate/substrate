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

"""Builds tasks from task records and failure lines, and classifies them good or bad.

A finished task is one task record. Each task runs on a fresh actor, so an
actor that logged a failure before the cut-off but no record is one task that
never finished.

A task is bad when it failed (G1), or when it breaks one of its mode's checks:
generic rules over task record fields, against a fixed limit or a ratio of a
baseline value (see README.md).
"""

import operator
from dataclasses import dataclass, field
from typing import Any, Optional

import parse

GOOD = "good"
BAD = "bad"

REASON_RESUME = "resume_failed"
REASON_EXECUTE = "execute_failed"
REASON_SUSPEND = "suspend_failed"
REASON_FAILED = "failed"  # a failed record without a known fail_step

STEP_REASON = {"resume": REASON_RESUME, "execute": REASON_EXECUTE, "suspend": REASON_SUSPEND}
FAILURE_REASONS = {*STEP_REASON.values(), REASON_FAILED}
LINE_STEP = {parse.MSG_RESUME_FAIL: "resume", parse.MSG_EXEC_FAIL: "execute", parse.MSG_SUSPEND_FAIL: "suspend"}

# Task record measurements a check (and a baseline) can use. Not timestamps,
# ids or fail_cycle, which is a cycle index rather than a measurement.
CHECK_FIELDS = ("wall_ms", "cel_ms", "resume_max_ms", "ttfe_max_ms", "startup_ms", "think_ms",
                "retries", "cycles", "steps")
# A task passes a check when `value op threshold` holds.
OPS = {"<=": operator.le, "<": operator.lt, ">=": operator.ge, ">": operator.gt}
CONFIG_VERSION = 2
CONFIG_KEYS = {"version", "modes"}
MODE_KEYS = {"checks"}
CHECK_KEYS = {"field", "op", "limit", "baseline_ratio", "reason"}


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


def validate_config(cfg: Any) -> None:
    """Raises ValueError unless cfg is a valid version 2 config.

    Unknown keys are errors, so a version 1 config (g2, s_max, baselines, ...)
    or a typo fails here instead of passing every task. A mode with an empty
    checks list judges failures only (G1). Each check in a mode has its own
    reason, and none reuses a failure reason, so bad-reason counts stay per check.
    """
    if not isinstance(cfg, dict):
        raise ValueError("config: not a JSON object")
    if unknown := set(cfg) - CONFIG_KEYS:
        raise ValueError(f"config: unknown keys {sorted(unknown)} (version 1 configs are no longer supported; "
                         "see README.md)")
    if "version" in cfg and cfg["version"] != CONFIG_VERSION:
        raise ValueError(f"config: version {cfg['version']!r}, want {CONFIG_VERSION} (see README.md)")
    modes = cfg.get("modes")
    if not isinstance(modes, dict) or not modes:
        raise ValueError("config: needs a non-empty 'modes' object")
    for name, mode in modes.items():
        where = f"config: mode {name!r}"
        if not isinstance(mode, dict) or set(mode) - MODE_KEYS:
            raise ValueError(f"{where}: a mode has only 'checks'")
        checks = mode.get("checks")
        if not isinstance(checks, list):
            raise ValueError(f"{where}: needs a 'checks' list (an empty list judges failures only)")
        for i, c in enumerate(checks):
            at = f"{where} check {i}"
            if not isinstance(c, dict):
                raise ValueError(f"{at}: not an object")
            if unknown := set(c) - CHECK_KEYS:
                raise ValueError(f"{at}: unknown keys {sorted(unknown)}")
            if c.get("field") not in CHECK_FIELDS:
                raise ValueError(f"{at}: field {c.get('field')!r} is not one of {list(CHECK_FIELDS)}")
            if c.get("op") not in OPS:
                raise ValueError(f"{at}: op {c.get('op')!r} is not one of {list(OPS)}")
            if ("limit" in c) == ("baseline_ratio" in c):
                raise ValueError(f"{at}: needs exactly one of 'limit' and 'baseline_ratio'")
            if "limit" in c and not parse.is_number(c["limit"]):
                raise ValueError(f"{at}: limit must be a number")
            if "baseline_ratio" in c and not (parse.is_number(c["baseline_ratio"]) and c["baseline_ratio"] > 0):
                raise ValueError(f"{at}: baseline_ratio must be a positive number")
            if not isinstance(c.get("reason"), str) or not c["reason"]:
                raise ValueError(f"{at}: needs a 'reason'")
        reasons = [c["reason"] for c in checks]
        if dupes := sorted({r for r in reasons if reasons.count(r) > 1}):
            raise ValueError(f"{where}: each check needs its own reason; repeated {dupes}")
        if taken := sorted(set(reasons) & FAILURE_REASONS):
            raise ValueError(f"{where}: reasons {taken} name failures")


def _ref(check: dict[str, Any], refs: Optional[dict[str, float]]) -> Optional[float]:
    """The baseline value a baseline_ratio check compares against; None when there isn't one above 0."""
    ref = (refs or {}).get(check["field"])
    return ref if ref is not None and ref > 0 else None


def split_checks(mode: dict[str, Any], refs: Optional[dict[str, float]]) -> tuple[list[str], list[str]]:
    """Reasons of the mode's checks that apply to this run, and of those skipped
    because the baseline has no value above 0 for their field. A limit check
    always applies (a task without the value just isn't judged by it)."""
    applied, skipped = [], []
    for c in mode["checks"]:
        if "baseline_ratio" in c and _ref(c, refs) is None:
            skipped.append(c["reason"])
        else:
            applied.append(c["reason"])
    return applied, skipped


def ratio_fields(mode: dict[str, Any], refs: Optional[dict[str, float]]) -> list[str]:
    """Fields of the mode's baseline_ratio checks that apply (the baseline has a value above 0), in order."""
    fields = [c["field"] for c in mode["checks"] if "baseline_ratio" in c and _ref(c, refs) is not None]
    return list(dict.fromkeys(fields))


def classify(task: Task, mode: dict[str, Any], refs: Optional[dict[str, float]]) -> tuple[str, list[str]]:
    """good or bad, with the reasons.

    G1: any failure (an unfinished task has one), named by the first failure.
    Then every check of the mode, in order: the task is bad with the reason of
    each check it breaks. A check is skipped when the task has no value for its
    field, and a baseline_ratio check also when refs has no value above 0.
    """
    if not task.finished:
        return BAD, [STEP_REASON[task.fail_step]]
    r = task.record
    if r.failed:
        return BAD, [STEP_REASON.get(r.fail_step, REASON_FAILED)]
    reasons = []
    for c in mode["checks"]:
        value = getattr(r, c["field"])
        if value is None:
            continue
        if "baseline_ratio" in c:
            ref = _ref(c, refs)
            if ref is None:
                continue
            threshold = float(c["baseline_ratio"]) * ref
        else:
            threshold = float(c["limit"])
        if not OPS[c["op"]](value, threshold):
            reasons.append(c["reason"])
    return (BAD, reasons) if reasons else (GOOD, [])
