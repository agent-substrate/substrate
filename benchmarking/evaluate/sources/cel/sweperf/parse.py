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

"""Reads one benchmark run: task records and events, the run config, server_summary.json."""

import calendar
import csv
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# --- The task log lines ---------------------------------------------------------
# Each is a "[boomer] {json}" line with "time" and "msg"; see "Sweperf Log Lines"
# in docs/guides/benchmarking.md for the fields.
LOG_PREFIX = "[boomer] "
# One task record per finished task; interrupted tasks log none. The
# workload-neutral form names its workload in a `workload` field. Sweperf logs
# the older form, which implies its workload.
MSG_TASK_RECORD = "task record"
MSG_TASK_RECORD_SWEPERF = "sweperf task record"
RECORD_MSGS = {MSG_TASK_RECORD: None, MSG_TASK_RECORD_SWEPERF: "sweperf"}
RECORD_VERSION = 1
MSG_RESUME_FAIL = "ResumeActor failed in step"
MSG_EXEC_FAIL = "execute steps failed"
# The suspend failure names the RPC: SuspendActorAsync in the async lifecycle
# mode (the default), SuspendActor in the sync one.
MSG_SUSPEND_FAIL = "SuspendActor failed"
MSG_SUSPEND_ASYNC_FAIL = "SuspendActorAsync failed"
MSG_ONSTART_FAIL_PREFIX = "sweperf on_start failed"
MSG_SHUTDOWN = "sweperf shutdown started"
MSG_BOOMER_QUIT = "Recv quit message from master, all the goroutines are stopped"

FAILURE_MSGS = {MSG_RESUME_FAIL, MSG_EXEC_FAIL, MSG_SUSPEND_FAIL, MSG_SUSPEND_ASYNC_FAIL}
EVENT_MSGS = FAILURE_MSGS | {MSG_SHUTDOWN, MSG_BOOMER_QUIT}

# --- How the run suspended actors -----------------------------------------------
# runner.py logs each extra flag on a line of its own in the run config at the
# top of logs.txt ("    --lifecycle-mode async"; see runner.log_run_config).
LIFECYCLE_FLAG = re.compile(r"^\s+--lifecycle-mode[ =](\S+)\s*$")
RUN_CONFIG_END = "===================="
# stats.jsonl rows of the suspend RPC, one per lifecycle mode.
SUSPEND_METRICS = {"grpc_SuspendActorAsync": "async", "grpc_SuspendActor": "sync"}
LIFECYCLE_MODES = ("async", "sync")


@dataclass
class Event:
    """A task log line other than a task record: a failure, the end of the load, or a startup failure."""
    t: float  # unix seconds
    msg: str
    actor: str = ""
    cycle: Optional[int] = None
    user: Optional[int] = None


@dataclass
class TaskRecord:
    """One task record line. Durations in ms, times in unix seconds.

    Optional fields are None when the record doesn't log them; a logged 0 stays 0.
    """
    t: float  # when the line was logged
    actor: str
    end: float
    wall_ms: float
    cel_ms: float
    resume_max_ms: float
    failed: bool
    workload: str = ""
    user: Optional[int] = None
    template: str = ""
    cycles: Optional[int] = None
    steps: Optional[int] = None
    task_start: Optional[float] = None
    startup_ms: Optional[float] = None
    think_ms: Optional[float] = None
    ttfe_max_ms: Optional[float] = None
    retries: Optional[int] = None
    fail_step: str = ""
    fail_cycle: Optional[int] = None  # None unless the task failed


@dataclass
class RunLog:
    """What read_run takes from logs.txt."""
    records: list[TaskRecord] = field(default_factory=list)
    events: list[Event] = field(default_factory=list)
    skipped_records: int = 0  # record lines of another version, or missing a required field
    lifecycle_flag: str = ""  # the --lifecycle-mode flag in the run config, "" when not given


def parse_time(s: str) -> float:
    """RFC 3339 UTC with up to nanoseconds ("2026-10-11T06:57:35.684909393Z") to unix seconds."""
    s = s.strip()
    if not s.endswith("Z"):
        raise ValueError(f"not a UTC timestamp: {s!r}")
    base, _, frac = s[:-1].partition(".")
    secs = calendar.timegm(time.strptime(base, "%Y-%m-%dT%H:%M:%S"))
    return secs + (float("0." + frac) if frac else 0.0)


def _json_line(line: str) -> Optional[dict[str, Any]]:
    """The JSON object of a boomer log line, else None."""
    if not line.startswith(LOG_PREFIX):
        return None
    try:
        j = json.loads(line[len(LOG_PREFIX):])
    except ValueError:
        return None
    return j if isinstance(j, dict) else None


def is_number(v: Any) -> bool:
    """A JSON number; bools are not."""
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _int(v: Any) -> Optional[int]:
    """An int from a JSON number; None for anything else, bools included."""
    return int(v) if is_number(v) else None


def _float(v: Any) -> Optional[float]:
    """A float from a JSON number; None for anything else, bools included."""
    return float(v) if is_number(v) else None


def parse_line(line: str) -> Optional[Event]:
    """An Event for a line in EVENT_MSGS or a startup failure, else None."""
    j = _json_line(line)
    if j is None:
        return None
    msg = j.get("msg")
    if not isinstance(msg, str) or not (msg in EVENT_MSGS or msg.startswith(MSG_ONSTART_FAIL_PREFIX)):
        return None
    if not isinstance(j.get("time"), str):
        return None
    try:
        t = parse_time(j["time"])
    except ValueError:
        return None
    return Event(t=t, msg=msg, actor=str(j.get("actor") or ""), cycle=_int(j.get("cycle")), user=_int(j.get("user")))


def parse_record(line: str) -> Optional[TaskRecord]:
    """A TaskRecord for a task record line (either form), else None.

    Raises ValueError for a task record line of another version or with a
    missing or mistyped required field.
    """
    if MSG_TASK_RECORD not in line:  # a substring of both forms
        return None
    j = _json_line(line)
    if j is None or not isinstance(j.get("msg"), str) or j["msg"] not in RECORD_MSGS:
        return None
    if j.get("v") != RECORD_VERSION:
        raise ValueError(f"task record version {j.get('v')!r}, want {RECORD_VERSION}")
    try:
        t = parse_time(j["time"])
        end = parse_time(j["end"])
        task_start = parse_time(j["task_start"]) if isinstance(j.get("task_start"), str) else None
    except (KeyError, AttributeError, ValueError) as e:
        raise ValueError(f"task record with a missing or bad time: {e}") from e
    actor, failed = j.get("actor"), j.get("failed")
    wall, cel, resume = _float(j.get("wall_ms")), _float(j.get("cel_ms")), _float(j.get("resume_max_ms"))
    if not isinstance(actor, str) or not actor or not isinstance(failed, bool) or None in (wall, cel, resume):
        raise ValueError("task record missing actor, failed, wall_ms, cel_ms or resume_max_ms")
    workload = RECORD_MSGS[j["msg"]] or str(j.get("workload") or "")
    return TaskRecord(
        t=t, actor=actor, end=end, wall_ms=wall, cel_ms=cel, resume_max_ms=resume, failed=failed,
        workload=workload, user=_int(j.get("user")), template=str(j.get("template") or ""),
        cycles=_int(j.get("cycles")), steps=_int(j.get("steps")), task_start=task_start,
        startup_ms=_float(j.get("startup_ms")), think_ms=_float(j.get("think_ms")),
        ttfe_max_ms=_float(j.get("ttfe_max_ms")), retries=_int(j.get("retries")),
        fail_step=str(j.get("fail_step") or ""),
        # Cycles are 1-based; the benchmark logs fail_cycle 0 for a task that didn't fail.
        fail_cycle=(_int(j.get("fail_cycle")) or None) if failed else None)


def read_run(path: Path) -> RunLog:
    """Task records and events from one logs.txt, in file order, and the
    --lifecycle-mode flag of its run config (the lines up to RUN_CONFIG_END)."""
    run = RunLog()
    in_config = True
    with open(path, encoding="utf-8", errors="replace") as f:
        for raw in f:
            text = raw.rstrip("\n")
            if in_config and text.startswith(RUN_CONFIG_END):
                in_config = False
            elif in_config and not run.lifecycle_flag and (m := LIFECYCLE_FLAG.match(text)):
                run.lifecycle_flag = m[1]
                continue
            try:
                rec = parse_record(text)
            except ValueError:
                run.skipped_records += 1
                continue
            if rec:
                run.records.append(rec)
            elif ev := parse_line(text):
                run.events.append(ev)
    return run


def lifecycle_mode(stats: dict[str, dict[str, Any]], flag: str) -> tuple[str, str]:
    """How the run suspended actors, and what says so.

    The suspend RPC rows in stats.jsonl come first, as they show what the run
    did: "async" (SuspendActorAsync), "sync" (SuspendActor) or "mixed". Then
    the run's --lifecycle-mode flag. Otherwise ("", "none"). A task's wall_ms
    covers the snapshot upload only in the sync mode, so tasks of the two
    modes don't compare.
    """
    seen = sorted({mode for metric, mode in SUSPEND_METRICS.items() if metric in stats})
    if seen:
        return ("mixed" if len(seen) > 1 else seen[0]), "stats"
    if flag in LIFECYCLE_MODES:
        return flag, "flag"
    return "", "none"


def read_server_metadata(path: Optional[Path]) -> dict[str, Any]:
    """server_summary.json's metadata (steady window, run start/end, test name, tag)."""
    if path is None or not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            j = json.load(f)
    except ValueError:
        return {}
    meta = j.get("metadata") if isinstance(j, dict) else None
    return meta if isinstance(meta, dict) else {}


def steady_from_history(path: Path, start_ts: float, end_ts: float) -> Optional[tuple[float, float]]:
    """The runner's rule (server_telemetry.get_steady_state_window): first to last
    stats_history sample at >= 90% of the peak user count, clamped to the run."""
    if not path.exists():
        return None
    samples = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("Name", "") not in ("", "Aggregated", "Total"):
                continue
            try:
                samples.append((float(row["Timestamp"]), float(row["User Count"])))
            except (KeyError, TypeError, ValueError):
                continue
    peak = max((u for _, u in samples), default=0.0)
    if peak <= 0:
        return None
    steady = [ts for ts, u in samples if u >= 0.9 * peak]
    lo, hi = steady[0], steady[-1]
    if start_ts and end_ts:
        lo, hi = max(lo, start_ts), min(hi, end_ts)
    return (lo, hi) if hi > lo else None


def num(v: Any) -> Optional[float]:
    """A number, or a numeric string as stats.jsonl stores measurements; None otherwise."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def epoch(v: Any) -> Optional[float]:
    """A unix time given as a number, numeric string or RFC 3339 UTC string."""
    n = num(v)
    if n is not None or not isinstance(v, str):
        return n
    try:
        return parse_time(v)
    except ValueError:
        return None
