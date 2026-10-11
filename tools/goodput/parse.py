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

"""Reads one benchmark run: task log events, stats.jsonl and server_summary.json."""

import calendar
import csv
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

# --- The task log lines (currently the sweperf benchmark's) --------------------
# Each is a "[boomer] {json}" line with "time", "msg" and, per line, "actor",
# "cycle" and "err".
LOG_PREFIX = "[boomer] "
MSG_START = "Starting sweperf cycle"
MSG_DONE = "Completed sweperf cycle"
# Two task-end wordings: an actor that serves task after task ("... and reset
# session back to cycle 1"), or a fresh actor per task ("... and was deleted; a
# new actor will run the trajectory next").
MSG_FINISHED_PREFIX = "Actor finished all cycles"
MSG_FINISHED_REUSED = "Actor finished all cycles and reset session back to cycle 1"
MSG_RESUME_FAIL = "ResumeActor failed in step"  # one per failed resume, with the cycle
MSG_EXEC_FAIL = "execute steps failed"
MSG_SUSPEND_FAIL = "SuspendActor failed"
MSG_ONSTART_FAIL_PREFIX = "sweperf on_start failed"  # the actor is only in err
MSG_SHUTDOWN = "sweperf shutdown started"
MSG_BOOMER_QUIT = "Recv quit message from master, all the goroutines are stopped"
ACTOR_IN_TEXT = re.compile(r"\b(sb-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b")

KNOWN = {MSG_START, MSG_DONE, MSG_RESUME_FAIL, MSG_EXEC_FAIL, MSG_SUSPEND_FAIL, MSG_SHUTDOWN,
         MSG_BOOMER_QUIT}

# Task log wordings, from the task-end lines.
FORMAT_REUSED_ACTOR = "reused_actor"
FORMAT_FRESH_ACTOR = "fresh_actor"
FORMAT_UNKNOWN = "unknown"


@dataclass
class Event:
    """One task log line the script uses."""
    t: float  # unix seconds
    msg: str
    actor: str = ""
    cycle: Optional[int] = None


def parse_time(s: str) -> float:
    """RFC 3339 UTC with up to nanoseconds ("2026-10-11T06:57:35.684909393Z") to unix seconds."""
    s = s.strip()
    if not s.endswith("Z"):
        raise ValueError(f"not a UTC timestamp: {s!r}")
    base, _, frac = s[:-1].partition(".")
    secs = calendar.timegm(time.strptime(base, "%Y-%m-%dT%H:%M:%S"))
    return secs + (float("0." + frac) if frac else 0.0)


def is_known(msg: str) -> bool:
    return msg in KNOWN or msg.startswith((MSG_FINISHED_PREFIX, MSG_ONSTART_FAIL_PREFIX))


def parse_line(line: str) -> Optional[Event]:
    """An Event for a known task log line, else None."""
    if not line.startswith(LOG_PREFIX):
        return None
    try:
        j = json.loads(line[len(LOG_PREFIX):])
    except ValueError:
        return None
    if not isinstance(j, dict):
        return None
    msg = j.get("msg")
    if not isinstance(msg, str) or not is_known(msg) or not isinstance(j.get("time"), str):
        return None
    try:
        t = parse_time(j["time"])
    except ValueError:
        return None
    actor = str(j.get("actor") or "")
    if not actor and msg.startswith(MSG_ONSTART_FAIL_PREFIX):
        m = ACTOR_IN_TEXT.search(str(j.get("err") or ""))
        actor = m.group(1) if m else ""
    cycle = j.get("cycle")
    return Event(t=t, msg=msg, actor=actor, cycle=int(cycle) if isinstance(cycle, (int, float)) else None)


def read_events(path: Path) -> list[Event]:
    """Known task log events in file order (each actor's lines are in order)."""
    with open(path, encoding="utf-8", errors="replace") as f:
        return [ev for ev in (parse_line(line.rstrip("\n")) for line in f) if ev]


def log_format(events: list[Event]) -> str:
    """The task log wording the run used, from its first task-end line."""
    for ev in events:
        if ev.msg.startswith(MSG_FINISHED_PREFIX):
            return FORMAT_REUSED_ACTOR if ev.msg == MSG_FINISHED_REUSED else FORMAT_FRESH_ACTOR
    return FORMAT_UNKNOWN


def read_stats(path: Path) -> dict[str, dict[str, Any]]:
    """stats.jsonl rows by metric name (trial_summary, actor_TaskCEL, ...)."""
    rows: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return rows
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                j = json.loads(line)
            except ValueError:
                continue
            if isinstance(j, dict) and j.get("metric"):
                rows[j["metric"]] = j
    return rows


def read_server_metadata(path: Path) -> dict[str, Any]:
    """server_summary.json's metadata (steady window, run start/end, test name, tag)."""
    if not path.exists():
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
