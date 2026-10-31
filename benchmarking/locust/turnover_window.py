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

"""The turnover window of an agentsession run in turnover mode.

In turnover mode (--agentsession-awake) a run first registers its whole
fleet: each session creates its actor, runs step 1 and parks. The conductor
then wakes the awake sessions, and only after that fill does it swap at
--target-rps. stats.jsonl's other rows cover the whole run, so their rates
divide by the time spent registering. This finds the end of the fill in the
line boomer logs then, and writes one `turnover_window` row with each
request's count, failures and rate from there to the end of the run, read
from stats_history.csv. runner.py also starts the server telemetry's
steady-state window there.

Like the server telemetry, this is additive: runner.py logs a failure here
and keeps the locust measurements.
"""

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, TextIO

# What agentsession logs once its conductor has woken the awake sessions,
# right before it starts to swap.
FILL_DONE_MSG = "agentsession: turnover fill done"

# The prefix runner.py puts on each line boomer writes to the run's log.
BOOMER_PREFIX = "[boomer] "

# An RFC 3339 time as Go writes it. Its fraction can hold nanoseconds, which
# datetime does not parse, and only the seconds matter here.
_TIME_RE = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.\d+)?(Z|[+-]\d\d:\d\d)$")


def _unix(ts: str) -> int:
    m = _TIME_RE.match(ts)
    if not m:
        raise ValueError(f"not an RFC 3339 time: {ts!r}")
    t = datetime.strptime(m[1], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    offset = 0
    if m[2] != "Z":
        sign = 1 if m[2][0] == "+" else -1
        offset = sign * (int(m[2][1:3]) * 3600 + int(m[2][4:6]) * 60)
    return int(t.timestamp()) - offset


def fill_done_ts(lines: Iterable[str]) -> int | None:
    """Unix seconds of the first fill-done line in the run's log, or None
    when there is none: the run was not in turnover mode, or its fill never
    finished."""
    for line in lines:
        if not line.startswith(BOOMER_PREFIX) or FILL_DONE_MSG not in line:
            continue
        try:
            entry = json.loads(line[len(BOOMER_PREFIX):])
            if entry.get("msg") == FILL_DONE_MSG:
                return _unix(entry["time"])
        except (AttributeError, KeyError, TypeError, ValueError):
            continue
    return None


def window_counts(
    stats_history_csv: Path, start: int
) -> tuple[int, dict[str, tuple[int, int]]] | None:
    """The time of the last sample in stats_history.csv, and each request's
    requests and failures from start to it, or None when no sample comes
    after start. The counts come from the cumulative columns of the
    per-request rows that --csv-full-history writes: the last sample's minus
    the last one at or before start."""
    before: dict[str, tuple[int, int]] = {}
    after: dict[str, tuple[int, int]] = {}
    end = None
    with open(stats_history_csv, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row.get("Name") or ""
            if name in ("", "Aggregated", "Total"):
                continue
            try:
                ts = int(row["Timestamp"])
                counts = (int(row["Total Request Count"]), int(row["Total Failure Count"]))
            except (KeyError, TypeError, ValueError):
                continue
            if ts <= start:
                before[name] = counts
            else:
                after[name] = counts
                end = ts if end is None else max(end, ts)
    if end is None:
        return None
    return end, {
        name: (requests - before.get(name, (0, 0))[0], failures - before.get(name, (0, 0))[1])
        for name, (requests, failures) in after.items()
    }


def window_record(
    start: int,
    end: int,
    counts: dict[str, tuple[int, int]],
    timestamp: str,
    tag: str,
    test_name: str,
) -> dict[str, Any]:
    """The window as a stats.jsonl row: each request's count, failures and
    rate. The rate counts every request, failed or not, like locust's
    Requests/s."""
    secs = end - start
    m: dict[str, Any] = {"window_start": start, "window_end": end, "window_s": secs}
    for name, (requests, failures) in sorted(counts.items()):
        m[f"requests_{name}"] = requests
        m[f"failures_{name}"] = failures
        m[f"rps_{name}"] = round(requests / secs, 4) if secs > 0 else None
    return {
        "timestamp": timestamp,
        "tag": tag,
        "test_name": test_name,
        "metric": "turnover_window",
        "measurements": m,
    }


def append_turnover_window(
    jsonl_path: Path,
    start: int,
    stats_history_csv: Path,
    timestamp: str,
    tag: str,
    test_name: str,
    logs: TextIO,
) -> bool:
    """Append the turnover_window row of a run whose fill ended at start to
    jsonl_path, and return whether there was one to append."""
    if not stats_history_csv.exists():
        _log(logs, f"No {stats_history_csv}; no turnover_window row")
        return False
    counts = window_counts(stats_history_csv, start)
    if counts is None:
        _log(logs, f"No stats_history sample after the turnover start {start}; no turnover_window row")
        return False
    end, per_request = counts
    row = window_record(start, end, per_request, timestamp, tag, test_name)
    with open(jsonl_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")
    return True


def _log(logs: TextIO, msg: str) -> None:
    print(msg, flush=True)
    logs.write(msg + "\n")
    logs.flush()
