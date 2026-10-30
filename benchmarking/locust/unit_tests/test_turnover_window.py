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

"""Unit tests for turnover_window.py.

Run via: python3 benchmarking/locust/unit_tests/test_turnover_window.py
"""

import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import server_telemetry
import turnover_window

T0 = 1793318400  # 2026-10-30 00:00:00 UTC

LOG = """\
[locust] [2026-10-30 00:00:00,000] host/INFO/locust.main: Starting Locust 2.32.7
[locust] [2026-10-30 00:00:01,000] host/INFO/x: agentsession: turnover fill done
[boomer] {"time":"2026-10-30T00:01:05.123456789Z","level":"INFO","msg":"agentsession: loaded script"}
[boomer] {"time":"2026-10-30T00:04:10.987654321Z","level":"INFO","msg":"agentsession: turnover fill done","awake":350,"parked":850}
[boomer] {"time":"2026-10-30T00:09:00Z","level":"INFO","msg":"agentsession: turnover fill done","awake":350,"parked":850}
"""

HEADER = "Timestamp,User Count,Type,Name,Requests/s,Failures/s,Total Request Count,Total Failure Count\n"


def history(rows):
    """A stats_history.csv body from (offset from T0, name, requests, failures)."""
    return HEADER + "".join(
        f"{T0 + t},1200,{'' if name == 'Aggregated' else 'grpc'},{name},0,0,{n},{f}\n"
        for t, name, n, f in rows
    )


HISTORY = history([
    (240, "SuspendActorAsync_Register", 1200, 3),
    (240, "ResumeActor", 350, 0),
    (240, "Aggregated", 1550, 3),
    (250, "SuspendActorAsync_Register", 1200, 3),
    (250, "ResumeActor", 350, 0),
    (250, "Aggregated", 1550, 3),
    (260, "SuspendActorAsync_Register", 1200, 3),
    (260, "ResumeActor", 400, 1),
    (260, "SuspendActorAsync", 50, 0),
    (260, "Aggregated", 1650, 4),
    (350, "SuspendActorAsync_Register", 1200, 3),
    (350, "ResumeActor", 850, 2),
    (350, "SuspendActorAsync", 500, 1),
    (350, "Aggregated", 2550, 6),
])


class FillDoneTest(unittest.TestCase):
    # Only boomer's own line counts, and only its first.
    def test_finds_the_first_boomer_fill_done_line(self):
        self.assertEqual(turnover_window.fill_done_ts(LOG.splitlines()), T0 + 250)

    def test_a_run_without_one_has_none(self):
        self.assertIsNone(turnover_window.fill_done_ts(LOG.splitlines()[:3]))

    def test_reads_an_offset_time(self):
        line = '[boomer] {"time":"2026-10-30T02:04:10+02:00","msg":"agentsession: turnover fill done"}'
        self.assertEqual(turnover_window.fill_done_ts([line]), T0 + 250)

    def test_skips_a_line_it_cannot_read(self):
        lines = [
            '[boomer] {"msg":"agentsession: turnover fill done"',
            '[boomer] {"time":"yesterday","msg":"agentsession: turnover fill done"}',
            '[boomer] {"time":"2026-10-30T00:09:00Z","msg":"agentsession: turnover fill done"}',
        ]
        self.assertEqual(turnover_window.fill_done_ts(lines), T0 + 540)


class WindowCountsTest(unittest.TestCase):
    def counts(self, body, start):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "stats_history.csv"
            path.write_text(body)
            return turnover_window.window_counts(path, start)

    # Counts from the last sample at or before the start; a request first
    # seen after it starts from zero.
    def test_counts_from_the_start_to_the_last_sample(self):
        self.assertEqual(self.counts(HISTORY, T0 + 250), (T0 + 350, {
            "ResumeActor": (500, 2),
            "SuspendActorAsync": (500, 1),
            "SuspendActorAsync_Register": (0, 0),
        }))

    def test_no_sample_after_the_start_gives_none(self):
        self.assertIsNone(self.counts(HISTORY, T0 + 350))


class AppendTest(unittest.TestCase):
    def append(self, body, start):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            if body is not None:
                (d / "stats_history.csv").write_text(body)
            jsonl = d / "stats.jsonl"
            jsonl.write_text('{"metric": "locust"}\n')
            ok = turnover_window.append_turnover_window(
                jsonl, start, d / "stats_history.csv", "ts", "tag", "test", io.StringIO())
            return ok, [json.loads(line) for line in jsonl.read_text().splitlines()]

    def test_appends_the_window_row(self):
        ok, rows = self.append(HISTORY, T0 + 250)
        self.assertTrue(ok)
        self.assertEqual(rows[1], {
            "timestamp": "ts",
            "tag": "tag",
            "test_name": "test",
            "metric": "turnover_window",
            "measurements": {
                "window_start": T0 + 250,
                "window_end": T0 + 350,
                "window_s": 100,
                "requests_ResumeActor": 500,
                "failures_ResumeActor": 2,
                "rps_ResumeActor": 5.0,
                "requests_SuspendActorAsync": 500,
                "failures_SuspendActorAsync": 1,
                "rps_SuspendActorAsync": 5.0,
                "requests_SuspendActorAsync_Register": 0,
                "failures_SuspendActorAsync_Register": 0,
                "rps_SuspendActorAsync_Register": 0.0,
            },
        })

    def test_appends_nothing_without_a_history(self):
        self.assertEqual(self.append(None, T0 + 250), (False, [{"metric": "locust"}]))

    def test_appends_nothing_without_a_sample_after_the_start(self):
        self.assertEqual(self.append(HISTORY, T0 + 350), (False, [{"metric": "locust"}]))


class SteadyWindowTest(unittest.TestCase):
    def test_a_start_inside_the_window_starts_it(self):
        logged: list[str] = []
        self.assertEqual(
            server_telemetry.start_steady_window_at((100, 500), 250, log=logged.append),
            (250, 500),
        )
        self.assertEqual(
            server_telemetry.start_steady_window_at((100, 500), 100, log=logged.append),
            (100, 500),
        )
        self.assertEqual(
            server_telemetry.start_steady_window_at((100, 500), None, log=logged.append),
            (100, 500),
        )
        self.assertEqual(logged, [])

    def test_a_start_outside_the_window_logs_and_leaves_it(self):
        for not_before in (50, 500, 600):
            with self.subTest(not_before=not_before):
                logged: list[str] = []
                self.assertEqual(
                    server_telemetry.start_steady_window_at(
                        (100, 500), not_before, log=logged.append
                    ),
                    (100, 500),
                )
                self.assertEqual(len(logged), 1)
                self.assertIn(str(not_before), logged[0])


if __name__ == "__main__":
    unittest.main()
