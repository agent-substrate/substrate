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

"""Unit tests for ladder_steps.py.

Run via: python3 benchmarking/locust/unit_tests/test_ladder_steps.py
"""

import io
import json
import sys
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import ladder_steps

LOG = """\
[locust] [2026-10-12 00:19:07,123] host/INFO/ladder_shape: Ladder step 1/2: users=100 trace_probability=unchanged target_rps=500.0
[boomer] {"msg":"dynconfig applied"}
[locust] [2026-10-12 00:22:07,456] host/INFO/ladder_shape: Ladder step 2/2: users=100 trace_probability=unchanged target_rps=1000.0
[locust] [2026-10-12 00:25:07,789] host/INFO/ladder_shape: Ladder complete after 360s
"""
T0 = 1791764347  # 2026-10-12 00:19:07 UTC


class StepsTest(unittest.TestCase):
    def test_parses_steps_and_their_ends(self):
        self.assertEqual(ladder_steps.steps(LOG.splitlines()), [
            {"step": 1, "users": 100, "target_rps": 500.0, "start": T0, "end": T0 + 180},
            {"step": 2, "users": 100, "target_rps": 1000.0, "start": T0 + 180, "end": T0 + 360},
        ])

    def test_unfinished_ladder_leaves_the_last_end_open(self):
        got = ladder_steps.steps(LOG.splitlines()[:3])
        self.assertEqual([s["end"] for s in got], [T0 + 180, None])

    # A step that changes only the users still bounds its neighbors.
    def test_unchanged_rate_keeps_the_previous_one(self):
        log = (
            "[locust] [2026-10-12 00:19:07,1] x: Ladder step 1/3: users=100 trace_probability=unchanged target_rps=unchanged\n"
            "[locust] [2026-10-12 00:22:07,1] x: Ladder step 2/3: users=100 trace_probability=unchanged target_rps=500.0\n"
            "[locust] [2026-10-12 00:25:07,1] x: Ladder step 3/3: users=200 trace_probability=unchanged target_rps=unchanged\n"
            "[locust] [2026-10-12 00:28:07,1] x: Ladder complete after 540s\n"
        )
        got = ladder_steps.steps(log.splitlines())
        self.assertEqual([(s["step"], s["target_rps"]) for s in got], [(1, None), (2, 500.0), (3, 500.0)])
        self.assertEqual([s["end"] for s in got], [T0 + 180, T0 + 360, T0 + 540])


def prometheus(answers):
    """A fake Prometheus API: the first matching answer for each query, by
    substring; a query no answer matches gets an empty result."""
    def get_json(url):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["query"][0]
        for needle, result in answers:
            if needle in q:
                return {"status": "success", "data": {"result": result}}
        return {"status": "success", "data": {"result": []}}
    return get_json


def value(v, **labels):
    return [{"metric": labels, "value": [0, str(v)]}]


class SummarizeTest(unittest.TestCase):
    STEP = {"step": 1, "users": 100, "target_rps": 500.0, "start": T0, "end": T0 + 180}

    def summarize(self, answers):
        with mock.patch.object(ladder_steps, "_get_json", prometheus(answers)):
            return ladder_steps.summarize("http://prom", self.STEP)

    def test_window_trims_both_ends(self):
        got = self.summarize([])
        self.assertEqual(got["window"], [T0 + ladder_steps.SETTLE, T0 + 180 - ladder_steps.TAIL])

    def test_missing_series_are_none_and_nan_is_none(self):
        got = self.summarize([('status="success"', value("NaN"))])
        self.assertIsNone(got["resume_rps"])
        self.assertIsNone(got["postgres_cores"])
        self.assertIsNone(got["pg_cache_hit_ratio"])
        self.assertEqual(got["ateapi_cores_per_replica"], [])

    def test_per_replica_cores_busiest_first(self):
        answers = [("sum by (pod)", [
            {"metric": {"pod": "a"}, "value": [0, "290"]},
            {"metric": {"pod": "b"}, "value": [0, "435"]},
        ])]
        got = self.summarize(answers)
        self.assertEqual(got["ateapi_cores_per_replica"], [435.0, 290.0])

    def test_atelet_cores_come_from_the_atelet_container(self):
        got = self.summarize([('container="atelet"', value(0.25))])
        self.assertEqual(got["atelet_cores"], 0.25)

    def test_cache_hit_ratio(self):
        got = self.summarize([("blks_hit", value(990)), ("blks_read", value(10))])
        self.assertAlmostEqual(got["pg_cache_hit_ratio"], 0.99)
        self.assertAlmostEqual(got["pg_blks_read_per_s"], 10 / 145)

    def test_walwrite_waiters_come_from_their_own_query(self):
        got = self.summarize([('wait_event="WALWrite"', value(42)), ('wait_event_type="Lock"', value(3))])
        self.assertEqual(got["pg_walwrite_wait_conns"], 42.0)
        self.assertEqual(got["pg_lock_wait_conns"], 3.0)

    def test_query_trims_the_url_and_times_out(self):
        opened = []

        def urlopen(url, timeout=None):
            opened.append((url, timeout))
            return mock.MagicMock(__enter__=lambda self: self)

        with mock.patch.object(ladder_steps.urllib.request, "urlopen", urlopen), \
                mock.patch.object(ladder_steps.json, "load", lambda r: {"status": "success", "data": {"result": []}}):
            ladder_steps.query("http://prom/", "up", T0)
        url, timeout = opened[0]
        self.assertTrue(url.startswith("http://prom/api/v1/query?"), url)
        self.assertEqual(timeout, ladder_steps.QUERY_TIMEOUT_S)

    def test_failed_query_raises(self):
        with mock.patch.object(ladder_steps, "_get_json", lambda url: {"status": "error"}):
            with self.assertRaises(RuntimeError):
                ladder_steps.summarize("http://prom", self.STEP)


class StatsRecordTest(unittest.TestCase):
    def test_flattens_the_summary(self):
        summary = {"step": 2, "users": 100, "target_rps": 500.0, "window": [10, 20],
                   "failures": {"ResumeActorCold": 3.0}, "ateapi_cores_per_replica": [1.5, 0.5],
                   "resume_rps": 499.0}
        self.assertEqual(ladder_steps.stats_record(summary, "ts", "c", "t"), {
            "timestamp": "ts", "tag": "c", "test_name": "t", "metric": "ladder_step_002",
            "measurements": {"users": 100, "target_rps": 500.0, "resume_rps": 499.0,
                             "window_start": 10, "window_end": 20,
                             "ateapi_cores_busiest_replica": 1.5,
                             "failures_ResumeActorCold": 3.0},
        })

    def test_no_replicas_reported(self):
        summary = {"step": 1, "window": [10, 20], "failures": {}, "ateapi_cores_per_replica": []}
        m = ladder_steps.stats_record(summary, "ts", "c", "t")["measurements"]
        self.assertIsNone(m["ateapi_cores_busiest_replica"])


class AppendLadderStepsTest(unittest.TestCase):
    def append(self, log, get_json=None, existing=""):
        with tempfile.TemporaryDirectory() as d:
            log_path, jsonl_path = Path(d, "logs.txt"), Path(d, "stats.jsonl")
            log_path.write_text(log)
            if existing:
                jsonl_path.write_text(existing)
            logs = io.StringIO()
            with mock.patch.object(ladder_steps, "_get_json", get_json or prometheus([])), \
                    mock.patch("builtins.print"):
                n = ladder_steps.append_ladder_steps(
                    jsonl_path, log_path, "http://prom", "ts", "c", "t", logs)
            rows = jsonl_path.read_text().splitlines() if jsonl_path.exists() else None
            return n, rows, logs.getvalue()

    def test_appends_a_row_per_finished_step(self):
        n, rows, _ = self.append(LOG, existing='{"metric": "locust"}\n')
        self.assertEqual(n, 2)
        self.assertEqual(json.loads(rows[0]), {"metric": "locust"})
        self.assertEqual([json.loads(r)["metric"] for r in rows[1:]],
                         ["ladder_step_001", "ladder_step_002"])

    def test_skips_a_step_too_short_to_measure(self):
        short = LOG.replace("00:22:07,456", "00:19:37,456")  # step 1 lasts 30s
        n, rows, logged = self.append(short)
        self.assertEqual(n, 1)
        self.assertEqual(json.loads(rows[0])["metric"], "ladder_step_002")
        self.assertIn("too short", logged)

    def test_unfinished_step_is_skipped(self):
        n, _, logged = self.append("\n".join(LOG.splitlines()[:3]))
        self.assertEqual(n, 1)
        self.assertIn("no end", logged)

    # A run without the ladder shape leaves stats.jsonl alone, and creates
    # none when the run produced none.
    def test_no_ladder_writes_nothing(self):
        self.assertEqual(self.append("[locust] no ladder here\n"), (0, None, ""))

    def test_failed_query_writes_no_partial_ladder(self):
        calls = []

        def get_json(url):
            calls.append(url)
            # Step 1 takes some 35 queries, so this fails within step 2.
            if len(calls) > 40:
                return {"status": "error"}
            return {"status": "success", "data": {"result": []}}

        with tempfile.TemporaryDirectory() as d:
            log_path, jsonl_path = Path(d, "logs.txt"), Path(d, "stats.jsonl")
            log_path.write_text(LOG)
            jsonl_path.write_text('{"metric": "locust"}\n')
            with mock.patch.object(ladder_steps, "_get_json", get_json):
                with self.assertRaises(RuntimeError):
                    ladder_steps.append_ladder_steps(
                        jsonl_path, log_path, "http://prom", "ts", "c", "t", io.StringIO())
            self.assertGreater(len(calls), 40)
            self.assertEqual(jsonl_path.read_text(), '{"metric": "locust"}\n')


if __name__ == "__main__":
    unittest.main()
