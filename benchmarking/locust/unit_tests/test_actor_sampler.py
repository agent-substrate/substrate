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

"""Unit tests for actor_sampler.py.

Run via: python3 benchmarking/locust/unit_tests/test_actor_sampler.py
The sampler test scrapes a local HTTP server standing in for boomer. Needs
prometheus_client: pip install -r benchmarking/locust/requirements.txt
"""

import csv
import http.server
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import actor_sampler

# A boomer scrape: the gauge, plus an unrelated family the parser must skip.
SCRAPE = """\
# HELP locust_actors Actors the worker drives, by the lifecycle state it last observed.
# TYPE locust_actors gauge
locust_actors{state="hibernated",user_class="GluttonUser"} 7
locust_actors{state="running",user_class="GluttonUser"} 3
locust_actors{state="crashed",user_class="GluttonUser"} 0
# HELP locust_requests_total Requests.
# TYPE locust_requests_total counter
locust_requests_total{method="grpc",name="ResumeActor"} 42
"""


def write_history(directory, rows):
    path = Path(directory) / "actors_history.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(actor_sampler.HISTORY_COLUMNS)
        writer.writerows(rows)
    return path


class ParseActorCountsTest(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(actor_sampler.parse_actor_counts(SCRAPE), {
            ("GluttonUser", "hibernated"): 7.0,
            ("GluttonUser", "running"): 3.0,
            ("GluttonUser", "crashed"): 0.0,
        })

    def test_no_gauge(self):
        # A worker that has not driven an actor yet exports no series.
        self.assertEqual(actor_sampler.parse_actor_counts(""), {})


class SummarizeActorHistoryTest(unittest.TestCase):
    def test_unmeasured(self):
        with tempfile.TemporaryDirectory() as td:
            self.assertEqual(actor_sampler.summarize_actor_history(None),
                             actor_sampler.EMPTY_ACTOR_SUMMARY)
            self.assertEqual(
                actor_sampler.summarize_actor_history(Path(td) / "absent.csv"),
                actor_sampler.EMPTY_ACTOR_SUMMARY)
            # Header only: the sampler started but no scrape succeeded.
            self.assertEqual(
                actor_sampler.summarize_actor_history(write_history(td, [])),
                actor_sampler.EMPTY_ACTOR_SUMMARY)

    def test_summary(self):
        rows = [
            (100, "GluttonUser", "hibernated", 10),
            (105, "GluttonUser", "running", 4),
            (105, "GluttonUser", "hibernated", 6),
            (110, "GluttonUser", "running", 8),
            (110, "GluttonUser", "hibernate_pending", 2),
            (110, "GluttonUser", "crashed", 1),
            (115, "GluttonUser", "running", 2),
            (115, "GluttonUser", "hibernated", 7),
            (115, "GluttonUser", "crashed", 1),
            (115, "bad", "running", "not-a-number"),  # skipped
        ]
        with tempfile.TemporaryDirectory() as td:
            got = actor_sampler.summarize_actor_history(write_history(td, rows))
        self.assertEqual(got, {
            "running_actors_peak": 8,
            "running_actors_p50": 4,   # sorted running [0, 2, 4, 8]
            "running_actors_p90": 8,
            "live_actors_peak": 11,    # 8 + 2 + 1 at t=110
            "hibernate_pending_actors_peak": 2,
            "crashed_actors_final": 1,
        })

    def test_sums_across_user_classes(self):
        rows = [
            (100, "GluttonUser", "running", 3),
            (100, "DurDirUser", "running", 5),
        ]
        with tempfile.TemporaryDirectory() as td:
            got = actor_sampler.summarize_actor_history(write_history(td, rows))
        self.assertEqual(got["running_actors_peak"], 8)
        self.assertEqual(got["live_actors_peak"], 8)


class ActorSamplerTest(unittest.TestCase):
    def test_samples_into_csv(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = SCRAPE.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "actors_history.csv"
            logs = []
            sampler = actor_sampler.ActorSampler(
                f"http://127.0.0.1:{server.server_port}/metrics", out, 0.05,
                logs.append)
            sampler.start()
            deadline = time.monotonic() + 5
            # Header plus one scrape's three rows.
            while time.monotonic() < deadline and (
                    not out.exists() or len(out.read_text().splitlines()) < 4):
                time.sleep(0.02)
            sampler.stop()
            with open(out, encoding="utf-8") as f:
                rows = list(csv.DictReader(f))

        self.assertEqual(logs, [])
        self.assertGreaterEqual(len(rows), 3)
        self.assertEqual({(r["User Class"], r["State"], r["Count"]) for r in rows[:3]}, {
            ("GluttonUser", "crashed", "0"),
            ("GluttonUser", "hibernated", "7"),
            ("GluttonUser", "running", "3"),
        })

    def test_unreachable_logs_once(self):
        # Bind then release a port so nothing is listening on it.
        server = http.server.HTTPServer(("127.0.0.1", 0), http.server.BaseHTTPRequestHandler)
        port = server.server_port
        server.server_close()

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "actors_history.csv"
            logs = []
            sampler = actor_sampler.ActorSampler(
                f"http://127.0.0.1:{port}/metrics", out, 0.02, logs.append)
            sampler.start()
            time.sleep(0.2)
            sampler.stop()
            lines = out.read_text().splitlines()

        self.assertEqual(lines, [",".join(actor_sampler.HISTORY_COLUMNS)])
        self.assertEqual(len(logs), 1)


if __name__ == "__main__":
    unittest.main()
