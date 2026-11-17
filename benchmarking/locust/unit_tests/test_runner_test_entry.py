# Copyright 2026 The Agent Substrate Authors
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

"""Unit tests for runner.py's --test-entry and --run-ts, and the artifacts it stores."""

import argparse
import contextlib
import io
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import runner  # noqa: E402


class TestEntryTest(unittest.TestCase):
    def test_json_object(self):
        self.assertEqual(runner.json_object('{"name": "t", "observers": []}'), {"name": "t", "observers": []})
        for bad in ("{oops", "[]"):
            with self.subTest(bad=bad), self.assertRaises(argparse.ArgumentTypeError):
                runner.json_object(bad)

    def test_the_run_config_shows_the_whole_entry(self):
        a = argparse.Namespace(name="t", tag="c", file="tests/glutton.py", duration="1m", users=1,
                               allow_empty_stats=False, locust_extra=[],
                               test_entry={"name": "t", "evaluations": [{"name": "e"}]})
        logs = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()):
            runner.log_run_config(a, "d", Path("w"), logs)
        self.assertIn('  test entry:\n    {\n      "name": "t",\n      "evaluations": [\n', logs.getvalue())


class StoredArtifactsTest(unittest.TestCase):
    ARGV = ["runner.py", "-f", "tests/glutton.py", "-t", "1m", "-u", "10", "--tag", "unit", "--name", "unit-run",
            "--no-cluster-facts", "--no-phase-breakdown", "--allow-empty-stats"]

    @staticmethod
    def locust(_args, csv_prefix, _logs, _traces):
        """What locust leaves: its CSVs, named after the --csv prefix."""
        Path(f"{csv_prefix}_stats.csv").write_text("Type,Name,Request Count\nPOST,CreateActor,3\n")
        Path(f"{csv_prefix}_failures.csv").write_text("Method,Name,Error,Occurrences\n")
        return 0

    def stored(self, *flags: str) -> list[str]:
        uploaded = []
        with mock.patch.object(runner, "run_test", self.locust), \
             mock.patch.object(runner, "upload", lambda src, dest: uploaded.append(dest)), \
             mock.patch.object(runner, "append_trial_summary"), \
             mock.patch.object(runner, "extract_and_record_server_telemetry"), \
             mock.patch.object(runner, "append_ladder_steps", return_value=0), \
             mock.patch.object(sys, "argv", self.ARGV + list(flags)), \
             contextlib.redirect_stdout(io.StringIO()):
            runner.main()
        return uploaded

    def test_artifacts_are_stored_by_name_under_the_run_ts(self):
        prefix = "/dest/runs/unit-run/run_date=2026-10-11/run_ts=1791701190/run_tag=unit"
        self.assertEqual(self.stored("--dest", "/dest", "--run-ts", "1791701190"), [
            f"{prefix}/{name}" for name in
            ("failures.csv", "logs.txt", "stats.csv", "stats.jsonl", "status.json", "traces.txt")])


if __name__ == "__main__":
    unittest.main()
