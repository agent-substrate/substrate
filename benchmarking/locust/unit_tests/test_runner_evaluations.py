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

"""Unit tests for runner.py's evaluations (the --test-entry's evaluations)."""

import argparse
import io
import json
import sys
import tempfile
import unittest
import unittest.mock
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
# The evaluate package is /app/evaluate in the image; here it is
# benchmarking/evaluate. Appended, so nothing else on the path is shadowed.
sys.path.append(str(Path(__file__).resolve().parents[2]))

import runner  # noqa: E402

RUN_TS = 1791701190  # 2026-10-11T07:26:30Z
RECORD_SELECT = "has(line.msg) && line.msg == 'sweperf task record' && has(line.v) && line.v == 1.0"
EVALUATIONS = [{
    "name": "goodput_strict",
    "variables": [{"name": "goodput", "source": "cel", "source_cfg": {
        "inputs": {
            "records": {"reader": "json_lines", "file": "logs.txt", "prefix": "[boomer] ", "select": RECORD_SELECT},
            "one_user": {"reader": "json_lines", "file": "logs.txt", "prefix": "[boomer] ", "select": RECORD_SELECT,
                         "test": "t1"},
        },
        "locals": [
            {"name": "ref_wall_ms", "cel": "percentile(one_user.lines.filter(t, !t.failed).map(t, t.wall_ms), 50.0)"},
            {"name": "good",
             "cel": "records.lines.filter(t, !t.failed && t.resume_max_ms <= 1500.0 && t.wall_ms <= 1.5 * ref_wall_ms)"},
        ],
        "values": {
            "good_ratio_pct": "round(100.0 * double(size(good)) / double(size(records.lines)), 2)",
            "ref_wall_ms": "ref_wall_ms",
            "one_user_run": "one_user.uri",
        },
    }}],
    "conditions": [{"name": "good_ratio", "cel": "goodput.good_ratio_pct >= 90"}],
}]


def boomer(ts: int, msg: str, **kw) -> str:
    t = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return "[boomer] " + json.dumps({"time": t, "msg": msg, **kw})


def write_run(logs: Path, stats: Path, run_ts: int, wall_ms: float) -> None:
    """What a run of runner.py leaves: one sweperf task record, and a trial_summary row."""
    end = datetime.fromtimestamp(run_ts + 30, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logs.parent.mkdir(parents=True, exist_ok=True)
    logs.write_text("\n".join([
        boomer(run_ts + 30, "sweperf task record", v=1, actor="a1", end=end, cel_ms=6000.0, wall_ms=wall_ms,
               resume_max_ms=200.0, failed=False),
        boomer(run_ts + 60, "sweperf shutdown started")]) + "\n")
    trial = {"timestamp": "", "tag": "abc", "test_name": "t", "metric": "trial_summary",
             "measurements": {"allocatable_cores": "100", "run_start": str(run_ts), "run_end": str(run_ts + 60)}}
    stats.write_text(json.dumps(trial) + "\n")


def args(evaluations: object, dest: str) -> argparse.Namespace:
    return argparse.Namespace(test_entry={"name": "t150", "type": "locust", "evaluations": evaluations}, tag="abc",
                              name="t150", dest=dest)


class EvaluationNamesTest(unittest.TestCase):
    def test_names(self):
        self.assertEqual(runner.evaluation_names(None), "(none)")
        self.assertEqual(runner.evaluation_names(EVALUATIONS * 2), "goodput_strict, goodput_strict")
        self.assertEqual(runner.evaluation_names([]), "(empty)")
        self.assertEqual(runner.evaluation_names("oops"), "(unreadable, see below)")


class RunEvaluationsTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dest = Path(tmp.name) / "dest"
        self.work = Path(tmp.name) / "work"
        self.files = {"logs.txt": self.work / "t150_logs.txt", "stats.jsonl": self.work / "t150.jsonl"}
        self.run_dir = self.work / "run"
        runner.link_artifacts([(path, name) for name, path in self.files.items()], self.run_dir)
        self.identity = runner.run_identity(args(None, str(self.dest)), "2026-10-11", RUN_TS,
                                            f"{self.dest}/runs/t150/run_date=2026-10-11/run_ts={RUN_TS}/run_tag=abc")

    def run_step(self, a: argparse.Namespace) -> tuple[dict, list[str], str]:
        """prepare_evaluations at the start of the run, then the run, then run_evaluations."""
        logs = io.StringIO()
        evaluator, error = runner.prepare_evaluations(a, self.run_dir, self.identity, logs)
        write_run(self.files["logs.txt"], self.files["stats.jsonl"], RUN_TS, wall_ms=8000.0)
        out = self.work / "t150_evaluation.json"
        runner.run_evaluations(a, evaluator, error, self.identity, out, self.files["stats.jsonl"], "ts", logs)
        metrics = [json.loads(line)["metric"] for line in self.files["stats.jsonl"].read_text().splitlines()]
        return json.loads(out.read_text()), metrics, logs.getvalue()

    def test_judges_the_run_and_appends_rows(self):
        report, metrics, log = self.run_step(args(EVALUATIONS, str(self.dest)))
        self.assertEqual(report["run"], {"test_name": "t150", "tag": "abc", "run_date": "2026-10-11",
                                         "run_ts": str(RUN_TS), "uri": self.identity.uri})
        variable = report["evaluations"][0]["variables"][0]
        self.assertEqual((report["summary"]["status"], variable["values"]), ("invalid", {}))
        # No run of t1 under --dest: the input can't be read, and the reason says why.
        self.assertEqual(variable["invalid_reason"], f"input 'one_user': no run of t1 with tag abc under "
                         f"{self.dest}/runs/ in the 6 h before run_ts {RUN_TS}")
        self.assertEqual(metrics, ["trial_summary", "evaluation_goodput_strict", "evaluations"])
        self.assertIn("evaluation goodput_strict: invalid (invalid: good_ratio; "
                      "variable goodput is invalid: input 'one_user'", log)

    def test_paired_run_from_dest(self):
        paired = self.dest / "runs/t1/run_date=2026-10-11" / f"run_ts={RUN_TS - 600}/run_tag=abc"
        write_run(paired / "logs.txt", paired / "stats.jsonl", RUN_TS - 600, wall_ms=4000.0)
        report, _, log = self.run_step(args(EVALUATIONS, str(self.dest)))
        values = report["evaluations"][0]["variables"][0]["values"]
        self.assertEqual((values["one_user_run"], values["ref_wall_ms"]), (str(paired), 4000.0))
        # 8000 ms against 1.5 x the 1-user run's 4000 ms.
        self.assertEqual((values["good_ratio_pct"], report["summary"]["status"]), (0.0, "fail"))
        self.assertIn("evaluation goodput_strict: fail (failed: good_ratio)", log)

    def test_prepare_never_raises(self):
        logs = io.StringIO()
        with unittest.mock.patch("evaluate.check_config", side_effect=TypeError("unhashable type: 'list'")):
            evaluator, error = runner.prepare_evaluations(args([], str(self.dest)), self.run_dir, self.identity, logs)
        self.assertIsNone(evaluator)
        self.assertEqual(error, "evaluate failed to load or prepare the evaluations: TypeError: unhashable type: 'list'")
        self.assertIn("Warning: evaluations will not be judged", logs.getvalue())

    def test_config_error_is_found_before_the_run_and_recorded(self):
        report, metrics, log = self.run_step(args([{"name": "Bad"}], str(self.dest)))
        self.assertTrue(report["error"].startswith("config: evaluations: 0.name: String should match pattern"))
        self.assertEqual((report["summary"]["status"], report["evaluations"]), ("invalid", []))
        self.assertEqual(metrics, ["trial_summary", "evaluations"])
        self.assertIn("Warning: evaluations will not be judged: config: ", log)


if __name__ == "__main__":
    unittest.main()
