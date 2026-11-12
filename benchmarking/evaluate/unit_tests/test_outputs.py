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

"""Unit tests for the outputs: evaluation.json, the stats.jsonl rows and the log lines."""

import json
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evaluate import outputs  # noqa: E402
from evaluate.evaluator import (  # noqa: E402
    ConditionResult, EvaluationResult, Report, Summary, VariableResult, error_report)
from evaluate.sources.base import RunIdentity  # noqa: E402

TAG = "abc123"
WORKER_MEMORY = "server.working_set_node_max_gb < 0.85 * trial.allocatable_ram_gb"
FAILURE_RATE = ConditionResult(name="failure_rate", cel="trial.aggregate_failure_ratio <= 0.001", status="pass",
                               passed=True, reason="")
ENVELOPE = EvaluationResult(
    name="envelope_health", description="", status="fail", passed=False, reason="failed: worker_memory",
    conditions=(FAILURE_RATE, ConditionResult(name="worker_memory", cel=WORKER_MEMORY, status="fail", passed=False,
                                              reason=f"worker_memory: {WORKER_MEMORY}")),
    variables=(VariableResult(name="trial", source="run_results", status="ok", invalid_reason=None,
                              values={"aggregate_failure_ratio": 0.0005, "allocatable_ram_gb": 400.0}),
               VariableResult(name="server", source="run_results", status="ok", invalid_reason=None,
                              values={"working_set_node_max_gb": 350.0})),
    config={"name": "envelope_health"})


def report(ev: EvaluationResult, summary: Summary) -> Report:
    return Report(run={"test_name": "t150", "tag": TAG}, evaluated_at="2026-11-02T10:00:00Z", error=None,
                  summary=summary, evaluations=(ev,))


FAILED = report(ENVELOPE, Summary(status="fail", passed=False, failed=("envelope_health",), invalid=()))


class OutputsTest(unittest.TestCase):
    def test_rows(self):
        rows = outputs.stats_rows(FAILED, "ts", TAG, "t150")
        self.assertEqual([r["metric"] for r in rows], ["evaluation_envelope_health", "evaluations"])
        self.assertTrue(all(set(r) == {"timestamp", "tag", "test_name", "metric", "measurements"} for r in rows))
        self.assertEqual(rows[0]["measurements"], {
            "status": "fail", "passed": "false", "reason": "failed: worker_memory",
            "failed_conditions": "worker_memory", "invalid_conditions": "",
            "trial__aggregate_failure_ratio": "0.0005", "trial__allocatable_ram_gb": "400",
            "server__working_set_node_max_gb": "350"})
        self.assertEqual(rows[1]["measurements"], {"status": "fail", "passed": "false", "failed": "envelope_health",
                                                   "invalid": "", "error": ""})

    def test_value_names_are_kept_and_nulls_stay_null(self):
        nh = VariableResult(name="nh", source="run_results", status="ok", invalid_reason=None,
                            values={"metric_nighthawk.builtin_attempted_rps": 600.0, "p99.9": None,
                                    "release": "2026_11"})
        passed = replace(ENVELOPE, status="pass", passed=True, reason="", conditions=(FAILURE_RATE,), variables=(nh,))
        m = outputs.stats_rows(report(passed, Summary(status="pass", passed=True, failed=(), invalid=())),
                               "ts", TAG, "t")[0]["measurements"]
        self.assertEqual({k: v for k, v in m.items() if k.startswith("nh__")}, {
            "nh__metric_nighthawk.builtin_attempted_rps": "600", "nh__p99.9": None, "nh__release": "2026_11"})

    def test_summary_lines(self):
        self.assertEqual(outputs.summary_lines(FAILED), [
            f"evaluation envelope_health: fail (failed: worker_memory)\n  worker_memory: {WORKER_MEMORY}",
            "summary: fail (1 evaluations)",
        ])
        why = "server is invalid: no stats.jsonl row server_summary"
        invalid = replace(ENVELOPE, status="invalid", reason="invalid: worker_memory, headroom", conditions=(
            FAILURE_RATE,
            ConditionResult(name="worker_memory", cel=WORKER_MEMORY, status="invalid", passed=False, reason=why),
            ConditionResult(name="headroom", cel="server.working_set_node_max_gb < 100", status="invalid",
                            passed=False, reason=why)))
        summary = Summary(status="invalid", passed=False, failed=(), invalid=("envelope_health",))
        self.assertEqual(outputs.summary_lines(report(invalid, summary)), [
            f"evaluation envelope_health: invalid (invalid: worker_memory, headroom)\n  {why}",
            "summary: invalid (1 evaluations)",
        ])
        passed = replace(ENVELOPE, status="pass", passed=True, reason="", conditions=(FAILURE_RATE,))
        self.assertEqual(outputs.summary_lines(report(passed, Summary(status="pass", passed=True, failed=(),
                                                                      invalid=()))), [
            "evaluation envelope_health: pass",
            "summary: pass (1 evaluations)",
        ])

    def test_error_report(self):
        failed_to_load = error_report(RunIdentity(uri="/runs/t150", test_name="t150"),
                                      "config: evaluations: 0.name: String should match pattern")
        self.assertEqual(failed_to_load.run, {"test_name": "t150", "uri": "/runs/t150"})
        rows = outputs.stats_rows(failed_to_load, "ts", TAG, "t150")
        self.assertEqual([(r["metric"], r["measurements"]) for r in rows], [("evaluations", {
            "status": "invalid", "passed": "false", "failed": "", "invalid": "",
            "error": "config: evaluations: 0.name: String should match pattern"})])
        self.assertEqual(outputs.summary_lines(failed_to_load),
                         ["evaluations not run: config: evaluations: 0.name: String should match pattern"])

    def test_evaluation_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "run" / "evaluation.json"
            outputs.write_evaluation_json(FAILED, out)
            doc = json.loads(out.read_text())
            # The report is serialized before anything is written.
            bad = Path(tmp) / "bad.json"
            with self.assertRaises(TypeError):
                outputs.write_evaluation_json(replace(FAILED, run={"bad": object()}), bad)
            self.assertFalse(bad.exists())
        self.assertEqual(list(doc), ["run", "evaluated_at", "error", "summary", "evaluations"])
        self.assertEqual(doc["summary"], {"status": "fail", "passed": False, "failed": ["envelope_health"],
                                          "invalid": []})
        (ev,) = doc["evaluations"]
        self.assertEqual(list(ev), ["name", "description", "status", "passed", "reason", "conditions", "variables",
                                    "config"])
        self.assertEqual(list(ev["conditions"][0].items()), [
            ("name", "failure_rate"), ("cel", "trial.aggregate_failure_ratio <= 0.001"), ("status", "pass"),
            ("passed", True), ("reason", "")])
        self.assertEqual(list(ev["variables"][1].items()), [
            ("name", "server"), ("source", "run_results"), ("status", "ok"), ("invalid_reason", None),
            ("values", {"working_set_node_max_gb": 350.0})])


if __name__ == "__main__":
    unittest.main()
