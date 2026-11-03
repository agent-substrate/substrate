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

"""Unit tests for config loading, the evaluator and its source hooks, run_results and the outputs.

Runs are stand-in directories built by the tests.
"""

import json
import re
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evaluate import config, evaluator, outputs  # noqa: E402
from evaluate.sources import SOURCES, BenchmarkRunContext, Source, SourceInvalid  # noqa: E402
from evaluate.sources.run_results import RunResults  # noqa: E402

TAG = "abc123"
NOW = datetime(2026, 11, 2, 10, 0, 0, tzinfo=timezone.utc)
IDENTITY = {"test_name": "t150", "tag": TAG, "run_ts": "1791700000"}


def stats_row(metric: str, **m) -> dict:
    return {"timestamp": "2026-10-11T07:06:40Z", "tag": TAG, "test_name": "t150", "metric": metric,
            "measurements": {k: None if v is None else str(v) for k, v in m.items()}}


def write_run(d: Path, rows: list[dict] = ()) -> BenchmarkRunContext:
    d.mkdir(parents=True, exist_ok=True)
    trial = stats_row("trial_summary", aggregate_failure_ratio=0.0005, allocatable_ram_gb=400, ok=True)
    (d / "stats.jsonl").write_text("".join(json.dumps(r) + "\n" for r in [trial, *rows]))
    return BenchmarkRunContext(files={"stats.jsonl": d / "stats.jsonl", "logs.txt": d / "logs.txt"})


def threshold(value: str, op: str, limit) -> dict:
    return {"value": value, "op": op, "limit": limit}


def evaluations(limit: float = 0.001) -> list[dict]:
    """envelope_health from the design: run_results over two stats rows."""
    return [{
        "name": "envelope_health",
        "variables": [
            {"name": "trial", "source": "run_results",
             "source_cfg": {"metric": "trial_summary", "measurements": ["aggregate_failure_ratio", "allocatable_ram_gb"]}},
            {"name": "server", "source": "run_results",
             "source_cfg": {"metric": "server_summary", "measurements": ["working_set_node_max_gb"]}}],
        "conditions": [
            {"name": "failure_rate", "type": "threshold", "check": threshold("trial.aggregate_failure_ratio", "<=", limit)},
            {"name": "worker_memory", "type": "threshold",
             "check": threshold("server.working_set_node_max_gb", "<", {"ref": "trial.allocatable_ram_gb", "times": 0.85})}],
    }]


def judge(run: BenchmarkRunContext, evs: list[dict]) -> dict:
    return evaluator.Evaluator(config.load(evs), run).evaluate(IDENTITY, now=NOW)


class Fake(Source):
    """A source that records its hooks, for the lifecycle tests."""
    calls: list = []

    @classmethod
    def validate(cls, cfg):
        if set(cfg) - {"answer", "invalid", "boom"}:
            raise ValueError("unknown keys")

    def init(self, run):
        super().init(run)
        Fake.calls.append(("init", run))

    def query(self, cfg):
        Fake.calls.append(("query", cfg))
        if cfg.get("boom"):
            raise RuntimeError("boom")
        if cfg.get("invalid"):
            raise SourceInvalid(cfg["invalid"], {"answer": cfg.get("answer")})
        return {"answer": cfg.get("answer")}


def fake_evaluation(name: str, cfg: dict, limit=42) -> dict:
    return {"name": name, "variables": [{"name": "f", "source": "fake", "source_cfg": cfg}],
            "conditions": [{"name": "c", "type": "threshold", "check": threshold("f.answer", "==", limit)}]}


class ConfigTest(unittest.TestCase):
    def test_load_and_echo(self):
        evs = config.load(evaluations())
        self.assertEqual([(e.name, len(e.variables), len(e.conditions)) for e in evs], [("envelope_health", 2, 2)])
        echoed = config.to_json(evs)
        self.assertEqual(config.to_json(config.load(echoed)), echoed)  # what evaluation.json echoes reloads
        self.assertEqual(echoed[0]["description"], "")

    def test_errors(self):
        def broken(path: str, value) -> list:
            evs = evaluations()
            obj = evs[0]
            *head, last = path.split(".")
            for key in head:
                obj = obj[int(key)] if key.isdigit() else obj[key]
            if value is KeyError:
                del obj[last]
            else:
                obj[int(last) if last.isdigit() else last] = value
            return evs
        cases = {
            "evaluations: needs a non-empty list": {"name": "x"},
            "evaluations: needs a non-empty list (no evaluations": [],
            "source ['run_results'] is not one of": broken("variables.0.source", ["run_results"]),
            "check.op: ['<=']": broken("conditions.0.check.op", ["<="]),
            "type ['threshold'] is not one of": broken("conditions.0.type", ["threshold"]),
            "unknown keys ['severity']": broken("severity", "record"),
            "unknown keys ['actions']": broken("actions", []),
            "name 'Envelope'": broken("name", "Envelope"),
            "the name is reserved": broken("name", "summary"),
            "needs variables": broken("variables", []),
            "needs conditions": broken("conditions", KeyError),
            "description must be a string": broken("description", 1),
            "source 'prometheus' is not one of": broken("variables.0.source", "prometheus"),
            "source_cfg must be a mapping": broken("variables.0.source_cfg", ["x"]),
            "source_cfg: needs measurements": broken("variables.0.source_cfg", {"metric": "trial_summary"}),
            "type 'cel' is not one of ['threshold']": broken("conditions.0.type", "cel"),
            "names no variable of this evaluation": broken("conditions.0.check.value", "gp.goodput_ratio_pct"),
            "is not <variable>.<value>": broken("conditions.0.check.value", "aggregate_failure_ratio"),
            "'Ratio' is not a value name": broken("conditions.0.check.value", "trial.Ratio"),
            "condition names repeated": broken("conditions", evaluations()[0]["conditions"] * 2),
            "variable names repeated": broken("variables.1.name", "trial"),
        }
        for message, evs in cases.items():
            with self.assertRaisesRegex(config.ConfigError, re.escape(message), msg=message):
                config.load(evs)
        with self.assertRaisesRegex(config.ConfigError, "evaluation names repeated"):
            config.load(evaluations() + evaluations())

    def test_source_cfg_defaults_to_empty(self):
        with mock.patch.dict(SOURCES, {"fake": Fake}):
            ev = fake_evaluation("e", {})
            del ev["variables"][0]["source_cfg"]
            self.assertEqual(config.load([ev])[0].variables[0].source_cfg, {})
            with self.assertRaisesRegex(config.ConfigError, "variable 'f': source_cfg: unknown keys"):
                config.load([fake_evaluation("e", {"what": 1})])


class RunResultsTest(unittest.TestCase):
    def query(self, run: BenchmarkRunContext, cfg: dict):
        RunResults.validate(cfg)
        source = RunResults()
        source.init(run)
        return source.query(cfg)

    def test_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), [stats_row("grpc_ResumeActor", p90=420), stats_row("grpc_ResumeActor", p90=430),
                                        stats_row("server_summary", oom_events_node=None, mode="async")])
            self.assertEqual(self.query(run, {"metric": "trial_summary", "measurements": ["ok", "allocatable_ram_gb",
                                                                                          "nope"]}),
                             {"ok": True, "allocatable_ram_gb": 400.0, "nope": None})
            self.assertEqual(self.query(run, {"metric": "grpc_ResumeActor", "measurements": ["p90"]}),
                             {"p90": 430.0})  # the last row of a metric wins
            self.assertEqual(self.query(run, {"metric": "server_summary", "measurements": ["oom_events_node", "mode"]}),
                             {"oom_events_node": None, "mode": "async"})

    def test_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp))
            with self.assertRaisesRegex(SourceInvalid, "no stats.jsonl row ladder_step_001"):
                self.query(run, {"metric": "ladder_step_001", "measurements": ["users"]})
            with self.assertRaisesRegex(SourceInvalid, "stats.jsonl absent"):
                self.query(BenchmarkRunContext(files={}), {"metric": "trial_summary", "measurements": ["ok"]})

    def test_validate(self):
        for message, cfg in {"needs metric": {}, "needs measurements": {"metric": "m"},
                             "a non-empty list": {"metric": "m", "measurements": []},
                             "listed twice": {"metric": "m", "measurements": ["a", "a"]},
                             "'p99.9' is not a value name": {"metric": "m", "measurements": ["p99.9"]},
                             "unknown keys ['run']": {"metric": "m", "measurements": ["a"], "run": "baseline"}}.items():
            with self.assertRaisesRegex(ValueError, re.escape(message), msg=message):
                RunResults.validate(cfg)


class EvaluateTest(unittest.TestCase):
    def test_report_and_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), [stats_row("server_summary", working_set_node_max_gb=350)])
            report = judge(run, evaluations())
        (ev,) = report["evaluations"]
        self.assertEqual(report["run"], IDENTITY)
        self.assertEqual((ev["status"], ev["reason"]), ("fail", "failed: worker_memory"))
        mem = ev["conditions"][1]
        self.assertEqual((mem["value"], mem["limit"], mem["limit_ref"]), (350.0, 340.0, "trial.allocatable_ram_gb"))
        self.assertEqual(mem["reason"], "server.working_set_node_max_gb = 350, needs < 340 (0.85 x trial.allocatable_ram_gb)")
        self.assertEqual(ev["variables"][0]["values"], {"aggregate_failure_ratio": 0.0005, "allocatable_ram_gb": 400.0})
        self.assertEqual(report["summary"], {"status": "fail", "passed": False, "failed": ["envelope_health"],
                                             "invalid": []})
        rows = outputs.stats_rows(report, "ts", TAG, "t150")
        self.assertEqual([r["metric"] for r in rows], ["evaluation_envelope_health", "evaluation_summary"])
        self.assertTrue(all(set(r) == {"timestamp", "tag", "test_name", "metric", "measurements"} for r in rows))
        m = rows[0]["measurements"]
        self.assertEqual((m["status"], m["passed"], m["failed_conditions"], m["server__working_set_node_max_gb"],
                          m["trial__aggregate_failure_ratio"]), ("fail", "false", "worker_memory", "350", "0.0005"))
        self.assertTrue(all(v is None or isinstance(v, str) for r in rows for v in r["measurements"].values()))
        self.assertEqual(rows[1]["measurements"], {"status": "fail", "passed": "false", "failed": "envelope_health",
                                                   "invalid": "", "error": ""})
        self.assertEqual(outputs.summary_lines(report)[0].splitlines()[0],
                         "evaluation envelope_health: fail (failed: worker_memory)")

    def test_null_measurement_is_invalid_not_zero(self):
        """A measurement recorded as null (e.g. a Prometheus-backed count on a cluster without
        Prometheus) makes its condition invalid; it never passes as 0."""
        evs = [{"name": "health", "variables": [{"name": "server", "source": "run_results", "source_cfg": {
                    "metric": "server_summary", "measurements": ["oom_events_node", "oom_events_worker_restarts"]}}],
                "conditions": [
                    {"name": "node_ooms", "type": "threshold", "check": threshold("server.oom_events_node", "==", 0)},
                    {"name": "restarts", "type": "threshold",
                     "check": threshold("server.oom_events_worker_restarts", "==", 0)}]}]
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), [stats_row("server_summary", oom_events_node=None, oom_events_worker_restarts=0)])
            report = judge(run, evs)
        conds = {c["name"]: c for c in report["evaluations"][0]["conditions"]}
        self.assertEqual((conds["node_ooms"]["status"], conds["node_ooms"]["reason"]),
                         ("invalid", "server.oom_events_node not measured"))
        self.assertEqual(conds["restarts"]["status"], "pass")
        self.assertEqual((report["evaluations"][0]["status"], report["summary"]["invalid"]), ("invalid", ["health"]))
        self.assertIsNone(outputs.stats_rows(report, "ts", TAG, "t")[0]["measurements"]["server__oom_events_node"])

    def test_invalid_variable_makes_its_conditions_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = judge(write_run(Path(tmp)), evaluations())
        (ev,) = report["evaluations"]
        self.assertEqual((ev["variables"][1]["status"], ev["variables"][1]["invalid_reason"]),
                         ("invalid", "no stats.jsonl row server_summary"))
        self.assertEqual([c["status"] for c in ev["conditions"]], ["pass", "invalid"])
        self.assertEqual(ev["conditions"][1]["reason"], "server is invalid: no stats.jsonl row server_summary")
        self.assertEqual((ev["status"], ev["reason"]), ("invalid", "invalid: worker_memory"))
        row = outputs.stats_rows(report, "ts", TAG, "t")[0]["measurements"]
        self.assertEqual(row["server__invalid_reason"], "no stats.jsonl row server_summary")

    def test_error_report(self):
        report = evaluator.error_report(IDENTITY, "config: evaluations[0]: name 'Bad'")
        rows = outputs.stats_rows(report, "ts", TAG, "t150")
        self.assertEqual((report["summary"]["status"], report["evaluations"]), ("invalid", []))
        self.assertEqual([r["metric"] for r in rows], ["evaluation_summary"])
        self.assertEqual(rows[0]["measurements"]["error"], "config: evaluations[0]: name 'Bad'")
        self.assertEqual(outputs.summary_lines(report), ["evaluations not run: config: evaluations[0]: name 'Bad'"])


class HooksTest(unittest.TestCase):
    """The evaluator's side of the source hooks: validate at load, init once, query per distinct cfg."""

    def setUp(self):
        Fake.calls = []
        patcher = mock.patch.dict(SOURCES, {"fake": Fake})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_init_once_then_one_query_per_distinct_cfg(self):
        run = BenchmarkRunContext(files={})
        evs = config.load([fake_evaluation("a", {"answer": 42}), fake_evaluation("b", {"answer": 42}),
                           fake_evaluation("c", {"answer": 7}, limit=7)])
        e = evaluator.Evaluator(evs, run)
        self.assertEqual(Fake.calls, [("init", run)])  # at creation, before any data is read
        report = e.evaluate({}, now=NOW)
        self.assertEqual(Fake.calls[1:], [("query", {"answer": 42}), ("query", {"answer": 7})])
        self.assertEqual(report["summary"]["status"], "pass")

    def test_source_invalid_keeps_its_values(self):
        report = judge(BenchmarkRunContext(files={}), [fake_evaluation("a", {"answer": 42, "invalid": "no baseline"})])
        (var,) = report["evaluations"][0]["variables"]
        self.assertEqual((var["status"], var["invalid_reason"], var["values"]), ("invalid", "no baseline", {"answer": 42}))
        self.assertEqual(report["evaluations"][0]["conditions"][0]["reason"], "f is invalid: no baseline")

    def test_errors_become_invalid(self):
        report = judge(BenchmarkRunContext(files={}), [fake_evaluation("a", {"boom": True})])
        self.assertEqual(report["evaluations"][0]["variables"][0]["invalid_reason"], "error: RuntimeError: boom")
        with mock.patch.object(Fake, "init", side_effect=OSError("no disk")):
            report = judge(BenchmarkRunContext(files={}), [fake_evaluation("a", {"answer": 42})])
        self.assertEqual(report["evaluations"][0]["variables"][0]["invalid_reason"], "init failed: OSError: no disk")

    def test_values_contract(self):
        for values, why in (({"Answer": 1}, "value names that aren't names: ['Answer']"),
                            ({"invalid_reason": "x"}, "reserved value names ['invalid_reason']"),
                            ({"answer": [1, 2]}, "values that aren't scalars: ['answer']")):
            with mock.patch.object(Fake, "query", return_value=values):
                report = judge(BenchmarkRunContext(files={}), [fake_evaluation("a", {"answer": 42})])
            self.assertEqual(report["evaluations"][0]["variables"][0]["invalid_reason"],
                             f"error: source fake returned {why}")


if __name__ == "__main__":
    unittest.main()
