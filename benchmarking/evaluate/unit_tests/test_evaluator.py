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

"""Unit tests for config loading, CEL conditions, the evaluator and run_results.

Runs are stand-in directories built by the tests.
"""

import json
import re
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evaluate import cel_env, config, evaluator  # noqa: E402
from evaluate.evaluator import ConditionResult, Summary  # noqa: E402
from evaluate.sources import SOURCES, BenchmarkRunContext, Source, SourceInvalid  # noqa: E402
from evaluate.sources.base import LocalArtifacts, RunIdentity  # noqa: E402
from evaluate.sources.run_results import RunResults, coerce  # noqa: E402

TAG = "abc123"
NOW = datetime(2026, 11, 2, 10, 0, 0, tzinfo=timezone.utc)
IDENTITY = RunIdentity(uri="gs://b/runs/t150", test_name="t150", tag=TAG, run_ts=1791700000)


def local_run(d: Path) -> BenchmarkRunContext:
    return BenchmarkRunContext(IDENTITY, LocalArtifacts(d))


NO_RUN = local_run(Path(tempfile.gettempdir()) / "evaluate-unit-tests-no-run")  # holds no files


def stats_row(metric: str, **m) -> dict:
    return {"timestamp": "2026-10-11T07:06:40Z", "tag": TAG, "test_name": "t150", "metric": metric,
            "measurements": {k: None if v is None else str(v) for k, v in m.items()}}


def write_run(d: Path, rows: list[dict] = ()) -> BenchmarkRunContext:
    d.mkdir(parents=True, exist_ok=True)
    trial = stats_row("trial_summary", aggregate_failure_ratio=0.0005, allocatable_ram_gb=400, ok=True)
    (d / "stats.jsonl").write_text("".join(json.dumps(r) + "\n" for r in [trial, *rows]))
    return local_run(d)


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
            {"name": "failure_rate", "cel": f"trial.aggregate_failure_ratio <= {limit}"},
            {"name": "worker_memory", "cel": "server.working_set_node_max_gb < 0.85 * trial.allocatable_ram_gb"}],
    }]


def judge(run: BenchmarkRunContext, evs: list[dict]) -> evaluator.Report:
    return evaluator.Evaluator(config.load(evs), run).evaluate(now=NOW)


class Fake(Source):
    """A source that records its hooks, for the lifecycle and scalar type tests."""
    calls: list = []

    @classmethod
    def validate(cls, cfg):
        if set(cfg) - {"answer", "values", "invalid", "boom"}:
            raise ValueError("unknown keys")

    def init(self, run):
        super().init(run)
        Fake.calls.append(("init", run))

    def query(self, cfg):
        Fake.calls.append(("query", cfg))
        if cfg.get("boom"):
            raise RuntimeError("boom")
        if "values" in cfg:
            values = dict(cfg["values"])
        else:
            values = {"answer": cfg.get("answer")}
        if "invalid" in cfg:
            raise SourceInvalid(cfg["invalid"], values)
        return values


def fake_evaluation(name: str, cfg: dict, cel_expr: str = "f.answer == 42") -> dict:
    return {"name": name, "variables": [{"name": "f", "source": "fake", "source_cfg": cfg}],
            "conditions": [{"name": "c", "cel": cel_expr}]}


class ConfigTest(unittest.TestCase):
    def test_load_and_echo(self):
        evs = config.load(evaluations())
        self.assertEqual([(e.name, len(e.variables), len(e.conditions)) for e in evs], [("envelope_health", 2, 2)])
        echoed = config.to_json(evs)
        self.assertEqual(config.to_json(config.load(echoed)), echoed)
        self.assertEqual(echoed[0]["description"], "")
        # Evaluation and condition names allow double underscores; external keys allow capitals and dots.
        dotted = [{
            "name": "ingress__rps",
            "variables": [{"name": "nh", "source": "run_results", "source_cfg": {
                "metric": "nighthawk", "measurements": ["metric_nighthawk.builtin_attempted_rps", "p99.9"]}}],
            "conditions": [{"name": "rps__min",
                            "cel": 'nh["metric_nighthawk.builtin_attempted_rps"] >= 2.0 * nh["p99.9"]'}],
        }]
        self.assertEqual(config.load(dotted)[0].conditions[0].name, "rps__min")

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
            "evaluations: Input should be a valid list": {"name": "x"},
            "evaluations: List should have at least 1 item": [],
            "evaluations: 0.name: String should match pattern '^[a-z][a-z0-9_]*$'": [{"name": "Bad"}],
            "0.7: Keys should be strings": [{**evaluations()[0], 7: "x"}],
            "0.variables.0.source: Input should be a valid string": broken("variables.0.source", ["run_results"]),
            "0.severity: Extra inputs are not permitted": broken("severity", "record"),
            "0.actions: Extra inputs are not permitted": broken("actions", []),
            "0.name: String should match pattern": broken("name", "Envelope"),
            "0.variables.0.name: String should have at most 64 characters": broken("variables.0.name", "a" * 65),
            "0.variables: List should have at least 1 item": broken("variables", []),
            "0.conditions: Field required": broken("conditions", KeyError),
            "0.conditions: List should have at least 1 item": broken("conditions", []),
            "0.description: Input should be a valid string": broken("description", 1),
            "0.variables.0.source: source 'prometheus' is not one of": broken("variables.0.source", "prometheus"),
            "0.variables.0.source_cfg: Input should be a valid dictionary": broken("variables.0.source_cfg", ["x"]),
            "0.variables.0.source_cfg.0.[key]: Input should be a valid string": broken("variables.0.source_cfg", {0: "x"}),
            "0.variables.0: source_cfg: measurements: Field required": broken(
                "variables.0.source_cfg", {"metric": "trial_summary"}),
            "0.conditions.0.0: Keys should be strings": broken("conditions.0", {"name": "failure_rate", 0: "x"}),
            "0.conditions.0.type: Extra inputs are not permitted": broken(
                "conditions.0", {"name": "failure_rate", "type": "threshold", "check": {}}),
            "0.conditions.0.cel: Field required": broken("conditions.0", {"name": "failure_rate"}),
            "0.conditions.0.cel: Input should be a valid string": broken("conditions.0.cel", ["<="]),
            "Syntax error": broken("conditions.0.cel", "trial.aggregate_failure_ratio <="),
            "undeclared reference to 'gp.goodput_ratio_pct'": broken("conditions.0.cel", "gp.goodput_ratio_pct >= 80"),
            "condition 'failure_rate': cel: must give a bool, gives DOUBLE": broken(
                "conditions.0.cel", "trial.aggregate_failure_ratio + 1.0"),
            "no matching overload": broken("conditions.0.cel", "trial.aggregate_failure_ratio + 1 > 1.0"),
            "0: condition names repeated": broken("conditions", evaluations()[0]["conditions"] * 2),
            "0: variable names repeated": broken("variables.1.name", "trial"),
            "evaluations: evaluation names repeated": evaluations() + evaluations(),
            # trial__x with value y and trial with value x__y would both be trial__x__y.
            "0.variables.0.name: String should match pattern '^[a-z][a-z0-9]*(_[a-z0-9]+)*$'": broken(
                "variables.0.name", "trial__x"),
            "0.variables.1.name: String should match pattern": broken("variables.1.name", "server_"),
        }
        for message, evs in cases.items():
            with self.assertRaisesRegex(config.ConfigError, re.escape(message), msg=message):
                config.load(evs)

    def test_one_bad_item_is_one_error(self):
        evs = evaluations()
        evs[0]["variables"] = evs[0]["variables"][:1]
        evs[0]["conditions"] = [{"name": "c", "cel": 1}]
        evs[0]["variables"][0]["source_cfg"] = None  # an error; leaving source_cfg out means {}
        with self.assertRaises(config.ConfigError) as e:
            config.load(evs)
        self.assertEqual(str(e.exception), "evaluations: 0.variables.0.source_cfg: Input should be a valid dictionary; "
                                           "0.conditions.0.cel: Input should be a valid string")

    def test_cel_names_cannot_name_a_variable(self):
        # CEL rejects a built-in type name as a variable, whether or not a condition reads it.
        for type_name in ("int", "double", "map", "dyn", "type"):
            evs = evaluations()
            evs[0]["variables"][0]["name"] = type_name
            with self.assertRaisesRegex(config.ConfigError, f"variable '{type_name}' declared multiple times"):
                config.load(evs)
        # A keyword parses as itself, so a condition that uses one as a variable doesn't compile.
        for keyword in ("true", "null", "in", "return"):
            evs = evaluations()
            evs[0]["variables"][0]["name"] = keyword
            evs[0]["conditions"][0]["cel"] = f"{keyword}.aggregate_failure_ratio <= 0.001"
            with self.assertRaisesRegex(config.ConfigError, "condition 'failure_rate': cel: INVALID_ARGUMENT"):
                config.load(evs)

    def test_conditions_need_not_read_every_variable(self):
        evs = evaluations()
        evs[0]["conditions"] = [{"name": "constant", "cel": "1 < 2"}]
        self.assertEqual(len(config.load(evs)[0].variables), 2)

    def test_source_cfg_is_json(self):
        with mock.patch.dict(SOURCES, {"fake": Fake}):
            cases = {
                "0.variables.0.source_cfg.values.dict.when: input was not a valid JSON value":
                    {"values": {"when": date(2026, 11, 11)}},
                "0.variables.0.source_cfg.values.dict.1.[key]: Input should be a valid string": {"values": {1: "x"}},
            }
            for message, cfg in cases.items():
                with self.assertRaisesRegex(config.ConfigError, re.escape(message), msg=message):
                    config.load([fake_evaluation("e", cfg)])

    def test_source_cfg_is_a_copy_of_the_input(self):
        with mock.patch.dict(SOURCES, {"fake": Fake}):
            evs = [fake_evaluation("e", {"values": {"answer": 42, "list": [{"a": 1}]}})]
            loaded = config.load(evs)
            evs[0]["variables"][0]["source_cfg"]["values"]["answer"] = 0
            evs[0]["variables"][0]["source_cfg"]["values"]["list"][0]["a"] = 2
            self.assertEqual(loaded[0].variables[0].source_cfg, {"values": {"answer": 42, "list": [{"a": 1}]}})

    def test_source_cfg_defaults_to_empty(self):
        with mock.patch.dict(SOURCES, {"fake": Fake}):
            ev = fake_evaluation("e", {})
            del ev["variables"][0]["source_cfg"]
            self.assertEqual(config.load([ev])[0].variables[0].source_cfg, {})
            with self.assertRaisesRegex(config.ConfigError, "0.variables.0: source_cfg: unknown keys"):
                config.load([fake_evaluation("e", {"what": 1})])


class RunResultsTest(unittest.TestCase):
    def query(self, run: BenchmarkRunContext, cfg: dict):
        RunResults.validate(cfg)
        source = RunResults()
        source.init(run)
        return source.query(cfg)

    def test_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            nh_row = {"timestamp": "2026-10-11T07:06:40Z", "tag": TAG, "test_name": "t150", "metric": "nighthawk",
                      "measurements": {"metric_nighthawk.builtin_attempted_rps": "1250.5", "p99.9": "4.2",
                                       "release": "2026_11"}}
            run = write_run(Path(tmp), [stats_row("grpc_ResumeActor", p90=420), stats_row("grpc_ResumeActor", p90=430),
                                        stats_row("server_summary", oom_events_node=None, mode="async"),
                                        stats_row("phases", locust_ResumeActor=812.5), nh_row])
            self.assertEqual(self.query(run, {"metric": "trial_summary", "measurements": ["ok", "allocatable_ram_gb",
                                                                                          "nope"]}),
                             {"ok": True, "allocatable_ram_gb": 400.0, "nope": None})
            self.assertEqual(self.query(run, {"metric": "grpc_ResumeActor", "measurements": ["p90"]}),
                             {"p90": 430.0})
            self.assertEqual(self.query(run, {"metric": "phases", "measurements": ["locust_ResumeActor"]}),
                             {"locust_ResumeActor": 812.5})
            self.assertEqual(self.query(run, {"metric": "nighthawk",
                                              "measurements": ["metric_nighthawk.builtin_attempted_rps", "p99.9",
                                                               "release"]}),
                             {"metric_nighthawk.builtin_attempted_rps": 1250.5, "p99.9": 4.2, "release": "2026_11"})
            self.assertEqual(self.query(run, {"metric": "server_summary", "measurements": ["oom_events_node", "mode"]}),
                             {"oom_events_node": None, "mode": "async"})

    def test_numbers_that_are_not_finite_are_null(self):
        self.assertEqual(coerce("1_000"), "1_000")
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp))
            with open(run.artifacts.dir / "stats.jsonl", "a", encoding="utf-8") as f:
                f.write('{"metric": "m", "measurements": {"nan": NaN, "inf": -Infinity, "big": "1e999", '
                        '"snan": "nan", "sinf": "-inf", "ok": "2"}}\n')
            self.assertEqual(self.query(run, {"metric": "m",
                                              "measurements": ["nan", "inf", "big", "snan", "sinf", "ok"]}),
                             {"nan": None, "inf": None, "big": None, "snan": None, "sinf": None, "ok": 2.0})

    def test_blank_lines_are_skipped_and_other_lines_must_be_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp))
            with open(run.artifacts.dir / "stats.jsonl", "a", encoding="utf-8") as f:
                f.write("\n   \n")
            self.assertEqual(self.query(run, {"metric": "trial_summary", "measurements": ["ok"]}), {"ok": True})
            for line, message in (("not json", "stats.jsonl line 4 isn't JSON"),
                                  ("[1, 2]", "stats.jsonl line 4 isn't an object with a metric"),
                                  ('{"measurements": {}}', "stats.jsonl line 4 isn't an object with a metric"),
                                  ('{"metric": ""}', "stats.jsonl line 4 isn't an object with a metric")):
                bad = write_run(Path(tmp) / "bad")
                with open(bad.artifacts.dir / "stats.jsonl", "a", encoding="utf-8") as f:
                    f.write(f"\n\n{line}\n")
                with self.assertRaisesRegex(SourceInvalid, re.escape(message), msg=line):
                    self.query(bad, {"metric": "trial_summary", "measurements": ["ok"]})

    def test_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp))
            with self.assertRaisesRegex(SourceInvalid, "no stats.jsonl row ladder_step_001"):
                self.query(run, {"metric": "ladder_step_001", "measurements": ["users"]})
            with self.assertRaisesRegex(SourceInvalid, "stats.jsonl absent"):
                self.query(NO_RUN, {"metric": "trial_summary", "measurements": ["ok"]})

    def test_validate(self):
        RunResults.validate({"metric": "m", "measurements": ["locust_ResumeActor", "p99.9", "a__b"]})
        cases = {
            "Input should be a valid dictionary": [],
            "0: Keys should be strings": {"metric": "m", "measurements": ["a"], 0: "x"},
            "run: Extra inputs are not permitted": {"metric": "m", "measurements": ["a"], "run": "baseline"},
            "metric: Field required": {"measurements": ["a"]},
            "measurements: Field required": {"metric": "m"},
            "metric: String should have at least 1 character": {"metric": "", "measurements": ["a"]},
            "measurements: List should have at least 1 item": {"metric": "m", "measurements": []},
            "measurements.0: String should have at least 1 character": {"metric": "m", "measurements": [""]},
            "measurements: listed twice": {"metric": "m", "measurements": ["a", "a"]},
        }
        for message, cfg in cases.items():
            with self.assertRaisesRegex(ValueError, re.escape(message), msg=message):
                RunResults.validate(cfg)


class EvaluateTest(unittest.TestCase):
    def test_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), [stats_row("server_summary", working_set_node_max_gb=350)])
            report = judge(run, evaluations())
            pass_run = write_run(Path(tmp) / "pass", [stats_row("server_summary", working_set_node_max_gb=300)])
            pass_report = judge(pass_run, evaluations())
        (ev,) = report.evaluations
        self.assertEqual((report.run, report.evaluated_at), (
            {"test_name": "t150", "tag": TAG, "run_ts": "1791700000", "uri": "gs://b/runs/t150"}, "2026-11-02T10:00:00Z"))
        self.assertEqual((ev.status, ev.passed, ev.reason), ("fail", False, "failed: worker_memory"))
        worker_memory = "server.working_set_node_max_gb < 0.85 * trial.allocatable_ram_gb"
        self.assertEqual(ev.conditions, (
            ConditionResult(name="failure_rate", cel="trial.aggregate_failure_ratio <= 0.001", status="pass",
                            passed=True, reason=""),
            ConditionResult(name="worker_memory", cel=worker_memory, status="fail", passed=False,
                            reason=f"worker_memory: {worker_memory}"),
        ))
        self.assertEqual(ev.variables[0].values, {"aggregate_failure_ratio": 0.0005, "allocatable_ram_gb": 400.0})
        self.assertEqual(ev.config, config.to_json(config.load(evaluations()))[0])
        self.assertEqual(report.summary, Summary(status="fail", passed=False, failed=("envelope_health",), invalid=()))
        self.assertEqual((pass_report.evaluations[0].reason, pass_report.summary),
                         ("", Summary(status="pass", passed=True, failed=(), invalid=())))

    def test_cel_operators_and_int_float_arithmetic(self):
        with mock.patch.dict(SOURCES, {"fake": Fake}):
            evs = [{
                "name": "math",
                "variables": [{"name": "counts", "source": "fake",
                               "source_cfg": {"values": {"good": 9, "records": 10, "pct": 90.04, "ok": True}}}],
                "conditions": [
                    {"name": "ratio", "cel": "counts.good / counts.records >= 0.9"},
                    {"name": "scaled", "cel": "2.0 * counts.good == 18.0 && counts.records / 2.0 == 5.0"},
                    {"name": "scaled_float", "cel": "1.5 * counts.records <= 15.0"},
                    {"name": "rounded_pct", "cel": "round(counts.pct, 1) >= 90.0"},
                    {"name": "rounded_int", "cel": "round(counts.good, 0) == 9.0"},
                    {"name": "int_literal_compare", "cel": "counts.records <= 1500 && counts.pct > 90"},
                    {"name": "logic", "cel": "counts.ok && !(counts.good < 1 || counts.records == 0)"},
                ],
            }]
            report = judge(NO_RUN, evs)
        (ev,) = report.evaluations
        self.assertEqual(ev.status, "pass")
        # Source int and bool values stay untouched in the results.
        self.assertEqual(ev.variables[0].values, {"good": 9, "records": 10, "pct": 90.04, "ok": True})
        self.assertTrue(all(isinstance(ev.variables[0].values[k], int) for k in ("good", "records")))
        self.assertIs(ev.variables[0].values["ok"], True)

    def test_int_literal_with_a_value_of_unknown_type_makes_the_condition_invalid(self):
        with mock.patch.dict(SOURCES, {"fake": Fake}):
            report = judge(NO_RUN, [fake_evaluation("e", {"answer": 1}, "f.answer * 100 >= 99")])
        (c,) = report.evaluations[0].conditions
        self.assertEqual(c.status, "invalid")
        self.assertIn("No matching overloads found : _*_(double, int64)", c.reason)

    def test_int_literal_mixed_with_a_double_is_a_config_error(self):
        # Values are doubles in CEL, which has no int-double overloads for arithmetic, nor for comparing
        # an expression known to be a double: write 2.0. A value compared directly is dyn, so f.answer <= 1500 works.
        with mock.patch.dict(SOURCES, {"fake": Fake}):
            for expression in ("2 * f.answer > 1.0", "round(f.answer, 1) >= 90", "f.answer / 2.0 == 1"):
                with self.assertRaisesRegex(config.ConfigError, "no matching overload", msg=expression):
                    config.load([fake_evaluation("e", {}, cel_expr=expression)])

    def test_external_measurement_keys(self):
        evs = [{"name": "phases", "variables": [
                    {"name": "phases", "source": "run_results",
                     "source_cfg": {"metric": "phase_breakdown_summary", "measurements": ["locust_ResumeActor"]}},
                    {"name": "nh", "source": "run_results",
                     "source_cfg": {"metric": "nighthawk",
                                    "measurements": ["metric_nighthawk.builtin_attempted_rps", "p99.9", "release"]}}],
                "conditions": [
                    {"name": "resume", "cel": "phases.locust_ResumeActor <= 1000"},
                    {"name": "rps", "cel": 'nh["metric_nighthawk.builtin_attempted_rps"] >= 500'},
                    {"name": "p999", "cel": 'nh["p99.9"] <= 5 && nh.release == "2026_11"'}]}]
        phase_row = {"timestamp": "2026-10-11T07:06:40Z", "tag": TAG, "test_name": "t150",
                     "metric": "phase_breakdown_summary", "measurements": {"locust_ResumeActor": "812.5"}}
        nh_row = {"timestamp": "2026-10-11T07:06:40Z", "tag": TAG, "test_name": "t150", "metric": "nighthawk",
                  "measurements": {"metric_nighthawk.builtin_attempted_rps": "600", "p99.9": "4.2",
                                   "release": "2026_11"}}
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), [phase_row, nh_row])
            report = judge(run, evs)
        self.assertEqual(report.summary.status, "pass")
        self.assertEqual(report.evaluations[0].variables[1].values,
                         {"metric_nighthawk.builtin_attempted_rps": 600.0, "p99.9": 4.2, "release": "2026_11"})

    def test_cross_type_equality_and_eval_errors(self):
        evs = [{"name": "types",
                "variables": [{"name": "build", "source": "run_results",
                               "source_cfg": {"metric": "build", "measurements": ["version", "tag"]}}],
                "conditions": [
                    {"name": "numeric", "cel": "build.version == 1.1"},
                    {"name": "cross_eq", "cel": 'build.version == "1.10"'},
                    {"name": "cross_neq", "cel": 'build.version != "1.10"'},
                    {"name": "words", "cel": 'build.tag == "rel-1.10"'},
                    {"name": "overflow", "cel": "int(build.version * 1e300) > int(0)"},
                ]}]
        with tempfile.TemporaryDirectory() as tmp:
            report = judge(write_run(Path(tmp), [stats_row("build", version="1.10", tag="rel-1.10")]), evs)
        by_name = {c.name: c for c in report.evaluations[0].conditions}
        self.assertEqual([by_name[k].status for k in ("numeric", "cross_eq", "cross_neq", "words", "overflow")],
                         ["pass", "fail", "pass", "pass", "invalid"])
        self.assertIn("overflow: OUT_OF_RANGE:", by_name["overflow"].reason)

    def test_null_and_missing_keys_with_has(self):
        """Null or missing measurements omit the map key: direct reads become invalid (even with !=),
        while has() tests presence cleanly."""
        evs = [{"name": "health", "variables": [{"name": "server", "source": "run_results", "source_cfg": {
                    "metric": "server_summary", "measurements": ["oom_events_node", "oom_events_worker_restarts"]}}],
                "conditions": [
                    {"name": "node_ooms_eq", "cel": "server.oom_events_node == 0"},
                    {"name": "node_ooms_neq", "cel": "server.oom_events_node != 0"},
                    {"name": "missing_neq", "cel": "server.unlisted != 0"},
                    {"name": "has_null", "cel": "has(server.oom_events_node)"},
                    {"name": "not_has_null", "cel": "!has(server.oom_events_node)"},
                    {"name": "has_restarts", "cel": "has(server.oom_events_worker_restarts)"},
                    {"name": "restarts", "cel": "server.oom_events_worker_restarts == 0"}]}]
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), [stats_row("server_summary", oom_events_node=None, oom_events_worker_restarts=0)])
            report = judge(run, evs)
        conds = {c.name: c for c in report.evaluations[0].conditions}
        self.assertEqual(conds["node_ooms_eq"].status, "invalid")
        self.assertIn("node_ooms_eq: NOT_FOUND:", conds["node_ooms_eq"].reason)
        self.assertEqual(conds["node_ooms_neq"].status, "invalid")
        self.assertEqual(conds["missing_neq"].status, "invalid")
        self.assertEqual((conds["has_null"].status, conds["not_has_null"].status), ("fail", "pass"))
        self.assertEqual((conds["has_restarts"].status, conds["restarts"].status), ("pass", "pass"))
        self.assertIsNone(report.evaluations[0].variables[0].values["oom_events_node"])

    def test_a_condition_reading_an_invalid_variable_is_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = judge(write_run(Path(tmp)), evaluations())
        (ev,) = report.evaluations
        self.assertEqual((ev.variables[1].status, ev.variables[1].invalid_reason),
                         ("invalid", "no stats.jsonl row server_summary"))
        self.assertEqual([c.status for c in ev.conditions], ["pass", "invalid"])
        self.assertEqual(ev.conditions[1].reason,
                         'worker_memory: UNKNOWN: No value with name "server" found in Activation')
        self.assertEqual((ev.status, ev.passed, ev.reason), (
            "invalid", False, "invalid: worker_memory; variable server is invalid: no stats.jsonl row server_summary"))
        self.assertEqual(report.summary,
                         Summary(status="invalid", passed=False, failed=(), invalid=("envelope_health",)))

    def test_an_invalid_variable_no_condition_reads_makes_the_evaluation_invalid(self):
        evs = evaluations()
        evs[0]["conditions"] = evs[0]["conditions"][:1]  # reads trial only; server has no row
        with tempfile.TemporaryDirectory() as tmp:
            report = judge(write_run(Path(tmp)), evs)
        (ev,) = report.evaluations
        self.assertEqual([c.status for c in ev.conditions], ["pass"])
        self.assertEqual((ev.status, ev.passed, ev.reason),
                         ("invalid", False, "variable server is invalid: no stats.jsonl row server_summary"))

    def test_a_failed_condition_beats_an_invalid_variable(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = judge(write_run(Path(tmp)), evaluations(limit=0.0001))
        (ev,) = report.evaluations
        self.assertEqual(ev.variables[1].status, "invalid")
        self.assertEqual([c.status for c in ev.conditions], ["fail", "invalid"])
        self.assertEqual((ev.status, ev.reason), ("fail", "failed: failure_rate"))

    def test_a_condition_may_be_a_bare_value(self):
        with mock.patch.dict(SOURCES, {"fake": Fake}):
            def status(values, cel_expr):
                return judge(NO_RUN, [fake_evaluation("e", {"values": values}, cel_expr)]).evaluations[0].conditions[0]
            self.assertEqual(status({"ok": True}, "f.ok").status, "pass")
            self.assertEqual(status({"ok": False}, "f.ok").status, "fail")
            c = status({"name": "n"}, "f.name")
            self.assertEqual((c.status, c.reason), ("invalid", "c: gives a string, not a bool"))

    def test_evaluated_at_is_utc(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), [stats_row("server_summary", working_set_node_max_gb=300)])
            pacific = timezone(timedelta(hours=-8))
            report = evaluator.Evaluator(config.load(evaluations()), run).evaluate(
                now=datetime(2026, 11, 2, 2, 0, 0, tzinfo=pacific))
        self.assertEqual(report.evaluated_at, "2026-11-02T10:00:00Z")

    def test_evaluate_condition_turns_a_runtime_error_into_a_value_error(self):
        # A stub, since the real trigger (a string with a lone surrogate) is undefined behavior in the macOS build.
        expr = mock.Mock()
        expr.eval.side_effect = RuntimeError("basic_string: construction from null is not valid")
        with self.assertRaisesRegex(ValueError, "^basic_string: construction from null is not valid$"):
            cel_env.evaluate_condition(expr, {})


class HooksTest(unittest.TestCase):
    """The evaluator's side of the source hooks: validate at load, init once, query per distinct cfg."""

    def setUp(self):
        Fake.calls = []
        patcher = mock.patch.dict(SOURCES, {"fake": Fake})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_init_once_then_one_query_per_distinct_cfg(self):
        run = NO_RUN
        evs = config.load([fake_evaluation("a", {"answer": 42}), fake_evaluation("b", {"answer": 42}),
                           fake_evaluation("c", {"answer": 7}, cel_expr="f.answer == 7")])
        e = evaluator.Evaluator(evs, run)
        self.assertEqual(Fake.calls, [("init", run)])
        report = e.evaluate(now=NOW)
        self.assertEqual(Fake.calls[1:], [("query", {"answer": 42}), ("query", {"answer": 7})])
        self.assertEqual(report.summary.status, "pass")

    def test_source_invalid_keeps_its_values(self):
        for reason in ("no baseline", ""):  # the exception makes the variable invalid, whatever its reason says
            report = judge(NO_RUN, [fake_evaluation("a", {"answer": 42, "invalid": reason})])
            (var,) = report.evaluations[0].variables
            self.assertEqual((var.status, var.invalid_reason, var.values), ("invalid", reason, {"answer": 42}))
            self.assertEqual(report.evaluations[0].conditions[0].reason,
                             'c: UNKNOWN: No value with name "f" found in Activation')
            self.assertEqual(report.evaluations[0].reason, f"invalid: c; variable f is invalid: {reason}")
            self.assertEqual(report.summary.status, "invalid")

    def test_non_finite_floats_from_source_are_normalized_at_boundary(self):
        for val in (float("nan"), float("inf"), float("-inf")):
            with mock.patch.object(Fake, "query", return_value={"answer": val}):
                report = judge(NO_RUN, [fake_evaluation("a", {"answer": 1})])
            self.assertEqual(report.evaluations[0].variables[0].values, {"answer": None})
            self.assertEqual(report.evaluations[0].conditions[0].status, "invalid")
        with mock.patch.object(Fake, "query", side_effect=SourceInvalid("bad", {"answer": float("nan")})):
            report = judge(NO_RUN, [fake_evaluation("a", {"answer": 1})])
        self.assertEqual(report.evaluations[0].variables[0].values, {"answer": None})

    def test_values_must_be_scalars_with_string_names(self):
        scalar = "a value must be a number, bool, string or null"
        cases = {
            f"value 'nested' is a dict: {scalar}": {"answer": 42, "nested": {"p50": 1.0}},
            f"value 'nested' is a list: {scalar}": {"answer": 42, "nested": [1.0]},
            f"value 'nested' is a tuple: {scalar}": {"answer": 42, "nested": (1.0,)},
            "value name '' must be a non-empty string": {"answer": 42, "": 1.0},
            "value name 1 must be a non-empty string": {"answer": 42, 1: 1.0},
        }
        for reason, values in cases.items():
            for query in ({"return_value": values}, {"side_effect": SourceInvalid("bad", values)}):
                with mock.patch.object(Fake, "query", **query):
                    report = judge(NO_RUN, [fake_evaluation("a", {"answer": 42})])
                (var,) = report.evaluations[0].variables
                self.assertEqual((var.status, var.invalid_reason, var.values), ("invalid", reason, {}))
                self.assertEqual(report.evaluations[0].conditions[0].status, "invalid")

    def test_errors_become_invalid(self):
        report = judge(NO_RUN, [fake_evaluation("a", {"boom": True})])
        self.assertEqual(report.evaluations[0].variables[0].invalid_reason, "error: RuntimeError: boom")
        with mock.patch.object(Fake, "init", side_effect=OSError("no disk")):
            report = judge(NO_RUN, [fake_evaluation("a", {"answer": 42})])
        self.assertEqual(report.evaluations[0].variables[0].invalid_reason, "init failed: OSError: no disk")


if __name__ == "__main__":
    unittest.main()
