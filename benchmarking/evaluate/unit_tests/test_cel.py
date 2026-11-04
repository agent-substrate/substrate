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

"""Unit tests for the cel source with json_lines records, its values and its config.

The tests for sweperf_tasks records are in test_sweperf_tasks.py.
"""

import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evaluate import config  # noqa: E402
from evaluate.sources import cel  # noqa: E402
from evaluate.sources.base import BenchmarkRunContext, SourceInvalid  # noqa: E402
from evaluate.sources.cel import rules  # noqa: E402

FAILED = {"name": "failed_requests", "cel": "line.measurements.failure_count == 0.0"}
GRPC = 'line.metric.startsWith("grpc_")'
RATIO = {"good_ratio_pct": "round(100.0 * good / records, 2)"}


def stats_row(metric: str, **measurements) -> dict:
    return {"timestamp": "2026-10-24T11:16:49Z", "tag": "abc", "test_name": "t", "metric": metric,
            "measurements": measurements}


def query(lines: list, cfg: dict) -> tuple[dict, Optional[str]]:
    """One json_lines query of a run whose stats.jsonl holds lines (dicts are dumped as JSON): the values, and why
    the run is invalid (None when valid)."""
    cfg = {"kind": "json_lines", "file": "stats.jsonl", **cfg}
    cel.Cel.validate(cfg)
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "stats.jsonl"
        path.write_text("".join((json.dumps(x) if isinstance(x, dict) else x) + "\n" for x in lines),
                        encoding="utf-8")
        source = cel.Cel()
        source.init(BenchmarkRunContext(files={"stats.jsonl": path, "logs.txt": Path(tmp) / "logs.txt"}))
        try:
            return dict(source.query(cfg)), None
        except SourceInvalid as e:
            return e.values, e.reason


class JsonLinesTest(unittest.TestCase):
    LINES = [stats_row("trial_summary", aggregate_failure_ratio="0.0"),
             stats_row("grpc_CreateActor", failure_count="0", request_count="331"),
             stats_row("grpc_ResumeActor", failure_count="2", request_count="900"),
             "",
             stats_row("grpc_SuspendActorAsync", failure_count="0", request_count="880")]

    def test_select_and_rules(self):
        v, invalid = query(self.LINES, {"select": GRPC, "rules": [FAILED], "values": RATIO})
        self.assertIsNone(invalid)
        self.assertEqual({k: v[k] for k in ("records", "good", "bad", "good_ratio_pct", "bad_failed_requests")},
                         {"records": 3, "good": 2, "bad": 1, "good_ratio_pct": 66.67, "bad_failed_requests": 1})
        everything, _ = query(self.LINES, {"rules": []})  # no select: every line, blank ones skipped
        self.assertEqual((everything["records"], everything["good"]), (4, 4))

    def test_values_are_coerced_as_in_run_results(self):
        # A numeric string compares as a double, and so does a JSON int.
        rules = [{"name": "too_many", "cel": "line.measurements.request_count < 500.0"},
                 {"name": "few_users", "cel": "line.users >= 2.0"}]
        v, invalid = query([{**stats_row("grpc_CreateActor", request_count="331"), "users": 3},
                            {**stats_row("grpc_ResumeActor", request_count="900"), "users": 1}], {"rules": rules})
        self.assertIsNone(invalid)
        self.assertEqual((v["bad_too_many"], v["bad_few_users"]), (1, 1))
        flags, _ = query([stats_row("m", ok="true"), stats_row("m", ok="false")],
                         {"rules": [{"name": "not_ok", "cel": "line.measurements.ok == true"}]})
        self.assertEqual(flags["good"], 1)

    def test_errors_make_the_run_invalid(self):
        lacking = {"name": "failed_requests", "cel": "line.measurements.failures == 0.0"}
        v, invalid = query(self.LINES, {"select": GRPC, "rules": [lacking]})
        self.assertEqual((v, invalid), ({}, "rule 'failed_requests' on stats.jsonl:2: NOT_FOUND: "
                                            "Key not found in map : \"failures\""))
        guarded = {"name": "failed_requests", "cel": "!has(line.measurements.failures)"}
        self.assertEqual(query(self.LINES, {"select": GRPC, "rules": [guarded]})[0]["good"], 3)
        self.assertEqual(query(self.LINES, {"select": "line.metrics == \"x\"", "rules": []})[1],
                         "select on stats.jsonl:1: NOT_FOUND: Key not found in map : \"metrics\"")
        self.assertEqual(query(["{not json"], {"rules": []})[1],
                         "stats.jsonl:1: not JSON: Expecting property name enclosed in double quotes: "
                         "line 1 column 2 (char 1)")
        self.assertEqual(query(["[1, 2]"], {"rules": []})[1], "stats.jsonl:1: not a JSON object")
        self.assertEqual(query(self.LINES, {"select": 'line.metric == "none"', "rules": [], "values": RATIO}),
                         ({"records": 0, "good": 0, "bad": 0, "good_ratio_pct": None},
                          "no line of stats.jsonl selected"))
        self.assertEqual(query(self.LINES, {"file": "evaluation.json", "rules": []}), ({}, "evaluation.json absent"))


class ValuesTest(unittest.TestCase):
    LINES = [stats_row("grpc_CreateActor", failure_count="0"), stats_row("grpc_ResumeActor", failure_count="2")]

    def test_values(self):
        values = {**RATIO, "ratio_of_bad": "bad / good", "all_good": "bad == 0.0",
                  "good_per_bad": "good / bad", "nothing": "bad / (good - 1.0)"}
        v, invalid = query(self.LINES, {"rules": [FAILED], "values": values})
        self.assertIsNone(invalid)
        self.assertEqual({k: v[k] for k in values}, {"good_ratio_pct": 50.0, "ratio_of_bad": 1.0, "all_good": False,
                                                     "good_per_bad": 1.0, "nothing": None})  # 1 / 0: null
        clean, _ = query(self.LINES[:1], {"rules": [FAILED], "values": values})
        self.assertEqual((clean["good_per_bad"], clean["nothing"]), (None, None))  # x / 0 and 0 / 0

    def test_a_value_that_fails_to_evaluate_makes_the_run_invalid(self):
        self.assertEqual(query(self.LINES, {"rules": [], "values": {"too_big": "double(int(good * 1e300))"}}),
                         ({}, "value 'too_big': OUT_OF_RANGE: double out of int64 range"))


class ValueNamesTest(unittest.TestCase):
    def test_a_condition_reads_only_values_the_source_gives(self):
        cfg = {"kind": "json_lines", "file": "stats.jsonl", "select": GRPC, "rules": [FAILED], "values": RATIO}
        self.assertEqual(set(query(JsonLinesTest.LINES, cfg)[0]), cel.Cel.value_names(cfg))

        def evaluations(ref: str) -> list:
            check = {"value": ref, "op": ">=", "limit": 0}
            return [{"name": "e", "variables": [{"name": "grpc", "source": "cel", "source_cfg": cfg}],
                     "conditions": [{"name": "c", "type": "threshold", "check": check}]}]
        config.load(evaluations("grpc.bad_failed_requests"))
        with self.assertRaisesRegex(config.ConfigError, "'grpc.good_ratio': grpc gives no value 'good_ratio'"):
            config.load(evaluations("grpc.good_ratio"))


class WholeNumberTest(unittest.TestCase):
    def test_integer_literals_become_doubles(self):
        for written, compiled in (("task.resume_max_ms <= 1500", "task.resume_max_ms <= 1500.0"),
                                  ("-1 + 2 * 3", "-1.0 + 2.0 * 3.0"), ("p99 <= 1 // 2", "p99 <= 1.0 // 2")):
            self.assertEqual(rules.ints_as_doubles(written), compiled)

    def test_other_tokens_stay(self):
        for kept in ('"1500" + \'1\'', '"""a "1" \'2\' b"""', "\'\'\'3\'\'\'", 'r"\\d1"', 'b"1"', 'rb"1"',
                     'BR\'1\'', '"\\"1"', 'r"""\\"1"""', "x_1 + p99", "0x1F + 1u + 1e3 + 1.5 + .5 + 2E-3",
                     "a.`b1`", "// 1"):
            self.assertEqual(rules.ints_as_doubles(kept), kept)

    def test_where_doubles_are_all_there_is(self):
        good, _ = query(ValuesTest.LINES, {"rules": [FAILED], "values": {"pct": "round(100 * good / records, 2)"}})
        self.assertEqual(good["pct"], 50.0)
        self.assertEqual(query(ValuesTest.LINES, {"rules": [], "values": {"x": "round(good, 2.5)"}}),
                         ({}, "value 'x': OUT_OF_RANGE: round: digits 2.5 isn't a whole number"))
        # json_lines keeps CEL's own types: line is dyn, and size() gives an int.
        sized, _ = query(ValuesTest.LINES, {"rules": [{"name": "named", "cel": "size(line.metric) > 0"}]})
        self.assertEqual(sized["good"], 2)

    def test_errors_quote_the_rule_as_compiled(self):
        with self.assertRaisesRegex(ValueError, re.escape("undeclared reference to 'task.nope' (in container '')\n"
                                                          " | 1.0 + 2.0 <= task.nope")):
            cel.Cel.validate({"kind": "sweperf_tasks", "rules": [{"name": "x", "cel": "1 + 2 <= task.nope"}]})


class ConfigTest(unittest.TestCase):
    def test_valid(self):
        for cfg in ({"kind": "json_lines", "file": "stats.jsonl", "rules": []},
                    {"kind": "json_lines", "file": "stats.jsonl", "select": GRPC, "rules": [FAILED]},
                    {"kind": "sweperf_tasks", "rules": []}):
            cel.Cel.validate(cfg)

    def test_invalid(self):
        json_lines = {"kind": "json_lines", "file": "stats.jsonl", "rules": []}
        cases = {
            "kind: None is not one of ['json_lines', 'sweperf_tasks']": {"rules": []},
            "kind: 'tasks' is not one of": {"kind": "tasks", "rules": []},
            "kind: ['sweperf_tasks'] is not one of": {"kind": ["sweperf_tasks"], "rules": []},
            "unknown keys ['window']": {**json_lines, "window": "steady"},
            "unknown keys ['window', 0]": {**json_lines, "window": "steady", 0: "x"},  # YAML {0: x}
            "rules[0]: unknown keys ['type', 1]": {**json_lines, "rules": [{**FAILED, "type": "t", 1: 2}]},
            "unknown keys ['file']": {"kind": "sweperf_tasks", "file": "stats.jsonl", "rules": []},
            "needs rules": {"kind": "json_lines", "file": "stats.jsonl"},
            "needs file": {"kind": "json_lines", "rules": []},
            "select: must give a bool, gives DYN": {**json_lines, "select": "line.metric"},
            "select: needs a CEL expression": {**json_lines, "select": 1},
            "undeclared reference to 'task.cel_ms'": {**json_lines,
                                                      "rules": [{"name": "x", "cel": "task.cel_ms <= 1.0"}]},
            "rule 'x': cel: must give a bool, gives DYN": {**json_lines, "rules": [{"name": "x", "cel": "line.x"}]},
            "values: needs a mapping": {**json_lines, "values": ["good"]},
            "values: 'Good' must match": {**json_lines, "values": {"Good": "good"}},
            "values: 'records' is a value the source gives": {**json_lines, "values": {"records": "good"}},
            "values: 'bad_x' is a value the source gives": {**json_lines, "values": {"bad_x": "bad"}},
            "values: 'window_s' is a value the source gives": {"kind": "sweperf_tasks", "rules": [],
                                                               "values": {"window_s": "good"}},
            "values: 'cel_ms_ratio_p50' is a value the source gives": {
                "kind": "sweperf_tasks", "rules": [{"name": "x", "cel": "task.cel_ms <= 1.5 * baseline.cel_ms"}],
                "values": {"cel_ms_ratio_p50": "good"}},
            "'_/_' applied to '(double, int)'": {**json_lines, "values": {"x": "good / int(2)"}},
            "value 'x': must give a double or a bool, gives STRING": {**json_lines, "values": {"x": "string(good)"}},
            "undeclared reference to 'good_ratio_pct'": {**json_lines, "values": {**RATIO, "x": "good_ratio_pct"}},
            "undeclared reference to 'vcpus'": {**json_lines, "values": {"x": "good / vcpus"}},
        }
        for message, cfg in cases.items():
            with self.assertRaisesRegex(ValueError, re.escape(message), msg=message):
                cel.Cel.validate(cfg)


if __name__ == "__main__":
    unittest.main()
