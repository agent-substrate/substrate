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

"""Unit tests for the cel source, its CEL helpers, and the json / json_lines readers."""

import dataclasses
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Optional
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cel_expr_python import cel as cel_lib  # noqa: E402
from cel_expr_python.ext import ext_math  # noqa: E402

from evaluate import cel_env  # noqa: E402
from evaluate.sources import cel  # noqa: E402
from evaluate.sources.base import BenchmarkRunContext, LocalArtifacts, RunIdentity, SourceInvalid  # noqa: E402
from evaluate.sources.cel import json_file, json_lines, reader  # noqa: E402

STATS = {"stats": {"reader": "json_lines", "file": "stats.jsonl"}}


def stats_row(metric: str, **measurements) -> dict:
    return {"timestamp": "2026-10-24T11:16:49Z", "tag": "abc", "test_name": "t", "metric": metric,
            "measurements": measurements}


def local_run(d: Path, uri: Optional[str] = None) -> BenchmarkRunContext:
    return BenchmarkRunContext(RunIdentity(uri or str(d)), LocalArtifacts(d))


NO_RUN = local_run(Path(tempfile.gettempdir()) / "evaluate-unit-tests-no-run")  # holds no files


def write_stats(d: Path, lines: list, *, summary: Optional[object] = None) -> BenchmarkRunContext:
    """A run whose stats.jsonl holds lines (dicts are dumped as JSON), plus optional summary.json."""
    d.mkdir(parents=True, exist_ok=True)
    path = d / "stats.jsonl"
    path.write_text("".join((json.dumps(x) if isinstance(x, dict) else x) + "\n" for x in lines), encoding="utf-8")
    if summary is not None:
        spath = d / "summary.json"
        spath.write_text(json.dumps(summary) if isinstance(summary, (dict, list)) else str(summary), encoding="utf-8")
    return local_run(d)


def query(run: BenchmarkRunContext, cfg: dict) -> tuple[dict, Optional[str]]:
    """One query of run through the hooks: the values, and why the variable is invalid (None when valid)."""
    cel.Cel.validate(cfg)
    source = cel.Cel()
    source.init(run)
    try:
        return dict(source.query(cfg)), None
    except SourceInvalid as e:
        return e.values, e.reason


def query_lines(lines: list, cfg: dict) -> tuple[dict, Optional[str]]:
    with tempfile.TemporaryDirectory() as tmp:
        return query(write_stats(Path(tmp), lines), {"inputs": STATS, **cfg})


def evaluate(expression: str, **data):
    """expression over data, with the cel source's functions and math extension."""
    env = cel_env.Env(
        {k: cel_lib.Type.DYN for k in data},
        (cel_env.ROUND, cel_env.PERCENTILE),
        extensions=(ext_math.ExtMath(),),
    )
    result = env.compile(expression, "test", None).eval(data=data)
    if result.type() == cel_lib.Type.ERROR:
        raise ValueError(result.value())
    return result.plain_value()


class FunctionsTest(unittest.TestCase):
    def test_percentile(self):
        self.assertEqual(evaluate("percentile(xs, 50.0)", xs=[4.0, 1.0, 3.0, 2.0]), 2.5)
        self.assertEqual(evaluate("percentile(xs, 90.0)", xs=[1.0, 2.0, 3.0, 4.0, 5.0]), 4.6)
        self.assertEqual(evaluate("percentile(xs, 0.0)", xs=[3.0, 1.0]), 1.0)
        self.assertEqual(evaluate("percentile(xs, 100.0)", xs=[3.0, 1.0]), 3.0)
        self.assertEqual(evaluate("percentile(xs, 99.0)", xs=[7.0]), 7.0)
        with self.assertRaisesRegex(ValueError, "empty list"):
            evaluate("percentile(xs, 50.0)", xs=[])
        with self.assertRaisesRegex(ValueError, "non-finite"):
            evaluate("percentile(xs, 50.0)", xs=[1.0, float("nan")])
        with self.assertRaisesRegex(ValueError, "between 0 and 100"):
            evaluate("percentile(xs, 101.0)", xs=[1.0])

    def test_ext_math(self):
        self.assertEqual(evaluate("[math.least(xs), math.greatest(xs)]", xs=[2.0, -1.0, 5.0]), [-1.0, 5.0])
        self.assertEqual(evaluate("math.least(2.0, -1.0)"), -1.0)
        with self.assertRaises(ValueError):
            evaluate("math.least(xs)", xs=[])

    def test_a_list_of_another_type_is_an_error(self):
        with self.assertRaises(ValueError):
            evaluate("percentile(xs, 50.0)", xs=["a"])


class ConfigTest(unittest.TestCase):
    VALID = {"inputs": STATS, "locals": [{"name": "rows", "cel": "stats.lines"}],
             "checks": {"some": "size(rows) > 0"}, "values": {"n": "double(size(rows))"}}

    def test_valid(self):
        cel.Cel.validate(self.VALID)
        cel.Cel.validate({"inputs": STATS, "values": {"n": "double(size(stats.lines))"}})
        cel.Cel.validate({"inputs": STATS, "values": {"n": "size(stats.lines) > 0 ? dyn(1.0) : null"}})

    def test_invalid(self):
        cases = {
            "rules: Extra inputs are not permitted": {**self.VALID, "rules": []},
            "inputs: Field required": {"values": {"n": "1.0"}},
            "inputs: Dictionary should have at least 1 item": {**self.VALID, "inputs": {}},
            "inputs.Stats.\\[key\\]: String should match pattern": {**self.VALID, "inputs": {"Stats": STATS["stats"]}},
            "values: Dictionary should have at least 1 item": {**self.VALID, "values": {}},
            "locals: Input should be a valid list": {**self.VALID, "locals": {"rows": "stats.lines"}},
            "locals.0.name: String should match pattern": {**self.VALID, "locals": [{"name": "Rows", "cel": "stats.lines"}]},
            "locals: names repeated: \\['rows'\\]": {**self.VALID, "locals": [
                {"name": "rows", "cel": "stats.lines"}, {"name": "rows", "cel": "stats.lines"}]},
            "locals.0.extra: Extra inputs are not permitted": {**self.VALID, "locals": [
                {"name": "rows", "cel": "stats.lines", "extra": 1}]},
            "reader 'csv' is not one of": {**self.VALID, "inputs": {"stats": {"reader": "csv"}}},
            "inputs.stats: file: Field required": {**self.VALID, "inputs": {"stats": {"reader": "json_lines"}}},
            "inputs.stats: unknown: Extra inputs are not permitted": {
                **self.VALID, "inputs": {"stats": {**STATS["stats"], "unknown": "true"}}},
            "inputs.stats: prefix: Input should be a valid string": {
                **self.VALID, "inputs": {"stats": {**STATS["stats"], "prefix": 1}}},
            "inputs.stats: unique_by: String should have at least 1 character": {
                **self.VALID, "inputs": {"stats": {**STATS["stats"], "unique_by": ""}}},
            "inputs.stats: select: .*Syntax error": {
                **self.VALID, "inputs": {"stats": {**STATS["stats"], "select": "line.a +"}}},
            "inputs.stats: until: must give a bool": {
                **self.VALID, "inputs": {"stats": {**STATS["stats"], "until": "size(line)"}}},
            "inputs.stats.test: String should have at least 1 character": {
                **self.VALID, "inputs": {"stats": {**STATS["stats"], "test": ""}}},
            "locals: \\['stats'\\] are also inputs": {**self.VALID, "locals": [
                {"name": "stats", "cel": "1.0"}, {"name": "rows", "cel": "stats"}]},
            "local 'rows': .*Syntax error": {**self.VALID, "locals": [
                {"name": "in", "cel": "1.0"}, {"name": "rows", "cel": "in"}]},
            "variable 'int' declared multiple times": {**self.VALID, "locals": [
                {"name": "int", "cel": "1.0"}, {"name": "rows", "cel": "stats.lines"}]},
            "check 'some'": {**self.VALID, "checks": {"some": "size(rows) +"}},
            "check 'some': must give a bool": {**self.VALID, "checks": {"some": "size(rows)"}},
            "value 'n': must give a double or a bool or a string or a null": {
                **self.VALID, "values": {"n": "size(rows)"}},
            "undeclared reference to 'nope'": {**self.VALID, "values": {"n": "nope"}},
            "undeclared reference to 'later'": {**self.VALID, "locals": [
                {"name": "rows", "cel": "later"}, {"name": "later", "cel": "stats.lines"}]},
            "undeclared reference to 'rows'": {**self.VALID, "locals": [
                {"name": "rows", "cel": "rows"}]},
            "value 'n': .*no matching overload": {**self.VALID, "values": {"n": "double(size(rows)) / 2"}},
        }
        for want, cfg in cases.items():
            with self.subTest(want=want), self.assertRaisesRegex(ValueError, want):
                cel.Cel.validate(cfg)

    def test_inputs_and_locals_need_not_be_read(self):
        """Entries can share inputs and locals (a YAML anchor) and use some of them."""
        cel.Cel.validate({**self.VALID, "inputs": {**STATS, "other": STATS["stats"]}})
        cel.Cel.validate({**self.VALID, "checks": {}, "values": {"n": "1.0"}})

    def test_a_failed_local_fails_only_what_reads_it(self):
        locals_ = [{"name": "rows", "cel": "stats.lines"},
                   {"name": "sixth", "cel": "rows[5]"},
                   {"name": "via", "cel": "rows"}]
        v, why = query_lines([stats_row("trial_summary")], {"locals": locals_, "values": {"n": "double(size(via))"}})
        self.assertIsNone(why)
        self.assertEqual(v, {"n": 1.0})
        v, why = query_lines([stats_row("trial_summary")], {"locals": locals_, "values": {"n": "sixth.metric"}})
        self.assertRegex(why, "^value 'n' failed to evaluate: .*No value with name \"sixth\".*; "
                              "local 'sixth' failed to evaluate: .*Index error")


class QueryTest(unittest.TestCase):
    LINES = [stats_row("trial_summary", allocatable_cores="192"), stats_row("grpc_ResumeActor", failure_count=2),
             stats_row("trial_summary", allocatable_cores=88)]

    def test_locals_checks_and_values(self):
        v, why = query_lines(self.LINES, {
            "locals": [
                {"name": "trials", "cel": "stats.lines.filter(l, l.metric == 'trial_summary')"},
                {"name": "cores", "cel": "trials.map(l, l.measurements.allocatable_cores)"},
                {"name": "last", "cel": "cores[size(cores) - 1]"},
            ],
            "checks": {"has_trial": "size(trials) > 0"},
            "values": {"cores": "last", "most": "math.greatest(cores)", "name": "trials[0].metric",
                       "many": "size(cores) > 1", "per_zero": "last / 0.0",
                       "nullable": "size(cores) > 5 ? dyn(last) : null"},
        })
        self.assertIsNone(why)
        self.assertEqual(v, {"cores": 88.0, "most": 192.0, "name": "trial_summary", "many": True,
                             "per_zero": None, "nullable": None})

    def test_timestamps_and_durations_in_locals(self):
        v, why = query_lines([
            {"time": "2026-10-11T07:00:00.250Z", "val": 10.0},
            {"time": "2026-10-11T07:01:00.750Z", "val": 20.0},
            {"time": "2026-10-11T07:03:00.000Z", "val": 30.0},
        ], {
            "locals": [
                {"name": "lo", "cel": "timestamp(int(1791702000.0))"},
                {"name": "hi", "cel": "timestamp('2026-10-11T07:02:00Z')"},
                {"name": "span", "cel": "hi - lo"},
                {"name": "kept", "cel": "stats.lines.filter(r, timestamp(r.time) >= lo && timestamp(r.time) <= hi)"},
            ],
            "values": {
                "count": "double(size(kept))",
                "span_s": "double(span.getSeconds())",
            },
        })
        self.assertIsNone(why)
        self.assertEqual(v, {"count": 2.0, "span_s": 120.0})

    def test_a_false_check_makes_the_variable_invalid(self):
        v, why = query_lines(self.LINES, {"checks": {"no_failures": "stats.lines.all(l, !has(l.measurements.failure_count)"
                                                                    " || l.measurements.failure_count == 0.0)"},
                                          "values": {"n": "double(size(stats.lines))"}})
        self.assertEqual((v, why), ({}, "check 'no_failures' is false"))

    def test_errors_make_the_variable_invalid_and_name_the_expression(self):
        cases = {
            "check 'c' failed to evaluate: .*Key not found": (
                {"checks": {"c": "stats.lines.all(l, l.measurements.allocatable_cores > 0.0)"}}, {}),
            "local 'l' failed to evaluate: .*Key not found": (
                {"locals": [{"name": "l", "cel": "stats.lines.map(l, l.measurements.allocatable_cores)"}]},
                {"n": "math.greatest(l)"}),
            "value 'n' failed to evaluate: .*Key not found": (
                {}, {"n": "math.greatest(stats.lines.map(l, l.measurements.allocatable_cores))"}),
            "value 'n' failed to evaluate: .*empty list": (
                {}, {"n": "percentile(stats.lines.filter(l, false).map(l, 1.0), 50.0)"}),
        }
        for want, (cfg, values) in cases.items():
            with self.subTest(want=want):
                v, why = query_lines(self.LINES, {**cfg, "values": values or {"n": "double(size(stats.lines))"}})
                self.assertEqual(v, {})
                self.assertRegex(why, want)

    def test_an_input_that_cant_be_read_makes_the_variable_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = local_run(Path(tmp))
            cfg = {"inputs": STATS, "values": {"n": "double(size(stats.lines))"}}
            self.assertEqual(query(run, cfg), ({}, "input 'stats': stats.jsonl absent"))
            other = {"inputs": {"other": {**STATS["stats"], "test": "t1"}}, "values": {"n": "double(size(other.lines))"}}
            self.assertEqual(query(write_stats(Path(tmp), self.LINES), other),
                             ({}, "input 'other': no run of t1 to pair with"))

    def test_an_input_with_test_reads_the_paired_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            this = write_stats(Path(tmp) / "this", self.LINES)
            paired = write_stats(Path(tmp) / "t1", [stats_row("trial_summary", allocatable_cores=4)])
            asked = []

            def paired_run(test: str) -> BenchmarkRunContext:
                asked.append(test)
                return paired

            cfg = {"inputs": {**STATS, "other": {**STATS["stats"], "test": "t1"}},
                   "values": {"ratio": "stats.lines[0].measurements.allocatable_cores / "
                                       "other.lines[0].measurements.allocatable_cores",
                              "other_uri": "other.uri"}}
            self.assertEqual(query(dataclasses.replace(this, paired_run=paired_run), cfg),
                             ({"ratio": 48.0, "other_uri": str(Path(tmp) / "t1")}, None))
            self.assertEqual(asked, ["t1"])

    def test_each_input_is_read_once_per_run(self):
        cfg = {"inputs": STATS, "values": {"n": "double(size(stats.lines))"}}
        other = {"inputs": {"s": STATS["stats"]}, "values": {"m": "double(size(s.lines))"}}
        with tempfile.TemporaryDirectory() as tmp:
            source = cel.Cel()
            source.init(write_stats(Path(tmp), self.LINES))
            with mock.patch.object(json_lines.JsonLines, "read", autospec=True,
                                   side_effect=json_lines.JsonLines.read) as read:
                self.assertEqual(source.query(cfg), {"n": 3.0})
                self.assertEqual(source.query(other), {"m": 3.0})
            self.assertEqual(read.call_count, 1)

    def test_init_reads_nothing(self):
        source = cel.Cel()
        with mock.patch.object(json_lines.JsonLines, "read") as read:
            source.init(NO_RUN)
        read.assert_not_called()


class JsonLinesTest(unittest.TestCase):
    def test_lines_are_coerced_as_in_run_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = json_lines.JsonLines().read(write_stats(Path(tmp), [
                {"metric": "m", "measurements": {"n": "12.5", "i": 3, "flag": "true", "s": "2026_11", "none": None,
                                                 "nan": "NaN"}, "list": [1, "2"]},
                "", "not json", "[1, 2]",
            ]), {"file": "stats.jsonl"})
        self.assertEqual(data["lines"], [{"metric": "m", "measurements": {"n": 12.5, "i": 3.0, "flag": True,
                                                                         "s": "2026_11"}, "list": [1.0, 2.0]}])
        self.assertEqual([(u["line_no"], u["text"]) for u in data["unparsed_lines"]], [(3.0, "not json"), (4.0, "[1, 2]")])
        self.assertRegex(data["unparsed_lines"][0]["error"], "^not JSON: ")
        self.assertEqual(data["unparsed_lines"][1]["error"], "not a JSON object")
        self.assertTrue(data["uri"])

    def test_prefix_until_select_and_unique_by(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            log = d / "logs.txt"
            log.write_text("\n".join([
                "plain runner line not json",
                '[boomer] {"msg": "fail", "actor": "a1", "step": "resume"}',
                '[boomer] {"msg": "fail", "actor": "a1", "step": "execute"}',
                '[boomer] {"msg": "other", "actor": "a9"}',
                '[boomer] {"msg": "fail", "actor": "a2", "step": "suspend"}',
                '[boomer] {"msg": "shutdown"}',
                '[boomer] {"msg": "fail", "actor": "a3", "step": "resume"}',
            ]) + "\n", encoding="utf-8")
            run = local_run(log.parent, uri="run1")
            data = json_lines.JsonLines().read(run, {
                "file": "logs.txt",
                "prefix": "[boomer] ",
                "until": "has(line.msg) && line.msg == 'shutdown'",
                "select": "has(line.msg) && line.msg == 'fail'",
                "unique_by": "actor",
            })
        self.assertEqual(data["lines"], [
            {"msg": "fail", "actor": "a1", "step": "resume"},
            {"msg": "fail", "actor": "a2", "step": "suspend"},
        ])
        self.assertEqual(data["unparsed_lines"], [])
        self.assertEqual(data["uri"], "run1")

    def test_unique_by_distinguishes_bool_and_int(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = json_lines.JsonLines().read(write_stats(Path(tmp), [
                {"k": True, "n": 1}, {"k": 1, "n": 2}, {"k": True, "n": 3},
            ]), {"file": "stats.jsonl", "unique_by": "k"})
        self.assertEqual([x["n"] for x in data["lines"]], [1.0, 2.0])

    def test_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(reader.ReadError, "stats.jsonl absent"):
                json_lines.JsonLines().read(NO_RUN, {"file": "stats.jsonl"})
            run = write_stats(Path(tmp), [{"a": {"nested": 1}}])
            with self.assertRaisesRegex(reader.ReadError, "line 1: select failed: .*Key not found"):
                json_lines.JsonLines().read(run, {"file": "stats.jsonl", "select": "line.missing == 1.0"})
            with self.assertRaisesRegex(reader.ReadError, "line 1: unique_by 'missing': field missing"):
                json_lines.JsonLines().read(run, {"file": "stats.jsonl", "unique_by": "missing"})
            with self.assertRaisesRegex(reader.ReadError, "line 1: unique_by 'a': unhashable dict"):
                json_lines.JsonLines().read(run, {"file": "stats.jsonl", "unique_by": "a"})
            null_run = write_stats(Path(tmp) / "null", [{"a": None, "b": 1}, {"a": "x", "b": 2}])
            selected = json_lines.JsonLines().read(null_run, {"file": "stats.jsonl", "select": "has(line.a)"})
            self.assertEqual(selected["lines"], [{"a": "x", "b": 2.0}])
            with self.assertRaisesRegex(reader.ReadError, "line 1: unique_by 'a': field missing"):
                json_lines.JsonLines().read(null_run, {"file": "stats.jsonl", "unique_by": "a"})


class JsonFileTest(unittest.TestCase):
    def test_reads_and_coerces_json_object(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_stats(Path(tmp), [], summary={"metadata": {"steady_start_ts": "100", "ok": "true", "x": None}})
            data = json_file.Json().read(run, {"file": "summary.json"})
        self.assertEqual(data["data"], {"metadata": {"steady_start_ts": 100.0, "ok": True}})
        self.assertEqual(data["uri"], str(Path(tmp)))

    def test_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(reader.ReadError, "summary.json absent"):
                json_file.Json().read(NO_RUN, {"file": "summary.json"})
            bad = write_stats(Path(tmp) / "bad", [], summary="{oops")
            with self.assertRaisesRegex(reader.ReadError, "summary.json: not JSON: "):
                json_file.Json().read(bad, {"file": "summary.json"})
            arr = write_stats(Path(tmp) / "arr", [], summary=[1, 2])
            with self.assertRaisesRegex(reader.ReadError, "summary.json: not a JSON object"):
                json_file.Json().read(arr, {"file": "summary.json"})


class ReaderTest(unittest.TestCase):
    def test_check_data(self):
        class R(reader.Reader):
            fields = (reader.Field("xs", "list"), reader.Field("n", "double"), reader.Field("ok", "bool", optional=True))

        reader.check_data(R, {"xs": [], "n": 1.0})
        reader.check_data(R, {"xs": [], "n": 1.0, "ok": False})
        for bad, want in (({"xs": []}, "gave no n"), ({"xs": [], "n": 1.0, "extra": 1.0}, "undeclared fields"),
                          ({"xs": [], "n": True}, "n is a bool, not a double"), ({"xs": [], "n": 1}, "not a double"),
                          ({"xs": [], "n": 1.0, "ok": 1.0}, "ok is a float, not a bool")):
            with self.subTest(bad=bad), self.assertRaisesRegex(TypeError, want):
                reader.check_data(R, bad)

    def test_to_data(self):
        @dataclasses.dataclass
        class D:
            a: int
            b: Optional[float] = None
            c: float = math.inf

        self.assertEqual(reader.to_data({"d": [D(1)], "n": None, "s": "x", "t": True}),
                         {"d": [{"a": 1.0}], "s": "x", "t": True})


if __name__ == "__main__":
    unittest.main()
