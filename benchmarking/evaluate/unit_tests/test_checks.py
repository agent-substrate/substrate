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

"""Unit tests for the threshold check."""

import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evaluate import checks  # noqa: E402


def threshold(value: str, op: str, limit) -> dict:
    return {"value": value, "op": op, "limit": limit}


def lookup(values: dict):
    def f(ref):
        if values.get(ref) is None:
            raise checks.Missing(f"{ref} not measured")
        return values[ref]
    return f


class ThresholdTest(unittest.TestCase):
    def test_numeric_ops(self):
        for op, want in (("<=", "pass"), ("<", "fail"), (">=", "pass"), (">", "fail"), ("==", "pass"),
                         ("!=", "fail")):
            self.assertEqual(checks.threshold(threshold("v.x", op, 5), lookup({"v.x": 5.0})).status, want, op)

    def test_relative_limit_and_clamps(self):
        rel = threshold("v.p90", "<=", {"ref": "base.p90", "times": 2.0, "at_least": 1000, "at_most": 1500})
        v = checks.threshold(rel, lookup({"v.p90": 1200, "base.p90": 300}))
        self.assertEqual((v.status, v.limit), ("fail", 1000.0))  # 2 x 300 is raised to at_least
        self.assertEqual(checks.threshold(rel, lookup({"v.p90": 1200, "base.p90": 700})).limit, 1400.0)
        self.assertEqual(checks.threshold(rel, lookup({"v.p90": 1200, "base.p90": 900})).limit, 1500.0)
        self.assertEqual(checks.threshold(rel, lookup({"v.p90": 1200, "base.p90": 900})).reason,
                         "v.p90 = 1200, needs <= 1500 (2 x base.p90, at_least 1000, at_most 1500)")

    def test_a_limit_that_overflows_is_invalid(self):
        rel = threshold("v.x", "<=", {"ref": "b.x", "times": 10.0})
        v = checks.threshold(rel, lookup({"v.x": 1.0, "b.x": 1e308}))
        self.assertEqual((v.status, v.limit, v.reason), ("invalid", None, "10 x b.x is not a finite number"))
        clamped = threshold("v.x", "<=", {"ref": "b.x", "times": 10.0, "at_most": 5.0})
        self.assertEqual(checks.threshold(clamped, lookup({"v.x": 1.0, "b.x": 1e308})).limit, 5.0)

    def test_missing_values_are_invalid_never_zero(self):
        v = checks.threshold(threshold("v.x", "<=", 5), lookup({}))
        self.assertEqual((v.status, v.reason), ("invalid", "v.x not measured"))
        self.assertEqual(checks.threshold(threshold("v.x", "<=", {"ref": "b.x"}), lookup({"v.x": 1})).status,
                         "invalid")
        self.assertEqual(checks.threshold(threshold("v.x", "<=", {"ref": "b.x"}), lookup({"v.x": 1, "b.x": True}))
                         .status, "invalid")

    def test_types(self):
        def status(op, limit, value):
            return checks.threshold(threshold("v.x", op, limit), lookup({"v.x": value})).status
        self.assertEqual(status("<=", 5, "fast"), "invalid")
        self.assertEqual(status("<=", 5, True), "invalid")  # a bool is not a number
        self.assertEqual(status("<=", 5, float("nan")), "invalid")
        self.assertEqual(status("==", True, True), "pass")
        self.assertEqual(status("==", "async", "sync"), "fail")
        # A bool against a number or a string doesn't compare: invalid, never a pass or a fail.
        self.assertEqual(status("==", 1, True), "invalid")
        self.assertEqual(status("!=", "true", True), "invalid")

    def test_a_numeric_string_limit_is_a_number(self):
        def verdict(op, limit, value):
            return checks.threshold(threshold("v.x", op, limit), lookup({"v.x": value}))
        self.assertEqual(verdict("==", "1.10", 1.1)[:3], ("pass", 1.1, 1.1))
        self.assertEqual(verdict("<", "2", 1.0).status, "pass")
        self.assertEqual(verdict("==", "v1.10", "v1.10")[:3], ("pass", "v1.10", "v1.10"))  # not a number: a string

    def test_validate(self):
        ok = lambda ref: None  # noqa: E731
        checks.validate(threshold("v.x", "<", 1), ok)
        checks.validate(threshold("v.x", "==", "async"), ok)
        checks.validate(threshold("v.x", "<", "1"), ok)  # a numeric string is a number
        cases = {
            "check: not a mapping": [],
            "unknown keys ['limits']": {**threshold("v.x", "<", 1), "limits": 2},
            "needs 'limit'": {"value": "v.x", "op": "<"},
            "check.op: '=~'": threshold("v.x", "=~", 1),
            "< needs a number, got 'one'": threshold("v.x", "<", "one"),
            "needs a number, bool or string, got '1e999'": threshold("v.x", "==", "1e999"),
            "a relative limit needs 'ref'": threshold("v.x", "<", {"times": 2}),
            "a relative limit needs one of": threshold("v.x", "==", {"ref": "v.y"}),
            "at_least is above at_most": threshold("v.x", "<", {"ref": "v.y", "at_least": 2, "at_most": 1}),
            "check.limit.times: must be a number": threshold("v.x", "<", {"ref": "v.y", "times": "2"}),
            "needs a number, bool or string": threshold("v.x", "==", [1]),
            "needs a ref string": threshold(3, "<", 1),
        }
        for message, check in cases.items():
            with self.assertRaisesRegex(ValueError, re.escape(message), msg=message):
                checks.validate(check, ok)
        with self.assertRaisesRegex(ValueError, "nope"):
            checks.validate(threshold("v.x", "<", 1), lambda ref: "nope")


if __name__ == "__main__":
    unittest.main()
