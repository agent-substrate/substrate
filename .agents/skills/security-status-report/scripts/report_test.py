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

"""Tests for compile_report.py and render_table.py.

Run with: python3 -m unittest discover -s .agents/skills/security-status-report/scripts
"""

import json
import os
import tempfile
import unittest

from compile_report import compile_report
from render_table import render_table


class CompileReportTest(unittest.TestCase):
    def test_copies_summaries(self):
        with tempfile.TemporaryDirectory() as d:
            threats = os.path.join(d, "threats.json")
            with open(threats, "w") as f:
                json.dump([{"threat_id": "T-01", "priority": "High", "threat": "x"}], f)
            with open(os.path.join(d, "T-01.json"), "w") as f:
                json.dump({"threat_id": "T-01", "quality": 0.5, "mitigation_summary": "m", "remaining_gap": "g"}, f)
            out = os.path.join(d, "final.json")
            compile_report(threats, d, out)
            with open(out) as f:
                got = json.load(f)[0]
        self.assertEqual(got["mitigation_summary"], "m")
        self.assertEqual(got["remaining_gap"], "g")


class RenderTableTest(unittest.TestCase):
    def test_sorts_by_priority_then_id(self):
        data = [
            {"threat_id": "T-10", "priority": "Low", "quality": 0.1},
            {"threat_id": "T-03", "priority": "Critical", "quality": 0.2},
            {"threat_id": "T-02", "priority": "High", "quality": 0.3},
            {"threat_id": "T-01", "priority": "Critical", "quality": 0.4},
        ]
        rows = [l.split(" | ")[0].lstrip("| ") for l in render_table(data).splitlines() if l.startswith("| T-")]
        self.assertEqual(rows, ["T-01", "T-03", "T-02", "T-10"])

    def test_error_row_and_pipe_escaping(self):
        data = [
            {"threat_id": "T-01", "priority": "High", "error": "Missing result file"},
            {"threat_id": "T-02", "priority": "High", "quality": 1.0, "mitigation_summary": "a|b", "remaining_gap": "None"},
        ]
        out = render_table(data)
        self.assertIn("| T-01 | High | ERROR |  | Missing result file |", out)
        self.assertIn("| T-02 | High | 1.00 | a/b | None |", out)
        self.assertIn("Mean score: 1.00", out)


if __name__ == "__main__":
    unittest.main()
