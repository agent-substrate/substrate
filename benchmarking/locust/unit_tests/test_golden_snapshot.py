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

"""Unit tests for golden_snapshot.py."""

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import golden_snapshot

CREATE = datetime(2026, 1, 1, 12, 0, 0)


class GoldenSnapshotTest(unittest.TestCase):
    def test_rows(self):
        cases = [
            # label, snapshot_time, tagged, want warmup_s, want golden_snapshot_s
            ("tagged", CREATE + timedelta(seconds=4.5), CREATE + timedelta(seconds=9.25), "4.5", "9.25"),
            ("warming", None, None, None, None),
            ("not tagged yet", CREATE + timedelta(seconds=4.5), None, "4.5", None),
        ]
        for label, snapshot, tagged, want_warmup, want_golden in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as d:
                out = Path(d) / "stats.jsonl"
                with mock.patch.object(golden_snapshot, "fetch_template_times",
                                       return_value=(CREATE, snapshot, tagged)):
                    n = golden_snapshot.append_golden_snapshots(out, ["glutton"], "ts", "tag", "test")
                self.assertEqual(n, 1)
                row = json.loads(out.read_text())
                self.assertEqual(row["metric"], "golden_snapshot")
                self.assertEqual(row["measurements"], {
                    "template": "glutton",
                    "warmup_s": want_warmup,
                    "golden_snapshot_s": want_golden,
                })


if __name__ == "__main__":
    unittest.main()
