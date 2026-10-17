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

"""Unit tests for common/boomer_config.py."""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common import boomer_config


class TestBoomerConfig(unittest.TestCase):
    def test_build_config_json_lifecycle_mode_none(self):
        raw = boomer_config.build_config_json([
            "--lifecycle-mode=none",
            "--agentsession-script=personal-assistant-light",
        ])
        parsed = json.loads(raw)
        self.assertEqual(parsed["lifecycle_mode"], "none")
        self.assertEqual(
            parsed["agentsession_script"], "personal-assistant-light"
        )

    def test_build_config_json_snapshot_steps(self):
        raw = boomer_config.build_config_json([
            "--lifecycle-mode",
            "suspend",
            "--agentsession-snapshot-steps",
            "3,10,20,30,40,50,61",
        ])
        parsed = json.loads(raw)
        self.assertEqual(parsed["lifecycle_mode"], "suspend")
        self.assertEqual(
            parsed["agentsession_snapshot_steps"], "3,10,20,30,40,50,61"
        )


if __name__ == "__main__":
    unittest.main()
