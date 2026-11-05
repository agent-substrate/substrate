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

"""Every evaluations block checked into the repository loads: the tests*.yaml
entries the orchestrator runs (its image can only check their shape), and the
examples in the docs."""

import re
import sys
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "benchmarking"))

from evaluate import config  # noqa: E402


class TestEntriesTest(unittest.TestCase):
    def test_tests_yaml_evaluations_load(self):
        files = sorted((ROOT / "benchmarking" / "automation").glob("tests*.yaml"))
        self.assertTrue(files)
        for path in files:
            for test in yaml.safe_load(path.read_text())["tests"]:
                if "evaluations" in test:
                    with self.subTest(file=path.name, test=test["name"]):
                        config.load(test["evaluations"])

    def test_documented_examples_load(self):
        docs = [ROOT / "benchmarking" / "evaluate" / "README.md", ROOT / "benchmarking" / "automation" / "tests.yaml"]
        blocks = []
        for path in docs:
            text = path.read_text()
            if path.suffix == ".yaml":  # the commented example in the header
                lines = text.splitlines()
                start = lines.index("#   evaluations:")
                example = []
                for line in lines[start:]:
                    if not line.startswith("#   "):
                        break
                    example.append(line[4:])
                text = "```yaml\n" + "\n".join(example) + "\n```"
            for block in re.findall(r"```yaml\n(.*?)```", text, re.S):
                doc = yaml.safe_load(block)
                entries = doc if isinstance(doc, list) else [doc]
                blocks += [(path.name, e["evaluations"]) for e in entries if isinstance(e, dict) and "evaluations" in e]
        self.assertGreaterEqual(len(blocks), 3)
        for name, block in blocks:
            with self.subTest(doc=name):
                config.load(block)


if __name__ == "__main__":
    unittest.main()
