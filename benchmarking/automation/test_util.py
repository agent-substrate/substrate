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

"""Unit tests for util.py: python3 benchmarking/automation/test_util.py"""

import unittest
from unittest import mock

import util


class ParseDurationSecondsTest(unittest.TestCase):
    def test_units(self):
        self.assertEqual(util.parse_duration_seconds("30"), 30)
        self.assertEqual(util.parse_duration_seconds("30s"), 30)
        self.assertEqual(util.parse_duration_seconds("5m"), 300)
        self.assertEqual(util.parse_duration_seconds("2h"), 7200)

    def test_whitespace(self):
        self.assertEqual(util.parse_duration_seconds(" 10 m "), 600)

    def test_invalid(self):
        for bad in ("10 parsecs", "", "m", "1d", "-5s"):
            with self.assertRaises(ValueError):
                util.parse_duration_seconds(bad)


class BuildAndPushTest(unittest.TestCase):
    @mock.patch("util.run")
    def test_builds_amd64_and_pushes(self, run_mock):
        image = util.build_and_push("gcr.io/p/repo/img:tag", "path/Dockerfile")

        self.assertEqual(image, "gcr.io/p/repo/img:tag")
        build_cmd, push_cmd = (c.args[0] for c in run_mock.call_args_list)
        self.assertEqual(build_cmd[:2], ["docker", "build"])
        self.assertIn("--platform", build_cmd)
        self.assertEqual(build_cmd[build_cmd.index("--platform") + 1], "linux/amd64")
        self.assertEqual(build_cmd[build_cmd.index("-t") + 1], "gcr.io/p/repo/img:tag")
        self.assertEqual(build_cmd[build_cmd.index("-f") + 1], "path/Dockerfile")
        self.assertEqual(push_cmd, ["docker", "push", "gcr.io/p/repo/img:tag"])


class IsolateArgsTest(unittest.TestCase):
    def test_absent(self):
        self.assertEqual(util.isolate_args(None), [])

    def test_empty_block_selects_the_mode(self):
        self.assertEqual(util.isolate_args({}), ["--isolate"])

    def test_every_key(self):
        self.assertEqual(
            util.isolate_args({
                "nodes": 4,
                "run": "nightly",
                "delay": "250ms",
            }),
            [
                "--isolate",
                "--isolate-nodes", "4",
                "--isolate-run", "nightly",
                "--isolate-delay", "250ms",
            ],
        )

    def test_durations(self):
        for delay in ("0", "0s", "1.5s", "1m30s", "250ms", "100us"):
            with self.subTest(delay=delay):
                self.assertEqual(
                    util.isolate_args({"delay": delay}),
                    ["--isolate", "--isolate-delay", delay],
                )

    def test_invalid(self):
        for block in (
            {"node": 4},
            {"nodes": 0},
            {"nodes": "4"},
            {"nodes": True},
            {"workersPerNode": 100},
            {"run": "Nightly"},
            {"run": "ninechars"},
            {"run": ""},
            {"run": 7},
            {"delay": "fast"},
            {"delay": "500"},
            {"delay": "-1s"},
            {"delay": 0},
            ["nodes"],
        ):
            with self.subTest(block=block), self.assertRaises(ValueError):
                util.isolate_args(block)


if __name__ == "__main__":
    unittest.main()
