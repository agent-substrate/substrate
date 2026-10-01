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

"""Unit tests for gcs_prewarm.py: python3 benchmarking/automation/test_gcs_prewarm.py"""

import os
import re
import signal
import subprocess
import time
import unittest
from pathlib import Path
from unittest import mock

import gcs_prewarm
import orchestrator

TOOL_MAIN = Path(__file__).resolve().parents[2] / "tools" / "gcs-prewarm" / "main.go"
TESTS_YAML = Path(__file__).resolve().parent / "tests.yaml"


class ValidateTest(unittest.TestCase):
    def test_accepts_empty_and_full(self):
        gcs_prewarm.validate("t", {})
        gcs_prewarm.validate(
            "t",
            {
                "bucket": "b",
                "prefix": "p",
                "startRate": 50,
                "targetRate": 800.5,
                "doubleEvery": "5m",
                "hold": "0",
                "objectBytes": 4096,
                "workers": 64,
                "cleanup": False,
            },
        )

    def test_rejects(self):
        for cfg in (
            None,
            True,
            ["startRate"],
            {"startrate": 50},
            {"bucket": ""},
            {"startRate": 0},
            {"startRate": True},
            {"targetRate": "800"},
            {"doubleEvery": "0s"},
            {"doubleEvery": "5 minutes"},
            {"hold": "1h30m"},
            {"workers": 0},
            {"objectBytes": 1.5},
            {"cleanup": "false"},
        ):
            with self.subTest(cfg=cfg), self.assertRaises(ValueError):
                gcs_prewarm.validate("t", cfg)

    def test_orchestrator_validates_block(self):
        test = {"name": "t", "type": "locust", "targetCluster": "dev",
                "file": "f", "duration": "1m", "users": 1}
        orchestrator.validate_and_normalize_tests([{**test, "gcsPrewarm": {}}])
        with self.assertRaisesRegex(ValueError, "gcsPrewarm.workers"):
            orchestrator.validate_and_normalize_tests(
                [{**test, "gcsPrewarm": {"workers": -1}}]
            )

    def test_checked_in_tests_yaml_is_valid(self):
        tests = orchestrator.yaml.safe_load(TESTS_YAML.read_text())["tests"]
        orchestrator.validate_and_normalize_tests(tests)


class CommandTest(unittest.TestCase):
    def test_defaults(self):
        self.assertEqual(
            gcs_prewarm.command({}, "snap-bucket"),
            [
                "go", "-C", "tools/gcs-prewarm", "run", ".",
                "--bucket=snap-bucket",
                f"--prefix={gcs_prewarm.GLUTTON_SNAPSHOT_PREFIX}",
            ],
        )

    def test_every_field(self):
        cmd = gcs_prewarm.command(
            {
                "cleanup": False,
                "hold": "1h",
                "doubleEvery": "90",
                "startRate": 50,
                "targetRate": 800,
                "objectBytes": 1024,
                "workers": 32,
                "prefix": "custom/prefix",
                "bucket": "explicit",
            },
            "ignored",
        )
        self.assertEqual(
            cmd[5:],
            [
                "--bucket=explicit",
                "--prefix=custom/prefix",
                "--start-rate=50",
                "--target-rate=800",
                "--double-every=90s",
                "--hold=3600s",
                "--object-bytes=1024",
                "--workers=32",
                "--cleanup=false",
            ],
        )

    def test_no_bucket(self):
        with self.assertRaisesRegex(RuntimeError, "BUCKET_NAME"):
            gcs_prewarm.command({}, "")

    def test_flags_exist_in_tool(self):
        # Guards against the tool renaming a flag out from under tests.yaml.
        tool_flags = set(re.findall(r'flag\.\w+\("([\w-]+)"', TOOL_MAIN.read_text()))
        for field, (flag, _, _) in gcs_prewarm.FIELDS.items():
            with self.subTest(field=field):
                self.assertIn(flag.removeprefix("--"), tool_flags)


class ProcessTest(unittest.TestCase):
    def test_wait(self):
        gcs_prewarm.wait(None)
        gcs_prewarm.wait(subprocess.Popen(["true"]))
        with self.assertRaisesRegex(RuntimeError, "code 3"):
            gcs_prewarm.wait(subprocess.Popen(["sh", "-c", "exit 3"]))

    def test_kill_reaps_process_group(self):
        # The child sleep stands in for the binary `go run` compiles and
        # execs: killing only the parent would leave it running.
        proc = subprocess.Popen(
            ["sh", "-c", "sleep 60 & echo $!; wait"],
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        child = int(proc.stdout.readline())
        proc.stdout.close()
        gcs_prewarm.kill(proc)
        self.assertEqual(proc.returncode, -signal.SIGKILL)
        with self.assertRaises(ProcessLookupError):
            for _ in range(50):
                # Signal 0 probes for existence; the reaped child goes away.
                os.kill(child, 0)
                time.sleep(0.1)

    def test_kill_noop(self):
        gcs_prewarm.kill(None)
        done = subprocess.Popen(["true"])
        done.wait()
        gcs_prewarm.kill(done)

    @mock.patch.dict("os.environ", {"BUCKET_NAME": "env-bucket"})
    @mock.patch("subprocess.Popen")
    def test_start_uses_target_cluster_bucket(self, popen):
        gcs_prewarm.start({"hold": "5m"})
        cmd = popen.call_args.args[0]
        self.assertIn("--bucket=env-bucket", cmd)
        self.assertIn("--hold=300s", cmd)
        self.assertTrue(popen.call_args.kwargs["start_new_session"])


if __name__ == "__main__":
    unittest.main()
