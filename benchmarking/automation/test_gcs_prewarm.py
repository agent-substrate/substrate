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
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import gcs_prewarm
import orchestrator

REPO = Path(__file__).resolve().parents[2]
TOOL_MAIN = REPO / "tools" / "gcs-prewarm" / "main.go"
TESTS_YAML = Path(__file__).resolve().parent / "tests.yaml"
REQUIRED = {"actorTemplate": "web", "atespace": "load"}


def write_template(dir_: str, name: str, location: str | None) -> None:
    snapshot = f"snapshotConfig:\n  storageLocation: {location}\n" if location else ""
    Path(dir_, f"{name}-template.yaml.tmpl").write_text(
        f"metadata:\n  name: {name}\n{snapshot}"
    )


class TemplatesTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = self.enterContext(tempfile.TemporaryDirectory())
        write_template(self.dir, "web", "gs://${BUCKET_NAME}/workloads/web/")


class ValidateTest(TemplatesTestCase):
    def test_accepts_required_only_and_full(self):
        gcs_prewarm.validate("t", REQUIRED, self.dir)
        gcs_prewarm.validate(
            "t",
            {
                **REQUIRED,
                "startRate": 50,
                "targetRate": 800.5,
                "doubleEvery": "5m",
                "hold": "0",
                "objectBytes": 4096,
                "workers": 64,
                "cleanup": False,
            },
            self.dir,
        )

    def test_rejects(self):
        for cfg in (
            None,
            {},
            {"actorTemplate": "web"},
            {"atespace": "load"},
            {**REQUIRED, "atespace": ""},
            {**REQUIRED, "atespace": "a/b"},
            {**REQUIRED, "actorTemplate": "missing"},
            {**REQUIRED, "bucket": "b"},
            {**REQUIRED, "startrate": 50},
            {**REQUIRED, "startRate": 0},
            {**REQUIRED, "startRate": True},
            {**REQUIRED, "targetRate": "800"},
            {**REQUIRED, "doubleEvery": "0s"},
            {**REQUIRED, "doubleEvery": "5 minutes"},
            {**REQUIRED, "hold": "1h30m"},
            {**REQUIRED, "workers": 0},
            {**REQUIRED, "objectBytes": 1.5},
            {**REQUIRED, "cleanup": "false"},
        ):
            with self.subTest(cfg=cfg), self.assertRaises(ValueError):
                gcs_prewarm.validate("t", cfg, self.dir)

    def test_rejects_template_without_gcs_location(self):
        write_template(self.dir, "local", "file:///tmp/snapshots")
        write_template(self.dir, "none", None)
        for template in ("local", "none"):
            with self.subTest(template=template), self.assertRaisesRegex(
                ValueError, "gs://"
            ):
                gcs_prewarm.validate("t", {**REQUIRED, "actorTemplate": template}, self.dir)

    def test_orchestrator_validates_block(self):
        # The orchestrator validates from the repo root, where the real
        # benchmark templates are.
        cwd = os.getcwd()
        os.chdir(REPO)
        self.addCleanup(os.chdir, cwd)
        test = {"name": "t", "type": "locust", "targetCluster": "dev",
                "file": "f", "duration": "1m", "users": 1}
        cfg = {"actorTemplate": "sleep", "atespace": "benchmark-workloads"}
        orchestrator.validate_and_normalize_tests([{**test, "gcsPrewarm": cfg}])
        with self.assertRaisesRegex(ValueError, "gcsPrewarm.workers"):
            orchestrator.validate_and_normalize_tests(
                [{**test, "gcsPrewarm": {**cfg, "workers": -1}}]
            )

    def test_checked_in_tests_yaml_is_valid(self):
        cwd = os.getcwd()
        os.chdir(REPO)
        self.addCleanup(os.chdir, cwd)
        tests = orchestrator.yaml.safe_load(TESTS_YAML.read_text())["tests"]
        orchestrator.validate_and_normalize_tests(tests)


class CommandTest(TemplatesTestCase):
    def test_required_only(self):
        self.assertEqual(
            gcs_prewarm.command(REQUIRED, "snap-bucket", self.dir),
            [
                "go", "-C", "tools/gcs-prewarm", "run", ".",
                "--bucket=snap-bucket",
                "--prefix=workloads/web/atespaces/load/actors",
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
                **REQUIRED,
            },
            "snap-bucket",
            self.dir,
        )
        self.assertEqual(
            cmd[7:],
            [
                "--start-rate=50",
                "--target-rate=800",
                "--double-every=90s",
                "--hold=3600s",
                "--object-bytes=1024",
                "--workers=32",
                "--cleanup=false",
            ],
        )

    def test_location_shapes(self):
        for location, bucket, prefix in (
            ("gs://fixed-bucket/a/b", "fixed-bucket", "a/b/atespaces/load/actors"),
            ("gs://fixed-bucket/", "fixed-bucket", "atespaces/load/actors"),
            ("gs://fixed-bucket", "fixed-bucket", "atespaces/load/actors"),
        ):
            with self.subTest(location=location):
                write_template(self.dir, "web", location)
                # A literal bucket needs no BUCKET_NAME.
                cmd = gcs_prewarm.command(REQUIRED, "", self.dir)
                self.assertEqual(cmd[5:7], [f"--bucket={bucket}", f"--prefix={prefix}"])

    def test_no_bucket(self):
        with self.assertRaisesRegex(RuntimeError, "BUCKET_NAME"):
            gcs_prewarm.command(REQUIRED, "", self.dir)

    def test_real_templates(self):
        # Every benchmark template deploy.sh can deploy resolves.
        templates = sorted(
            p.name.removesuffix("-template.yaml.tmpl")
            for p in (REPO / gcs_prewarm.TEMPLATES_DIR).glob("*-template.yaml.tmpl")
        )
        self.assertIn("glutton", templates)
        for template in templates:
            with self.subTest(template=template):
                cmd = gcs_prewarm.command(
                    {"actorTemplate": template, "atespace": "benchmark"},
                    "snap-bucket",
                    str(REPO / gcs_prewarm.TEMPLATES_DIR),
                )
                self.assertEqual(
                    cmd[5:7],
                    [
                        "--bucket=snap-bucket",
                        f"--prefix=benchmark-workloads/{template}/atespaces/benchmark/actors",
                    ],
                )

    def test_flags_exist_in_tool(self):
        # Guards against the tool renaming a flag out from under tests.yaml.
        tool_flags = set(re.findall(r'flag\.\w+\("([\w-]+)"', TOOL_MAIN.read_text()))
        for flag in ["--bucket", "--prefix"] + [f for f, _, _ in gcs_prewarm.FIELDS.values()]:
            with self.subTest(flag=flag):
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
        cwd = os.getcwd()
        os.chdir(REPO)
        self.addCleanup(os.chdir, cwd)
        gcs_prewarm.start({"actorTemplate": "glutton", "atespace": "benchmark", "hold": "5m"})
        cmd = popen.call_args.args[0]
        self.assertIn("--bucket=env-bucket", cmd)
        self.assertIn("--hold=300s", cmd)
        self.assertTrue(popen.call_args.kwargs["start_new_session"])


if __name__ == "__main__":
    unittest.main()
