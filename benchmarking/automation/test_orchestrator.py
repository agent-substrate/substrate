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

"""Unit tests for orchestrator.py: python3 benchmarking/automation/test_orchestrator.py"""

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import orchestrator

ENV = (
    "export PROJECT_ID=p\n"
    "export CLUSTER_NAME=c\n"
    "export CLUSTER_LOCATION=l\n"
    "export KO_DOCKER_REPO=r/x\n"
)


class ParseArgsTest(unittest.TestCase):
    def test_repo_required_without_in_place(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            orchestrator.parse_args(["--dest", "gs://b"])

    def test_in_place_needs_no_repo(self):
        args = orchestrator.parse_args(["--dest", "gs://b", "--in-place"])
        self.assertTrue(args.in_place)
        self.assertIsNone(args.repo)


class ApplyTargetClusterTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.checkout = Path(tmp.name) / "substrate"
        self.checkout.mkdir()
        self.clusters = Path(tmp.name) / "clusters"
        self.clusters.mkdir()
        patches = [
            mock.patch.object(orchestrator, "SUBSTRATE_DIR", str(self.checkout)),
            mock.patch.dict(os.environ, {}),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_in_place_sources_existing_env_file(self):
        env_file = self.checkout / orchestrator.ENV_FILE_NAME
        env_file.write_text(ENV)
        orchestrator.apply_target_cluster(
            "unused", str(self.clusters), in_place=True
        )
        self.assertEqual(os.environ["CLUSTER_NAME"], "c")
        self.assertEqual(env_file.read_text(), ENV)

    def test_in_place_requires_env_file(self):
        with self.assertRaises(FileNotFoundError):
            orchestrator.apply_target_cluster(
                "unused", str(self.clusters), in_place=True
            )

    def test_copies_target_cluster_without_in_place(self):
        (self.clusters / "dev.sh").write_text(ENV)
        orchestrator.apply_target_cluster("dev", str(self.clusters))
        self.assertEqual(
            (self.checkout / orchestrator.ENV_FILE_NAME).read_text(), ENV
        )
        self.assertEqual(os.environ["PROJECT_ID"], "p")


if __name__ == "__main__":
    unittest.main()
