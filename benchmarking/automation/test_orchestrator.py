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

import unittest
from unittest import mock

import orchestrator


class DeploySubstrateTest(unittest.TestCase):
    """Each test reinstalls substrate, and an install exports no telemetry
    unless the command line says where to. Thus deploy_substrate names a
    collector, or workloads/deploy.sh stops the run at setup."""

    def deploy(self, ate_args=(), env=None):
        # ATE_OBSERVABILITY of the developer must not reach the test.
        environ = {k: v for k, v in orchestrator.os.environ.items()
                   if k != "ATE_OBSERVABILITY"}
        environ.update(env or {})
        with mock.patch.dict(orchestrator.os.environ, environ, clear=True):
            with mock.patch.object(orchestrator, "run") as run:
                orchestrator.deploy_substrate(ate_args)
        return run.call_args[0][0]

    def test_default_is_the_gke_collector(self):
        self.assertIn("--observability=gke", self.deploy())

    def test_env_selects_the_mode(self):
        cmd = self.deploy(env={"ATE_OBSERVABILITY": "none"})
        self.assertIn("--observability=none", cmd)
        self.assertNotIn("--observability=gke", cmd)

    def test_test_args_keep_their_own_mode(self):
        cmd = self.deploy(["--observability=none"])
        self.assertEqual([a for a in cmd if a.startswith("--observability")],
                         ["--observability=none"])

    def test_test_args_keep_their_own_endpoint(self):
        # --otlp-endpoint selects mode otlp on its own, and the install refuses
        # the two flags together.
        cmd = self.deploy(["--otlp-endpoint", "http://meter.benchmarking.svc:4317"])
        self.assertFalse([a for a in cmd if a.startswith("--observability")])

    def test_other_args_stay(self):
        cmd = self.deploy(["--atenet-router=agentgateway"])
        self.assertIn("--atenet-router=agentgateway", cmd)
        self.assertIn("--deploy-ate-system", cmd)


if __name__ == "__main__":
    unittest.main()
