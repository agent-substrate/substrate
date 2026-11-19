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

"""Unit tests for workloads/: python3 benchmarking/automation/test_workloads.py"""

import unittest
from unittest import mock

import workloads
from workloads import deploy_sh


class InstallAllTest(unittest.TestCase):
    def setUp(self):
        self.installed = []
        fake = mock.Mock(install=self.installed.append)
        patcher = mock.patch.dict(workloads.WORKLOADS, {"a": fake, "b": fake})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_installs_in_order(self):
        workloads.install_all([("b", {"x": 1}), ("a", {"x": 2})])
        self.assertEqual(self.installed, [{"x": 1}, {"x": 2}])

    def test_rejects_before_installing_anything(self):
        with self.assertRaisesRegex(ValueError, "unknown workload 'nope'"):
            workloads.install_all([("a", {}), ("nope", {})])
        self.assertEqual(self.installed, [])


class DeployShTest(unittest.TestCase):
    def cmd(self, test):
        with mock.patch("workloads.deploy_sh.run") as run:
            deploy_sh.install(test)
        run.assert_called_once()
        return run.call_args.args[0]

    def test_defaults_omit_optional_flags(self):
        self.assertEqual(
            self.cmd({"name": "t"}),
            ["benchmarking/workloads/deploy.sh", "--deploy", "--worker-count", "1", "--sandbox-class", "gvisor"],
        )

    def test_whole_core_actor_cpu_is_a_string(self):
        cmd = self.cmd({"actorCPU": 1})
        self.assertEqual(cmd[cmd.index("--actor-cpu") + 1], "1")

    def test_fake_atelet_tuning(self):
        cmd = self.cmd({"isolate": {"delay": "500ms"}})
        self.assertEqual(cmd[cmd.index("--fake-atelet-env") + 1], "FAKE_ATELET_DELAY=500ms")

    def test_all_options(self):
        self.assertEqual(
            self.cmd(
                {
                    "workerCount": 3,
                    "sandboxClass": "microvm",
                    "actorMemory": "1536Mi",
                    "workerWaitTimeout": 600,
                    "workerMemory": "5Gi",
                    "actorCPU": "250m",
                }
            ),
            [
                "benchmarking/workloads/deploy.sh",
                "--deploy",
                "--worker-count", "3",
                "--sandbox-class", "microvm",
                "--actor-memory", "1536Mi",
                "--actor-cpu", "250m",
                "--worker-memory", "5Gi",
                "--wait-timeout", "600",
            ],
        )


if __name__ == "__main__":
    unittest.main()
