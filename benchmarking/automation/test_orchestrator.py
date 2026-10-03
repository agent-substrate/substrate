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

import os
import unittest
from unittest import mock

import yaml

import orchestrator
from testtypes import locust


class DeployWorkloadsTest(unittest.TestCase):
    @mock.patch("orchestrator.run")
    def test_defaults_omit_optional_flags(self, run):
        orchestrator.deploy_workloads()
        run.assert_called_once_with(
            [
                "benchmarking/workloads/deploy.sh",
                "--deploy",
                "--worker-count",
                "1",
                "--sandbox-class",
                "gvisor",
            ]
        )

    @mock.patch("orchestrator.run")
    def test_worker_memory(self, run):
        orchestrator.deploy_workloads(worker_memory="12Gi")
        cmd = run.call_args.args[0]
        i = cmd.index("--worker-memory")
        self.assertEqual(cmd[i + 1], "12Gi")

    @mock.patch("orchestrator.run")
    def test_all_options(self, run):
        orchestrator.deploy_workloads(
            worker_count=3,
            sandbox_class="microvm",
            actor_memory="1536Mi",
            wait_timeout_secs=600,
            worker_memory="5Gi",
        )
        run.assert_called_once_with(
            [
                "benchmarking/workloads/deploy.sh",
                "--deploy",
                "--worker-count",
                "3",
                "--sandbox-class",
                "microvm",
                "--actor-memory",
                "1536Mi",
                "--worker-memory",
                "5Gi",
                "--wait-timeout",
                "600",
            ]
        )


class RunnerSizingTest(unittest.TestCase):
    TMPL = os.path.join(os.path.dirname(__file__), "manifests", "runner-job.yaml.tmpl")

    def render(self, test):
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d"}
        subs.update(locust.job_subs(test))
        text = orchestrator.render_template(self.TMPL, subs)
        job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
        return job["spec"]["template"]["spec"]["containers"][0]["resources"]

    def test_defaults(self):
        res = self.render({"file": "f", "duration": "1m", "users": 1})
        self.assertEqual(res, {"requests": {"cpu": "500m", "memory": "512Mi"}})

    def test_runner_cpu_and_memory(self):
        res = self.render(
            {"file": "f", "duration": "1m", "users": 1000, "runnerCpu": "4", "runnerMemory": "8Gi"}
        )
        self.assertEqual(res["requests"], {"cpu": "4", "memory": "8Gi"})
        self.assertNotIn("limits", res)

    def test_no_placeholder_survives(self):
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d"}
        subs.update(locust.job_subs({"file": "f", "duration": "1m", "users": 1}))
        self.assertNotIn("${", orchestrator.render_template(self.TMPL, subs))


if __name__ == "__main__":
    unittest.main()
