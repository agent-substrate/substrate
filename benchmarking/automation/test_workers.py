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

"""Unit tests for workers.py: python3 benchmarking/automation/test_workers.py"""

import os
import subprocess
import unittest
from pathlib import Path
from unittest import mock

import yaml

import workers

REPO_ROOT = Path(__file__).resolve().parents[2]


class ConfigTest(unittest.TestCase):
    def test_defaults(self):
        self.assertEqual(
            workers.config({"name": "t", "workerMemory": None}),
            workers.Config(count=1, sandbox_class="gvisor", memory="", wait_timeout_secs=300, fake_atelet_env=()),
        )

    def test_from_test(self):
        cfg = workers.config(
            {
                "name": "t",
                "workerCount": 4,
                "sandboxClass": "microvm",
                "workerMemory": "8Gi",
                "workerWaitTimeout": "900",
                "isolate": {"delay": "500ms", "mintActorCertificate": False},
            }
        )
        self.assertEqual(
            cfg,
            workers.Config(
                count=4,
                sandbox_class="microvm",
                memory="8Gi",
                wait_timeout_secs=900,
                fake_atelet_env=("FAKE_ATELET_DELAY=500ms", "FAKE_ATELET_MINT_ACTOR_CERTIFICATE=false"),
            ),
        )

    def test_invalid(self):
        for bad in (
            {"workerCount": -1},
            {"workerCount": "2"},
            {"workerCount": True},
            {"sandboxClass": "kata"},
            {"workerMemory": 8},
            {"workerMemory": 0},
            {"workerMemory": False},
            {"workerWaitTimeout": "5m"},
            {"workerWaitTimeout": "²"},
            {"workerWaitTimeout": True},
            {"isolate": {"bogus": "1s"}},
        ):
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, "test 't'"):
                workers.config({"name": "t", **bad})


class RenderPoolTest(unittest.TestCase):
    def setUp(self):
        # render_pool reads the manifest relative to the repo root.
        cwd = os.getcwd()
        os.chdir(REPO_ROOT)
        self.addCleanup(os.chdir, cwd)

    def docs(self, cfg):
        rendered = workers.render_pool(cfg)
        self.assertNotIn("${", rendered)
        return list(yaml.safe_load_all(rendered))

    def test_unsized(self):
        namespace, pool = self.docs(workers.Config(count=3))
        self.assertEqual(namespace["kind"], "Namespace")
        self.assertEqual(namespace["metadata"]["name"], workers.NAMESPACE)
        self.assertEqual(pool["kind"], "WorkerPool")
        self.assertEqual(pool["metadata"]["name"], workers.POOL_NAME)
        self.assertEqual(pool["spec"]["replicas"], 3)
        self.assertEqual(pool["spec"]["sandboxClasses"], [{"name": "gvisor"}])
        self.assertEqual(pool["spec"]["workerImage"], "ko://go.ate.dev/substrate/cmd/ateworker-gvisor")
        self.assertNotIn("resources", pool["spec"]["template"])

    def test_sized_microvm(self):
        _, pool = self.docs(workers.Config(sandbox_class="microvm", memory="8Gi"))
        self.assertEqual(pool["spec"]["sandboxClasses"], [{"name": "microvm"}])
        self.assertEqual(pool["spec"]["workerImage"], "ko://go.ate.dev/substrate/cmd/ateworker-microvm")
        self.assertEqual(
            pool["spec"]["template"]["resources"],
            {"requests": {"memory": "8Gi"}, "limits": {"memory": "8Gi"}},
        )


def completed(stdout=""):
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout)


class InstallTest(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch("workers.render_pool", return_value="POOL")
        patcher.start()
        self.addCleanup(patcher.stop)

    def install(self, test, fake=False, tuning_before="", tuning_after=""):
        """Run install with run() faked; return the commands it ran."""
        tuning = iter([tuning_before, tuning_after])

        def fake_run(cmd, **kwargs):
            if cmd[:3] == ["kubectl", "get", "deployment/fake-workersync"]:
                return completed("deployment.apps/fake-workersync" if fake else "")
            if cmd[:4] == ["kubectl", "get", "configmap", "fake-atelet-tuning"]:
                return completed(next(tuning))
            if cmd[:3] == ["kubectl", "get", "daemonset"]:
                return completed("daemonset.apps/atelet-v1")
            return completed()

        with mock.patch("workers.run", side_effect=fake_run) as run:
            workers.install({"name": "t", **test})
        return run.call_args_list

    @staticmethod
    def cmds(calls):
        return [c.args[0] for c in calls]

    def test_gvisor(self):
        calls = self.install({"workerCount": 2})
        cmds = self.cmds(calls)
        apply = next(c for c in calls if c.args[0][:3] == ["hack/run-tool.sh", "ko", "apply"])
        self.assertEqual(apply.kwargs["input"], "POOL")
        self.assertEqual(
            cmds[-2:],
            [
                ["kubectl", "wait", "--for=create", "deployment/benchmark-worker",
                 "--namespace=benchmark-workloads", "--timeout=300s"],
                ["kubectl", "rollout", "status", "deployment/benchmark-worker",
                 "--namespace=benchmark-workloads", "--timeout=300s"],
            ],
        )

    def test_fake_data_plane_waits_on_pool_status(self):
        cmds = self.cmds(self.install({"workerCount": 5, "workerWaitTimeout": 60}, fake=True))
        self.assertEqual(
            cmds[-1],
            ["kubectl", "wait", "--for=jsonpath={.status.readyReplicas}=5",
             "workerpool/benchmark-worker", "--namespace=benchmark-workloads", "--timeout=60s"],
        )
        self.assertFalse(any(c[:2] == ["kubectl", "rollout"] for c in cmds))

    def test_fake_data_plane_zero_workers_skips_wait(self):
        cmds = self.cmds(self.install({"workerCount": 0}, fake=True))
        self.assertEqual(cmds[-1], ["hack/run-tool.sh", "ko", "apply", "-f", "-"])

    def test_tuning_restarts_fake_atelet_when_changed(self):
        cmds = self.cmds(
            self.install({"isolate": {"delay": "1s"}}, fake=True, tuning_before="{}", tuning_after='{"X":"1"}')
        )
        create = next(c for c in cmds if c[:3] == ["kubectl", "create", "configmap"])
        self.assertIn("--from-literal=FAKE_ATELET_DELAY=1s", create)
        restart = cmds.index(["kubectl", "rollout", "restart", "--namespace=ate-system", "daemonset.apps/atelet-v1"])
        self.assertLess(restart, cmds.index(["hack/run-tool.sh", "ko", "apply", "-f", "-"]))

    def test_tuning_unchanged_skips_restart(self):
        cmds = self.cmds(self.install({"isolate": None}, fake=True, tuning_before="{}", tuning_after="{}"))
        self.assertFalse(any(c[:3] == ["kubectl", "rollout", "restart"] for c in cmds))

    def test_isolate_without_fake_data_plane_fails(self):
        with self.assertRaises(RuntimeError):
            self.install({"isolate": {"delay": "1s"}}, fake=False)


class TeardownTest(unittest.TestCase):
    @mock.patch("workers.run_no_check")
    def test_removes_pool_and_namespace(self, run_no_check):
        workers.teardown()
        self.assertEqual(
            [c.args[0] for c in run_no_check.call_args_list],
            [
                ["kubectl", "delete", "workerpool", "benchmark-worker",
                 "--namespace=benchmark-workloads", "--ignore-not-found"],
                ["kubectl", "delete", "namespace", "benchmark-workloads", "--ignore-not-found"],
            ],
        )


if __name__ == "__main__":
    unittest.main()
