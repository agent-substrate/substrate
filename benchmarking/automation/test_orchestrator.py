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


class DeployWorkloadsFakeAteletTest(unittest.TestCase):
    @mock.patch("orchestrator.run")
    def test_fake_atelet_tuning_goes_to_deploy_sh(self, run):
        orchestrator.deploy_workloads(
            fake_atelet_args=["--fake-atelet-env", "FAKE_ATELET_DELAY=500ms"],
        )
        cmd = run.call_args.args[0]
        i = cmd.index("--fake-atelet-env")
        self.assertEqual(cmd[i + 1], "FAKE_ATELET_DELAY=500ms")


class SubstrateArgsTest(unittest.TestCase):
    def test_ate_args_only(self):
        self.assertEqual(
            orchestrator.substrate_args({"ateArgs": ["--cluster-size=size10"]}),
            ["--cluster-size=size10"],
        )

    def test_isolate_installs_the_fake_data_plane(self):
        self.assertEqual(
            orchestrator.substrate_args({
                "ateArgs": ["--cluster-size=size10"],
                "isolate": {"delay": "500ms"},
            }),
            # The delay tunes fake-atelet through deploy.sh, not the install.
            ["--cluster-size=size10", "--fake-data-plane"],
        )

    def test_neither(self):
        self.assertEqual(orchestrator.substrate_args({}), [])

    def test_bare_isolate_key(self):
        self.assertEqual(orchestrator.substrate_args({"isolate": None}), ["--fake-data-plane"])


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


class RunnerNamespaceTest(unittest.TestCase):
    """Every runner Job's namespace carries the label ate-system's
    NetworkPolicy admits API clients by; without it the runner cannot reach
    ate-api-server."""

    HERE = os.path.dirname(__file__)
    POLICIES = os.path.join(HERE, "..", "..", "manifests", "ate-install", "ate-system-networkpolicies.yaml")

    def api_client_labels(self):
        for doc in yaml.safe_load_all(open(self.POLICIES)):
            if not doc or doc["metadata"]["name"] != "ate-api-server":
                continue
            for rule in doc["spec"]["ingress"]:
                for peer in rule.get("from", []):
                    if "namespaceSelector" in peer:
                        return peer["namespaceSelector"]["matchLabels"]
        self.fail("ate-api-server NetworkPolicy admits no namespace by label")

    def test_namespace_has_api_access_label(self):
        want = self.api_client_labels()
        for name in ("runner-job.yaml.tmpl", "nighthawk-ingress-runner-job.yaml.tmpl"):
            with self.subTest(template=name):
                text = open(os.path.join(self.HERE, "manifests", name)).read()
                ns = next(
                    yaml.safe_load(d)
                    for d in text.split("\n---\n")
                    if "kind: Namespace" in d
                )
                self.assertEqual(ns["metadata"]["name"], "benchmarking")
                for k, v in want.items():
                    self.assertEqual(ns["metadata"].get("labels", {}).get(k), v)


class JobNameTest(unittest.TestCase):
    COMMIT = "ac41c06deadbeef"

    def test_short_name_keeps_full_test_name(self):
        name = orchestrator.job_name("Eng Review", self.COMMIT)
        self.assertRegex(name, r"^runner-eng-review-ac41c06-[0-9a-f]{6}$")

    def test_long_name_fits_a_label_and_keeps_suffix(self):
        test = "very-long-benchmark-name-that-overflows-the-kubernetes-label-limit"
        name = orchestrator.job_name(test, self.COMMIT)
        self.assertLessEqual(len(name), orchestrator.MAX_JOB_NAME_LEN)
        self.assertRegex(name, r"-ac41c06-[0-9a-f]{6}$")
        self.assertTrue(name.startswith("runner-very-long-benchmark-name"))

    def test_truncation_never_leaves_a_double_hyphen(self):
        # Cutting right after a hyphen must not yield "...-x--ac41c06-...".
        for i in range(1, 80):
            test = "-".join(["ab"] * i)
            name = orchestrator.job_name(test, self.COMMIT)
            self.assertLessEqual(len(name), orchestrator.MAX_JOB_NAME_LEN)
            self.assertNotIn("--", name, test)
            self.assertRegex(name, r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

    def test_two_runs_of_the_same_long_test_differ(self):
        test = "x" * 100
        self.assertNotEqual(
            orchestrator.job_name(test, self.COMMIT),
            orchestrator.job_name(test, self.COMMIT),
        )


if __name__ == "__main__":
    unittest.main()
