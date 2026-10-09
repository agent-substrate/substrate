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

import json
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


CLUSTER_ENV = {"CLUSTER_NAME": "c", "CLUSTER_LOCATION": "l", "PROJECT_ID": "p"}


class RunMetadataTest(unittest.TestCase):
    def test_cluster_env(self):
        with mock.patch.dict(os.environ, CLUSTER_ENV, clear=True):
            self.assertEqual(
                orchestrator.run_metadata(),
                {"cluster_name": "c", "cluster_location": "l", "project_id": "p"},
            )

    def test_benchmark_metadata_is_merged(self):
        env = {**CLUSTER_ENV, "BENCHMARK_METADATA": '{"build_id": "1"}'}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(orchestrator.run_metadata()["build_id"], "1")

    def test_bad_benchmark_metadata(self):
        for bad in ("not json", "[1]", '"text"'):
            env = {**CLUSTER_ENV, "BENCHMARK_METADATA": bad}
            with self.subTest(bad=bad), mock.patch.dict(os.environ, env, clear=True), \
                 self.assertRaisesRegex(ValueError, "BENCHMARK_METADATA"):
                orchestrator.run_metadata()

    def submitted_args(self, ttype):
        """The extra args run_test hands to render_template for a test type."""

        class Rendered(Exception):
            pass

        fake = mock.Mock(job_tmpl=mock.Mock(return_value="t"), job_subs=mock.Mock(return_value={}))
        test = {"name": "n", "type": ttype, "duration": "1m", "flags": ["--x"]}
        with mock.patch.dict(orchestrator.TYPES, {ttype: fake}), \
             mock.patch.dict(os.environ, CLUSTER_ENV, clear=True), \
             mock.patch.object(orchestrator, "render_template", side_effect=Rendered) as render, \
             self.assertRaises(Rendered):
            orchestrator.run_test(test, "img", "dest", "abcdef0123")
        return render.call_args.args[2]

    def test_locust_job_gets_metadata(self):
        args = self.submitted_args("locust")
        self.assertEqual(args[:2], ["--x", "--metadata"])
        self.assertEqual(json.loads(args[2])["cluster_name"], "c")

    def test_other_types_do_not(self):
        self.assertEqual(self.submitted_args("nighthawk-ingress"), ["--x"])

    def test_metadata_survives_yaml(self):
        md = json.dumps({"cluster_name": "c: d", "q": "\"'"})
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d"}
        subs.update(locust.job_subs({"file": "f", "duration": "1m", "users": 1}))
        text = orchestrator.render_template(RunnerSizingTest.TMPL, subs, ["--metadata", md])
        job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
        self.assertEqual(job["spec"]["template"]["spec"]["containers"][0]["args"][-1], md)


if __name__ == "__main__":
    unittest.main()
