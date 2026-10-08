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

import argparse
import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
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
            storage_class_name="csi-nfs-sc",
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
                "--storage-class-name",
                "csi-nfs-sc",
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


def locust_entry(**fields):
    """A tests.yaml entry that passes validation, plus fields."""
    entry = {
        "name": "t",
        "type": "locust",
        "targetCluster": "dev",
        "file": "/app/tests/durdir.py",
        "duration": "1m",
        "users": 1,
    }
    entry.update(fields)
    return entry


class AdditionalManifestsTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)

    def touch(self, name):
        path = self.dir / name
        path.write_text("")
        return path

    def test_validate_accepts_paths(self):
        orchestrator.validate_and_normalize_tests(
            [locust_entry(additionalManifests=["a.yaml", "/abs/b.yaml"])]
        )

    def test_validate_rejects_bad_shapes(self):
        for bad in ("a.yaml", None, {"a": "b"}, [""], [1]):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                orchestrator.validate_and_normalize_tests(
                    [locust_entry(additionalManifests=bad)]
                )

    def test_paths_resolve_against_tests_dir(self):
        test = {"additionalManifests": ["test-manifests/a.yaml", "/abs/b.yaml"]}
        self.assertEqual(
            orchestrator.additional_manifests(test, Path("/etc/orchestrator")),
            [Path("/etc/orchestrator/test-manifests/a.yaml"), Path("/abs/b.yaml")],
        )

    def test_no_field_means_no_manifests(self):
        self.assertEqual(orchestrator.additional_manifests({}, Path("/x")), [])

    @mock.patch("orchestrator.run")
    def test_apply_in_order(self, run):
        a, b = self.touch("a.yaml"), self.touch("b.yaml")
        orchestrator.apply_additional_manifests([a, b])
        self.assertEqual(
            run.call_args_list,
            [
                mock.call(["kubectl", "apply", "-f", str(a)]),
                mock.call(["kubectl", "apply", "-f", str(b)]),
            ],
        )

    @mock.patch("orchestrator.run")
    def test_apply_missing_file_applies_nothing(self, run):
        a = self.touch("a.yaml")
        with self.assertRaises(FileNotFoundError):
            orchestrator.apply_additional_manifests([a, self.dir / "missing.yaml"])
        run.assert_not_called()

    @mock.patch("orchestrator.run_no_check")
    def test_delete_in_reverse_skipping_missing(self, run_no_check):
        a, b = self.touch("a.yaml"), self.touch("b.yaml")
        orchestrator.delete_additional_manifests([a, self.dir / "missing.yaml", b])
        self.assertEqual(
            run_no_check.call_args_list,
            [
                mock.call(["kubectl", "delete", "--ignore-not-found", "-f", str(b)]),
                mock.call(["kubectl", "delete", "--ignore-not-found", "-f", str(a)]),
            ],
        )


class MainAdditionalManifestsTest(unittest.TestCase):
    """main() applies a test's additionalManifests between substrate and the
    workloads, and deletes them in the sweep and the teardown."""

    # The steps whose order the tests check.
    STEPS = (
        "teardown_workloads",
        "delete_additional_manifests",
        "teardown_microvm_deps",
        "teardown_substrate",
        "deploy_substrate",
        "apply_additional_manifests",
        "deploy_workloads",
        "run_test",
    )
    TEARDOWN = [
        "teardown_workloads",
        "delete_additional_manifests",
        "teardown_microvm_deps",
        "teardown_substrate",
    ]

    def run_main(self, **side_effects):
        """Run main() on one entry with every cluster call mocked. Returns the
        mock that recorded STEPS in order, the exit code, and the manifest path
        main() should resolve."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tests_yaml = Path(tmp.name, "tests.yaml")
        entry = locust_entry(additionalManifests=["test-manifests/sc.yaml"])
        tests_yaml.write_text(yaml.safe_dump({"tests": [entry]}))
        args = argparse.Namespace(
            repo="r",
            branch="b",
            dest="d",
            tests=str(tests_yaml),
            target_cluster_dir="c",
            manifests_dir="m",
            junit_output=None,
        )
        steps = mock.Mock()
        with contextlib.ExitStack() as stack:
            for name in self.STEPS:
                patched = stack.enter_context(mock.patch(f"orchestrator.{name}"))
                steps.attach_mock(patched, name)
            for name, effect in side_effects.items():
                getattr(steps, name).side_effect = effect
            for name in (
                "wait_for_docker",
                "run",
                "apply_target_cluster",
                "gcloud_setup_for_target_cluster",
                "clear_target_cluster",
            ):
                stack.enter_context(mock.patch(f"orchestrator.{name}"))
            stack.enter_context(mock.patch("orchestrator.parse_args", return_value=args))
            stack.enter_context(mock.patch("orchestrator.os.chdir"))
            stack.enter_context(
                mock.patch("orchestrator.subprocess.check_output", return_value="abc1234\n")
            )
            stack.enter_context(mock.patch.dict(orchestrator.TYPES, {"locust": mock.Mock()}))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            with self.assertRaises(SystemExit) as exited:
                orchestrator.main()
        return steps, exited.exception.code, Path(tmp.name, "test-manifests", "sc.yaml")

    def test_teardown_deletes_manifests_after_failed_test(self):
        steps, code, manifest = self.run_main(run_test=RuntimeError("boom"))
        self.assertEqual(code, 1)
        self.assertEqual(
            [c[0] for c in steps.mock_calls],
            self.TEARDOWN
            + ["deploy_substrate", "apply_additional_manifests", "deploy_workloads", "run_test"]
            + self.TEARDOWN,
        )
        steps.apply_additional_manifests.assert_called_once_with([manifest])
        self.assertEqual(
            steps.delete_additional_manifests.call_args_list, [mock.call([manifest])] * 2
        )

    def test_missing_manifest_fails_test_before_workloads(self):
        steps, code, _ = self.run_main(apply_additional_manifests=FileNotFoundError("gone"))
        self.assertEqual(code, 1)
        self.assertEqual(
            [c[0] for c in steps.mock_calls],
            self.TEARDOWN + ["deploy_substrate", "apply_additional_manifests"] + self.TEARDOWN,
        )


if __name__ == "__main__":
    unittest.main()
