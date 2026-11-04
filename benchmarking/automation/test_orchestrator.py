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

"""Unit tests for orchestrator.py: python3 benchmarking/automation/test_orchestrator.py"""

import json
import os
import unittest
from unittest import mock

import yaml

import orchestrator
from testtypes import locust


class SelectTestsTest(unittest.TestCase):
    TESTS = [{"name": "a"}, {"name": "b"}, {"name": "c"}]

    def test_no_names_runs_all(self):
        self.assertEqual(orchestrator.select_tests(self.TESTS, []), self.TESTS)

    def test_keeps_file_order(self):
        got = orchestrator.select_tests(self.TESTS, ["c", "a"])
        self.assertEqual([t["name"] for t in got], ["a", "c"])

    def test_unknown_name(self):
        with self.assertRaisesRegex(ValueError, "x, y"):
            orchestrator.select_tests(self.TESTS, ["a", "y", "x"])


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
            ["--cluster-size=size10", "--sandbox-classes=gvisor"],
        )

    def test_isolate_installs_the_fake_data_plane(self):
        self.assertEqual(
            orchestrator.substrate_args({
                "ateArgs": ["--cluster-size=size10"],
                "isolate": {"delay": "500ms"},
            }),
            # The delay tunes fake-atelet through deploy.sh, not the install.
            ["--cluster-size=size10", "--fake-data-plane", "--sandbox-classes=gvisor"],
        )

    def test_neither(self):
        self.assertEqual(orchestrator.substrate_args({}), ["--sandbox-classes=gvisor"])

    def test_bare_isolate_key(self):
        self.assertEqual(
            orchestrator.substrate_args({"isolate": None}),
            ["--fake-data-plane", "--sandbox-classes=gvisor"],
        )

    def test_microvm_selects_microvm_sandbox_class(self):
        self.assertEqual(
            orchestrator.substrate_args({
                "sandboxClass": "microvm",
                "ateArgs": ["--cluster-size=size10"],
            }),
            ["--cluster-size=size10", "--sandbox-classes=microvm"],
        )


class DeploySubstrateTest(unittest.TestCase):
    # The runner Job authenticates with its pod certificate, so ate-api-server
    # denies it unless the install makes it an owner, and the Envoy ingress
    # router refuses to deploy without an allowed SPIFFE ID.
    @mock.patch("orchestrator.run")
    def test_deploy_substrate_auth(self, run):
        orchestrator.deploy_substrate(["--credential-provider={}"])
        cmd = run.call_args.args[0]
        self.assertEqual(cmd, ["go", "run", "./cmd/ate-setup", "deploy", "ate-system", "--credential-provider={}"])
        env = run.call_args.kwargs["env"]
        owners = env["ATE_API_AUTHZ_BOOTSTRAP_OWNERS"].split(",")
        self.assertEqual(
            owners,
            [
                "system:serviceaccount:ate-system:ate-client",
                "spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner",
            ],
        )
        self.assertEqual(
            env["ATE_INGRESS_ALLOWED_SPIFFE_IDS"],
            "spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner",
        )

        orchestrator.deploy_substrate([], extra_ingress_spiffe_ids=("spiffe://cluster.local/ns/foo/sa/bar",))
        env = run.call_args.kwargs["env"]
        self.assertEqual(
            env["ATE_INGRESS_ALLOWED_SPIFFE_IDS"],
            "spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner,spiffe://cluster.local/ns/foo/sa/bar",
        )

    @mock.patch("orchestrator.run")
    def test_keeps_the_rest_of_the_environment(self, run):
        with mock.patch.dict(os.environ, {"KO_DOCKER_REPO": "example"}):
            orchestrator.deploy_substrate()
        self.assertEqual(run.call_args.kwargs["env"]["KO_DOCKER_REPO"], "example")

    @mock.patch("orchestrator.run_no_check")
    def test_teardown_substrate(self, run_no_check):
        orchestrator.teardown_substrate()
        run_no_check.assert_called_once_with(["go", "run", "./cmd/ate-setup", "delete", "ate-system"])


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
    def test_whole_core_actor_cpu_is_a_string(self, run):
        orchestrator.deploy_workloads(actor_cpu=1)
        cmd = run.call_args.args[0]
        i = cmd.index("--actor-cpu")
        self.assertEqual(cmd[i + 1], "1")

    @mock.patch("orchestrator.run")
    def test_all_options(self, run):
        orchestrator.deploy_workloads(
            worker_count=3,
            sandbox_class="microvm",
            actor_memory="1536Mi",
            wait_timeout_secs=600,
            worker_memory="5Gi",
            actor_cpu="250m",
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
                "--actor-cpu",
                "250m",
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
        for name in (
            "runner-job.yaml.tmpl",
            "nighthawk-ingress-runner-job.yaml.tmpl",
            "nighthawk-egress-runner-job.yaml.tmpl",
        ):
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


class NighthawkEgressTest(unittest.TestCase):
    def test_nighthawk_egress_validation_bounds(self):
        from testtypes import TYPES
        egress = TYPES["nighthawk-egress"]
        base = {"name": "t", "duration": "1s", "type": "nighthawk-egress", "nighthawk-egress": {"envoyCpu": 1}}

        # Valid
        b1 = dict(base)
        b1["nighthawk-egress"] = {"envoyCpu": 1, "tailLatencySloMs": 0}
        egress.validate(b1)

        # Invalid expFactor
        b2 = dict(base)
        b2["nighthawk-egress"] = {"envoyCpu": 1, "expFactor": 1.0}
        with self.assertRaisesRegex(ValueError, "expFactor must be > 1.0"):
            egress.validate(b2)
        b3 = dict(base)
        b3["nighthawk-egress"] = {"envoyCpu": 1, "expFactor": 0.5}
        with self.assertRaisesRegex(ValueError, "expFactor must be > 1.0"):
            egress.validate(b3)

        # Invalid sendRateThreshold
        b4 = dict(base)
        b4["nighthawk-egress"] = {"envoyCpu": 1, "sendRateThreshold": 1.5}
        with self.assertRaisesRegex(ValueError, "sendRateThreshold must be > 0 and <= 1.0"):
            egress.validate(b4)

        # Invalid tailLatencySloMs
        b5 = dict(base)
        b5["nighthawk-egress"] = {"envoyCpu": 1, "tailLatencySloMs": -1}
        with self.assertRaisesRegex(ValueError, "tailLatencySloMs must be >= 0"):
            egress.validate(b5)

        # Invalid bool connections
        b6 = dict(base)
        b6["nighthawk-egress"] = {"envoyCpu": 1, "connections": True}
        with self.assertRaisesRegex(ValueError, "connections must be an int >= 1"):
            egress.validate(b6)

    def _entry(self, **overrides):
        cfg = {"envoyCpu": 2, "mode": "mitm", "tailLatencySloMs": 25, **overrides}
        return {"name": "egress_mitm", "type": "nighthawk-egress", "targetCluster": "dev", "duration": "30m", "nighthawk-egress": cfg}

    def test_validate_and_registration(self):
        from testtypes import nighthawk_egress

        self.assertIn("nighthawk-egress", orchestrator.TYPES)

        self.assertFalse(getattr(nighthawk_egress, "USES_SHARED_WORKLOADS", True))
        for mode in ("http", "mitm", "passthrough"):
            nighthawk_egress.validate(self._entry(mode=mode))
        for bad, pattern in (({"mode": "grpc"}, "mode"), ({"unknownKnob": 1}, "unknown knob"), ({"envoyCpu": 0}, "envoyCpu")):
            with self.assertRaisesRegex(ValueError, pattern):
                nighthawk_egress.validate(self._entry(**bad))

    def test_job_template_renders_args_labels_and_toleration(self):
        from testtypes import nighthawk_egress

        test = self._entry(mode="passthrough", envoyCpu=4, tailLatencySloMs=50)
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "6949841e", "NAME": test["name"], "DEST": "gs://b/p", **nighthawk_egress.job_subs(test)}
        text = orchestrator.render_template(nighthawk_egress.job_tmpl(os.path.join(os.path.dirname(__file__), "manifests")), subs)
        self.assertNotIn("${", text)
        job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
        self.assertEqual(job["spec"]["ttlSecondsAfterFinished"], 86400)
        self.assertEqual(job["spec"]["template"]["metadata"]["labels"]["tag"], "6949841e")
        args = job["spec"]["template"]["spec"]["containers"][0]["args"]
        self.assertEqual(args[args.index("--mode") + 1], "passthrough")
        self.assertEqual(args[args.index("--envoy-cpu") + 1], "4")
        self.assertEqual(args[args.index("--connections") + 1], "16")
        self.assertEqual(args[args.index("--tail-latency-slo-ms") + 1], "50")
        mounts = job["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
        self.assertIn({"name": "servicedns-ca", "mountPath": "/run/servicedns-ca", "readOnly": True}, mounts)
        vols = job["spec"]["template"]["spec"]["volumes"]
        ca_vol = next(v for v in vols if v["name"] == "servicedns-ca")
        ctb = ca_vol["projected"]["sources"][0]["clusterTrustBundle"]
        self.assertEqual(ctb["signerName"], "servicedns.podcert.ate.dev/identity")
        self.assertEqual(ctb["path"], "ca.crt")

    def test_tests_yaml_egress_suites(self):
        from testtypes import nighthawk_egress

        path = os.path.join(os.path.dirname(__file__), "tests.yaml")
        with open(path) as f:
            tests = [t for t in yaml.safe_load(f)["tests"] if t.get("type") == "nighthawk-egress"]
        for t in tests:
            nighthawk_egress.validate(t)
            cfg = t["nighthawk-egress"]
            self.assertIn(cfg["envoyCpu"], (1, 2))
            self.assertEqual(cfg["tailLatencySloMs"], 25)

    @mock.patch("testtypes.nighthawk_egress.run")
    def test_pre_test(self, run):
        from testtypes import nighthawk_egress

        nighthawk_egress.pre_test(self._entry(envoyCpu=2, mode="mitm"))
        cmds = [c.args[0] for c in run.call_args_list]
        for expected in (
            "python3",
            "hack/setup-e2e-egress-tls-kind.sh",
            "benchmarking/nighthawk-egress/deploy.sh",
            "benchmarking/nighthawk-egress/deploy-traffic-driver.sh"
        ):
            self.assertTrue(any(expected in cmd for cmd in cmds), expected)

        shape_cmd = next(cmd for cmd in cmds if any("cluster_setup.py" in arg for arg in cmd))
        self.assertEqual(shape_cmd, ["python3", "benchmarking/nighthawk-egress/cluster_setup.py", "shape", "--envoy-cpu", "2"])

    @mock.patch("subprocess.run")
    @mock.patch("orchestrator.run_no_check")
    def test_teardown_workloads_contract(self, run_no_check, sub_run):
        from testtypes import nighthawk_egress

        # Test nighthawk_egress.teardown_workloads directly
        nighthawk_egress.teardown_workloads()
        self.assertEqual(
            [c.args[0] for c in sub_run.call_args_list],
            [
                ["benchmarking/nighthawk-egress/deploy-traffic-driver.sh", "--delete"],
                ["benchmarking/nighthawk-egress/deploy.sh", "--delete"],
            ],
        )

        sub_run.reset_mock()
        run_no_check.reset_mock()

        # Teardown with locust only: runs shared workloads delete, never touches nighthawk-egress scripts
        orchestrator.teardown_test_workloads(["locust"])
        run_no_check.assert_called_once_with(["benchmarking/workloads/deploy.sh", "--delete"])
        sub_run.assert_not_called()

        sub_run.reset_mock()
        run_no_check.reset_mock()

        # Teardown with nighthawk-ingress only: runs shared workloads delete, never touches nighthawk-egress scripts
        orchestrator.teardown_test_workloads(["nighthawk-ingress"])
        run_no_check.assert_called_once_with(["benchmarking/workloads/deploy.sh", "--delete"])
        sub_run.assert_not_called()

        sub_run.reset_mock()
        run_no_check.reset_mock()

        # Teardown with nighthawk-egress only: skips shared workloads delete, runs custom module teardown
        orchestrator.teardown_test_workloads(["nighthawk-egress"])
        run_no_check.assert_not_called()
        self.assertEqual(
            [c.args[0] for c in sub_run.call_args_list],
            [
                ["benchmarking/nighthawk-egress/deploy-traffic-driver.sh", "--delete"],
                ["benchmarking/nighthawk-egress/deploy.sh", "--delete"],
            ],
        )

        sub_run.reset_mock()
        run_no_check.reset_mock()

        # Teardown with all / None: runs both shared workloads and custom module teardown
        orchestrator.teardown_test_workloads(None)
        run_no_check.assert_called_once_with(["benchmarking/workloads/deploy.sh", "--delete"])
        self.assertEqual(
            [c.args[0] for c in sub_run.call_args_list],
            [
                ["benchmarking/nighthawk-egress/deploy-traffic-driver.sh", "--delete"],
                ["benchmarking/nighthawk-egress/deploy.sh", "--delete"],
            ],
        )


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

    def submitted_args(self, ttype, flags=("--x",)):
        """The extra args run_test hands to render_template for a test type."""

        class Rendered(Exception):
            pass

        fake = mock.Mock(job_tmpl=mock.Mock(return_value="t"), job_subs=mock.Mock(return_value={}))
        test = {"name": "n", "type": ttype, "duration": "1m", "flags": list(flags)}
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

    def test_test_metadata_is_merged_not_overridden(self):
        for own in (["--metadata", '{"cluster_name": "mine", "suite": "s"}'],
                    ['--metadata={"cluster_name": "mine", "suite": "s"}']):
            with self.subTest(own=own):
                args = self.submitted_args("locust", ["--x", *own, "--y"])
                # One merged --metadata, after the test's other flags.
                self.assertEqual(args.count("--metadata"), 1)
                self.assertFalse([a for a in args if a.startswith("--metadata=")])
                self.assertEqual(args[:3], ["--x", "--y", "--metadata"])
                md = json.loads(args[3])
                # Test keys win; run keys the test does not set are kept.
                self.assertEqual(md["cluster_name"], "mine")
                self.assertEqual(md["suite"], "s")
                self.assertEqual(md["project_id"], "p")

    def test_bad_test_metadata_fails_clearly(self):
        for own in (["--metadata", "not json"], ["--metadata=[1]"], ["--metadata"]):
            with self.subTest(own=own), \
                 self.assertRaisesRegex(ValueError, "test 'n': --metadata"):
                orchestrator.split_metadata_flag({"name": "n", "flags": own})

        # Caught by validation, before any cluster work.
        test = {"name": "n", "type": "locust", "targetCluster": "t",
                "file": "f", "duration": "1m", "users": 1,
                "flags": ["--metadata", "{bad"]}
        with self.assertRaisesRegex(ValueError, "not valid JSON"):
            orchestrator.validate_and_normalize_tests([test])

    def test_metadata_survives_yaml(self):
        md = json.dumps({"cluster_name": "c: d", "q": "\"'"})
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d"}
        subs.update(locust.job_subs({"file": "f", "duration": "1m", "users": 1}))
        text = orchestrator.render_template(RunnerSizingTest.TMPL, subs, ["--metadata", md])
        job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
        self.assertEqual(job["spec"]["template"]["spec"]["containers"][0]["args"][-1], md)





if __name__ == "__main__":
    unittest.main()
