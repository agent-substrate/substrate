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

import argparse
import json
import os
import subprocess
import tempfile
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
    def test_runner_and_client_are_bootstrap_owners(self, run):
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
    def _entry(self, **overrides):
        cfg = {"envoyCpu": 2, "mode": "mitm", **overrides}
        return {
            "name": "egress_mitm",
            "type": "nighthawk-egress",
            "targetCluster": "dev",
            "duration": "30m",
            "nighthawk-egress": cfg,
        }

    def test_validate_rejects_bad_entries(self):
        from testtypes import nighthawk_egress

        cases = [
            # label, changes to the entry, changes to its nighthawk-egress block, want error
            ("no duration", {"duration": None}, {}, "missing 'duration'"),
            ("microvm", {"sandboxClass": "microvm"}, {}, "only supports sandboxClass 'gvisor'"),
            ("shared-workload field", {"workerCount": 3}, {}, "shared-workload field"),
            ("flags at the top level", {"flags": ["--x"]}, {}, "put knobs under 'nighthawk-egress:'"),
            ("no block", {"nighthawk-egress": None}, {}, "requires 'nighthawk-egress:' block"),
            ("unknown knob", {}, {"connections": 8}, "unknown knob"),
            ("no envoyCpu", {}, {"envoyCpu": None}, "missing nighthawk-egress.envoyCpu"),
            ("envoyCpu zero", {}, {"envoyCpu": 0}, "envoyCpu must be a positive int"),
            ("envoyCpu bool", {}, {"envoyCpu": True}, "envoyCpu must be a positive int"),
            ("envoyCpu string", {}, {"envoyCpu": "1"}, "envoyCpu must be a positive int"),
            ("no mode", {}, {"mode": None}, "missing nighthawk-egress.mode"),
            ("unknown mode", {}, {"mode": "tcp"}, "mode must be one of"),
        ]
        for label, changes, cfg_changes, want in cases:
            with self.subTest(label=label):
                test = {**self._entry(**cfg_changes), **changes}
                # None removes the key.
                test = {k: v for k, v in test.items() if v is not None}
                if test.get("nighthawk-egress"):
                    test["nighthawk-egress"] = {k: v for k, v in test["nighthawk-egress"].items() if v is not None}
                with self.assertRaisesRegex(ValueError, want):
                    nighthawk_egress.validate(test)

    def test_job_template_renders_args_labels_and_toleration(self):
        from testtypes import nighthawk_egress

        test = self._entry(mode="passthrough", envoyCpu=4)
        subs = {
            "JOB_NAME": "j",
            "IMAGE": "i",
            "TAG": "6949841e",
            "NAME": test["name"],
            "DEST": "gs://b/p",
            **nighthawk_egress.job_subs(test),
        }
        tmpl = nighthawk_egress.job_tmpl(os.path.join(os.path.dirname(__file__), "manifests"))
        text = orchestrator.render_template(tmpl, subs)
        self.assertNotIn("${", text)
        job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
        self.assertEqual(job["spec"]["ttlSecondsAfterFinished"], 86400)
        self.assertEqual(job["spec"]["template"]["metadata"]["labels"]["tag"], "6949841e")
        self.assertIn(
            {"key": "ate.dev/benchmarking", "operator": "Equal", "value": "true", "effect": "NoSchedule"},
            job["spec"]["template"]["spec"]["tolerations"],
        )
        args = job["spec"]["template"]["spec"]["containers"][0]["args"]
        self.assertEqual(args[args.index("--mode") + 1], "passthrough")
        self.assertEqual(args[args.index("--envoy-cpu") + 1], "4")
        mounts = job["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
        self.assertIn({"name": "servicedns-ca", "mountPath": "/run/servicedns-ca", "readOnly": True}, mounts)
        vols = job["spec"]["template"]["spec"]["volumes"]
        ca_vol = next(v for v in vols if v["name"] == "servicedns-ca")
        ctb = ca_vol["projected"]["sources"][0]["clusterTrustBundle"]
        self.assertEqual(ctb["signerName"], "servicedns.podcert.ate.dev/identity")
        self.assertEqual(ctb["path"], "ca.crt")

    def test_job_template_runs_the_invoker_sidecar(self):
        from testtypes import nighthawk_egress

        test = self._entry()
        subs = {
            "JOB_NAME": "j",
            "IMAGE": "i",
            "TAG": "6949841e",
            "NAME": test["name"],
            "DEST": "gs://b/p",
            **nighthawk_egress.job_subs(test),
        }
        tmpl = nighthawk_egress.job_tmpl(os.path.join(os.path.dirname(__file__), "manifests"))
        docs = [d for d in yaml.safe_load_all(orchestrator.render_template(tmpl, subs)) if d]
        pod = next(d for d in docs if d["kind"] == "Job")["spec"]["template"]["spec"]
        self.assertEqual(pod["serviceAccountName"], "benchmark-runner")

        # restartPolicy Always makes the init container a sidecar: it runs
        # beside the runner and does not keep the Job from completing.
        (invoker,) = pod["initContainers"]
        self.assertEqual(invoker["name"], "invoker")
        self.assertEqual(invoker["restartPolicy"], "Always")
        self.assertEqual(pod["containers"][0]["name"], "runner")

        volumes = {v["name"]: v for v in pod["volumes"]}
        mounts = {m["name"]: m["mountPath"] for m in invoker["volumeMounts"]}
        config = next(
            d for d in docs
            if d["kind"] == "ConfigMap" and d["metadata"]["name"] == volumes["invoker-config"]["configMap"]["name"]
        )
        self.assertEqual(config["metadata"]["namespace"], "benchmarking")
        envoy = yaml.safe_load(config["data"]["envoy.yaml"])

        # Only the runner, in the same pod, reaches the listener. A kubelet
        # probe cannot.
        (listener,) = envoy["static_resources"]["listeners"]
        self.assertEqual(listener["address"]["socket_address"], {"address": "127.0.0.1", "port_value": 19980})
        self.assertNotIn("startupProbe", invoker)

        # Every file the Envoy config names must exist in the sidecar's mounts.
        tls = envoy["static_resources"]["clusters"][0]["transport_socket"]["typed_config"]["common_tls_context"]
        cert_sds = tls["tls_certificate_sds_secret_configs"][0]["sds_config"]["path_config_source"]["path"]
        ca_sds = tls["combined_validation_context"]["validation_context_sds_secret_config"]["sds_config"][
            "path_config_source"
        ]["path"]
        prefix = mounts["invoker-config"] + "/"
        for path in (cert_sds, ca_sds):
            self.assertTrue(path.startswith(prefix), path)
            self.assertIn(path[len(prefix):], config["data"])
        cert = yaml.safe_load(config["data"][cert_sds[len(prefix):]])["resources"][0]["tls_certificate"]
        pod_cert = volumes["podidentity"]["projected"]["sources"][0]["podCertificate"]
        self.assertEqual(pod_cert["signerName"], "podidentity.podcert.ate.dev/identity")
        want = f"{mounts['podidentity']}/{pod_cert['credentialBundlePath']}"
        self.assertEqual(cert["certificate_chain"]["filename"], want)
        self.assertEqual(cert["private_key"]["filename"], want)
        ca = yaml.safe_load(config["data"][ca_sds[len(prefix):]])["resources"][0]["validation_context"]
        bundle = volumes["servicedns-ca"]["projected"]["sources"][0]["clusterTrustBundle"]
        self.assertEqual(ca["trusted_ca"]["filename"], f"{mounts['servicedns-ca']}/{bundle['path']}")

    def _deployment_json(self, envoy_args: list[str]) -> str:
        containers = [{"name": "envoy", "args": envoy_args}, {"name": "ext-proc", "args": ["router", "--mode=egress"]}]
        return json.dumps({"spec": {"replicas": 2, "template": {"spec": {"containers": containers}}}})

    @mock.patch("testtypes.nighthawk_egress.run")
    def test_shape_atenet_egress(self, run):
        from testtypes import nighthawk_egress

        cases = [
            # label, envoy args of the live Deployment, want envoy args in the patch
            (
                "no concurrency flag",
                ["-c", "/etc/envoy/envoy.yaml", "--service-node", "atenet-egress"],
                ["-c", "/etc/envoy/envoy.yaml", "--service-node", "atenet-egress", "--concurrency", "2"],
            ),
            (
                "two-argument flag",
                ["-c", "/etc/envoy/envoy.yaml", "--concurrency", "1", "--service-node", "atenet-egress"],
                ["-c", "/etc/envoy/envoy.yaml", "--service-node", "atenet-egress", "--concurrency", "2"],
            ),
        ]
        cpu = {"requests": {"cpu": "2000m"}, "limits": {"cpu": "2000m"}}
        for label, envoy_args, want_args in cases:
            with self.subTest(label=label):
                run.reset_mock()
                run.side_effect = [
                    subprocess.CompletedProcess(args=[], returncode=0, stdout=self._deployment_json(envoy_args)),
                    subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
                    subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
                ]
                nighthawk_egress.shape_atenet_egress(2)

                get, patch, rollout = (c.args[0] for c in run.call_args_list)
                self.assertEqual(get, ["kubectl", "-n", "ate-system", "get", "deployment/atenet-egress", "-o", "json"])
                # stdout only, so the stderr of a failed read stays on the log.
                self.assertEqual(run.call_args_list[0].kwargs, {"stdout": subprocess.PIPE, "text": True})
                self.assertEqual(
                    patch[:7],
                    ["kubectl", "-n", "ate-system", "patch", "deployment/atenet-egress", "--type=strategic", "-p"],
                )
                self.assertEqual(
                    json.loads(patch[7]),
                    {
                        "spec": {
                            "replicas": 1,
                            "template": {
                                "spec": {
                                    "containers": [
                                        {"name": "envoy", "args": want_args, "resources": cpu},
                                        {"name": "ext-proc", "resources": cpu},
                                    ]
                                }
                            },
                        }
                    },
                )
                self.assertEqual(
                    rollout,
                    ["kubectl", "-n", "ate-system", "rollout", "status", "deployment/atenet-egress", "--timeout=300s"],
                )

    @mock.patch("testtypes.nighthawk_egress.run")
    def test_shape_atenet_egress_needs_the_envoy_container(self, run):
        from testtypes import nighthawk_egress

        dep = {"spec": {"template": {"spec": {"containers": [{"name": "ext-proc"}]}}}}
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=json.dumps(dep))
        with self.assertRaisesRegex(RuntimeError, "no envoy container"):
            nighthawk_egress.shape_atenet_egress(1)
        run.assert_called_once()

    @mock.patch("testtypes.nighthawk_egress.run")
    def test_pre_test(self, run):
        from testtypes import nighthawk_egress

        run.side_effect = [
            subprocess.CompletedProcess(args=[], returncode=0, stdout=self._deployment_json(["-c", "/etc/envoy/envoy.yaml"])),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout="gke_proj_loc_cluster\n"),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
        ]
        nighthawk_egress.pre_test(self._entry(envoyCpu=2, mode="mitm"))

        # The first three calls pin the gateway, see test_shape_atenet_egress.
        context, tls, server, driver = run.call_args_list[3:]
        self.assertEqual(context.args[0], ["kubectl", "config", "current-context"])
        self.assertEqual(context.kwargs, {"stdout": subprocess.PIPE, "text": True})
        self.assertEqual(tls.args[0], ["hack/setup-e2e-egress-tls-kind.sh"])
        self.assertEqual(tls.kwargs["env"]["KUBECTL_CONTEXT"], "gke_proj_loc_cluster")
        self.assertEqual(server.args[0], ["benchmarking/nighthawk-egress/deploy.sh", "--deploy"])
        self.assertEqual(driver.args[0], ["benchmarking/nighthawk-egress/deploy-traffic-driver.sh", "--deploy"])

    def test_main_deploys_shared_workloads_only_for_types_that_use_them(self):
        from testtypes import nighthawk_egress

        ingress_entry = {
            "name": "ingress_2cpu",
            "type": "nighthawk-ingress",
            "targetCluster": "dev",
            "duration": "30m",
            "workerCount": 50,
            "nighthawk-ingress": {"envoyCpu": 2, "tailLatencySloMs": 25},
        }
        cases = [
            # label, tests.yaml entry, want deploy_workloads calls, want egress teardown calls
            ("nighthawk-ingress", ingress_entry, 1, 1),  # the sweep before the test
            ("nighthawk-egress", self._entry(), 0, 2),  # the sweep, and the teardown after the test
        ]
        for label, entry, want_deploys, want_egress_teardowns in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                tests_path = os.path.join(tmp, "tests.yaml")
                with open(tests_path, "w") as f:
                    yaml.safe_dump({"tests": [entry]}, f)
                args = argparse.Namespace(
                    tests=tests_path,
                    test_name=None,
                    branch="b",
                    repo="r",
                    dest="gs://b/p",
                    target_cluster_dir=tmp,
                    manifests_dir=tmp,
                    junit_output=None,
                )
                ttype = orchestrator.TYPES[entry["type"]]
                with (
                    mock.patch("orchestrator.parse_args", return_value=args),
                    mock.patch("orchestrator.wait_for_docker"),
                    mock.patch("orchestrator.run"),
                    mock.patch("orchestrator.run_no_check"),
                    mock.patch("orchestrator.os.chdir"),
                    mock.patch("orchestrator.subprocess.check_output", return_value="abc123\n"),
                    mock.patch("orchestrator.apply_target_cluster"),
                    mock.patch("orchestrator.gcloud_setup_for_target_cluster"),
                    mock.patch("orchestrator.clear_target_cluster"),
                    mock.patch("orchestrator.deploy_substrate"),
                    mock.patch("orchestrator.teardown_substrate"),
                    mock.patch("orchestrator.run_test", return_value="complete"),
                    mock.patch("orchestrator.deploy_workloads") as deploy_workloads,
                    mock.patch.object(ttype, "build_image", return_value="img"),
                    mock.patch.object(ttype, "pre_test"),
                    mock.patch.object(nighthawk_egress, "teardown_workloads") as egress_teardown,
                    self.assertRaises(SystemExit) as exit_,
                ):
                    orchestrator.main()
                self.assertEqual(exit_.exception.code, 0)
                self.assertEqual(deploy_workloads.call_count, want_deploys)
                self.assertEqual(egress_teardown.call_count, want_egress_teardowns)

    @mock.patch("testtypes.nighthawk_egress.run_no_check")
    @mock.patch("orchestrator.run_no_check")
    def test_teardown_workloads_contract(self, orch_run_no_check, egress_run_no_check):
        # Locust-only teardown runs shared workloads delete and never touches nighthawk-egress.
        orchestrator.teardown_test_workloads(["locust"])
        orch_run_no_check.assert_called_once_with(["benchmarking/workloads/deploy.sh", "--delete"])
        egress_run_no_check.assert_not_called()

        orch_run_no_check.reset_mock()
        egress_run_no_check.reset_mock()

        # nighthawk-ingress-only teardown runs shared workloads delete and never touches nighthawk-egress.
        orchestrator.teardown_test_workloads(["nighthawk-ingress"])
        orch_run_no_check.assert_called_once_with(["benchmarking/workloads/deploy.sh", "--delete"])
        egress_run_no_check.assert_not_called()

        orch_run_no_check.reset_mock()
        egress_run_no_check.reset_mock()

        # The sweep over every type skips deploy-traffic-driver.sh --delete when namespace/nighthawk does not exist,
        # so locust / nighthawk-ingress runs do not need kubectl-ate.
        egress_run_no_check.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="")
        orchestrator.teardown_test_workloads(orchestrator.TYPES)
        orch_run_no_check.assert_called_once_with(["benchmarking/workloads/deploy.sh", "--delete"])
        egress_run_no_check.assert_called_once_with(
            ["kubectl", "get", "namespace", "nighthawk", "-o", "name", "--ignore-not-found"],
            stdout=subprocess.PIPE,
            text=True,
        )

        orch_run_no_check.reset_mock()
        egress_run_no_check.reset_mock()

        # nighthawk-egress teardown when ate-api-server exists runs both delete scripts and deletes the namespace.
        egress_run_no_check.side_effect = [
            subprocess.CompletedProcess(args=[], returncode=0, stdout="namespace/nighthawk\n"),
            subprocess.CompletedProcess(args=[], returncode=0, stdout="deployment.apps/ate-api-server\n"),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
        ]
        orchestrator.teardown_test_workloads(["nighthawk-egress"])
        orch_run_no_check.assert_not_called()
        self.assertEqual(
            [c.args[0] for c in egress_run_no_check.call_args_list],
            [
                ["kubectl", "get", "namespace", "nighthawk", "-o", "name", "--ignore-not-found"],
                ["kubectl", "get", "deployment", "ate-api-server", "-n", "ate-system", "-o", "name", "--ignore-not-found"],
                ["benchmarking/nighthawk-egress/deploy-traffic-driver.sh", "--delete"],
                ["benchmarking/nighthawk-egress/deploy.sh", "--delete"],
                ["kubectl", "delete", "namespace", "nighthawk", "--ignore-not-found"],
            ],
        )

        egress_run_no_check.reset_mock()

        # When namespace/nighthawk exists but ate-api-server is gone, delete K8s manifests directly, without kubectl-ate.
        egress_run_no_check.side_effect = [
            subprocess.CompletedProcess(args=[], returncode=0, stdout="namespace/nighthawk\n"),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
        ]
        orchestrator.teardown_test_workloads(["nighthawk-egress"])
        self.assertEqual(
            [c.args[0] for c in egress_run_no_check.call_args_list],
            [
                ["kubectl", "get", "namespace", "nighthawk", "-o", "name", "--ignore-not-found"],
                ["kubectl", "get", "deployment", "ate-api-server", "-n", "ate-system", "-o", "name", "--ignore-not-found"],
                ["kubectl", "delete", "--ignore-not-found", "-f", "benchmarking/nighthawk-egress/traffic-driver.yaml.tmpl"],
                ["benchmarking/nighthawk-egress/deploy.sh", "--delete"],
                ["kubectl", "delete", "namespace", "nighthawk", "--ignore-not-found"],
            ],
        )

        egress_run_no_check.reset_mock()

        # A failed read of namespace/nighthawk does not skip the teardown.
        egress_run_no_check.side_effect = [
            subprocess.CompletedProcess(args=[], returncode=1, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
        ]
        orchestrator.teardown_test_workloads(["nighthawk-egress"])
        self.assertEqual(
            [c.args[0] for c in egress_run_no_check.call_args_list],
            [
                ["kubectl", "get", "namespace", "nighthawk", "-o", "name", "--ignore-not-found"],
                ["kubectl", "get", "deployment", "ate-api-server", "-n", "ate-system", "-o", "name", "--ignore-not-found"],
                ["kubectl", "delete", "--ignore-not-found", "-f", "benchmarking/nighthawk-egress/traffic-driver.yaml.tmpl"],
                ["benchmarking/nighthawk-egress/deploy.sh", "--delete"],
                ["kubectl", "delete", "namespace", "nighthawk", "--ignore-not-found"],
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
