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
import contextlib
import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

import orchestrator
from testtypes import locust


class ParseArgsTest(unittest.TestCase):
    # This checkout, relative to the test's cwd, which pytest and the
    # unittest invocation in the docstring both make this directory.
    CHECKOUT = os.path.relpath(os.path.join(os.path.dirname(__file__), "..", ".."))

    def parse(self, *argv):
        return orchestrator.parse_args(["--dest", "gs://b", *argv])

    def assertRejected(self, *argv):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.parse(*argv)

    def test_repo_defaults(self):
        args = self.parse("--repo", "r")
        self.assertEqual(args.branch, "main")
        self.assertEqual(args.target_cluster_dir, orchestrator.TARGET_CLUSTER_DIR)
        self.assertIsNone(args.substrate_dir)
        self.assertIsNone(args.env_file)

    def test_repo_or_substrate_dir_required(self):
        self.assertRejected()

    def test_substrate_dir_rejects_clone_flags(self):
        self.assertIsNone(self.parse("--substrate-dir", self.CHECKOUT).branch)
        self.assertRejected("--substrate-dir", self.CHECKOUT, "--repo", "r")
        self.assertRejected("--substrate-dir", self.CHECKOUT, "--branch", "b")

    def test_substrate_dir_must_be_a_checkout(self):
        with tempfile.TemporaryDirectory() as empty:
            self.assertRejected("--substrate-dir", empty)
        self.assertRejected("--substrate-dir", "/nonexistent")

    def test_env_file_rejects_target_cluster_dir(self):
        self.assertIsNone(self.parse("--repo", "r", "--env-file", "e").target_cluster_dir)
        self.assertRejected("--repo", "r", "--env-file", "e", "--target-cluster-dir", "d")

    def test_paths_are_made_absolute(self):
        # main chdirs into the checkout before reading most of them.
        args = self.parse(
            "--substrate-dir", self.CHECKOUT, "--env-file", "e", "--tests", "t.yaml",
            "--manifests-dir", "m", "--junit-output", "out/j.xml",
        )
        for name in ("substrate_dir", "env_file", "tests", "manifests_dir", "junit_output"):
            self.assertTrue(os.path.isabs(getattr(args, name)), name)
        self.assertTrue(os.path.isabs(self.parse("--repo", "r").target_cluster_dir))


ENV = (
    "export PROJECT_ID=p\n"
    "export CLUSTER_NAME=c\n"
    "export CLUSTER_LOCATION=l\n"
    "export KO_DOCKER_REPO=r/x\n"
)


class EnvFileTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.checkout = Path(tmp.name) / "substrate"
        self.checkout.mkdir()
        self.own = self.checkout / orchestrator.ENV_FILE_NAME
        self.clusters = Path(tmp.name) / "clusters"
        self.clusters.mkdir()
        self.dev = self.clusters / "dev.sh"
        self.dev.write_text(ENV)
        patch = mock.patch.dict(os.environ, {}, clear=True)
        patch.start()
        self.addCleanup(patch.stop)

    def args(self, **kw):
        a = {"env_file": None, "target_cluster_dir": str(self.clusters), **kw}
        return mock.Mock(**a)

    def test_env_file_for(self):
        test = {"targetCluster": "dev"}
        self.assertEqual(orchestrator.env_file_for(test, self.args()), self.dev)
        self.assertEqual(
            orchestrator.env_file_for(test, self.args(env_file="/e")), Path("/e")
        )

    def test_target_cluster_file_is_copied_then_removed(self):
        orchestrator.apply_env_file(self.dev, str(self.checkout))
        self.assertEqual(self.own.read_text(), ENV)
        self.assertEqual(os.environ["PROJECT_ID"], "p")
        orchestrator.clear_env_file(self.dev, str(self.checkout))
        self.assertFalse(self.own.exists())

    def test_own_env_file_is_sourced_in_place_and_kept(self):
        self.own.write_text(ENV)
        orchestrator.apply_env_file(self.own, str(self.checkout))
        self.assertEqual(os.environ["CLUSTER_NAME"], "c")
        orchestrator.clear_env_file(self.own, str(self.checkout))
        self.assertEqual(self.own.read_text(), ENV)

    def test_missing_var(self):
        self.dev.write_text("export PROJECT_ID=p\n")
        with self.assertRaisesRegex(RuntimeError, "KO_DOCKER_REPO"):
            orchestrator.apply_env_file(self.dev, str(self.checkout))

    def test_exported_bash_function_does_not_pollute_env(self):
        # env prints a two-statement function body over two lines, so a
        # newline parser would turn the second into the key " Y".
        self.dev.write_text(ENV + "f() {\n  X=1\n  Y=2\n}\nexport -f f\n")
        orchestrator.apply_env_file(self.dev, str(self.checkout))
        self.assertEqual(os.environ["KO_DOCKER_REPO"], "r/x")
        self.assertNotIn(" Y", os.environ)
        self.assertTrue(os.environ["BASH_FUNC_f%%"].endswith("}"))

    def test_selector_and_unset_values_come_from_env_file(self):
        startup = dict.fromkeys(orchestrator.CLUSTER_SELECTORS, "caller")
        startup["BENCHMARK_METADATA"] = '{"build": "caller"}'
        self.dev.write_text("echo sourcing\nKO_DOCKER_REPO=r/x\n"
                            "KUBECTL_CONTEXT=bench\nunset BENCHMARK_METADATA\n")
        with mock.patch.object(orchestrator, "_ORIG_ENV", startup):
            orchestrator.apply_env_file(self.dev, str(self.checkout))
        self.assertEqual(
            {k: os.environ[k] for k in startup if k in os.environ},
            {"KUBECTL_CONTEXT": "bench"},
        )
        self.assertEqual(os.environ["KO_DOCKER_REPO"], "r/x")

    def test_check_env_files(self):
        dev = {"targetCluster": "dev"}
        prod = {"targetCluster": "prod"}
        checkout = str(self.checkout)
        orchestrator.check_env_files([dev], self.args(), checkout)
        # A missing target cluster file fails its own tests, in main.
        orchestrator.check_env_files([dev, prod], self.args(), checkout)
        own = self.args(env_file=str(self.own))
        with self.assertRaisesRegex(ValueError, "not found"):
            orchestrator.check_env_files([dev], own, checkout)
        self.own.write_text(ENV)
        # --env-file overrides targetCluster, so prod.sh need not exist.
        orchestrator.check_env_files([dev, prod], own, checkout)
        # A copied file would overwrite the checkout's own.
        with self.assertRaisesRegex(ValueError, "overwrite"):
            orchestrator.check_env_files([dev], self.args(), checkout)


class BuildTagTest(unittest.TestCase):
    @mock.patch("orchestrator.subprocess.check_output")
    def test_clean_tree_is_the_commit(self, check_output):
        check_output.side_effect = ["abc123\n", ""]
        self.assertEqual(orchestrator.build_tag("/s"), "abc123")

    @mock.patch("orchestrator.subprocess.check_output")
    def test_dirty_tree_gets_a_unique_suffix(self, check_output):
        check_output.side_effect = ["abc123\n", " M x.py\n", "abc123\n", " M x.py\n"]
        a, b = orchestrator.build_tag("/s"), orchestrator.build_tag("/s")
        self.assertRegex(a, r"^abc123-dirty-[0-9a-f]{6}$")
        self.assertNotEqual(a, b)


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
    def test_cluster_overrides_belong_in_the_env_file(self):
        for arg in ("--context=other", "--kubeconfig", "--kind", "--kind-cluster-name"):
            with self.subTest(arg=arg), self.assertRaisesRegex(ValueError, "target env file"):
                orchestrator.validate_and_normalize_tests([
                    {"name": "bench", "targetCluster": "dev", "ateArgs": [arg]},
                ])
        for arg in ("--config", "--config=install.yaml"):
            with self.subTest(arg=arg), self.assertRaisesRegex(ValueError, "not supported"):
                orchestrator.validate_and_normalize_tests([
                    {"name": "bench", "targetCluster": "dev", "ateArgs": [arg]},
                ])

    def test_ate_args_type_validation(self):
        self.assertEqual(
            orchestrator.substrate_args({"ateArgs": None}),
            ["--sandbox-classes=gvisor"],
        )
        orchestrator.validate_and_normalize_tests([
            {
                "name": "bench", "targetCluster": "dev", "type": "locust",
                "file": "f.py", "duration": "1m", "users": 1, "ateArgs": None,
            },
        ])
        for bad in ("--cluster-size=size10", [1], ["--cluster-size=size10", True]):
            with self.subTest(ateArgs=bad), self.assertRaisesRegex(ValueError, "list of strings"):
                orchestrator.validate_and_normalize_tests([
                    {"name": "bench", "targetCluster": "dev", "ateArgs": bad},
                ])

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
        cluster_args = ["--kubeconfig", "/config", "--context", "bench"]
        orchestrator.teardown_substrate(cluster_args)
        run_no_check.assert_called_once_with(
            ["go", "run", "./cmd/ate-setup", "delete", "ate-system", *cluster_args]
        )


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
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d", "RUN_TS": 1}
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
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d", "RUN_TS": 1}
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
            "RUN_TS": 1,
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
            "RUN_TS": 1,
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
                    substrate_dir=None,
                    dest="gs://b/p",
                    target_cluster_dir=tmp,
                    env_file=None,
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
                    # One patch for four: Python caps nested blocks at 20.
                    mock.patch.multiple(
                        "orchestrator",
                        build_tag=mock.DEFAULT,
                        check_env_files=mock.DEFAULT,
                        apply_env_file=mock.DEFAULT,
                        parse_target_cluster_env=mock.DEFAULT,
                        clear_env_file=mock.DEFAULT,
                    ),
                    mock.patch.dict(os.environ),
                    mock.patch("orchestrator.prepare_target_cluster",
                               return_value={"KUBECONFIG": "/config", "KUBECTL_CONTEXT": "bench"}),
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


class TargetClusterCredentialsTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.snapshot = self.root / "target.json"
        self.view = {
            "current-context": "bench", "clusters": [{"cluster": {"server": "https://bench.invalid"}}],
            "users": None,
        }

    @mock.patch("orchestrator.run")
    def test_auth_paths_use_the_first_file_defining_the_user(self, run):
        first, second, third = (self.root / name / "config" for name in ("a", "b", "c"))
        auth = {"tokenFile": "current/../token", "exec": {"command": "bin/../get-token"}}
        config = {"users": [{"name": "2026-11-01", "user": auth}]}
        run.side_effect = [mock.Mock(stdout='["other"]'), mock.Mock(stdout='["2026-11-01"]')]
        with mock.patch.dict(os.environ, {"KUBECONFIG": os.pathsep.join(map(str, (first, second, third)))}):
            orchestrator.prepare_kubeconfig_users(config)
        self.assertEqual([call.args[0][2] for call in run.call_args_list], [str(first), str(second)])
        self.assertEqual(auth["tokenFile"], str(second.parent / "token"))
        self.assertEqual(auth["exec"]["command"], str(second.parent / "get-token"))

    @mock.patch("orchestrator.run")
    def test_existing_credentials_use_private_kubeconfig(self, run):
        cases = [
            ({"KUBECONFIG": "/a:/b"}, "bench", []),
            ({"KUBECTL_CONTEXT": "explicit"}, "explicit", ["--context", "explicit"]),
            ({"ATE_INSTALL_KIND": "true", "KIND_CLUSTER_NAME": "local"},
             "kind-local", ["--context", "kind-local"]),
            ({"ATE_INSTALL_KIND": "1"}, "kind-kind", ["--context", "kind-kind"]),
        ]
        for env, context, flags in cases:
            with self.subTest(env=env), mock.patch.dict(os.environ, env, clear=True):
                run.reset_mock()
                view = {**self.view, "current-context": context}
                run.return_value.stdout = json.dumps(view)
                self.snapshot.write_text("previous kubeconfig")
                target = orchestrator.prepare_target_cluster(self.snapshot, configure_docker=True)
                config, probe = run.call_args_list
                self.assertEqual(config.args[0], [
                    "kubectl", "config", "view", "--minify", "-o", "json",
                    *flags, "--flatten", "--raw",
                ])
                self.assertEqual(target, {"KUBECONFIG": str(self.snapshot), "KUBECTL_CONTEXT": context})
                self.assertEqual(probe.kwargs["env"], {**env, **target})
                self.assertEqual(json.loads(self.snapshot.read_text()), view)
                self.assertEqual(self.snapshot.stat().st_mode & 0o777, 0o600)

    @mock.patch("orchestrator.shutil.which", return_value="/tools/gcloud")
    @mock.patch("orchestrator.run")
    def test_gke_uses_private_kubeconfig(self, run, which):
        def run_in_target(cmd, **kwargs):
            if cmd[:3] == ["gcloud", "container", "clusters"]:
                self.assertEqual(os.environ["KUBECONFIG"], str(self.snapshot))
                self.assertFalse(self.snapshot.exists())
                self.snapshot.write_text(json.dumps(self.view))
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(self.view))

        run.side_effect = run_in_target
        env = {**CLUSTER_ENV, "KO_DOCKER_REPO": "gcr.io/p"}
        with mock.patch.dict(os.environ, env, clear=True):
            self.snapshot.write_text("previous target")
            target = orchestrator.prepare_target_cluster(self.snapshot, configure_docker=True)
        self.assertEqual([c.args[0] for c in run.call_args_list[:2]], [
            ["gcloud", "auth", "configure-docker", "gcr.io", "--quiet"],
            ["gcloud", "container", "clusters", "get-credentials", "c",
             "--location", "l", "--project", "p"],
        ])
        self.assertEqual(target, {"KUBECONFIG": str(self.snapshot), "KUBECTL_CONTEXT": "bench"})
        self.assertEqual(json.loads(self.snapshot.read_text()), self.view)

    @mock.patch("orchestrator.run")
    def test_missing_or_mixed_credentials_fail_before_kubectl(self, run):
        cases = [
            ({}, "Set KUBECONFIG or KUBECTL_CONTEXT"),
            ({"PROJECT_ID": "p"}, "CLUSTER_NAME, CLUSTER_LOCATION missing"),
            ({"ATE_CONFIG": "/install.yaml", "KUBECTL_CONTEXT": "bench"}, "ATE_CONFIG is not supported"),
            ({"ATE_INSTALL_KIND": "TRUE"}, "ATE_INSTALL_KIND must be true or false"),
            ({"ATE_INSTALL_KIND": "t"}, "ATE_INSTALL_KIND must be true or false"),
            ({"ATE_INSTALL_KIND": "false"}, "Set KUBECONFIG or KUBECTL_CONTEXT"),
            ({**CLUSTER_ENV, "KUBECTL_CONTEXT": "legacy"}, "remove KUBECTL_CONTEXT from the env file"),
            ({**CLUSTER_ENV, "KUBECONFIG": "/config"}, "remove KUBECONFIG from the env file"),
            ({**CLUSTER_ENV, "ATE_INSTALL_KIND": "true"}, "remove ATE_INSTALL_KIND from the env file"),
            (
                {**CLUSTER_ENV, "KUBECTL_CONTEXT": "legacy", "KUBECONFIG": "/config"},
                "remove KUBECTL_CONTEXT, KUBECONFIG from the env file",
            ),
        ]
        for env, error in cases:
            with self.subTest(env=env), mock.patch.dict(os.environ, env, clear=True):
                with self.assertRaisesRegex(ValueError, error):
                    orchestrator.prepare_target_cluster(self.snapshot, configure_docker=True)
        run.assert_not_called()

    @mock.patch("orchestrator.teardown_substrate")
    @mock.patch("orchestrator.teardown_test_workloads")
    @mock.patch("orchestrator.run")
    def test_failed_preflight_skips_cleanup(self, run, workloads, substrate):
        oidc = {"name": "oidc", "config": {"refresh-token": "fixture"}}
        cases = [
            ({"clusters": [{"cluster": {"server": ""}}]}, "no cluster server", 1),
            ({"users": [{"name": "bench", "user": {"auth-provider": oidc}}]}, "exec credential plugin", 1),
            ({}, "returned non-zero exit status", 2),
        ]
        test = {"name": "sample", "targetCluster": "bench", "type": "locust"}
        script = self.root / "bench.sh"
        script.write_text("KO_DOCKER_REPO=r/x\nKUBECTL_CONTEXT=bench\n")
        args = argparse.Namespace(substrate_dir=str(self.root), env_file=str(script))
        for fields, message, expected_calls in cases:
            with self.subTest(message=message), mock.patch.dict(os.environ):
                run.reset_mock()
                run.side_effect = [
                    mock.Mock(stdout=json.dumps({**self.view, **fields})),
                    subprocess.CalledProcessError(1, ["kubectl", "get"]),
                ]
                result, = orchestrator.run_tests([test], args, "tag", 0, self.snapshot)
                self.assertEqual(result[1], "config-error")
                self.assertIn(message, result[3])
                self.assertEqual(run.call_count, expected_calls)
                workloads.assert_not_called()
                substrate.assert_not_called()

    def test_failed_target_script_logs_diagnostic_and_removes_copy(self):
        (self.root / "bench.sh").write_text("echo 'fixture diagnostic' >&2\nfalse\n")
        test = {"name": "sample", "targetCluster": "bench"}
        args = argparse.Namespace(
            substrate_dir=str(self.root), env_file=None, target_cluster_dir=str(self.root),
        )
        with (
            mock.patch.dict(os.environ),
            mock.patch("orchestrator.sys.stderr") as stderr,
            mock.patch("orchestrator.prepare_target_cluster") as prepare,
        ):
            result, = orchestrator.run_tests([test], args, "tag", 0, self.snapshot)
        self.assertEqual(result[1], "config-error")
        stderr.write.assert_any_call("fixture diagnostic\n")
        prepare.assert_not_called()
        self.assertFalse((self.root / orchestrator.ENV_FILE_NAME).exists())

    def test_env_file_override_pins_workloads_and_cleanup(self):
        env_file = self.root / orchestrator.ENV_FILE_NAME
        content = "KO_DOCKER_REPO=r/x\nKUBECONFIG=/input\nKUBECTL_CONTEXT=bench\n"
        env_file.write_text(content)
        args = argparse.Namespace(
            substrate_dir=str(self.root), env_file=str(env_file), dest="dest", manifests_dir="m",
        )
        tests = [{"name": c, "targetCluster": c, "type": "locust"} for c in ("dev", "prod")]
        target = {"KUBECONFIG": str(self.snapshot), "KUBECTL_CONTEXT": "bench"}

        def check_target(*_args):
            self.assertEqual({k: os.environ[k] for k in target}, target)
            self.assertEqual(os.environ["NO_DEV_ENV"], "1")

        ttype = mock.Mock(USES_SHARED_WORKLOADS=True)
        with (
            mock.patch.dict(orchestrator.TYPES, {"locust": ttype}, clear=True),
            mock.patch.dict(os.environ),
            mock.patch("orchestrator.prepare_target_cluster", return_value=target) as prepare,
            mock.patch("orchestrator.deploy_workloads", side_effect=check_target) as deploy,
            mock.patch("orchestrator.teardown_test_workloads", side_effect=check_target) as cleanup,
            mock.patch("orchestrator.deploy_substrate", side_effect=check_target) as install,
            mock.patch("orchestrator.teardown_substrate", side_effect=check_target) as uninstall,
            mock.patch("orchestrator.run_test", return_value="complete"),
        ):
            results = orchestrator.run_tests(tests, args, "tag", 0, self.snapshot)
        self.assertEqual([result[1] for result in results], ["complete", "complete"])
        prepare.assert_called_once_with(self.snapshot, configure_docker=False)
        self.assertEqual(deploy.call_count, 2)
        self.assertEqual(cleanup.call_count, 4)
        flags = ["--kubeconfig", str(self.snapshot), "--context", "bench"]
        self.assertEqual(install.call_args.args[0][-len(flags):], flags)
        self.assertEqual(uninstall.call_args_list, [mock.call(flags)] * 4)
        self.assertEqual(env_file.read_text(), content)


    def test_failed_target_switch_invalidates_previous_kubeconfig(self):
        for context in ("bench", "broken"):
            (self.root / f"{context}.sh").write_text(f"KO_DOCKER_REPO=r/x\nKUBECTL_CONTEXT={context}\n")
        args = argparse.Namespace(
            substrate_dir=str(self.root), env_file=None, target_cluster_dir=str(self.root),
            dest="dest", manifests_dir="m",
        )
        tests = [
            {"name": c, "targetCluster": c, "type": "locust"}
            for c in ("bench", "broken", "bench")
        ]

        def prepare(path, **_kwargs):
            context = os.environ["KUBECTL_CONTEXT"]
            path.write_text(context)
            if context == "broken":
                raise ValueError("unreachable target")
            return {"KUBECONFIG": str(path), "KUBECTL_CONTEXT": context}

        def check_target(*_args):
            self.assertEqual(self.snapshot.read_text(), "bench")

        with (
            mock.patch.dict(orchestrator.TYPES, {"locust": mock.Mock()}, clear=True),
            mock.patch.dict(os.environ),
            mock.patch("orchestrator.prepare_target_cluster", side_effect=prepare) as select,
            mock.patch("orchestrator.teardown_test_workloads", side_effect=check_target) as cleanup,
            mock.patch("orchestrator.teardown_substrate"),
            mock.patch("orchestrator.deploy_workloads"),
            mock.patch("orchestrator.deploy_substrate"),
            mock.patch("orchestrator.run_test", return_value="complete"),
        ):
            results = orchestrator.run_tests(tests, args, "tag", 0, self.snapshot)
        self.assertEqual([result[1] for result in results], ["complete", "config-error", "complete"])
        self.assertEqual(select.call_count, 3)
        self.assertEqual(cleanup.call_count, 4)

    def test_reused_env_file_validates_on_each_test(self):
        env_file = self.root / orchestrator.ENV_FILE_NAME
        valid = "KO_DOCKER_REPO=r/x\nKUBECTL_CONTEXT=bench\n"
        args = argparse.Namespace(
            substrate_dir=str(self.root), env_file=str(env_file), dest="dest", manifests_dir="m",
        )
        tests = [{"name": c, "targetCluster": c, "type": "locust"} for c in ("first", "second")]
        target = {"KUBECONFIG": str(self.snapshot), "KUBECTL_CONTEXT": "bench"}
        cases = [
            ("ATE_CONFIG=second.yaml\n", "ATE_CONFIG is not supported"),
            ("ATE_INSTALL_KIND=TRUE\n", "ATE_INSTALL_KIND must be true or false"),
        ]
        for extra, error in cases:
            with self.subTest(extra=extra.strip()):
                env_file.write_text(valid)

                def mutate_after_first(*_args, extra=extra):
                    env_file.write_text(valid + extra)
                    return "complete"

                ttype = mock.Mock(USES_SHARED_WORKLOADS=True)
                with (
                    mock.patch.dict(orchestrator.TYPES, {"locust": ttype}, clear=True),
                    mock.patch.dict(os.environ),
                    mock.patch("orchestrator.prepare_target_cluster", return_value=target) as prepare,
                    mock.patch("orchestrator.deploy_workloads") as deploy,
                    mock.patch("orchestrator.teardown_test_workloads") as cleanup,
                    mock.patch("orchestrator.deploy_substrate") as install,
                    mock.patch("orchestrator.teardown_substrate") as uninstall,
                    mock.patch("orchestrator.run_test", side_effect=mutate_after_first) as run_test,
                ):
                    results = orchestrator.run_tests(tests, args, "tag", 0, self.snapshot)
                self.assertEqual([result[1] for result in results], ["complete", "config-error"])
                self.assertIn(error, results[1][3])
                prepare.assert_called_once_with(self.snapshot, configure_docker=False)
                self.assertEqual(cleanup.call_count, 2)
                self.assertEqual(uninstall.call_count, 2)
                self.assertEqual(install.call_count, 1)
                self.assertEqual(deploy.call_count, 1)
                self.assertEqual(run_test.call_count, 1)


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
            orchestrator.run_test(test, "img", "dest", "abcdef0123", 1700000000)
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

    def test_locust_job_gets_the_whole_test_entry(self):
        class Rendered(Exception):
            pass

        fake = mock.Mock(job_tmpl=mock.Mock(return_value="t"), job_subs=mock.Mock(return_value={}))
        test = {"name": "n", "type": "locust", "duration": "1m", "flags": ["--x"], "observers": [{"name": "pprof"}]}
        with mock.patch.dict(orchestrator.TYPES, {"locust": fake}), \
             mock.patch.dict(os.environ, CLUSTER_ENV, clear=True), \
             mock.patch.object(orchestrator, "render_template", side_effect=Rendered) as render, \
             self.assertRaises(Rendered):
            orchestrator.run_test(test, "img", "dest", "abcdef0123", 1700000000)
        args = render.call_args.args[2]
        self.assertEqual(json.loads(args[args.index("--test-entry") + 1]), test)

    def test_every_job_gets_the_invocation_run_ts(self):
        for ttype in orchestrator.TYPES:
            with self.subTest(ttype=ttype):
                tmpl = orchestrator.TYPES[ttype].job_tmpl(os.path.join(os.path.dirname(__file__), "manifests"))
                subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d", "RUN_TS": 1700000000}
                text = orchestrator.render_template(tmpl, subs)
                job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
                args = job["spec"]["template"]["spec"]["containers"][0]["args"]
                self.assertEqual(args[args.index("--run-ts") + 1], "1700000000")

    def test_test_names_must_be_unique(self):
        test = {"name": "n", "type": "locust", "targetCluster": "t", "file": "f", "duration": "1m", "users": 1}
        with self.assertRaisesRegex(ValueError, "test names must be unique: n$"):
            orchestrator.validate_and_normalize_tests([test, dict(test)])

    def test_evaluations_only_on_locust(self):
        test = {"name": "n", "type": "nighthawk-ingress", "targetCluster": "t", "evaluations": [{"name": "e"}]}
        with mock.patch.object(orchestrator.TYPES["nighthawk-ingress"], "validate"), \
             self.assertRaisesRegex(ValueError, "test 'n': 'evaluations' is supported only for type locust"):
            orchestrator.validate_and_normalize_tests([test])

    def test_metadata_survives_yaml(self):
        md = json.dumps({"cluster_name": "c: d", "q": "\"'"})
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d", "RUN_TS": 1}
        subs.update(locust.job_subs({"file": "f", "duration": "1m", "users": 1}))
        text = orchestrator.render_template(RunnerSizingTest.TMPL, subs, ["--metadata", md])
        job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
        self.assertEqual(job["spec"]["template"]["spec"]["containers"][0]["args"][-1], md)


if __name__ == "__main__":
    unittest.main()
