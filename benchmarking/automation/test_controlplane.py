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

"""Unit tests for controlplane.py:
python3 benchmarking/automation/test_controlplane.py"""

import os
import unittest
from unittest import mock

import yaml

import controlplane


def node(name, unschedulable=False, taints=(), cpu="88"):
    return {
        "metadata": {"name": name},
        "spec": {"unschedulable": unschedulable, "taints": list(taints)},
        "status": {"allocatable": {"cpu": cpu}},
    }


def stub(test, **fakes):
    for name, fake in fakes.items():
        patcher = mock.patch.object(controlplane, name, fake)
        patcher.start()
        test.addCleanup(patcher.stop)


class ConfigTest(unittest.TestCase):
    def test_defaults(self):
        cfg = controlplane.config({"cpu": 2})
        self.assertEqual(cfg["cpu"], 2)
        self.assertEqual(cfg["replicas"], 1)
        self.assertEqual(cfg["memory"], "4Gi")
        self.assertIsNone(cfg["poolMaxConns"])
        self.assertTrue(cfg["pgStats"])
        self.assertEqual(cfg["postgresArgs"], [])
        self.assertTrue(cfg["pinNodes"])


class ValidateTest(unittest.TestCase):
    def test_valid_knobs(self):
        controlplane.validate({"cpu": 2}, [])
        controlplane.validate(
            {"cpu": "1500m", "memory": "8Gi", "replicas": 2,
             "poolMaxConns": 16, "pgStats": False, "pinNodes": False}, [])
        controlplane.validate(
            {"cpu": 2, "postgresArgs": ["commit_delay=100", "max_wal_size=16GB"]}, [])

    def test_invalid(self):
        bad = [
            {},
            "2",
            {"cpu": 0},
            {"cpu": "two"},
            {"cpu": True},
            {"cpu": 2, "cpus": 2},
            {"cpu": 2, "replicas": 0},
            {"cpu": 2, "replicas": True},
            {"cpu": 2, "poolMaxConns": "64"},
            {"cpu": 2, "pgStats": "yes"},
            {"cpu": 2, "pinNodes": "yes"},
            {"cpu": 2, "memory": "4 GiB"},
            {"cpu": 2, "postgresArgs": "commit_delay=100"},
            {"cpu": 2, "postgresArgs": ["commit_delay"]},
            {"cpu": 2, "postgresArgs": ["commit_delay = 100"]},
            {"cpu": 2, "postgresArgs": ["-c commit_delay=100"]},
            {"cpu": 2, "postgresArgs": [100]},
            # The settings ride on the restart pgStats makes.
            {"cpu": 2, "pgStats": False, "postgresArgs": ["commit_delay=100"]},
        ]
        for block in bad:
            with self.assertRaises(ValueError, msg=repr(block)):
                controlplane.validate(block, [])

    def test_pin_nodes_needs_an_uncordoned_install(self):
        with self.assertRaisesRegex(ValueError, "cordon"):
            controlplane.validate({"cpu": 2}, ["--cordon-control-plane"])
        controlplane.validate({"cpu": 2, "pinNodes": False}, ["--cordon-control-plane"])
        controlplane.validate({"cpu": 2}, ["--cordon-control-plane=false"])


class AteapiPatchTest(unittest.TestCase):
    def test_guaranteed_and_pinned(self):
        patch = controlplane.ateapi_patch(controlplane.config({"cpu": 2}))
        self.assertEqual(patch["spec"]["replicas"], 1)
        pod = patch["spec"]["template"]["spec"]
        resources = pod["containers"][0]["resources"]
        self.assertEqual(resources["requests"], {"cpu": "2", "memory": "4Gi"})
        self.assertEqual(resources["limits"], resources["requests"])
        self.assertEqual(pod["affinity"], controlplane.role_affinity("ateapi"))

    def test_unpinned(self):
        cfg = controlplane.config({"cpu": 2, "pinNodes": False})
        self.assertNotIn("affinity", controlplane.ateapi_patch(cfg)["spec"]["template"]["spec"])


class PickRoleNodesTest(unittest.TestCase):
    def test_skips_postgres_and_unschedulable_nodes(self):
        nodes = [
            node("n-d"),
            node("n-a"),
            node("n-b", unschedulable=True),
            node("n-c", taints=[{"key": "k", "effect": "NoSchedule"}]),
            node("n-e", taints=[{"key": "k", "effect": "PreferNoSchedule"}]),
        ]
        self.assertEqual(controlplane.pick_role_nodes(nodes, "n-a"), ("n-d", "n-e"))

    def test_too_few_nodes(self):
        with self.assertRaises(RuntimeError):
            controlplane.pick_role_nodes([node("pg"), node("n-a")], "pg")

    def test_runner_takes_the_load_generator_pool(self):
        loadgen = [dict(controlplane.LOADGEN_TAINT)]
        nodes = [node("n-a"), node("n-b"), node("lg-b", taints=loadgen), node("lg-a", taints=loadgen)]
        self.assertEqual(controlplane.pick_role_nodes(nodes, "n-b"), ("n-a", "lg-a"))

    def test_ateapi_never_takes_the_load_generator_pool(self):
        loadgen = [dict(controlplane.LOADGEN_TAINT)]
        with self.assertRaises(RuntimeError):
            controlplane.pick_role_nodes(
                [node("pg"), node("lg-a", taints=loadgen), node("lg-b", taints=loadgen)], "pg")

    def test_each_role_takes_a_node_that_fits(self):
        loadgen = [dict(controlplane.LOADGEN_TAINT)]
        nodes = [node("n-a", cpu="3900m"), node("n-b", cpu="7950m"), node("n-c", cpu="15890m"),
                 node("lg-a", taints=loadgen, cpu="3920m"), node("lg-b", taints=loadgen, cpu="7920m")]
        # Two 4-CPU replicas share the ateapi node; the runner asks for 8.
        self.assertEqual(
            controlplane.pick_role_nodes(nodes, "pg", ateapi_millis=8000, runner_millis=7900),
            ("n-c", "lg-b"),
        )
        # No load-generator node fits the runner, so it takes an untainted one.
        self.assertEqual(
            controlplane.pick_role_nodes(nodes, "pg", ateapi_millis=8000, runner_millis=7930),
            ("n-c", "n-b"),
        )

    def test_no_node_fits(self):
        with self.assertRaisesRegex(RuntimeError, "8000m CPU free for ateapi"):
            controlplane.pick_role_nodes(
                [node("n-a", cpu="4"), node("n-b", cpu="4")], "pg", ateapi_millis=8000)
        with self.assertRaisesRegex(RuntimeError, "8000m CPU free for the runner"):
            controlplane.pick_role_nodes([node("n-a", cpu="16"), node("n-b", cpu="4")], "pg",
                                         ateapi_millis=8000, runner_millis=8000)

    def test_cpu_already_requested_is_not_free(self):
        loadgen = [dict(controlplane.LOADGEN_TAINT)]
        nodes = [node("n-a", cpu="16"), node("n-b", cpu="16"), node("lg-a", taints=loadgen, cpu="16")]
        self.assertEqual(
            controlplane.pick_role_nodes(nodes, "pg", ateapi_millis=8000, runner_millis=7000),
            ("n-a", "lg-a"),
        )
        # Pods already on n-a and lg-a leave them 7 and 6 CPUs free.
        self.assertEqual(
            controlplane.pick_role_nodes(nodes, "pg", ateapi_millis=8000, runner_millis=7000,
                                         requested={"n-a": 9000, "lg-a": 10000}),
            ("n-b", "n-a"),
        )
        with self.assertRaisesRegex(RuntimeError, r"\('n-a', 7000\)"):
            controlplane.pick_role_nodes(nodes, "pg", ateapi_millis=8000, runner_millis=8000,
                                         requested={"n-a": 9000, "lg-a": 10000})

    def test_other_taints_are_no_load_generator_pool(self):
        nodes = [
            node("n-a"),
            node("n-b"),
            node("x", taints=[{"key": "ate.dev/benchmarking", "value": "true", "effect": "NoExecute"}]),
            node("y", taints=[dict(controlplane.LOADGEN_TAINT), {"key": "k", "effect": "NoSchedule"}]),
        ]
        self.assertEqual(controlplane.pick_role_nodes(nodes, "pg"), ("n-a", "n-b"))


def pod(node_name, *cpus, namespace="ns", app="x", phase="Running", sidecar=None, init=None):
    p = {
        "metadata": {"namespace": namespace, "labels": {"app": app}},
        "spec": {"containers": [{"resources": {"requests": {"cpu": c}}} for c in cpus],
                 "initContainers": []},
        "status": {"phase": phase},
    }
    if node_name:
        p["spec"]["nodeName"] = node_name
    if sidecar:
        p["spec"]["initContainers"].append(
            {"restartPolicy": "Always", "resources": {"requests": {"cpu": sidecar}}})
    if init:
        p["spec"]["initContainers"].append({"resources": {"requests": {"cpu": init}}})
    return p


class RequestedMillisTest(unittest.TestCase):
    def test_sums_the_pods_that_hold_cpu(self):
        pods = [
            pod("n-a", "100m", "250m"),
            pod("n-a", "1", sidecar="10m", init="4"),
            pod("n-b", "500m"),
            pod("n-b"),  # no requests
            pod("n-b", "2", phase="Succeeded"),
            pod("n-b", "2", phase="Failed"),
            pod(None, "2", phase="Pending"),
            # pin_ateapi replaces these, freeing their CPU.
            pod("n-a", "2", namespace="ate-system", app="ate-api-server"),
            pod("n-b", "100m", namespace="ate-system", app="postgres"),
        ]
        self.assertEqual(controlplane.requested_millis(pods), {"n-a": 1360, "n-b": 600})


class CpuMillisTest(unittest.TestCase):
    def test_spellings(self):
        cases = {2: 2000, 0.5: 500, "8": 8000, "1.5": 1500, "1500m": 1500,
                 "87810m": 87810, "1e1": 10000, "2k": 2000000}
        for q, want in cases.items():
            with self.subTest(q=q):
                self.assertEqual(controlplane._cpu_millis(q), want)


class LabelNodesTest(unittest.TestCase):
    def test_ateapi_node_holds_every_replica(self):
        asked = {}

        def pick(nodes, postgres_node, **need):
            asked.update(need)
            return "n-a", "n-b"

        stub(self,
             _kubectl_out=lambda args: '{"items": []}' if {"nodes", "pods"} & set(args) else "pg",
             pick_role_nodes=pick,
             run=lambda *_: None,
             run_no_check=lambda *_: None)
        controlplane.label_nodes(controlplane.config({"cpu": "1500m", "replicas": 2}), "8")
        self.assertEqual(asked, {"ateapi_millis": 3000, "runner_millis": 8000, "requested": {}})


class ShapeTest(unittest.TestCase):
    # Enabling statistics restarts Postgres onto any node, so the roles that
    # must avoid its node are assigned only afterwards.
    def test_labels_nodes_after_postgres_restarts(self):
        calls = []
        stub(self, **{
            name: lambda *_, name=name: calls.append(name)
            for name in ("enable_postgres_stats", "label_nodes", "set_pool_max_conns", "pin_ateapi", "run_no_check")
        })
        controlplane.shape(controlplane.config({"cpu": 2, "poolMaxConns": 40}), "500m")
        self.assertEqual(
            [c for c in calls if c != "run_no_check"],
            ["enable_postgres_stats", "label_nodes", "set_pool_max_conns", "pin_ateapi"],
        )

    def test_passes_the_settings(self):
        got = []
        stub(self,
             enable_postgres_stats=lambda settings: got.append(settings),
             label_nodes=lambda *_: None,
             pin_ateapi=lambda *_: None,
             run_no_check=lambda *_: None)
        controlplane.shape(controlplane.config({"cpu": 2, "postgresArgs": ["commit_delay=100"]}), "500m")
        self.assertEqual(got, [["commit_delay=100"]])


class PostgresExporterTest(unittest.TestCase):
    """The exporter's port is one ate-system's observability NetworkPolicy
    admits; any other name leaves Prometheus timing out on the scrape."""

    POLICIES = os.path.join(
        os.path.dirname(__file__), "..", "..", "manifests", "ate-install", "ate-system-networkpolicies.yaml"
    )

    def admitted_ports(self):
        for doc in yaml.safe_load_all(open(self.POLICIES)):
            if doc and doc["metadata"]["name"] == "ate-system-observability":
                return {p["port"] for rule in doc["spec"]["ingress"] for p in rule["ports"]}
        self.fail("no ate-system-observability NetworkPolicy")

    def test_exporter_port_is_admitted(self):
        patches = []
        stub(self,
             _patch_and_wait=lambda kind, obj, patch: patches.append(patch),
             run=lambda *_, **__: None)
        controlplane.enable_postgres_stats([])
        containers = patches[0]["spec"]["template"]["spec"]["containers"]
        exporter = next(c for c in containers if c["name"] == "postgres-exporter")
        admitted = self.admitted_ports()
        for port in exporter["ports"]:
            self.assertIn(port["name"], admitted)


class PostgresArgsTest(unittest.TestCase):
    def test_base_args_first_then_settings(self):
        self.assertEqual(
            controlplane.postgres_args(["commit_delay=100", "wal_buffers=64MB"]),
            [
                "-c", "config_file=/etc/postgresql/postgresql.conf",
                "-c", "shared_preload_libraries=pg_stat_statements",
                "-c", "commit_delay=100",
                "-c", "wal_buffers=64MB",
            ],
        )


class PoolMaxConnsTest(unittest.TestCase):
    def test_patch(self):
        self.assertEqual(
            controlplane.pool_max_conns_patch(40),
            {"data": {"ATE_API_POSTGRES_POOL_MAX_CONNS": "40"}},
        )


if __name__ == "__main__":
    unittest.main()
