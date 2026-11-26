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

"""Unit tests for controlplane.py:
python3 benchmarking/automation/test_controlplane.py"""

import os
import unittest
from unittest import mock

import yaml

import controlplane


def stub(test, **fakes):
    for name, fake in fakes.items():
        patcher = mock.patch.object(controlplane, name, fake)
        patcher.start()
        test.addCleanup(patcher.stop)


API = {"apiServer": {"cpu": 2}}


class ConfigTest(unittest.TestCase):
    def test_defaults(self):
        cfg = controlplane.config(API)
        # The install defines ate-api-server's shape; only what the block
        # sets is overridden.
        self.assertEqual(cfg["apiServer"], {"cpu": 2})
        self.assertEqual(cfg["postgres"], {"stats": True, "args": []})


class ValidateTest(unittest.TestCase):
    def test_valid_knobs(self):
        controlplane.validate(API)
        controlplane.validate(
            {"apiServer": {"cpu": "1500m", "memory": "8Gi", "replicas": 2, "poolMaxConns": 16},
             "postgres": {"stats": False}})
        controlplane.validate(
            {**API, "postgres": {"args": ["commit_delay=100", "max_wal_size=16GB"]}})

    def test_invalid(self):
        def api(**knobs):
            return {"apiServer": {"cpu": 2, **knobs}}

        def pg(**knobs):
            return {**API, "postgres": knobs}

        bad = [
            {},
            "2",
            {"apiServer": "2"},
            {"apiServer": {}},
            {**API, "ateapi": {}},
            {**API, "postgres": "on"},
            api(cpu=0),
            api(cpu="two"),
            api(cpu=True),
            api(cpus=2),
            api(replicas=0),
            api(replicas=True),
            api(poolMaxConns="64"),
            api(memory="4 GiB"),
            pg(stats="yes"),
            pg(statz=True),
            pg(args="commit_delay=100"),
            pg(args=["commit_delay"]),
            pg(args=["commit_delay = 100"]),
            pg(args=["-c commit_delay=100"]),
            pg(args=[100]),
            # The settings ride on the restart stats makes.
            pg(stats=False, args=["commit_delay=100"]),
        ]
        for block in bad:
            with self.assertRaises(ValueError, msg=repr(block)):
                controlplane.validate(block)

    def test_errors_name_the_knob(self):
        with self.assertRaisesRegex(ValueError, "controlPlane.apiServer.cpu"):
            controlplane.validate({"apiServer": {}})
        with self.assertRaisesRegex(ValueError, r"unknown controlPlane.postgres knob\(s\) \['statz'\]"):
            controlplane.validate({**API, "postgres": {"statz": True}})


class ApiServerPatchTest(unittest.TestCase):
    def patch(self, **knobs):
        return controlplane.api_server_patch(
            controlplane.config({"apiServer": {"cpu": 2, **knobs}})["apiServer"])

    def test_sets_the_knobs_given(self):
        patch = self.patch(memory="4Gi", replicas=1)
        self.assertEqual(patch["spec"]["replicas"], 1)
        pod = patch["spec"]["template"]["spec"]
        resources = pod["containers"][0]["resources"]
        self.assertEqual(resources["requests"], {"cpu": "2", "memory": "4Gi"})
        self.assertEqual(resources["limits"], resources["requests"])
        # Placement is the install's.
        self.assertNotIn("affinity", pod)

    def test_unset_knobs_keep_the_install(self):
        patch = self.patch()
        self.assertNotIn("replicas", patch["spec"])
        resources = patch["spec"]["template"]["spec"]["containers"][0]["resources"]
        self.assertEqual(resources, {"requests": {"cpu": "2"}, "limits": {"cpu": "2"}})


class ShapeTest(unittest.TestCase):
    # Postgres restarts once, then ate-api-server rolls once and reads the
    # pool size.
    def test_order(self):
        calls = []
        stub(self, **{
            name: lambda *_, name=name: calls.append(name)
            for name in ("enable_postgres_stats", "set_pool_max_conns", "resize_api_server", "run_no_check")
        })
        controlplane.shape(controlplane.config({"apiServer": {"cpu": 2, "poolMaxConns": 40}}))
        self.assertEqual(
            [c for c in calls if c != "run_no_check"],
            ["enable_postgres_stats", "set_pool_max_conns", "resize_api_server"],
        )

    def test_unset_pool_size_keeps_the_install(self):
        calls = []
        stub(self,
             enable_postgres_stats=lambda *_: None,
             set_pool_max_conns=lambda n: calls.append(n),
             resize_api_server=lambda *_: None,
             run_no_check=lambda *_: None)
        controlplane.shape(controlplane.config(API))
        self.assertEqual(calls, [])

    def test_passes_the_settings(self):
        got = []
        stub(self,
             enable_postgres_stats=lambda settings: got.append(settings),
             resize_api_server=lambda *_: None,
             run_no_check=lambda *_: None)
        controlplane.shape(controlplane.config({**API, "postgres": {"args": ["commit_delay=100"]}}))
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


class AteApiServerScrapeTest(unittest.TestCase):
    HERE = os.path.dirname(__file__)
    MONITORING = os.path.join(HERE, "..", "monitoring.yaml")
    DEPLOYMENT = os.path.join(HERE, "..", "..", "manifests", "ate-install", "ate-api-server.yaml")
    POLICIES = os.path.join(HERE, "..", "..", "manifests", "ate-install", "ate-system-networkpolicies.yaml")

    def test_scrape_job_matches_manifest_and_networkpolicy(self):
        with open(self.MONITORING, encoding="utf-8") as f:
            monitoring_docs = list(yaml.safe_load_all(f))
        prom_cm = next(
            d for d in monitoring_docs
            if d and d.get("kind") == "ConfigMap" and d["metadata"]["name"] == "prometheus-config"
        )
        prom_cfg = yaml.safe_load(prom_cm["data"]["prometheus.yml"])
        job = next(j for j in prom_cfg["scrape_configs"] if j["job_name"] == "ate-api-server")
        self.assertEqual(job["kubernetes_sd_configs"], [{"role": "pod", "namespaces": {"names": ["ate-system"]}}])
        keep = next(r for r in job["relabel_configs"] if r.get("action") == "keep")
        self.assertEqual(
            keep["source_labels"],
            ["__meta_kubernetes_pod_label_app", "__meta_kubernetes_pod_container_port_number"],
        )
        self.assertEqual(keep["regex"], "ate-api-server;9090")

        with open(self.DEPLOYMENT, encoding="utf-8") as f:
            deploy_docs = list(yaml.safe_load_all(f))
        dep = next(d for d in deploy_docs if d and d.get("kind") == "Deployment")
        pod_tpl = dep["spec"]["template"]
        self.assertEqual(pod_tpl["metadata"]["labels"]["app"], "ate-api-server")
        container = next(c for c in pod_tpl["spec"]["containers"] if c["name"] == "ate-api-server")
        port = next(p for p in container["ports"] if p["containerPort"] == 9090)
        self.assertEqual(port["name"], "prometheus")

        with open(self.POLICIES, encoding="utf-8") as f:
            policy_docs = list(yaml.safe_load_all(f))
        obs = next(d for d in policy_docs if d and d["metadata"]["name"] == "ate-system-observability")
        admitted = {p["port"] for rule in obs["spec"]["ingress"] for p in rule["ports"]}
        self.assertIn(port["name"], admitted)


if __name__ == "__main__":
    unittest.main()
