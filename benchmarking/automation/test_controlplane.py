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


class ValidateTest(unittest.TestCase):
    def test_valid_knobs(self):
        controlplane.validate({"cpu": 2})
        controlplane.validate(
            {"cpu": "1500m", "memory": "8Gi", "replicas": 2,
             "poolMaxConns": 16, "pgStats": False})
        controlplane.validate(
            {"cpu": 2, "postgresArgs": ["commit_delay=100", "max_wal_size=16GB"]})

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
                controlplane.validate(block)

class AteapiPatchTest(unittest.TestCase):
    def test_guaranteed(self):
        patch = controlplane.ateapi_patch(controlplane.config({"cpu": 2}))
        self.assertEqual(patch["spec"]["replicas"], 1)
        pod = patch["spec"]["template"]["spec"]
        resources = pod["containers"][0]["resources"]
        self.assertEqual(resources["requests"], {"cpu": "2", "memory": "4Gi"})
        self.assertEqual(resources["limits"], resources["requests"])
        # Placement is the install's.
        self.assertNotIn("affinity", pod)


class ShapeTest(unittest.TestCase):
    # Postgres restarts once, then ateapi rolls once and reads the pool size.
    def test_order(self):
        calls = []
        stub(self, **{
            name: lambda *_, name=name: calls.append(name)
            for name in ("enable_postgres_stats", "set_pool_max_conns", "resize_ateapi", "run_no_check")
        })
        controlplane.shape(controlplane.config({"cpu": 2, "poolMaxConns": 40}))
        self.assertEqual(
            [c for c in calls if c != "run_no_check"],
            ["enable_postgres_stats", "set_pool_max_conns", "resize_ateapi"],
        )

    def test_passes_the_settings(self):
        got = []
        stub(self,
             enable_postgres_stats=lambda settings: got.append(settings),
             resize_ateapi=lambda *_: None,
             run_no_check=lambda *_: None)
        controlplane.shape(controlplane.config({"cpu": 2, "postgresArgs": ["commit_delay=100"]}))
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
