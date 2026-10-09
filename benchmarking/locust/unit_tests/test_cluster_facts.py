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

"""Unit tests for cluster_facts.py.

Run via: python3 benchmarking/locust/unit_tests/test_cluster_facts.py
Never contacts a cluster. Nodes and pods are stand-in objects handed to a
mocked CoreV1Api. Needs the kubernetes client:
pip install -r benchmarking/locust/requirements.txt
"""

import argparse
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from kubernetes.client.rest import ApiException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import cluster_facts
import runner

# Real apiserver quantity strings: 3920m -> 3.92 cores, 13591700Ki -> 12.96 GiB.
NODE_CPU, NODE_MEMORY = "3920m", "13591700Ki"

# One successful discovery. 10 users against 5 worker pods puts actors per pod at 2.0.
FACTS = {"machine_type": "c3-standard-4", "node_count": 1,
         "allocatable_cores": 3.92, "allocatable_ram_gb": 12.96,
         "worker_pod_count": 5}

STATS_HEADER = "Type,Name,Request Count,Failure Count\n"
ARGV = ["runner.py", "-f", "tests/glutton.py", "-t", "1m", "-u", "10",
        "--tag", "unit", "--name", "unit-run", "--dest", "/tmp/unit"]


GKE_VERSION = "v1.33.5-gke.1080000"


def node(machine_type="c3-standard-4", name="node-a"):
    labels = {} if machine_type is None else {
        cluster_facts.MACHINE_TYPE_LABEL: machine_type}
    return SimpleNamespace(
        metadata=SimpleNamespace(labels=labels, name=name),
        status=SimpleNamespace(
            allocatable={"cpu": NODE_CPU, "memory": NODE_MEMORY},
            node_info=SimpleNamespace(kubelet_version=GKE_VERSION)))


def container(name="ateom", args=(), memory=None,
              image="registry/ateom-gvisor-0123abcd@sha256:00"):
    limits = None if memory is None else {"memory": memory}
    return SimpleNamespace(name=name, args=list(args), image=image,
                           resources=SimpleNamespace(limits=limits))


def pod(phase="Running", node_name="node-a", containers=()):
    return SimpleNamespace(
        spec=SimpleNamespace(node_name=node_name, containers=list(containers)),
        status=SimpleNamespace(phase=phase))


def fake_api(nodes=None, ns_pods=None):
    """Stand-in CoreV1Api; None raises 403 Forbidden."""
    def forbidden(*_args, **_kwargs):
        raise ApiException(status=403, reason="Forbidden")

    api = mock.Mock()
    for attr, items in (("list_node", nodes), ("list_namespaced_pod", ns_pods)):
        if items is None:
            getattr(api, attr).side_effect = forbidden
        else:
            getattr(api, attr).return_value = SimpleNamespace(items=items)
    return api


def discover(api):
    with mock.patch.object(cluster_facts, "_load_kube_config", return_value=True), \
         mock.patch.object(cluster_facts.client, "CoreV1Api", return_value=api), \
         contextlib.redirect_stdout(io.StringIO()):
        return cluster_facts.get_cluster_hardware_facts()


def summarize(facts, directory, stats=STATS_HEADER + ",Aggregated,100,25\n",
              users=10, user_counts=None, actors_per_user=None,
              run_start=None, run_end=None, metadata=None):
    """Writes CSV inputs and returns the emitted trial_summary row."""
    d = Path(directory)
    if stats is not None:
        (d / "stats.csv").write_text(stats)
    if user_counts is None:
        user_counts = [users] * 61
    (d / "stats_history.csv").write_text(
        "Timestamp,User Count,Type,Name,Requests/s,Failures/s\n"
        + "".join(f"{1788914584 + i},{u},,Aggregated,1.0,0.0\n"
                  for i, u in enumerate(user_counts)))
    out = d / "out.jsonl"
    with contextlib.redirect_stdout(io.StringIO()):
        cluster_facts.append_trial_summary(
            out, d / "stats.csv", d / "stats_history.csv",
            argparse.Namespace(users=users, tag="unit", name="unit-run",
                               actors_per_user=actors_per_user,
                               metadata=metadata),
            "2026-01-01", facts, run_start=run_start, run_end=run_end)
    return json.loads(out.read_text().splitlines()[0])


def parse(*extra):
    with mock.patch.object(sys, "argv", ARGV + list(extra)):
        return runner.parse_args()


class ClusterFactsTest(unittest.TestCase):
    def test_node_capacity(self):
        facts = discover(fake_api(
            nodes=[node(name="node-a"), node(None, name="node-b"),
                   node("n2-standard-8", name="node-c")],
            ns_pods=[pod(node_name="node-a"), pod(node_name="node-b"),
                     pod(node_name="node-c")]))
        self.assertEqual(facts["node_count"], 3)
        self.assertEqual(facts["allocatable_cores"], 11.76)   # 3 x 3.92
        self.assertEqual(facts["allocatable_ram_gb"], 38.89)
        self.assertEqual(facts["machine_type"], "c3-standard-4,n2-standard-8")

    def test_worker_pod_count(self):
        pods = [pod("Running"), pod("Pending"), pod("Succeeded"), pod("Failed")]
        api = fake_api(nodes=[node()], ns_pods=pods)
        self.assertEqual(discover(api)["worker_pod_count"], 2)
        api.list_namespaced_pod.assert_called_once_with(
            namespace="benchmark-workloads",
            label_selector="ate.dev/worker-pool",
            resource_version="0",
            _request_timeout=5,
        )
        api.list_node.assert_called_once_with(
            resource_version="0",
            _request_timeout=5,
        )

    def test_capacity_is_scoped_to_nodes_running_worker_pods(self):
        # Excludes nodes without worker pods (e.g. infra-a).
        facts = discover(fake_api(
            nodes=[node(name="node-a"), node(name="node-b"),
                   node(name="infra-a")],
            ns_pods=[pod(node_name="node-a"), pod(node_name="node-b"),
                     pod(node_name="node-b")]))
        self.assertEqual(facts["worker_pod_count"], 3)
        self.assertEqual(facts["node_count"], 2)
        self.assertEqual(facts["allocatable_cores"], 7.84)  # 2 x 3.92

    def test_zero_worker_pods_is_a_reading(self):
        # Empty pool -> 0; denied read -> None.
        facts = discover(fake_api(nodes=[node()], ns_pods=[]))
        self.assertEqual(facts["worker_pod_count"], 0)
        self.assertEqual(facts["node_count"], 0)

        denied = discover(fake_api(nodes=[node()], ns_pods=None))
        self.assertIsNone(denied["worker_pod_count"])
        self.assertIsNone(denied["node_count"])

    def test_unreadable_facts_are_none(self):
        # Nodes denied -> capacity None, pod count still recorded.
        facts = discover(fake_api(nodes=None, ns_pods=[pod(), pod()]))
        self.assertIsNone(facts["node_count"])
        self.assertIsNone(facts["allocatable_cores"])
        self.assertEqual(facts["worker_pod_count"], 2)

        # Everything denied or no credentials -> EMPTY_FACTS.
        self.assertEqual(discover(fake_api()), cluster_facts.EMPTY_FACTS)
        with mock.patch.object(cluster_facts, "_load_kube_config", return_value=False):
            self.assertEqual(cluster_facts.get_cluster_hardware_facts(),
                             cluster_facts.EMPTY_FACTS)

    def test_flags(self):
        extra = parse("--no-cluster-facts", "--max-wait-time", "1.0")
        self.assertNotIn("--no-cluster-facts", extra.locust_extra)
        self.assertEqual(extra.locust_extra, ["--max-wait-time", "1.0"])

    def test_no_cluster_facts_skips_the_api(self):
        def tripwire(*_args, **_kwargs):
            raise AssertionError("Kubernetes was contacted with --no-cluster-facts")

        with mock.patch.object(cluster_facts.client, "CoreV1Api", tripwire), \
             mock.patch.object(
                 cluster_facts.config, "load_incluster_config", tripwire
             ), \
             mock.patch.object(cluster_facts.config, "load_kube_config", tripwire), \
             contextlib.redirect_stdout(io.StringIO()):
            facts = runner.collect_cluster_facts(parse("--no-cluster-facts"),
                                                 io.StringIO())
        self.assertEqual(facts, cluster_facts.EMPTY_FACTS)

        with mock.patch.object(runner, "get_cluster_hardware_facts",
                               return_value={"node_count": 1}) as discovery, \
             contextlib.redirect_stdout(io.StringIO()):
            facts = runner.collect_cluster_facts(parse(), io.StringIO())
        discovery.assert_called_once()
        self.assertEqual(facts, {"node_count": 1})

    def test_worker_container_facts(self):
        facts = discover(fake_api(nodes=[node()], ns_pods=[
            pod(containers=[container(args=["--pod-uid=x", "--max-actors=50"],
                                      memory="16Gi"),
                            container(name="sidecar", args=["--max-actors=7"])]),
            pod(containers=[container(args=["--max-actors", "50"],
                                      memory="17179869184")]),
        ]))
        self.assertEqual(facts["worker_actor_capacity"], "50")
        self.assertEqual(facts["worker_memory_limit_gb"], "16.0")  # GiB
        self.assertEqual(facts["sandbox_class"], "gvisor")
        self.assertEqual(facts["gke_version"], GKE_VERSION)

        # Unset flag is ateom's default; no memory limit is unmeasured; a mixed
        # pool lists every value.
        facts = discover(fake_api(nodes=[node()], ns_pods=[
            pod(containers=[container()]),
            pod(containers=[container(
                args=["--max-actors=1"],
                image="registry/ateom-microvm-4567@sha256:00")]),
        ]))
        self.assertEqual(facts["worker_actor_capacity"], "1,1000")
        self.assertIsNone(facts["worker_memory_limit_gb"])
        self.assertEqual(facts["sandbox_class"], "gvisor,microvm")

        # No worker pods -> nothing to read.
        facts = discover(fake_api(nodes=[node()], ns_pods=[]))
        self.assertIsNone(facts["worker_actor_capacity"])
        self.assertIsNone(facts["sandbox_class"])

        # A malformed flag loses only these facts, not the node capacity.
        facts = discover(fake_api(nodes=[node()], ns_pods=[
            pod(containers=[container(args=["--max-actors=lots"])])]))
        self.assertIsNone(facts["worker_actor_capacity"])
        self.assertEqual(facts["node_count"], 1)

    def test_metadata_flag(self):
        self.assertEqual(parse().metadata, {})
        got = parse("--metadata", '{"cluster_name": "c", "nodes": 3}')
        self.assertEqual(got.metadata, {"cluster_name": "c", "nodes": 3})
        self.assertNotIn("--metadata", got.locust_extra)

        # Rejected before the run starts, not after it.
        for bad in ("not json", "[1, 2]", '"text"'):
            with self.subTest(bad=bad), \
                 contextlib.redirect_stderr(io.StringIO()), \
                 self.assertRaises(SystemExit):
                parse("--metadata", bad)

    def test_metadata_in_trial_summary(self):
        md = {"cluster_name": "sweperf-c3hm176nested", "nodes": 3,
              "labels": {"a": 1}, "unset": None}
        with tempfile.TemporaryDirectory() as td:
            m = summarize(FACTS, td, metadata=md)["measurements"]
        self.assertEqual(m["metadata_cluster_name"], "sweperf-c3hm176nested")
        # Non-strings are stored as JSON so BigQuery can parse them.
        self.assertEqual(m["metadata_nodes"], "3")
        self.assertEqual(json.loads(m["metadata_labels"]), {"a": 1})
        self.assertNotIn("metadata_unset", m)

        # No metadata, no metadata_* keys.
        with tempfile.TemporaryDirectory() as td:
            m = summarize(FACTS, td)["measurements"]
        self.assertFalse([k for k in m if k.startswith("metadata_")])

    def test_run_window_and_multi_actor_worker(self):
        facts = {**FACTS, "worker_actor_capacity": 1000}
        with tempfile.TemporaryDirectory() as td:
            m = summarize(facts, td, run_start=1788914580,
                          run_end=1788915180)["measurements"]
        self.assertEqual(m["run_start"], "1788914580")
        self.assertEqual(m["run_end"], "1788915180")
        self.assertEqual(m["worker_actor_capacity"], "1000")
        self.assertEqual(m["multi_actor_worker"], "true")   # 2 actors per pod

        # One actor per pod is not multi-actor; no pod count is unknown.
        with tempfile.TemporaryDirectory() as td:
            m = summarize({**FACTS, "worker_pod_count": 10}, td)["measurements"]
        self.assertEqual(m["multi_actor_worker"], "false")  # 10 users / 10 pods
        self.assertIsNone(m["run_start"])
        with tempfile.TemporaryDirectory() as td:
            m = summarize(dict(cluster_facts.EMPTY_FACTS), td)["measurements"]
        self.assertIsNone(m["multi_actor_worker"])

    def test_trial_summary(self):
        with tempfile.TemporaryDirectory() as td:
            row = summarize(FACTS, td)
        self.assertEqual(row["metric"], "trial_summary")
        self.assertEqual(set(row), {"timestamp", "tag", "test_name", "metric",
                                    "measurements"})
        m = row["measurements"]
        # Facts pass through untouched, just serialized.
        self.assertEqual({k: m[k] for k in FACTS},
                         {k: str(v) for k, v in FACTS.items()})
        self.assertEqual(m["actors_per_node"], "10.0")    # 10 users / 1 node
        self.assertEqual(m["actors_per_vcpu"], "2.55")    # 10 / 3.92
        self.assertEqual(m["actors_per_gb_ram"], "0.77")  # 10 / 12.96
        # Every value a string, so one row's types match every other row's.
        self.assertTrue(all(isinstance(v, str)
                            for v in m.values() if v is not None))

        # Unmeasured facts keep the same keys with None values.
        with tempfile.TemporaryDirectory() as td:
            row = summarize(dict(cluster_facts.EMPTY_FACTS), td)
        self.assertLessEqual(set(cluster_facts.EMPTY_FACTS),
                             set(row["measurements"]))
        for key in ("actors_per_node", "actors_per_vcpu", "actors_per_gb_ram",
                    "actors_per_pod_p50", "actors_per_pod_p90",
                    "actors_per_pod_p99"):
            self.assertIsNone(row["measurements"][key])

    def test_actors_per_user_scales_every_key(self):
        # One VU drives N actors, so the numerator is users * N everywhere.
        with tempfile.TemporaryDirectory() as td:
            row = summarize(FACTS, td, actors_per_user=3)
        m = row["measurements"]
        self.assertEqual(m["actors_per_node"], "30.0")    # 10 users * 3 / 1 node
        self.assertEqual(m["actors_per_vcpu"], "7.65")    # 30 / 3.92 cores
        self.assertEqual(m["actors_per_gb_ram"], "2.31")  # 30 / 12.96 GiB
        # The percentiles scale too, not just the three frontiers.
        self.assertEqual(m["actors_per_pod_p50"], "6.0")  # 30 / 5 pods

        # Unset is boomer's default of one actor per VU, so nothing moves.
        with tempfile.TemporaryDirectory() as td:
            unset = summarize(FACTS, td)["measurements"]
        with tempfile.TemporaryDirectory() as td:
            one = summarize(FACTS, td, actors_per_user=1)["measurements"]
        self.assertEqual(unset, one)

    def test_actors_per_pod_percentiles(self):
        with tempfile.TemporaryDirectory() as td:
            row = summarize(FACTS, td, users=100,
                            user_counts=[1, 50, 89] + list(range(100, 300)))
        m = row["measurements"]
        self.assertEqual([m["actors_per_pod_p50"], m["actors_per_pod_p90"],
                          m["actors_per_pod_p99"]],
                         ["39.6", "55.8", "59.4"])  # users 198, 279, 297 over 5 pods

        # Uses observed peak (60) rather than requested -u (10).
        with tempfile.TemporaryDirectory() as td:
            row = summarize(FACTS, td, users=10, user_counts=[20, 40, 60])
        self.assertEqual(row["measurements"]["actors_per_node"], "60.0")
        self.assertEqual(row["measurements"]["actors_per_pod_p50"], "8.0")

        # Empty history falls back to -u for frontiers and None for percentiles.
        with tempfile.TemporaryDirectory() as td:
            row = summarize(FACTS, td, users=10, user_counts=[])
        for key in ("actors_per_pod_p50", "actors_per_pod_p90",
                    "actors_per_pod_p99"):
            self.assertIsNone(row["measurements"][key])
        self.assertEqual(row["measurements"]["actors_per_node"], "10.0")

    def test_failure_ratio(self):
        def ratio(stats):
            with tempfile.TemporaryDirectory() as td:
                return summarize(FACTS, td, stats)["measurements"][
                    "aggregate_failure_ratio"]

        self.assertEqual(ratio(STATS_HEADER + ",Aggregated,100,25\n"), "0.25")
        self.assertEqual(ratio(STATS_HEADER + ",Aggregated,1708,0\n"), "0.0")
        self.assertIsNone(ratio(STATS_HEADER + ",Aggregated,100\n"))    # truncated
        self.assertIsNone(ratio(STATS_HEADER + ",Aggregated,bad,5\n"))  # corrupt int
        self.assertIsNone(ratio(STATS_HEADER + ",Aggregated,0,0\n"))    # 0/0
        self.assertIsNone(ratio(None))                                  # file absent

    def test_per_rpc_failure_ratios(self):
        # First resume is its own Locust row, so it gets its own key.
        stats = (STATS_HEADER
                 + "grpc,ResumeActor,100,2\n"
                 + "grpc,ResumeActorFirstResume,100,99\n"
                 + "grpc,SuspendActor,200,0\n"
                 + ",Aggregated,400,101\n")
        with tempfile.TemporaryDirectory() as td:
            f = summarize(FACTS, td, stats)["measurements"]
        self.assertEqual(f["resume_actor_failure_ratio"], "0.02")               # 2 / 100
        self.assertEqual(f["resume_actor_first_resume_failure_ratio"], "0.99")  # 99 / 100
        self.assertEqual(f["suspend_actor_failure_ratio"], "0.0")
        self.assertEqual(f["aggregate_failure_ratio"], "0.2525")     # 101 / 400

        # An RPC the test never ran has no key at all.
        with tempfile.TemporaryDirectory() as td:
            row = summarize(FACTS, td, STATS_HEADER + ",Aggregated,10,0\n")
        self.assertNotIn("resume_actor_failure_ratio", row["measurements"])
        self.assertNotIn("suspend_actor_failure_ratio", row["measurements"])


if __name__ == "__main__":
    unittest.main()
