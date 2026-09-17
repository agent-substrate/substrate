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

"""Unit tests for cluster_facts.py: python3 benchmarking/locust/unit_tests/test_cluster_facts.py

Never contacts a cluster. Nodes and pods are stand-in objects handed to a
mocked CoreV1Api. Needs the kubernetes client:
pip install -r benchmarking/locust/requirements.txt
"""

import argparse
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Probe only the third-party dep, so a bad import in our own modules fails the
# suite instead of skipping it.
try:
    import kubernetes  # noqa: F401

    HAS_KUBERNETES = True
except ImportError:  # pragma: no cover - depends on the local environment
    HAS_KUBERNETES = False

if HAS_KUBERNETES:
    import cluster_facts
    import runner
    from kubernetes.client.rest import ApiException

needs_kubernetes = unittest.skipUnless(
    HAS_KUBERNETES,
    "kubernetes client not installed; pip install -r benchmarking/locust/requirements.txt",
)

# Real apiserver quantity strings: 3920m -> 3.92 cores, 13591700Ki -> 12.96 GiB.
NODE_CPU, NODE_MEMORY = "3920m", "13591700Ki"

# One successful discovery. 10 users against 5 worker pods puts A/P at 2.0.
FACTS = {"machine_type": "c3-standard-4", "node_count": 1,
         "allocatable_cores": 3.92, "allocatable_ram_gb": 12.96,
         "worker_pod_count": 5}

STATS_HEADER = "Type,Name,Request Count,Failure Count\n"
ARGV = ["runner.py", "-f", "tests/glutton.py", "-t", "1m", "-u", "10",
        "--tag", "unit", "--name", "unit-run", "--dest", "/tmp/unit"]


def node(machine_type="c3-standard-4"):
    labels = {} if machine_type is None else {
        cluster_facts.MACHINE_TYPE_LABEL: machine_type}
    return SimpleNamespace(
        metadata=SimpleNamespace(labels=labels),
        status=SimpleNamespace(allocatable={"cpu": NODE_CPU, "memory": NODE_MEMORY}))


def pod(phase="Running"):
    return SimpleNamespace(status=SimpleNamespace(phase=phase))


def fake_api(nodes=None, ns_pods=None, all_pods=None):
    """A stand-in CoreV1Api. None means that call raises 403, which is how the
    apiserver answers a ServiceAccount that lacks the ClusterRole."""
    def forbidden(*_args, **_kwargs):
        raise ApiException(status=403, reason="Forbidden")

    api = mock.Mock()
    for attr, items in (("list_node", nodes), ("list_namespaced_pod", ns_pods),
                        ("list_pod_for_all_namespaces", all_pods)):
        if items is None:
            getattr(api, attr).side_effect = forbidden
        else:
            getattr(api, attr).return_value = SimpleNamespace(items=items)
    return api


def discover(api):
    """Notices print unconditionally, mirroring runner.tee, so stdout is
    swallowed to keep the suite quiet."""
    with mock.patch.object(cluster_facts, "_load_kube_config", return_value=True), \
         mock.patch.object(cluster_facts.client, "CoreV1Api", return_value=api), \
         contextlib.redirect_stdout(io.StringIO()):
        return cluster_facts.get_cluster_hardware_facts()


def summarize(facts, directory, stats=STATS_HEADER + ",Aggregated,100,25\n",
              users=10, user_counts=None):
    """Writes what append_trial_summary reads, returns the row it emitted."""
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
            argparse.Namespace(users=users, tag="unit", name="unit-run"),
            "2026-01-01", facts)
    return json.loads(out.read_text().splitlines()[0])


def parse(*extra):
    with mock.patch.object(sys, "argv", ARGV + list(extra)):
        return runner.parse_args()


@needs_kubernetes
class ClusterFactsTest(unittest.TestCase):
    def test_node_capacity(self):
        # An unlabeled node, as a pool can hit mid-upgrade, still counts.
        facts = discover(fake_api(nodes=[node(), node(None), node("n2-standard-8")],
                                  ns_pods=[pod()]))
        self.assertEqual(facts["node_count"], 3)
        self.assertEqual(facts["allocatable_cores"], 11.76)   # 3 x 3.92
        # Bytes are summed and rounded once, so this is not 3 x 12.96.
        self.assertEqual(facts["allocatable_ram_gb"], 38.89)
        # A mixed pool is reported in full rather than attributed to one node.
        self.assertEqual(facts["machine_type"], "c3-standard-4,n2-standard-8")

    def test_worker_pod_count(self):
        pods = [pod("Running"), pod("Pending"), pod("Succeeded"), pod("Failed")]
        self.assertEqual(discover(fake_api(nodes=[node()], ns_pods=pods))
                         ["worker_pod_count"], 2)

        # An empty namespace means the pool may live elsewhere, so scan wide.
        api = fake_api(nodes=[node()], ns_pods=[], all_pods=[pod(), pod()])
        self.assertEqual(discover(api)["worker_pod_count"], 2)
        api.list_pod_for_all_namespaces.assert_called_once()

        api = fake_api(nodes=[node()], ns_pods=[pod()], all_pods=[pod(), pod()])
        self.assertEqual(discover(api)["worker_pod_count"], 1)
        api.list_pod_for_all_namespaces.assert_not_called()

    def test_unreadable_facts_are_none(self):
        # Nodes denied. Those facts drop out, pods are still counted.
        facts = discover(fake_api(nodes=None, ns_pods=[pod(), pod()]))
        self.assertIsNone(facts["node_count"])
        self.assertIsNone(facts["allocatable_cores"])
        self.assertEqual(facts["worker_pod_count"], 2)

        # No pod carries the pool label. A guess here would skew every A/P ratio.
        facts = discover(fake_api(nodes=[node(), node()], ns_pods=[], all_pods=[]))
        self.assertIsNone(facts["worker_pod_count"])
        self.assertEqual(facts["node_count"], 2)

        # Everything denied, and no credentials at all.
        self.assertEqual(discover(fake_api()), cluster_facts.EMPTY_FACTS)
        with mock.patch.object(cluster_facts, "_load_kube_config", return_value=False):
            self.assertEqual(cluster_facts.get_cluster_hardware_facts(),
                             cluster_facts.EMPTY_FACTS)

    def test_flags(self):
        self.assertEqual(parse().prometheus_url, runner.DEFAULT_PROMETHEUS_URL)
        self.assertEqual(parse("--prometheus-url", "http://x:9090").prometheus_url,
                         "http://x:9090")
        # Neither flag is ours to hand on to locust.
        extra = parse("--no-cluster-facts", "--prometheus-url", "http://x:9090")
        self.assertNotIn("--no-cluster-facts", extra.locust_extra)
        self.assertNotIn("--prometheus-url", extra.locust_extra)

    def test_no_cluster_facts_skips_the_api(self):
        def tripwire(*_args, **_kwargs):
            raise AssertionError("Kubernetes was contacted with --no-cluster-facts")

        with mock.patch.object(cluster_facts.client, "CoreV1Api", tripwire), \
             mock.patch.object(cluster_facts.config, "load_incluster_config", tripwire), \
             mock.patch.object(cluster_facts.config, "load_kube_config", tripwire), \
             contextlib.redirect_stdout(io.StringIO()):
            facts = runner.collect_cluster_facts(parse("--no-cluster-facts"),
                                                 io.StringIO())
        self.assertEqual(facts, cluster_facts.EMPTY_FACTS)

        # Guards the assertion above: broken discovery also makes no API call.
        with mock.patch.object(runner, "get_cluster_hardware_facts",
                               return_value={"node_count": 1}) as discovery, \
             contextlib.redirect_stdout(io.StringIO()):
            runner.collect_cluster_facts(parse(), io.StringIO())
        discovery.assert_called_once()

    def test_trial_summary(self):
        with tempfile.TemporaryDirectory() as td:
            row = summarize(FACTS, td)
        self.assertEqual(row["metric"], "trial_summary")
        # Raw readings sit beside the derived numbers, so ratios can be re-derived.
        self.assertEqual(row["raw_configuration"], FACTS)
        f = row["frontiers"]
        self.assertEqual(f["actors_per_node"], 10.0)    # 10 users / 1 node
        self.assertEqual(f["actors_per_vcpu"], 2.55)    # 10 / 3.92
        self.assertEqual(f["actors_per_gb_ram"], 0.77)  # 10 / 12.96

        # Skipped or unreadable: same keys, so consumers need no special case.
        with tempfile.TemporaryDirectory() as td:
            row = summarize(dict(cluster_facts.EMPTY_FACTS), td)
        self.assertEqual(set(row["raw_configuration"]), set(cluster_facts.EMPTY_FACTS))
        for key in ("actors_per_node", "actors_per_vcpu", "actors_per_gb_ram",
                    "ap_ratio_p50", "ap_ratio_p90", "ap_ratio_p99"):
            self.assertIsNone(row["frontiers"][key])

    def test_ap_ratio_percentiles(self):
        # 200 steady samples behind three ramp-up ones the filter must drop,
        # each a different ratio so an off-by-one shows up.
        with tempfile.TemporaryDirectory() as td:
            row = summarize(FACTS, td, users=100,
                            user_counts=[1, 50, 89] + list(range(100, 300)))
        f = row["frontiers"]
        self.assertEqual([f["ap_ratio_p50"], f["ap_ratio_p90"], f["ap_ratio_p99"]],
                         [40.0, 56.0, 59.6])  # users 200, 280, 298 over 5 pods
        # p99 is not max: the top sample, 299 users, is 59.8.

        # No usable sample: a computed ratio here would be one nobody measured.
        with tempfile.TemporaryDirectory() as td:
            row = summarize(FACTS, td, users=10, user_counts=[])
        for key in ("ap_ratio_p50", "ap_ratio_p90", "ap_ratio_p99"):
            self.assertIsNone(row["frontiers"][key])

    def test_failure_ratio(self):
        # 0.0 is the most optimistic value here, so only a real count may give it.
        def ratio(stats):
            with tempfile.TemporaryDirectory() as td:
                return summarize(FACTS, td, stats)["frontiers"]["aggregate_failure_ratio"]

        self.assertEqual(ratio(STATS_HEADER + ",Aggregated,100,25\n"), 0.25)
        self.assertEqual(ratio(STATS_HEADER + ",Aggregated,1708,0\n"), 0.0)
        self.assertIsNone(ratio(STATS_HEADER + "not,a,valid\n"))      # truncated
        self.assertIsNone(ratio(STATS_HEADER + ",Aggregated,0,0\n"))  # 0/0
        self.assertIsNone(ratio(None))                                # file absent


if __name__ == "__main__":
    unittest.main()
