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

These never contact a cluster. Every node and pod is a stand-in object built
here and handed to a mocked CoreV1Api, so the results do not depend on which
cluster you happen to be pointed at, or on having one at all.

Requires the kubernetes client: pip install -r benchmarking/locust/requirements.txt
Tests that need it are skipped, loudly, when it is missing.
"""

import argparse
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

# Ensure benchmarking/locust is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    import cluster_facts
    import runner
    from kubernetes.client.rest import ApiException

    HAS_KUBERNETES = True
except ImportError:  # pragma: no cover - depends on the local environment
    HAS_KUBERNETES = False

needs_kubernetes = unittest.skipUnless(
    HAS_KUBERNETES,
    "kubernetes client not installed; pip install -r benchmarking/locust/requirements.txt",
)

# Fixture inputs, not readings from anyone's cluster. Shaped like a GKE
# c3-standard-4 (4 vCPU / 16 GiB, less kubelet reservations) so the parsing and
# arithmetic are exercised against quantity strings the apiserver really emits.
SAMPLE_CPU = "3920m"          # -> 3.92 cores
SAMPLE_MEMORY = "13591700Ki"  # -> 12.96 GiB
SAMPLE_CORES = 3.92
SAMPLE_RAM_GB = 12.96
SAMPLE_MACHINE_TYPE = "c3-standard-4"


def node(cpu=SAMPLE_CPU, memory=SAMPLE_MEMORY, machine_type=SAMPLE_MACHINE_TYPE):
    labels = {} if machine_type is None else {
        cluster_facts.MACHINE_TYPE_LABEL: machine_type
    }
    return SimpleNamespace(
        metadata=SimpleNamespace(labels=labels),
        status=SimpleNamespace(allocatable={"cpu": cpu, "memory": memory}),
    )


def pod(phase="Running"):
    return SimpleNamespace(status=SimpleNamespace(phase=phase))


def listing(items):
    return SimpleNamespace(items=items)


def forbidden(*_args, **_kwargs):
    raise ApiException(status=403, reason="Forbidden")


def fake_api(nodes=None, ns_pods=None, all_pods=None):
    """Builds a stand-in CoreV1Api.

    Any argument left as None means that call raises 403, which is how the
    apiserver answers a ServiceAccount that lacks the ClusterRole.
    """
    api = mock.Mock()
    api.list_node.return_value = listing(nodes) if nodes is not None else None
    if nodes is None:
        api.list_node.side_effect = forbidden
    api.list_namespaced_pod.return_value = listing(ns_pods) if ns_pods is not None else None
    if ns_pods is None:
        api.list_namespaced_pod.side_effect = forbidden
    api.list_pod_for_all_namespaces.return_value = (
        listing(all_pods) if all_pods is not None else None
    )
    if all_pods is None:
        api.list_pod_for_all_namespaces.side_effect = forbidden
    return api


def discover(api):
    """Runs get_cluster_hardware_facts against a stand-in API."""
    with mock.patch.object(cluster_facts, "_load_kube_config", return_value=True), \
         mock.patch.object(cluster_facts.client, "CoreV1Api", return_value=api):
        return cluster_facts.get_cluster_hardware_facts()


@needs_kubernetes
class NodeCapacityTest(unittest.TestCase):
    def test_single_gke_node(self):
        facts = discover(fake_api(nodes=[node()], ns_pods=[pod()]))
        self.assertEqual(facts["node_count"], 1)
        self.assertEqual(facts["allocatable_cores"], SAMPLE_CORES)
        self.assertEqual(facts["allocatable_ram_gb"], SAMPLE_RAM_GB)

    def test_capacity_sums_across_nodes(self):
        facts = discover(fake_api(nodes=[node(), node(), node()], ns_pods=[pod()]))
        self.assertEqual(facts["node_count"], 3)
        self.assertEqual(facts["allocatable_cores"], round(SAMPLE_CORES * 3, 2))
        # Bytes are summed and rounded once, so this is 38.89 rather than
        # 3 x 12.96 = 38.88; rounding per node first would compound the error.
        self.assertEqual(facts["allocatable_ram_gb"], 38.89)

    def test_quantity_suffixes(self):
        # Whole cores, and memory in a decimal rather than binary unit.
        facts = discover(fake_api(nodes=[node(cpu="4", memory="2Gi")], ns_pods=[pod()]))
        self.assertEqual(facts["allocatable_cores"], 4.0)
        self.assertEqual(facts["allocatable_ram_gb"], 2.0)

    def test_node_missing_allocatable(self):
        bare = SimpleNamespace(metadata=None, status=SimpleNamespace(allocatable=None))
        facts = discover(fake_api(nodes=[bare], ns_pods=[pod()]))
        self.assertEqual(facts["node_count"], 1)
        self.assertEqual(facts["allocatable_cores"], 0.0)
        # A node with no metadata must not abort the whole node read.
        self.assertIsNone(facts["machine_type"])


@needs_kubernetes
class MachineTypeTest(unittest.TestCase):
    """Machine type is recorded so results stay comparable across hardware.

    Frontier ratios are only meaningful next to the silicon that produced
    them; without this a c3-standard-4 run and a c3d-standard-8 run are
    indistinguishable in the emitted data.
    """

    def test_read_from_the_standard_label(self):
        facts = discover(fake_api(nodes=[node()], ns_pods=[pod()]))
        self.assertEqual(facts["machine_type"], SAMPLE_MACHINE_TYPE)

    def test_uniform_pool_reports_one_value(self):
        nodes = [node(), node(), node()]
        facts = discover(fake_api(nodes=nodes, ns_pods=[pod()]))
        self.assertEqual(facts["machine_type"], SAMPLE_MACHINE_TYPE)

    def test_mixed_pool_reports_every_type(self):
        # Deliberately not "pick the first node": a silently partial answer
        # here would misattribute the whole trial to one machine type.
        nodes = [node(machine_type="n2-standard-8"), node()]
        facts = discover(fake_api(nodes=nodes, ns_pods=[pod()]))
        self.assertEqual(facts["machine_type"], "c3-standard-4,n2-standard-8")

    def test_unlabelled_node_reports_unknown_not_a_guess(self):
        facts = discover(fake_api(nodes=[node(machine_type=None)], ns_pods=[pod()]))
        self.assertIsNone(facts["machine_type"])
        # The rest of the node read still succeeds.
        self.assertEqual(facts["allocatable_cores"], SAMPLE_CORES)

    def test_costs_no_extra_api_call(self):
        # The label rides along on the node listing we already perform.
        api = fake_api(nodes=[node()], ns_pods=[pod()])
        discover(api)
        self.assertEqual(api.list_node.call_count, 1)


@needs_kubernetes
class WorkerPodCountTest(unittest.TestCase):
    def test_counts_running_and_pending(self):
        pods = [pod("Running"), pod("Running"), pod("Pending")]
        facts = discover(fake_api(nodes=[node()], ns_pods=pods))
        self.assertEqual(facts["worker_pod_count"], 3)

    def test_ignores_terminal_pods(self):
        pods = [pod("Running"), pod("Succeeded"), pod("Failed"), pod("Unknown")]
        facts = discover(fake_api(nodes=[node()], ns_pods=pods))
        self.assertEqual(facts["worker_pod_count"], 1)

    def test_falls_back_to_all_namespaces(self):
        # Worker pool lives outside the dedicated namespace.
        api = fake_api(nodes=[node()], ns_pods=[], all_pods=[pod(), pod()])
        facts = discover(api)
        self.assertEqual(facts["worker_pod_count"], 2)
        api.list_pod_for_all_namespaces.assert_called_once()

    def test_skips_fallback_when_namespace_has_pods(self):
        api = fake_api(nodes=[node()], ns_pods=[pod()], all_pods=[pod(), pod(), pod()])
        facts = discover(api)
        self.assertEqual(facts["worker_pod_count"], 1)
        api.list_pod_for_all_namespaces.assert_not_called()

    def test_undiscoverable_pool_reports_unknown_not_a_guess(self):
        # Nodes are readable but no pod carries the pool label. Reporting the
        # node count here would be indistinguishable from a real reading and
        # would skew every A/P ratio derived from it.
        api = fake_api(nodes=[node(), node()], ns_pods=[], all_pods=[])
        facts = discover(api)
        self.assertIsNone(facts["worker_pod_count"])
        self.assertEqual(facts["node_count"], 2)  # nodes still discovered

    def test_listings_are_filtered_server_side(self):
        api = fake_api(nodes=[node()], ns_pods=[pod()])
        discover(api)
        _, kwargs = api.list_namespaced_pod.call_args
        self.assertEqual(kwargs["label_selector"], cluster_facts.WORKER_POOL_LABEL)
        self.assertEqual(kwargs["namespace"], cluster_facts.WORKER_POOL_NAMESPACE)

    def test_reads_are_served_from_the_watch_cache(self):
        # resource_version="0" keeps these cheap on large clusters.
        api = fake_api(nodes=[node()], ns_pods=[pod()])
        discover(api)
        self.assertEqual(api.list_node.call_args.kwargs["resource_version"], "0")
        self.assertEqual(api.list_namespaced_pod.call_args.kwargs["resource_version"], "0")


@needs_kubernetes
class DeniedAccessTest(unittest.TestCase):
    """A 403 must never fail the trial; the run still has to publish results."""

    def test_nodes_denied(self):
        facts = discover(fake_api(nodes=None, ns_pods=[pod(), pod()]))
        self.assertIsNone(facts["node_count"])
        self.assertIsNone(facts["allocatable_cores"])
        self.assertIsNone(facts["allocatable_ram_gb"])
        self.assertIsNone(facts["machine_type"])
        # Pods were still readable, so that fact survives.
        self.assertEqual(facts["worker_pod_count"], 2)

    def test_pods_denied(self):
        facts = discover(fake_api(nodes=[node()], ns_pods=None, all_pods=None))
        self.assertEqual(facts["node_count"], 1)
        self.assertIsNone(facts["worker_pod_count"])

    def test_everything_denied(self):
        self.assertEqual(discover(fake_api()), cluster_facts.EMPTY_FACTS)

    def test_no_credentials(self):
        with mock.patch.object(cluster_facts, "_load_kube_config", return_value=False):
            self.assertEqual(
                cluster_facts.get_cluster_hardware_facts(), cluster_facts.EMPTY_FACTS
            )


@needs_kubernetes
class EnvironmentIsNotConsultedTest(unittest.TestCase):
    """Hardware facts come from the cluster only.

    Hand-fed overrides were removed because a stale value silently produces
    frontiers that look real. This guards against them coming back.
    """

    def test_env_vars_cannot_override_discovery(self):
        bogus = {
            "NODE_COUNT": "9999",
            "ALLOCATABLE_VCPU": "8888",
            "ALLOCATABLE_CORES": "7777",
            "ALLOCATABLE_RAM_GB": "6666",
            "ALLOCATABLE_RAM_BYTES": "5555",
            "WORKER_POD_COUNT": "4444",
        }
        with mock.patch.dict("os.environ", bogus):
            facts = discover(fake_api(nodes=[node()], ns_pods=[pod()]))
        self.assertEqual(facts["node_count"], 1)
        self.assertEqual(facts["allocatable_cores"], SAMPLE_CORES)
        self.assertEqual(facts["allocatable_ram_gb"], SAMPLE_RAM_GB)
        self.assertEqual(facts["worker_pod_count"], 1)

    def test_env_vars_cannot_supply_facts_when_cluster_is_unreadable(self):
        with mock.patch.dict("os.environ", {"NODE_COUNT": "9999"}):
            self.assertEqual(discover(fake_api()), cluster_facts.EMPTY_FACTS)


BASE_ARGV = [
    "runner.py",
    "-f", "tests/glutton.py",
    "-t", "1m",
    "-u", "10",
    "--tag", "unit",
    "--name", "unit-run",
    "--dest", "/tmp/unit",
]


def parse(*extra):
    with mock.patch.object(sys, "argv", BASE_ARGV + list(extra)):
        return runner.parse_args()


@needs_kubernetes
class ClusterFactsFlagTest(unittest.TestCase):
    def test_enabled_by_default(self):
        self.assertTrue(parse().cluster_facts)

    def test_explicit_forms(self):
        self.assertTrue(parse("--cluster-facts").cluster_facts)
        self.assertFalse(parse("--no-cluster-facts").cluster_facts)

    def test_flag_is_not_forwarded_to_locust(self):
        self.assertNotIn("--no-cluster-facts", parse("--no-cluster-facts").locust_extra)

    def test_disabled_makes_no_api_call(self):
        def tripwire(*_a, **_kw):
            raise AssertionError("Kubernetes was contacted with --no-cluster-facts")

        with mock.patch.object(cluster_facts.client, "CoreV1Api", tripwire), \
             mock.patch.object(cluster_facts.config, "load_incluster_config", tripwire), \
             mock.patch.object(cluster_facts.config, "load_kube_config", tripwire):
            facts = runner.collect_cluster_facts(parse("--no-cluster-facts"), _sink())
        self.assertEqual(facts, cluster_facts.EMPTY_FACTS)

    def test_enabled_does_call_the_api(self):
        # Guards the test above: if discovery broke entirely it would also
        # "make no API call", and that must not read as a pass.
        with mock.patch.object(
            runner, "get_cluster_hardware_facts", return_value={"node_count": 1}
        ) as discovery:
            runner.collect_cluster_facts(parse(), _sink())
        discovery.assert_called_once()


def _sink():
    import io

    return io.StringIO()


@needs_kubernetes
class TrialSummaryTest(unittest.TestCase):
    STATS_CSV = (
        "Type,Name,Request Count,Failure Count\n"
        "grpc,ResumeActor,80,20\n"
        "grpc,SuspendActor,20,0\n"
        ",Aggregated,100,25\n"
    )

    def _write_history(self, directory, users=10, rows=61):
        path = Path(directory) / "stats_history.csv"
        lines = ["Timestamp,User Count,Type,Name,Requests/s,Failures/s"]
        lines += [f"{1788914584 + i},{users},,Aggregated,1.0,0.0" for i in range(rows)]
        path.write_text("\n".join(lines) + "\n")
        return path

    def _summarize(self, facts, directory):
        stats_csv = Path(directory) / "stats.csv"
        stats_csv.write_text(self.STATS_CSV)
        out = Path(directory) / "out.jsonl"
        args = argparse.Namespace(users=10, tag="unit", name="unit-run")
        cluster_facts.append_trial_summary(
            out, stats_csv, self._write_history(directory), args, "2026-01-01", facts
        )
        return json.loads(out.read_text().splitlines()[0])

    def test_frontiers_from_discovered_facts(self):
        # append_trial_summary takes facts as an argument, so these are chosen
        # numbers, not a reading. Any shape works; this one makes the expected
        # arithmetic below easy to check by hand.
        facts = {
            "machine_type": SAMPLE_MACHINE_TYPE,
            "node_count": 1,
            "allocatable_cores": SAMPLE_CORES,
            "allocatable_ram_gb": SAMPLE_RAM_GB,
            "worker_pod_count": 5,
        }
        with tempfile.TemporaryDirectory() as td:
            row = self._summarize(facts, td)

        self.assertEqual(row["metric"], "trial_summary")
        # Raw facts are persisted next to the derived numbers so the ratios
        # can be re-derived later.
        self.assertEqual(row["raw_configuration"], facts)

        frontiers = row["frontiers"]
        self.assertEqual(frontiers["actors_per_node"], 10.0)      # 10 users / 1 node
        self.assertEqual(frontiers["actors_per_vcpu"], 2.55)      # 10 / 3.92
        self.assertEqual(frontiers["actors_per_gb_ram"], 0.77)    # 10 / 12.96
        # A/P is reported as a distribution, not a single average.
        self.assertEqual(frontiers["ap_ratio_p50"], 2.0)          # 10 users / 5 pods
        self.assertEqual(frontiers["ap_ratio_p90"], 2.0)
        self.assertEqual(frontiers["ap_ratio_p99"], 2.0)
        self.assertEqual(frontiers["aggregate_failure_ratio"], 0.25)

    def test_schema_is_stable_without_facts(self):
        # --no-cluster-facts, or a cluster we could not read: the row still
        # appears with the same keys so consumers need no special case.
        with tempfile.TemporaryDirectory() as td:
            row = self._summarize(dict(cluster_facts.EMPTY_FACTS), td)

        self.assertEqual(set(row["raw_configuration"]), set(cluster_facts.EMPTY_FACTS))
        self.assertTrue(all(v is None for v in row["raw_configuration"].values()))

        frontiers = row["frontiers"]
        for key in ("actors_per_node", "actors_per_vcpu", "actors_per_gb_ram",
                    "ap_ratio_p50", "ap_ratio_p90", "ap_ratio_p99"):
            self.assertIn(key, frontiers)
            self.assertIsNone(frontiers[key])
        # Failure ratio comes from locust's own CSV, so it survives.
        self.assertEqual(frontiers["aggregate_failure_ratio"], 0.25)

    def test_missing_history_leaves_percentiles_unset(self):
        facts = {
            "machine_type": SAMPLE_MACHINE_TYPE,
            "node_count": 1,
            "allocatable_cores": SAMPLE_CORES,
            "allocatable_ram_gb": SAMPLE_RAM_GB,
            "worker_pod_count": 5,
        }
        with tempfile.TemporaryDirectory() as td:
            stats_csv = Path(td) / "stats.csv"
            stats_csv.write_text(self.STATS_CSV)
            out = Path(td) / "out.jsonl"
            args = argparse.Namespace(users=10, tag="unit", name="unit-run")
            cluster_facts.append_trial_summary(
                out, stats_csv, Path(td) / "absent.csv", args, "2026-01-01", facts
            )
            row = json.loads(out.read_text().splitlines()[0])

        self.assertIsNone(row["frontiers"]["ap_ratio_p50"])
        # Density frontiers do not depend on the history file.
        self.assertEqual(row["frontiers"]["actors_per_node"], 10.0)


if __name__ == "__main__":
    unittest.main()
