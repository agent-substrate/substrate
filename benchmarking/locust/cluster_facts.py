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


"""Discovers cluster hardware capacity and records per-trial density frontiers.

Reads allocatable CPU/RAM, node count and worker pod count from the Kubernetes
API, then derives the actor-density frontiers (actors per node / vCPU / GB RAM
and the A/P bin-packing percentiles) for a completed trial.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, TextIO

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.utils import parse_quantity

API_TIMEOUT_SECONDS = 5
WORKER_POOL_NAMESPACE = "benchmark-workloads"
WORKER_POOL_LABEL = "ate.dev/worker-pool"
LIVE_POD_PHASES = ("Running", "Pending")
MACHINE_TYPE_LABEL = "node.kubernetes.io/instance-type"

# Shape returned when the cluster cannot be read, or when discovery is skipped
# with --no-cluster-facts. Keeping one definition means a trial_summary row has
# the same keys either way, so consumers never have to special-case it.
EMPTY_FACTS: dict[str, Any] = {
    "machine_type": None,
    "node_count": None,
    "allocatable_cores": None,
    "allocatable_ram_gb": None,
    "worker_pod_count": None,
}


def _log(logs: TextIO | None, msg: str) -> None:
    """Mirrors runner.tee without importing it, to avoid a circular import."""
    print(msg, flush=True)
    if logs is not None:
        logs.write(msg + "\n")
        logs.flush()


def _load_kube_config(logs: TextIO | None = None) -> bool:
    """Loads in-cluster credentials, falling back to a local kubeconfig."""
    try:
        config.load_incluster_config()
        return True
    except config.ConfigException:
        pass
    try:
        config.load_kube_config()
        return True
    except config.ConfigException as e:
        _log(logs, f"Notice: no Kubernetes credentials available: {e}")
        return False


def _count_worker_pods(v1: client.CoreV1Api, logs: TextIO | None = None) -> int | None:
    """Counts live pods in the worker pool.

    Prefers the dedicated worker namespace and falls back to a cluster-wide
    lookup for clusters that place the pool elsewhere. Listing a namespace that
    does not exist returns an empty list rather than an error, so an empty
    result is what "the pool is somewhere else" looks like and it has to
    trigger the fallback.

    Both listings are filtered server-side by label and served from the watch
    cache, but the cluster-wide one still scans every pod, so it is logged
    whenever it happens. Use --no-cluster-facts to skip discovery entirely.
    """
    namespaced_failed = False
    try:
        pods = v1.list_namespaced_pod(
            namespace=WORKER_POOL_NAMESPACE,
            label_selector=WORKER_POOL_LABEL,
            resource_version="0",
            _request_timeout=API_TIMEOUT_SECONDS,
        ).items
    except ApiException as e:
        _log(logs, f"Notice: could not list pods in {WORKER_POOL_NAMESPACE}: {e.reason}")
        pods = []
        namespaced_failed = True

    if not pods:
        reason = (
            "the namespaced lookup failed"
            if namespaced_failed
            else f"no {WORKER_POOL_LABEL} pods in {WORKER_POOL_NAMESPACE}"
        )
        _log(logs, f"Notice: {reason}; scanning all namespaces for the worker pool")
        try:
            pods = v1.list_pod_for_all_namespaces(
                label_selector=WORKER_POOL_LABEL,
                resource_version="0",
                _request_timeout=API_TIMEOUT_SECONDS,
            ).items
        except ApiException as e:
            _log(logs, f"Notice: cluster-wide pod lookup failed: {e.reason}")
            return None

    live = [p for p in pods if p.status.phase in LIVE_POD_PHASES]
    return len(live) or None


def get_cluster_hardware_facts(logs: TextIO | None = None) -> dict[str, Any]:
    """Reads allocatable node capacity and worker pod count from the cluster.

    Never raises: a trial must still publish its results when the cluster is
    unreadable, so any failure leaves the affected facts as None.
    """
    facts: dict[str, Any] = dict(EMPTY_FACTS)
    if not _load_kube_config(logs):
        return facts

    v1 = client.CoreV1Api()

    # resource_version="0" is served from the apiserver's watch cache rather
    # than etcd, which keeps this cheap on large clusters.
    try:
        nodes = v1.list_node(
            resource_version="0", _request_timeout=API_TIMEOUT_SECONDS
        ).items
        total_cores = 0.0
        total_ram_bytes = 0
        machine_types = set()
        for node in nodes:
            allocatable = node.status.allocatable or {}
            if "cpu" in allocatable:
                total_cores += float(parse_quantity(allocatable["cpu"]))
            if "memory" in allocatable:
                total_ram_bytes += int(parse_quantity(allocatable["memory"]))
            labels = (node.metadata.labels or {}) if node.metadata else {}
            machine_type = labels.get(MACHINE_TYPE_LABEL)
            if machine_type:
                machine_types.add(machine_type)
        facts["node_count"] = len(nodes)
        facts["allocatable_cores"] = round(total_cores, 2)
        # GiB, as the apiserver and kubectl quote it. Named _gb for continuity
        # with rows already collected; renaming would break consumers.
        facts["allocatable_ram_gb"] = round(total_ram_bytes / (1024**3), 2)
        # Kept so results stay comparable across hardware changes. A mixed pool
        # is a sorted comma-joined list rather than one node picked at random.
        facts["machine_type"] = ",".join(sorted(machine_types)) or None
    except Exception as e:
        _log(logs, f"Notice: could not read node capacity: {e}")

    try:
        facts["worker_pod_count"] = _count_worker_pods(v1, logs)
    except Exception as e:
        _log(logs, f"Notice: could not count worker pods: {e}")

    return facts


def append_trial_summary(
    jsonl_path: Path,
    stats_csv: Path,
    stats_history_csv: Path,
    args: argparse.Namespace,
    data_ts: str,
    facts: dict[str, Any],
    logs: TextIO | None = None,
) -> None:
    active_users = args.users
    # Only the facts the frontier math divides by. machine_type is recorded
    # but never computed with, so it goes straight into raw_configuration.
    node_count = facts.get("node_count")
    cores = facts.get("allocatable_cores")
    ram_gb = facts.get("allocatable_ram_gb")
    pod_count = facts.get("worker_pod_count")

    actors_per_node = round(active_users / node_count, 2) if node_count else None
    actors_per_vcpu = round(active_users / cores, 2) if cores else None
    actors_per_gb_ram = round(active_users / ram_gb, 2) if ram_gb else None

    # A/P bin-packing percentiles over the steady-state samples. None when
    # unmeasurable: a ratio derived from configured user count is not a reading.
    ap_p50, ap_p90, ap_p99 = None, None, None
    if stats_history_csv.exists() and pod_count and pod_count > 0:
        try:
            observed: list[float] = []
            with open(stats_history_csv) as f:
                for row in csv.DictReader(f):
                    if row.get("Name", "") not in ("", "Aggregated", "Total"):
                        continue
                    try:
                        u = float(row.get("User Count", ""))
                    except (TypeError, ValueError):
                        continue
                    if u > 0:
                        observed.append(u)
            # Steady state is every sample at or above 90% of the target. A run
            # that never got there falls back to every non-zero sample.
            steady = [u for u in observed if u >= active_users * 0.9] or observed
            if steady:
                ratios = sorted(round(u / pod_count, 4) for u in steady)
                n = len(ratios)
                ap_p50 = round(ratios[int(n * 0.50)], 2)
                ap_p90 = round(ratios[min(int(n * 0.90), n - 1)], 2)
                ap_p99 = round(ratios[min(int(n * 0.99), n - 1)], 2)
        except Exception as e:
            _log(logs, f"Notice: Error calculating A/P ratio percentiles: {e}")

    total_requests = 0
    total_failures = 0
    stats_parsed = False
    if stats_csv.exists():
        try:
            with open(stats_csv) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = row.get("Name", "")
                    reqs = int(row.get("Request Count", 0) or 0)
                    fails = int(row.get("Failure Count", 0) or 0)
                    if name == "Aggregated":
                        total_requests = reqs
                        total_failures = fails
                        break
                    total_requests += reqs
                    total_failures += fails
            stats_parsed = True
        except Exception as e:
            _log(logs, f"Notice: could not parse {stats_csv}: {e}")
    else:
        _log(logs, f"Notice: {stats_csv} not found; failure ratio unknown")

    # None, not 0.0, when undetermined. A run with zero failures is a real
    # result and must not look like one where the stats file was unreadable.
    if stats_parsed and total_requests > 0:
        failure_ratio = round(total_failures / total_requests, 4)
    else:
        failure_ratio = None

    summary_entry = {
        "timestamp": data_ts,
        "tag": args.tag,
        "test_name": args.name,
        "metric": "trial_summary",
        # Keyed off EMPTY_FACTS so the raw block always carries every fact,
        # present or not, and the names are declared in one place.
        "raw_configuration": {k: facts.get(k) for k in EMPTY_FACTS},
        "frontiers": {
            "actors_per_node": actors_per_node,
            "actors_per_vcpu": actors_per_vcpu,
            "actors_per_gb_ram": actors_per_gb_ram,
            "ap_ratio_p50": ap_p50,
            "ap_ratio_p90": ap_p90,
            "ap_ratio_p99": ap_p99,
            "aggregate_failure_ratio": failure_ratio,
        },
    }
    with open(jsonl_path, "a") as f:
        f.write(json.dumps(summary_entry) + "\n")
    _log(logs, f"Appended trial_summary to {jsonl_path}")
