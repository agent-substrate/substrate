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
    lookup for clusters that place the pool elsewhere. Both listings are
    filtered server-side by label so the apiserver never streams us the full
    pod inventory.
    """
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

    if not pods:
        pods = v1.list_pod_for_all_namespaces(
            label_selector=WORKER_POOL_LABEL,
            resource_version="0",
            _request_timeout=API_TIMEOUT_SECONDS,
        ).items

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
        facts["allocatable_ram_gb"] = round(total_ram_bytes / (1024**3), 2)
        # Recorded so results stay comparable across hardware changes. A
        # heterogeneous pool is reported as a sorted comma-joined list rather
        # than picking one node arbitrarily.
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
    machine_type = facts.get("machine_type")
    node_count = facts.get("node_count")
    cores = facts.get("allocatable_cores")
    ram_gb = facts.get("allocatable_ram_gb")
    pod_count = facts.get("worker_pod_count")

    actors_per_node = round(active_users / node_count, 2) if node_count else None
    actors_per_vcpu = round(active_users / cores, 2) if cores else None
    actors_per_gb_ram = round(active_users / ram_gb, 2) if ram_gb else None

    # Calculate steady-state A/P percentiles from stats_history.csv
    ap_p50, ap_p90, ap_p99 = None, None, None
    if stats_history_csv.exists() and pod_count and pod_count > 0:
        try:
            user_counts: list[float] = []
            with open(stats_history_csv) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = row.get("Name", "")
                    if name in ("", "Aggregated", "Total") and "User Count" in row:
                        try:
                            u = float(row["User Count"])
                            # Steady-state window: when load reaches configured users
                            if u >= active_users * 0.9:
                                user_counts.append(u)
                        except ValueError:
                            pass
            if not user_counts:
                # Fallback: if no rows matched threshold, use non-zero samples
                with open(stats_history_csv) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        name = row.get("Name", "")
                        if name in ("", "Aggregated", "Total") and "User Count" in row:
                            try:
                                u = float(row["User Count"])
                                if u > 0:
                                    user_counts.append(u)
                            except ValueError:
                                pass
            if user_counts:
                ratios = sorted([round(u / pod_count, 4) for u in user_counts])
                n = len(ratios)
                ap_p50 = round(ratios[int(n * 0.50)], 2)
                ap_p90 = round(ratios[min(int(n * 0.90), n - 1)], 2)
                ap_p99 = round(ratios[min(int(n * 0.99), n - 1)], 2)
            else:
                static_ratio = round(active_users / pod_count, 2)
                ap_p50, ap_p90, ap_p99 = static_ratio, static_ratio, static_ratio
        except Exception as e:
            if logs:
                _log(logs, f"Notice: Error calculating A/P ratio percentiles: {e}")

    # Calculate aggregate failure ratio from stats_csv
    total_requests = 0
    total_failures = 0
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
        except Exception:
            pass

    failure_ratio = (
        round(total_failures / total_requests, 4) if total_requests > 0 else 0.0
    )

    summary_entry = {
        "timestamp": data_ts,
        "tag": args.tag,
        "test_name": args.name,
        "metric": "trial_summary",
        "raw_configuration": {
            "machine_type": machine_type,
            "node_count": node_count,
            "allocatable_cores": cores,
            "allocatable_ram_gb": ram_gb,
            "worker_pod_count": pod_count,
        },
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
    if logs:
        _log(logs, f"Appended trial_summary to {jsonl_path}")
