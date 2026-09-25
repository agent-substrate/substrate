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
and the actors-per-pod percentiles) for a completed trial.
"""

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any, TextIO

from kubernetes import client, config
from kubernetes.utils import parse_quantity

API_TIMEOUT_SECONDS = 5
WORKER_POOL_NAMESPACE = "benchmark-workloads"
WORKER_POOL_LABEL = "ate.dev/worker-pool"
LIVE_POD_PHASES = ("Running", "Pending")
MACHINE_TYPE_LABEL = "node.kubernetes.io/instance-type"

# Shape returned when the cluster cannot be read, or when discovery is skipped
# with --no-cluster-facts. Keeping one definition means a trial_summary row has
# the same hardware keys either way, so consumers never have to special-case it.
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


def _list_worker_pods(
    v1: client.CoreV1Api, logs: TextIO | None = None
) -> list[Any] | None:
    """Lists the live pods of the worker pool.

    The pool lives in one namespace by convention and the `WorkerPool` CRD is
    namespaced, so this is a single scoped read. The listing is filtered
    server-side by label and served from the watch cache. Use
    --no-cluster-facts to skip discovery entirely.

    Returns None only when the read failed. An empty list is a reading: the
    namespace holds no live worker pods.
    """
    try:
        pods = v1.list_namespaced_pod(
            namespace=WORKER_POOL_NAMESPACE,
            label_selector=WORKER_POOL_LABEL,
            resource_version="0",
            _request_timeout=API_TIMEOUT_SECONDS,
        ).items
        return [p for p in pods if p.status.phase in LIVE_POD_PHASES]
    except Exception as e:
        # An ApiException prints its whole HTTP response, so log the reason on
        # its own. Anything without one logs itself.
        reason = getattr(e, "reason", e)
        _log(logs,
             f"Notice: could not list pods in {WORKER_POOL_NAMESPACE}: {reason}")
        return None


def get_cluster_hardware_facts(logs: TextIO | None = None) -> dict[str, Any]:
    """Reads the worker pool size and the capacity of the nodes it runs on.

    Capacity is scoped to the nodes carrying worker pods, so a cluster that
    keeps its infrastructure on a separate pool does not count that pool's
    cores and memory against the density frontiers.

    Never raises: a trial must still publish its results when the cluster is
    unreadable, so any failure leaves the affected facts as None.
    """
    facts: dict[str, Any] = dict(EMPTY_FACTS)
    if not _load_kube_config(logs):
        return facts

    v1 = client.CoreV1Api()

    pods = _list_worker_pods(v1, logs)

    if pods is None:
        # Without a pod set there is no node set, so capacity stays unmeasured
        # rather than falling back to every node in the cluster.
        return facts

    facts["worker_pod_count"] = len(pods)

    try:
        # A Pending pod may not be scheduled yet, so it counts toward the pool
        # size without contributing a node.
        worker_nodes = {p.spec.node_name for p in pods if p.spec.node_name}
        # resource_version="0" is served from the apiserver's watch cache
        # rather than etcd, avoiding a quorum read on large clusters.
        nodes = v1.list_node(
            resource_version="0", _request_timeout=API_TIMEOUT_SECONDS
        ).items
        node_count = 0
        total_cores = 0.0
        total_ram_bytes = 0
        machine_types = set()
        for node in nodes:
            metadata = node.metadata
            if metadata is None or metadata.name not in worker_nodes:
                continue
            node_count += 1
            allocatable = node.status.allocatable or {}
            total_cores += float(parse_quantity(allocatable["cpu"]))
            total_ram_bytes += int(parse_quantity(allocatable["memory"]))
            machine_type = (metadata.labels or {}).get(MACHINE_TYPE_LABEL)
            if machine_type:
                machine_types.add(machine_type)
        facts["node_count"] = node_count
        facts["allocatable_cores"] = round(total_cores, 2)
        # GiB, as the apiserver and kubectl quote it.
        facts["allocatable_ram_gb"] = round(total_ram_bytes / (1024**3), 2)
        # Kept so results stay comparable across hardware changes. A mixed pool
        # is a sorted comma-joined list rather than one node picked at random.
        facts["machine_type"] = ",".join(sorted(machine_types)) or None
    except Exception as e:
        reason = getattr(e, "reason", e)
        _log(logs, f"Notice: could not read node capacity: {reason}")

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
    # Locust's own User Count samples. The -u flag is a request; under a custom
    # load shape what actually ran is whatever the shape asked for.
    observed: list[float] = []
    if stats_history_csv.exists():
        try:
            with open(stats_history_csv, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if row.get("Name", "") not in ("", "Aggregated", "Total"):
                        continue
                    try:
                        u = float(row.get("User Count", ""))
                    except (TypeError, ValueError):
                        continue
                    if u > 0:
                        observed.append(u)
        except Exception as e:
            _log(logs, f"Notice: could not read user counts: {e}")
            # A read that threw partway leaves a truncated sample behind, and
            # a truncated sample understates the peak without looking wrong.
            observed = []

    # The flag stands in only when no sample was read at all.
    peak_users = max(observed) if observed else args.users

    # Locust counts VUs, not actors, and one VU drives --actors-per-user of them.
    per_user = args.actors_per_user or 1
    peak_actors = peak_users * per_user

    node_count = facts.get("node_count")
    cores = facts.get("allocatable_cores")
    ram_gb = facts.get("allocatable_ram_gb")
    pod_count = facts.get("worker_pod_count")

    actors_per_node = round(peak_actors / node_count, 2) if node_count else None
    actors_per_vcpu = round(peak_actors / cores, 2) if cores else None
    actors_per_gb_ram = round(peak_actors / ram_gb, 2) if ram_gb else None

    # Actors per pod across every sample, ramp-up included. Under a load shape
    # there is no one target to measure steadiness against, so the
    # distribution covers the whole run.
    actors_per_pod_p50, actors_per_pod_p90, actors_per_pod_p99 = None, None, None
    if observed and pod_count:
        ratios = sorted(round(u * per_user / pod_count, 4) for u in observed)
        n = len(ratios)
        actors_per_pod_p50 = round(ratios[int(n * 0.50)], 2)
        actors_per_pod_p90 = round(ratios[min(int(n * 0.90), n - 1)], 2)
        actors_per_pod_p99 = round(ratios[min(int(n * 0.99), n - 1)], 2)

    # One ratio per row Locust reported, so a test's own operation names carry
    # through. Absent when the test has no such row, null when it ran nothing.
    failure_ratios: dict[str, float | None] = {"aggregate_failure_ratio": None}
    if stats_csv.exists():
        try:
            with open(stats_csv, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    name = row.get("Name", "")
                    reqs = row.get("Request Count")
                    fails = row.get("Failure Count")
                    # Both columns required so a missing one is not read as zero.
                    if not name or reqs is None or fails is None:
                        continue
                    requests, failures = int(reqs), int(fails)
                    ratio = round(failures / requests, 4) if requests else None
                    if name == "Aggregated":
                        failure_ratios["aggregate_failure_ratio"] = ratio
                        continue
                    key = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
                    key = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", key)
                    key = re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")
                    failure_ratios[f"{key}_failure_ratio"] = ratio
        except Exception as e:
            _log(logs, f"Notice: could not parse {stats_csv}: {e}")
            failure_ratios = {"aggregate_failure_ratio": None}
    else:
        _log(logs, f"Notice: {stats_csv} not found; failure ratios unknown")

    measurements = {
        **{k: facts.get(k) for k in EMPTY_FACTS},
        "actors_per_node": actors_per_node,
        "actors_per_vcpu": actors_per_vcpu,
        "actors_per_gb_ram": actors_per_gb_ram,
        "actors_per_pod_p50": actors_per_pod_p50,
        "actors_per_pod_p90": actors_per_pod_p90,
        "actors_per_pod_p99": actors_per_pod_p99,
        **failure_ratios,
    }

    summary_entry = {
        "timestamp": data_ts,
        "tag": args.tag,
        "test_name": args.name,
        "metric": "trial_summary",
        "measurements": {
            k: (str(v) if v is not None else None)
            for k, v in measurements.items()
        },
    }
    with open(jsonl_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(summary_entry) + "\n")
    _log(logs, f"Appended trial_summary to {jsonl_path}")
