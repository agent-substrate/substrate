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

"""Shapes the control plane, ate-api-server and Postgres, for a capacity
run, as a test entry's `ateapi:` block asks (see ATEAPI_DEFAULTS).

A test type validates the block with validate(), calls shape() from its
pre_test, and pins its runner with role_affinity("runner").
"""

import json
import re
import subprocess
from typing import Any

from util import run, run_no_check

NAMESPACE = "ate-system"

# Knobs of the `ateapi:` block. `cpu` has no default: it is the independent
# variable of the run.
ATEAPI_DEFAULTS = {
    # ate-api-server replicas, each with requests = limits of cpu and
    # memory. Go sizes GOMAXPROCS from the CPU limit.
    "replicas": 1,
    "memory": "4Gi",
    # pgxpool max connections per replica, set through ateapi's env
    # ConfigMap (see set_pool_max_conns). None keeps the installed value (64
    # on size10).
    "poolMaxConns": None,
    # Load pg_stat_statements and run postgres_exporter next to Postgres,
    # for Prometheus (benchmarking/monitoring.yaml) to scrape.
    "pgStats": True,
    # Postgres settings as name=value, passed to the server as -c flags in
    # the restart pgStats makes, over postgresql.conf.
    "postgresArgs": [],
    # Label one node for ateapi, which holds every replica, and another for
    # the runner, away from the Postgres node, and pin each there. The runner
    # prefers the node pool reserved for load generators.
    "pinNodes": True,
}


def config(block: dict[str, Any]) -> dict[str, Any]:
    """The `ateapi:` block merged over ATEAPI_DEFAULTS."""
    return {**ATEAPI_DEFAULTS, **block}


def validate(block: Any, ate_args: list[str]) -> None:
    """Raise ValueError unless block is a valid `ateapi:` block for an
    install with ate_args."""
    if not isinstance(block, dict):
        raise ValueError("ateapi must be a mapping")
    allowed = set(ATEAPI_DEFAULTS) | {"cpu"}
    unknown = set(block) - allowed
    if unknown:
        raise ValueError(
            f"unknown ateapi knob(s) {sorted(unknown)}; allowed: {sorted(allowed)}"
        )
    cfg = config(block)
    if "cpu" not in block or not _is_quantity(cfg["cpu"]):
        raise ValueError("needs ateapi.cpu, as in 2 or 1500m")
    if not _is_quantity(cfg["memory"]):
        raise ValueError("invalid ateapi.memory")
    if not _is_positive_int(cfg["replicas"]):
        raise ValueError("ateapi.replicas must be an int >= 1")
    if cfg["poolMaxConns"] is not None and not _is_positive_int(cfg["poolMaxConns"]):
        raise ValueError("ateapi.poolMaxConns must be an int >= 1")
    for knob in ("pgStats", "pinNodes"):
        if not isinstance(cfg[knob], bool):
            raise ValueError(f"ateapi.{knob} must be a bool")
    args = cfg["postgresArgs"]
    if not isinstance(args, list) or not all(
        isinstance(a, str) and POSTGRES_ARG_PATTERN.fullmatch(a) for a in args
    ):
        raise ValueError(
            "ateapi.postgresArgs must be a list of name=value settings, "
            "as in commit_delay=100"
        )
    if args and not cfg["pgStats"]:
        raise ValueError(
            "ateapi.postgresArgs needs pgStats: true, whose Postgres restart applies them"
        )
    # The cordoned install keeps ate-api-server on the tainted control-plane
    # pool, and pinNodes gives it only an untainted node: no node is both.
    if cfg["pinNodes"] and any(
        a in ("--cordon-control-plane", "--cordon-control-plane=true") for a in ate_args
    ):
        raise ValueError(
            "ateapi.pinNodes cannot place ate-api-server on a "
            "--cordon-control-plane install; set pinNodes: false"
        )


def _is_positive_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and v >= 1


def _is_quantity(v: Any) -> bool:
    """A Kubernetes quantity as tests.yaml spells it: 2, 0.5, "1500m", "4Gi"."""
    if isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        return v > 0
    return isinstance(v, str) and bool(QUANTITY_PATTERN.fullmatch(v))


# A Postgres setting as postgresArgs spells it: a lowercase name, an equals
# sign, and a value with no whitespace, which the -c flag takes as is.
POSTGRES_ARG_PATTERN = re.compile(r"[a-z_]+=\S+")


# A non-negative Kubernetes quantity: a decimal number with a binary-SI
# suffix (Ki to Ei), a decimal-SI suffix (m to E), or a decimal exponent.
QUANTITY_PATTERN = re.compile(
    r"([0-9]+(\.[0-9]*)?|\.[0-9]+)([KMGTPE]i|[mkMGTPE]|[eE][+-]?[0-9]+)?"
)


def shape(cfg: dict[str, Any], runner_cpu: Any) -> None:
    """Shape the control plane for the run: Postgres statistics, node roles
    (the runner's node must fit runner_cpu), the pool size, then the ateapi
    pin. No unpatch: every test redeploys substrate."""
    # Postgres first: enabling statistics restarts it, and the restarted pod
    # may land on another node, which the roles must then avoid.
    if cfg["pgStats"]:
        enable_postgres_stats(cfg["postgresArgs"])
    if cfg["pinNodes"]:
        label_nodes(cfg, runner_cpu)
    if cfg["poolMaxConns"] is not None:
        set_pool_max_conns(cfg["poolMaxConns"])
    pin_ateapi(cfg)
    # The placement of the run, for comparing runs: a step compares across
    # runs only when ateapi, Postgres, and the runner sat on the same nodes.
    run_no_check(
        ["kubectl", "-n", NAMESPACE, "get", "pods", "-o", "wide",
         "-l", "app in (ate-api-server,postgres)"]
    )
    run_no_check(["kubectl", "get", "nodes", "-L", ROLE_LABEL])


# Node label that gives ate-api-server and the runner a node each, so the
# load generator is never ateapi's neighbor. Both pods require it through a
# node affinity.
ROLE_LABEL = "ate.dev/benchmark-role"


# The taint of the node pool reserved for load generators. The runner Job
# tolerates it (manifests/runner-job.yaml.tmpl), so the runner may take a node
# there; ate-api-server does not, so it may not.
LOADGEN_TAINT = {"key": "ate.dev/benchmarking", "value": "true", "effect": "NoSchedule"}


def _repelling_taints(node: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {k: t.get(k) for k in ("key", "value", "effect")}
        for t in node.get("spec", {}).get("taints", [])
        if t.get("effect") in ("NoSchedule", "NoExecute")
    ]


def _cpu_millis(q: Any) -> int:
    """A CPU quantity, as tests.yaml or a node's allocatable spells it, in
    millicores."""
    if isinstance(q, (int, float)):
        return round(q * 1000)
    m = QUANTITY_PATTERN.fullmatch(q)
    if not m:
        raise ValueError(f"not a quantity: {q!r}")
    number, suffix = float(m.group(1)), m.group(3) or ""
    if suffix.startswith(("e", "E")):
        return round(number * 10 ** int(suffix[1:]) * 1000)
    scale = {"": 1, "m": 1e-3, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15, "E": 1e18,
             "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50, "Ei": 2**60}[suffix]
    return round(number * scale * 1000)


def _allocatable_millis(node: dict[str, Any]) -> int:
    return _cpu_millis(node.get("status", {}).get("allocatable", {}).get("cpu", "0"))


def pick_role_nodes(nodes: list[dict[str, Any]], postgres_node: str,
                    ateapi_millis: int = 0, runner_millis: int = 0) -> tuple[str, str]:
    """Return (ateapi node, runner node), never the Postgres node: Postgres on
    size10 requests nearly a whole node. Each node must be able to allocate
    the CPU its role asks for: ateapi_millis for every ateapi replica, which
    all share the node, and runner_millis for the runner. ateapi takes the
    first such untainted schedulable node by name. The runner takes the first
    such node of the load-generator pool, the one it is meant for, or else
    the next such untainted node."""
    schedulable = sorted(
        (n for n in nodes
         if n["metadata"]["name"] != postgres_node
         and not n.get("spec", {}).get("unschedulable")),
        key=lambda n: n["metadata"]["name"],
    )
    untainted = [n for n in schedulable if not _repelling_taints(n)]
    loadgen = [n for n in schedulable if _repelling_taints(n) == [LOADGEN_TAINT]]
    ateapi = next((n for n in untainted if _allocatable_millis(n) >= ateapi_millis), None)
    if ateapi is None:
        raise RuntimeError(
            f"pinNodes needs an untainted node besides the Postgres node {postgres_node!r} "
            f"that can allocate {ateapi_millis}m CPU for ateapi; found "
            f"{[(n['metadata']['name'], _allocatable_millis(n)) for n in untainted]}"
        )
    runner = next(
        (n for n in loadgen + untainted
         if n is not ateapi and _allocatable_millis(n) >= runner_millis),
        None,
    )
    if runner is None:
        raise RuntimeError(
            f"pinNodes needs a node besides the Postgres node {postgres_node!r} and the "
            f"ateapi node that can allocate {runner_millis}m CPU for the runner; found "
            f"load-generator pool {[n['metadata']['name'] for n in loadgen]}, untainted "
            f"{[n['metadata']['name'] for n in untainted if n is not ateapi]}"
        )
    return ateapi["metadata"]["name"], runner["metadata"]["name"]


def label_nodes(cfg: dict[str, Any], runner_cpu: Any) -> None:
    postgres_node = _kubectl_out(
        ["-n", NAMESPACE, "get", "pod", "postgres-0", "-o", "jsonpath={.spec.nodeName}"]
    )
    nodes = json.loads(_kubectl_out(["get", "nodes", "-o", "json"]))["items"]
    ateapi_node, runner_node = pick_role_nodes(
        nodes, postgres_node,
        ateapi_millis=cfg["replicas"] * _cpu_millis(cfg["cpu"]),
        runner_millis=_cpu_millis(runner_cpu),
    )
    # A label left by an earlier run on another node would give a role two
    # nodes.
    run_no_check(["kubectl", "label", "nodes", "--all", f"{ROLE_LABEL}-"])
    run(["kubectl", "label", "node", ateapi_node, f"{ROLE_LABEL}=ateapi", "--overwrite"])
    run(["kubectl", "label", "node", runner_node, f"{ROLE_LABEL}=runner", "--overwrite"])


def role_affinity(role: str) -> dict[str, Any]:
    return {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {"key": ROLE_LABEL, "operator": "In", "values": [role]}
                        ]
                    }
                ]
            }
        }
    }


POSTGRES_EXPORTER_IMAGE = "quay.io/prometheuscommunity/postgres-exporter:v0.17.1"


def postgres_args(settings: list[str]) -> list[str]:
    """The postgres container's args: the base manifest's config_file, which
    a strategic merge would otherwise drop, pg_stat_statements, then each of
    settings as a -c flag, which overrides postgresql.conf."""
    args = [
        "-c", "config_file=/etc/postgresql/postgresql.conf",
        "-c", "shared_preload_libraries=pg_stat_statements",
    ]
    for setting in settings:
        args += ["-c", setting]
    return args


def enable_postgres_stats(settings: list[str]) -> None:
    """Load pg_stat_statements, apply settings, and add a postgres_exporter
    sidecar, which reaches the server over the pod's unix socket (trusted by
    pg_hba). One patch, thus one Postgres restart, before ateapi rolls."""
    patch = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "postgres",
                            # Strategic merge replaces args whole.
                            "args": postgres_args(settings),
                        },
                        {
                            "name": "postgres-exporter",
                            "image": POSTGRES_EXPORTER_IMAGE,
                            "args": ["--collector.stat_statements"],
                            "env": [
                                {
                                    "name": "DATA_SOURCE_NAME",
                                    "value": "host=/var/run/postgresql user=postgres dbname=atepg sslmode=disable",
                                }
                            ],
                            # ate-system admits scrapes only to named ports
                            # such as metrics.
                            "ports": [{"name": "metrics", "containerPort": 9187}],
                            "volumeMounts": [
                                {"name": "socket", "mountPath": "/var/run/postgresql"}
                            ],
                            "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}},
                        },
                    ]
                }
            }
        }
    }
    _patch_and_wait("statefulset", "postgres", patch)
    run(
        ["kubectl", "-n", NAMESPACE, "exec", "postgres-0", "-c", "postgres", "--",
         "psql", "-U", "postgres", "-d", "atepg", "-v", "ON_ERROR_STOP=1",
         "-c", "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"]
    )


# ate-setup's ConfigMap of ateapi environment variables. ateapi reads
# ATE_API_POSTGRES_POOL_MAX_CONNS from it at start, over any pool_max_conns in
# the DSN.
ENV_CONFIGMAP = "ate-api-server-envvars"
POOL_MAX_CONNS_KEY = "ATE_API_POSTGRES_POOL_MAX_CONNS"


def pool_max_conns_patch(n: int) -> dict[str, Any]:
    return {"data": {POOL_MAX_CONNS_KEY: str(n)}}


def set_pool_max_conns(n: int) -> None:
    """Set the read/write pool size ateapi reads at start; the pin_ateapi
    rollout that follows picks it up. The owner and watch pools stay at 2
    and 3 connections per replica."""
    run(
        ["kubectl", "-n", NAMESPACE, "patch", "configmap", ENV_CONFIGMAP,
         "--type", "merge", "-p", json.dumps(pool_max_conns_patch(n))]
    )


def ateapi_patch(cfg: dict[str, Any]) -> dict[str, Any]:
    pod_spec: dict[str, Any] = {
        "containers": [
            {
                "name": "ate-api-server",
                "resources": {
                    "requests": {"cpu": str(cfg["cpu"]), "memory": cfg["memory"]},
                    "limits": {"cpu": str(cfg["cpu"]), "memory": cfg["memory"]},
                },
            }
        ]
    }
    if cfg["pinNodes"]:
        pod_spec["affinity"] = role_affinity("ateapi")
    return {"spec": {"replicas": cfg["replicas"], "template": {"spec": pod_spec}}}


def pin_ateapi(cfg: dict[str, Any]) -> None:
    _patch_and_wait("deployment", "ate-api-server", ateapi_patch(cfg))


def _patch_and_wait(kind: str, name: str, patch: dict[str, Any]) -> None:
    run(
        ["kubectl", "-n", NAMESPACE, "patch", kind, name,
         "--type=strategic", "-p", json.dumps(patch)]
    )
    run(
        ["kubectl", "-n", NAMESPACE, "rollout", "status", f"{kind}/{name}",
         "--timeout=600s"]
    )


def _kubectl_out(args: list[str]) -> str:
    return subprocess.run(
        ["kubectl", *args], capture_output=True, text=True, check=True
    ).stdout
