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

"""Shapes the control plane for a capacity run, as a test entry's
`controlPlane:` block asks: an `apiServer:` section for ate-api-server
(see API_SERVER_KNOBS) and a `postgres:` section (see POSTGRES_DEFAULTS).

A test type validates the block with validate() and calls shape() from its
pre_test. Isolation from the load is the install's: --cordon-control-plane
in ateArgs keeps ate-api-server and Postgres on their own tainted pools.
"""

import json
import re
from typing import Any

from util import run, run_no_check

NAMESPACE = "ate-system"

# Knobs of the `apiServer:` section. None has a default here: the install
# defines ate-api-server's shape, and a run overrides only the knobs it sets.
API_SERVER_KNOBS = {
    # Required, the independent variable of the run. CPU request and limit
    # of each replica; Go sizes GOMAXPROCS from the limit.
    "cpu",
    # Memory request and limit of each replica.
    "memory",
    "replicas",
    # pgxpool max connections per replica, set through ateapi's env
    # ConfigMap (see set_pool_max_conns).
    "poolMaxConns",
}

# Knobs of the `postgres:` section.
POSTGRES_DEFAULTS = {
    # Load pg_stat_statements and run postgres_exporter next to Postgres,
    # for Prometheus (benchmarking/monitoring.yaml) to scrape.
    "stats": True,
    # Postgres settings as name=value, passed to the server as -c flags in
    # the restart stats makes, over postgresql.conf.
    "args": [],
}


def config(block: dict[str, Any]) -> dict[str, Any]:
    """The `controlPlane:` block with `postgres:` merged over its defaults.
    `apiServer:` holds only the knobs the block sets."""
    return {
        "apiServer": dict(block.get("apiServer", {})),
        "postgres": {**POSTGRES_DEFAULTS, **block.get("postgres", {})},
    }


def validate(block: Any) -> None:
    """Raise ValueError unless block is a valid `controlPlane:` block."""
    if not isinstance(block, dict):
        raise ValueError("controlPlane must be a mapping")
    _check_knobs("controlPlane", block, {"apiServer", "postgres"})
    if "apiServer" not in block:
        raise ValueError("needs controlPlane.apiServer")
    _check_knobs("controlPlane.apiServer", block["apiServer"], API_SERVER_KNOBS)
    _check_knobs("controlPlane.postgres", block.get("postgres", {}), set(POSTGRES_DEFAULTS))
    cfg = config(block)
    api, pg = cfg["apiServer"], cfg["postgres"]
    if "cpu" not in api or not _is_quantity(api["cpu"]):
        raise ValueError("needs controlPlane.apiServer.cpu, as in 2 or 1500m")
    if "memory" in api and not _is_quantity(api["memory"]):
        raise ValueError("invalid controlPlane.apiServer.memory")
    if "replicas" in api and not _is_positive_int(api["replicas"]):
        raise ValueError("controlPlane.apiServer.replicas must be an int >= 1")
    if "poolMaxConns" in api and not _is_positive_int(api["poolMaxConns"]):
        raise ValueError("controlPlane.apiServer.poolMaxConns must be an int >= 1")
    if not isinstance(pg["stats"], bool):
        raise ValueError("controlPlane.postgres.stats must be a bool")
    args = pg["args"]
    if not isinstance(args, list) or not all(
        isinstance(a, str) and POSTGRES_ARG_PATTERN.fullmatch(a) for a in args
    ):
        raise ValueError(
            "controlPlane.postgres.args must be a list of name=value settings, "
            "as in commit_delay=100"
        )
    if args and not pg["stats"]:
        raise ValueError(
            "controlPlane.postgres.args needs stats: true, whose Postgres restart applies them"
        )


def _check_knobs(path: str, section: Any, allowed: set[str]) -> None:
    if not isinstance(section, dict):
        raise ValueError(f"{path} must be a mapping")
    unknown = set(section) - allowed
    if unknown:
        raise ValueError(
            f"unknown {path} knob(s) {sorted(unknown)}; allowed: {sorted(allowed)}"
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


# A Postgres setting as postgres.args spells it: a lowercase name, an equals
# sign, and a value with no whitespace, which the -c flag takes as is.
POSTGRES_ARG_PATTERN = re.compile(r"[a-z_]+=\S+")


# A non-negative Kubernetes quantity: a decimal number with a binary-SI
# suffix (Ki to Ei), a decimal-SI suffix (m to E), or a decimal exponent.
QUANTITY_PATTERN = re.compile(
    r"([0-9]+(\.[0-9]*)?|\.[0-9]+)([KMGTPE]i|[mkMGTPE]|[eE][+-]?[0-9]+)?"
)


# TODO: guard against drift in the manifests these patches assume, so a
# change fails the run instead of quietly skewing its numbers. Tests that
# read manifests/ate-install and check the ate-api-server container, the
# Postgres base args postgres_args restates, the socket volume, and the env
# ConfigMap name and key; and a check after shape() that the running pods
# carry the requested resources and Postgres the requested settings.
def shape(cfg: dict[str, Any]) -> None:
    """Shape the control plane for the run: Postgres statistics, the pool
    size, then ate-api-server's resources, whose rollout picks up the pool
    size. No unpatch: every test redeploys substrate."""
    api, pg = cfg["apiServer"], cfg["postgres"]
    if pg["stats"]:
        enable_postgres_stats(pg["args"])
    if "poolMaxConns" in api:
        set_pool_max_conns(api["poolMaxConns"])
    resize_api_server(api)
    # The placement of the run, for comparing runs: a step compares across
    # runs only when ateapi and Postgres sat on the same nodes.
    run_no_check(
        ["kubectl", "-n", NAMESPACE, "get", "pods", "-o", "wide",
         "-l", "app in (ate-api-server,postgres)"]
    )


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
    """Set the read/write pool size ateapi reads at start; the
    resize_api_server rollout that follows picks it up. The owner and watch
    pools stay at 2 and 3 connections per replica."""
    run(
        ["kubectl", "-n", NAMESPACE, "patch", "configmap", ENV_CONFIGMAP,
         "--type", "merge", "-p", json.dumps(pool_max_conns_patch(n))]
    )


def api_server_patch(api: dict[str, Any]) -> dict[str, Any]:
    """A strategic merge patch that sets only the knobs api holds, so the
    rest of the Deployment keeps the install's values."""
    resources = {"cpu": str(api["cpu"])}
    if "memory" in api:
        resources["memory"] = api["memory"]
    pod_spec = {
        "containers": [
            {
                "name": "ate-api-server",
                "resources": {"requests": resources, "limits": dict(resources)},
            }
        ]
    }
    spec: dict[str, Any] = {"template": {"spec": pod_spec}}
    if "replicas" in api:
        spec["replicas"] = api["replicas"]
    return {"spec": spec}


def resize_api_server(api: dict[str, Any]) -> None:
    _patch_and_wait("deployment", "ate-api-server", api_server_patch(api))


def _patch_and_wait(kind: str, name: str, patch: dict[str, Any]) -> None:
    run(
        ["kubectl", "-n", NAMESPACE, "patch", kind, name,
         "--type=strategic", "-p", json.dumps(patch)]
    )
    run(
        ["kubectl", "-n", NAMESPACE, "rollout", "status", f"{kind}/{name}",
         "--timeout=600s"]
    )

