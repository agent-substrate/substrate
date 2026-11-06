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

"""`type: nighthawk-egress` test definition for the benchmark orchestrator."""

import json
import os
from typing import Any

from util import build_and_push, parse_duration_seconds, run, run_no_check

TEST_TYPE = "nighthawk-egress"
DOCKERFILE = "benchmarking/nighthawk-egress/Dockerfile"
USES_SHARED_WORKLOADS = False
EXTRA_INGRESS_SPIFFE_IDS = ("spiffe://cluster.local/ns/nighthawk/sa/traffic-driver-invoker",)

DEFAULTS: dict[str, Any] = {
    "mode": "http",
    "clientConcurrency": 16,
    "connections": 16,
    "maxPending": 32,
    "initialRps": 500,
    "expFactor": 2.0,
    "measuringPeriod": "20s",
    "convergenceDeadline": "600s",
    "testingStageDuration": "60s",
    "successRateThreshold": 0.998,
    "sendRateThreshold": 0.9,
    "tailLatencySloMs": 25,
}

_REQUIRED = ("envoyCpu",)
_VALID_MODES = ("http", "mitm", "passthrough")
_ALLOWED = set(DEFAULTS) | set(_REQUIRED)
_UNSUPPORTED_TOP_LEVEL_KEYS = (
    "workerCount",
    "actorMemory",
    "actorCPU",
    "workerMemory",
    "workerWaitTimeout",
    "isolate",
)


def _cfg(test: dict[str, Any]) -> dict[str, Any]:
    return {**DEFAULTS, **(test.get("nighthawk-egress") or {})}


def _cpu_millicores(val: str | int | float | None) -> int | None:
    if val is None:
        return None
    s = str(val).strip()
    if s.endswith("m"):
        return int(round(float(s[:-1])))
    return int(round(float(s) * 1000))


def shape_atenet_egress(envoy_cpu: int) -> None:
    """Pin deployment/atenet-egress to 1 replica and envoy_cpu cores, then wait for rollout."""
    res = run(
        ["kubectl", "-n", "ate-system", "get", "deployment/atenet-egress", "-o", "json"],
        capture_output=True,
        text=True,
    )
    dep = json.loads(res.stdout)
    replicas = dep["spec"].get("replicas", 1)
    containers = dep["spec"]["template"]["spec"]["containers"]
    envoy = next((c for c in containers if c["name"] == "envoy"), None)
    if envoy is None:
        raise RuntimeError("deployment/atenet-egress has no 'envoy' container")
    ext_proc = next((c for c in containers if c["name"] == "ext-proc"), None)

    args = list(envoy.get("args", []))
    cpu_str = str(envoy_cpu)
    concurrency_vals: list[str] = []
    filtered: list[str] = []
    skip_next = False
    for i, a in enumerate(args):
        if skip_next:
            concurrency_vals.append(a)
            skip_next = False
            continue
        if a == "--concurrency":
            if i + 1 < len(args):
                skip_next = True
            else:
                concurrency_vals.append("")
            continue
        if a.startswith("--concurrency="):
            concurrency_vals.append(a.split("=", 1)[1])
            continue
        filtered.append(a)

    target_m = envoy_cpu * 1000

    def _matches_cpu(c: dict[str, Any]) -> bool:
        req = _cpu_millicores(c.get("resources", {}).get("requests", {}).get("cpu"))
        lim = _cpu_millicores(c.get("resources", {}).get("limits", {}).get("cpu"))
        return req == target_m and lim == target_m

    envoy_match = _matches_cpu(envoy)
    ext_proc_match = _matches_cpu(ext_proc) if ext_proc else True
    if replicas != 1 or concurrency_vals != [cpu_str] or not envoy_match or not ext_proc_match:
        # Envoy takes "--concurrency N" (patchAtenetEgressEnvoyConcurrency in cmd/ate-setup/internal/steps/overlay.go).
        filtered.extend(["--concurrency", str(envoy_cpu)])

        cpu_res = {"requests": {"cpu": f"{target_m}m"}, "limits": {"cpu": f"{target_m}m"}}
        patch_containers: list[dict[str, Any]] = [
            {"name": "envoy", "args": filtered, "resources": cpu_res}
        ]
        if ext_proc is not None:
            patch_containers.append({"name": "ext-proc", "resources": cpu_res})
        patch = {"spec": {"replicas": 1, "template": {"spec": {"containers": patch_containers}}}}

        run(
            [
                "kubectl",
                "-n",
                "ate-system",
                "patch",
                "deployment/atenet-egress",
                "--type=strategic",
                "-p",
                json.dumps(patch),
            ]
        )

    # Always wait: a spec that already matches may still be mid-rollout or
    # crash-looping from an earlier patch.
    run(
        [
            "kubectl",
            "-n",
            "ate-system",
            "rollout",
            "status",
            "deployment/atenet-egress",
            "--timeout=300s",
        ]
    )


def validate(test: dict[str, Any]) -> None:
    name = test.get("name", "<unnamed>")
    if "duration" not in test:
        raise ValueError(f"nighthawk-egress test '{name}' missing 'duration'")
    sandbox_class = test.get("sandboxClass", "gvisor")
    if sandbox_class != "gvisor":
        raise ValueError(
            f"nighthawk-egress test '{name}' only supports sandboxClass 'gvisor', got {sandbox_class!r}"
        )
    unsupported = sorted(set(_UNSUPPORTED_TOP_LEVEL_KEYS) & set(test))
    if unsupported:
        raise ValueError(
            f"nighthawk-egress test '{name}' does not support shared-workload field(s): {unsupported}"
        )
    cfg = test.get("nighthawk-egress")
    if not isinstance(cfg, dict):
        raise ValueError(f"nighthawk-egress test '{name}' requires 'nighthawk-egress:' block")
    unknown = set(cfg) - _ALLOWED
    if unknown:
        raise ValueError(
            f"nighthawk-egress test '{name}' has unknown knob(s): {sorted(unknown)}"
        )
    for k in _REQUIRED:
        if k not in cfg:
            raise ValueError(f"nighthawk-egress test '{name}' missing nighthawk-egress.{k}")
    if not isinstance(cfg["envoyCpu"], int) or isinstance(cfg["envoyCpu"], bool) or cfg["envoyCpu"] < 1:
        raise ValueError(f"nighthawk-egress test '{name}': envoyCpu must be a positive int")
    mode = cfg.get("mode", DEFAULTS["mode"])
    if mode not in _VALID_MODES:
        raise ValueError(
            f"nighthawk-egress test '{name}': mode must be one of {_VALID_MODES}, got {mode!r}"
        )
    if "flags" in test:
        raise ValueError(f"nighthawk-egress test '{name}': put knobs under 'nighthawk-egress:'")
    for field in ("clientConcurrency", "connections", "initialRps"):
        if field in cfg and (
            not isinstance(cfg[field], int) or isinstance(cfg[field], bool) or cfg[field] < 1
        ):
            raise ValueError(f"nighthawk-egress test '{name}': {field} must be an int >= 1")
    if "maxPending" in cfg and (
        not isinstance(cfg["maxPending"], int)
        or isinstance(cfg["maxPending"], bool)
        or cfg["maxPending"] < 0
    ):
        raise ValueError(f"nighthawk-egress test '{name}': maxPending must be an int >= 0")
    if "expFactor" in cfg and (
        not isinstance(cfg["expFactor"], (int, float))
        or isinstance(cfg["expFactor"], bool)
        or cfg["expFactor"] <= 1.0
    ):
        raise ValueError(f"nighthawk-egress test '{name}': expFactor must be > 1.0")
    for field in ("successRateThreshold", "sendRateThreshold"):
        if field in cfg and (
            not isinstance(cfg[field], (int, float))
            or isinstance(cfg[field], bool)
            or not (0 < cfg[field] <= 1.0)
        ):
            raise ValueError(f"nighthawk-egress test '{name}': {field} must be > 0 and <= 1.0")
    if "tailLatencySloMs" in cfg and (
        not isinstance(cfg["tailLatencySloMs"], (int, float))
        or isinstance(cfg["tailLatencySloMs"], bool)
        or cfg["tailLatencySloMs"] < 0
    ):
        raise ValueError(f"nighthawk-egress test '{name}': tailLatencySloMs must be >= 0")
    merged = _cfg(test)
    for field in ("measuringPeriod", "convergenceDeadline", "testingStageDuration"):
        parse_duration_seconds(str(merged[field]))


def build_image(commit: str) -> str:
    return build_and_push(
        f"{os.environ['KO_DOCKER_REPO']}/nighthawk-egress-test:{commit}",
        DOCKERFILE,
    )


def pre_test(test: dict[str, Any]) -> None:
    cfg = _cfg(test)
    shape_atenet_egress(cfg["envoyCpu"])
    # Pass the active kubeconfig context so setup-e2e-egress-tls-kind.sh does not default to kind-kind.
    ctx = run(["kubectl", "config", "current-context"], capture_output=True, text=True).stdout.strip()
    env = {**os.environ, **({"KUBECTL_CONTEXT": ctx} if ctx else {})}
    run(["hack/setup-e2e-egress-tls-kind.sh"], env=env)
    run(["benchmarking/nighthawk-egress/deploy.sh", "--deploy"])
    run(["benchmarking/nighthawk-egress/deploy-traffic-driver.sh", "--deploy"])


def teardown_workloads() -> None:
    ns = run_no_check(
        ["kubectl", "get", "namespace", "nighthawk", "-o", "name", "--ignore-not-found"],
        capture_output=True,
        text=True,
    )
    if not (ns.stdout or "").strip():
        return
    api = run_no_check(
        [
            "kubectl",
            "get",
            "deployment",
            "ate-api-server",
            "-n",
            "ate-system",
            "-o",
            "name",
            "--ignore-not-found",
        ],
        capture_output=True,
        text=True,
    )
    if (api.stdout or "").strip():
        run_no_check(["benchmarking/nighthawk-egress/deploy-traffic-driver.sh", "--delete"])
    else:
        run_no_check(
            [
                "kubectl",
                "delete",
                "--ignore-not-found",
                "-f",
                "benchmarking/nighthawk-egress/traffic-driver.yaml.tmpl",
            ]
        )
    run_no_check(["benchmarking/nighthawk-egress/deploy.sh", "--delete"])
    run_no_check(["kubectl", "delete", "namespace", "nighthawk", "--ignore-not-found"])


def job_tmpl(manifests_dir: str) -> str:
    return os.path.join(manifests_dir, "nighthawk-egress-runner-job.yaml.tmpl")


def job_subs(test: dict[str, Any]) -> dict[str, str]:
    cfg = _cfg(test)
    return {
        "ENVOY_CPU": str(cfg["envoyCpu"]),
        "MODE": str(cfg["mode"]),
        "CLIENT_CONCURRENCY": str(cfg["clientConcurrency"]),
        "CONNECTIONS": str(cfg["connections"]),
        "MAX_PENDING": str(cfg["maxPending"]),
        "INITIAL_RPS": str(cfg["initialRps"]),
        "EXP_FACTOR": str(cfg["expFactor"]),
        "MEASURING_PERIOD": str(cfg["measuringPeriod"]),
        "CONVERGENCE_DEADLINE": str(cfg["convergenceDeadline"]),
        "TESTING_STAGE_DURATION": str(cfg["testingStageDuration"]),
        "SUCCESS_RATE_THRESHOLD": str(cfg["successRateThreshold"]),
        "SEND_RATE_THRESHOLD": str(cfg["sendRateThreshold"]),
        "TAIL_LATENCY_SLO_MS": str(cfg["tailLatencySloMs"]),
    }
