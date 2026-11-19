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
import subprocess
from typing import Any

from util import build_and_push, run, run_no_check

TEST_TYPE = "nighthawk-egress"
DOCKERFILE = "benchmarking/nighthawk-egress/Dockerfile"
USES_SHARED_WORKLOADS = False

_REQUIRED = ("envoyCpu", "mode")
_VALID_MODES = ("http", "mitm", "passthrough")
_UNSUPPORTED_TOP_LEVEL_KEYS = (
    "workerCount",
    "actorMemory",
    "actorCPU",
    "workerMemory",
    "workerWaitTimeout",
    "isolate",
)


def shape_atenet_egress(envoy_cpu: int) -> None:
    """Pin deployment/atenet-egress to 1 replica and envoy_cpu cores, then wait for rollout."""
    # stdout only, so the stderr of a failed read stays on the log.
    res = run(
        ["kubectl", "-n", "ate-system", "get", "deployment/atenet-egress", "-o", "json"],
        stdout=subprocess.PIPE,
        text=True,
    )
    dep = json.loads(res.stdout)
    containers = dep["spec"]["template"]["spec"]["containers"]
    envoy = next((c for c in containers if c["name"] == "envoy"), None)
    if envoy is None:
        raise RuntimeError("deployment/atenet-egress has no envoy container")

    args = list(envoy.get("args", []))
    filtered: list[str] = []
    skip_next = False
    for a in args:
        if skip_next:
            skip_next = False
            continue
        if a == "--concurrency":
            skip_next = True
            continue
        filtered.append(a)
    # Envoy takes "--concurrency N" (patchAtenetEgressEnvoyConcurrency in cmd/ate-setup/internal/steps/overlay.go).
    filtered.extend(["--concurrency", str(envoy_cpu)])

    target_m = envoy_cpu * 1000
    cpu_res = {"requests": {"cpu": f"{target_m}m"}, "limits": {"cpu": f"{target_m}m"}}
    patch_containers = [
        {"name": "envoy", "args": filtered, "resources": cpu_res},
        {"name": "ext-proc", "resources": cpu_res},
    ]
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
    unknown = set(cfg) - set(_REQUIRED)
    if unknown:
        raise ValueError(
            f"nighthawk-egress test '{name}' has unknown knob(s): {sorted(unknown)}"
        )
    for k in _REQUIRED:
        if k not in cfg:
            raise ValueError(f"nighthawk-egress test '{name}' missing nighthawk-egress.{k}")
    if not isinstance(cfg["envoyCpu"], int) or isinstance(cfg["envoyCpu"], bool) or cfg["envoyCpu"] < 1:
        raise ValueError(f"nighthawk-egress test '{name}': envoyCpu must be a positive int")
    mode = cfg["mode"]
    if mode not in _VALID_MODES:
        raise ValueError(
            f"nighthawk-egress test '{name}': mode must be one of {_VALID_MODES}, got {mode!r}"
        )
    if "flags" in test:
        raise ValueError(f"nighthawk-egress test '{name}': put knobs under 'nighthawk-egress:'")


def build_image(commit: str) -> str:
    return build_and_push(
        f"{os.environ['KO_DOCKER_REPO']}/nighthawk-egress-test:{commit}",
        DOCKERFILE,
    )


def pre_test(test: dict[str, Any]) -> None:
    shape_atenet_egress(test["nighthawk-egress"]["envoyCpu"])
    # Pass the active kubeconfig context so setup-e2e-egress-tls-kind.sh does not default to kind-kind.
    ctx = run(["kubectl", "config", "current-context"], stdout=subprocess.PIPE, text=True).stdout.strip()
    env = {**os.environ, "KUBECTL_CONTEXT": ctx}
    run(["hack/setup-e2e-egress-tls-kind.sh"], env=env)
    run(["benchmarking/nighthawk-egress/deploy.sh", "--deploy"])
    run(["benchmarking/nighthawk-egress/deploy-traffic-driver.sh", "--deploy"])


def teardown_workloads() -> None:
    # stderr stays on the log, so a failed read does not look like a missing namespace.
    ns = run_no_check(
        ["kubectl", "get", "namespace", "nighthawk", "-o", "name", "--ignore-not-found"],
        stdout=subprocess.PIPE,
        text=True,
    )
    if ns.returncode == 0 and not ns.stdout.strip():
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
        stdout=subprocess.PIPE,
        text=True,
    )
    if api.stdout.strip():
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
    cfg = test["nighthawk-egress"]
    return {
        "ENVOY_CPU": str(cfg["envoyCpu"]),
        "MODE": str(cfg["mode"]),
    }
