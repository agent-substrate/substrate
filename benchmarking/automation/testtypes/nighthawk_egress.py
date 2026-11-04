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

import os

from util import build_and_push, parse_duration_seconds, run, run_no_check

TEST_TYPE = "nighthawk-egress"
DOCKERFILE = "benchmarking/nighthawk-egress/Dockerfile"
USES_SHARED_WORKLOADS = False
EXTRA_INGRESS_SPIFFE_IDS = ("spiffe://cluster.local/ns/nighthawk/sa/traffic-driver-invoker",)

DEFAULTS: dict = {
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


def _cfg(test: dict) -> dict:
    return {**DEFAULTS, **(test.get("nighthawk-egress") or {})}


def validate(test: dict) -> None:
    name = test.get("name", "<unnamed>")
    if "duration" not in test:
        raise ValueError(f"nighthawk-egress test '{name}' missing 'duration'")
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
    if not isinstance(cfg["envoyCpu"], int) or cfg["envoyCpu"] < 1:
        raise ValueError(f"nighthawk-egress test '{name}': envoyCpu must be a positive int")
    mode = cfg.get("mode", DEFAULTS["mode"])
    if mode not in _VALID_MODES:
        raise ValueError(
            f"nighthawk-egress test '{name}': mode must be one of {_VALID_MODES}, got {mode!r}"
        )
    if "flags" in test:
        raise ValueError(f"nighthawk-egress test '{name}': put knobs under 'nighthawk-egress:'")
    for field in ("clientConcurrency", "connections", "initialRps"):
        if field in cfg and (not isinstance(cfg[field], int) or isinstance(cfg[field], bool) or cfg[field] < 1):
            raise ValueError(f"nighthawk-egress test '{name}': {field} must be an int >= 1")
    if "maxPending" in cfg and (not isinstance(cfg["maxPending"], int) or isinstance(cfg["maxPending"], bool) or cfg["maxPending"] < 0):
        raise ValueError(f"nighthawk-egress test '{name}': maxPending must be an int >= 0")
    if "expFactor" in cfg and (not isinstance(cfg["expFactor"], (int, float)) or isinstance(cfg["expFactor"], bool) or cfg["expFactor"] <= 1.0):
        raise ValueError(f"nighthawk-egress test '{name}': expFactor must be > 1.0")
    for field in ("successRateThreshold", "sendRateThreshold"):
        if field in cfg and (not isinstance(cfg[field], (int, float)) or isinstance(cfg[field], bool) or not (0 < cfg[field] <= 1.0)):
            raise ValueError(f"nighthawk-egress test '{name}': {field} must be > 0 and <= 1.0")
    if "tailLatencySloMs" in cfg and (not isinstance(cfg["tailLatencySloMs"], (int, float)) or isinstance(cfg["tailLatencySloMs"], bool) or cfg["tailLatencySloMs"] < 0):
        raise ValueError(f"nighthawk-egress test '{name}': tailLatencySloMs must be >= 0")
    merged = _cfg(test)
    for field in ("measuringPeriod", "convergenceDeadline", "testingStageDuration"):
        parse_duration_seconds(str(merged[field]))


def build_image(commit: str) -> str:
    return build_and_push(
        f"{os.environ['KO_DOCKER_REPO']}/nighthawk-egress-test:{commit}",
        DOCKERFILE,
    )

def pre_test(test: dict) -> None:
    cfg = _cfg(test)
    run(["python3", "benchmarking/nighthawk-egress/cluster_setup.py", "shape", "--envoy-cpu", str(cfg["envoyCpu"])])
    ctx_res = run(["kubectl", "config", "current-context"], capture_output=True, text=True)
    ctx = ctx_res.stdout.strip()
    env = {**os.environ, **({"KUBECTL_CONTEXT": ctx} if ctx else {})}
    run(["hack/setup-e2e-egress-tls-kind.sh"], env=env)
    run(["benchmarking/nighthawk-egress/deploy.sh", "--deploy"])
    run(["benchmarking/nighthawk-egress/deploy-traffic-driver.sh", "--deploy"])


def teardown_workloads() -> None:
    for script in (
        "benchmarking/nighthawk-egress/deploy-traffic-driver.sh",
        "benchmarking/nighthawk-egress/deploy.sh",
    ):
        run_no_check([script, "--delete"])


def job_tmpl(manifests_dir: str) -> str:
    return os.path.join(manifests_dir, "nighthawk-egress-runner-job.yaml.tmpl")


def job_subs(test: dict) -> dict[str, str]:
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
