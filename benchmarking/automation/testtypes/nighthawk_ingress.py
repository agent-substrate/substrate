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

"""The `type: nighthawk-ingress` hooks (see the package docstring for the
lifecycle).

Measures atenet-router capacity at one Envoy CPU limit: pre_test pins the
router to the size under test, and the runner Job drives Nighthawk's
adaptive search against it.
"""

import json
import os
import re
from typing import Any

from util import build_and_push, parse_duration_seconds, run

TEST_TYPE = "nighthawk-ingress"

# Defaults for the tests.yaml `nighthawk-ingress:` block; envoyCpu has no default —
# it is the benchmark's independent variable. Documented in
# benchmarking/nighthawk-ingress/README.md, "Configuration knobs".
DEFAULTS = {
    # Actor namespace: one atespace per experiment, never per run —
    # nothing deletes atespaces automatically.
    "atespace": "ingress-benchmark",
    # Client sizing decoupled from envoyCpu so the harness never binds.
    "clientConcurrency": 16,
    "connections": 1000,
    "maxPendingRequests": 10000,
    "initialRps": 500,
    "exponentialFactor": 2.0,
    "measuringPeriod": "10s",
    "convergenceDeadline": "600s",
    "testingStageDuration": "60s",
    "successRateThreshold": 0.999,
    "sendRateThreshold": 0.9,
    # Tail-latency SLO (mean+2stdev) in ms; 0 disables.
    "tailLatencySloMs": 0,
}


def config(test: dict[str, Any]) -> dict[str, Any]:
    """The test's `nighthawk:` block merged over DEFAULTS."""
    return {**DEFAULTS, **test.get("nighthawk-ingress", {})}


def job_tmpl(manifests_dir: str) -> str:
    return os.path.join(manifests_dir, "nighthawk-ingress-runner-job.yaml.tmpl")


def validate(test: dict[str, Any]) -> None:
    """The nighthawk-specific half of validate_and_normalize_tests:
    raises ValueError on a malformed entry. workerCount is required — the
    benchmark warms one actor per worker (warm-path-only measurement), so
    it is also the actor fleet size."""
    name = test.get("name")
    if "duration" not in test:
        raise ValueError(f"nighthawk-ingress test {name!r} missing 'duration'")
    # Unknown knobs are rejected rather than silently merged: a typo (or a
    # knob removed in a schema change) must not run with defaults.
    allowed = set(DEFAULTS) | {"envoyCpu"}
    unknown = set(test.get("nighthawk-ingress", {})) - allowed
    if unknown:
        raise ValueError(
            f"nighthawk-ingress test {name!r} has unknown knob(s) "
            f"{sorted(unknown)}; allowed: {sorted(allowed)}"
        )
    nh = config(test)
    envoy_cpu = nh.get("envoyCpu")
    if not isinstance(envoy_cpu, int) or envoy_cpu < 1:
        raise ValueError(
            f"nighthawk-ingress test {name!r} needs nighthawk-ingress.envoyCpu (int >= 1)"
        )
    client_concurrency = nh["clientConcurrency"]
    if not isinstance(client_concurrency, int) or client_concurrency < 1:
        raise ValueError(
            f"nighthawk-ingress test {name!r} has invalid "
            f"nighthawk-ingress.clientConcurrency {client_concurrency!r}"
        )
    worker_count = test.get("workerCount")
    if not isinstance(worker_count, int) or worker_count < 1:
        raise ValueError(
            f"nighthawk-ingress test {name!r} needs workerCount (int >= 1): the "
            f"benchmark warms one actor per worker, so it is also the "
            f"actor fleet size"
        )
    for field in ("measuringPeriod", "convergenceDeadline", "testingStageDuration"):
        parse_duration_seconds(str(nh[field]))
    if not re.fullmatch(r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?", nh["atespace"]):
        raise ValueError(
            f"nighthawk-ingress test {name!r} atespace {nh['atespace']!r} "
            f"must be a DNS label (it is routed as a Host-header subdomain)"
        )


def build_image(commit: str) -> str:
    """Build & push the nighthawk capacity-benchmark runner image."""
    return build_and_push(
        f"{os.environ['KO_DOCKER_REPO']}/nighthawk-ingress-test:{commit}",
        "benchmarking/nighthawk-ingress/Dockerfile",
    )


def pre_test(test: dict[str, Any]) -> None:
    """Pin atenet-router to the test's envoyCpu: both containers get
    requests=limits=envoyCpu (Guaranteed QoS), and envoy's command is
    replaced to add --concurrency envoyCpu and drop the base manifest's
    debug log flags. Blocks on rollout. No unpatch: every test redeploys
    substrate."""
    cpu = str(config(test)["envoyCpu"])
    patch = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "atenet-router",
                            "resources": {
                                "requests": {"cpu": cpu},
                                "limits": {"cpu": cpu},
                            },
                        },
                        {
                            "name": "envoy",
                            "command": [
                                "/usr/local/bin/envoy",
                                "-c",
                                "/etc/envoy/envoy.yaml",
                                "--concurrency",
                                cpu,
                            ],
                            "resources": {
                                "requests": {"cpu": cpu},
                                "limits": {"cpu": cpu},
                            },
                        },
                    ]
                }
            }
        }
    }
    run(
        [
            "kubectl",
            "-n",
            "ate-system",
            "patch",
            "deployment",
            "atenet-router",
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
            "deployment/atenet-router",
            "--timeout=300s",
        ]
    )


def job_subs(test: dict[str, Any]) -> dict[str, Any]:
    """Template substitutions specific to the nighthawk runner Job."""
    nh = config(test)
    return {
        "ENVOY_CPU": nh["envoyCpu"],
        "ATESPACE": nh["atespace"],
        # Warm-path-only measurement: one running actor per worker, so
        # the fleet size is the worker count.
        "ACTORS": test["workerCount"],
        "CLIENT_CONCURRENCY": nh["clientConcurrency"],
        "CONNECTIONS": nh["connections"],
        "MAX_PENDING": nh["maxPendingRequests"],
        "INITIAL_RPS": nh["initialRps"],
        "EXP_FACTOR": nh["exponentialFactor"],
        "MEASURING_PERIOD": nh["measuringPeriod"],
        "CONVERGENCE_DEADLINE": nh["convergenceDeadline"],
        "TESTING_STAGE_DURATION": nh["testingStageDuration"],
        "SUCCESS_RATE": nh["successRateThreshold"],
        "SEND_RATE": nh["sendRateThreshold"],
        "TAIL_LATENCY_SLO_MS": nh["tailLatencySloMs"],
        # One core per event loop + one for the python runner.
        "RUNNER_CPU": nh["clientConcurrency"] + 1,
    }
