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

"""The `type: locust` hooks (see the package docstring for the lifecycle).

The runner Job wraps benchmarking/locust/runner.py, which drives locust
(and boomer for glutton tests) and uploads its own results.

An optional `controlPlane:` block shapes ate-api-server and Postgres for a
capacity run (see controlplane.py); without it pre_test leaves the cluster
as installed.
"""

import os
from typing import Any

import controlplane
from util import build_and_push

TEST_TYPE = "locust"


def control_plane_config(test: dict[str, Any]) -> dict[str, Any] | None:
    """The test's `controlPlane:` block merged over its defaults, or None."""
    if "controlPlane" not in test:
        return None
    return controlplane.config(test["controlPlane"])


def validate(test: dict[str, Any]) -> None:
    """Validate required locust entry fields (file, duration, users) and
    normalize optional cpuCores and cpuDutyCycle keys into runner flags."""
    name = test.get("name")
    for field in ("file", "duration", "users"):
        if field not in test:
            raise ValueError(f"locust test {name!r} missing {field!r}")
    if "controlPlane" in test:
        try:
            controlplane.validate(test["controlPlane"])
        except ValueError as e:
            raise ValueError(f"locust test {name!r}: {e}") from None

    flags = list(test.get("flags", []))
    cpu_cores = test.get("cpuCores")
    if cpu_cores is not None:
        if isinstance(cpu_cores, bool) or not isinstance(cpu_cores, int) or cpu_cores < 0:
            raise ValueError(
                f"test {name!r}: 'cpuCores' must be a non-negative integer, got {cpu_cores!r}"
            )
        flags.extend(["--cpu-cores", str(cpu_cores)])

    cpu_duty_cycle = test.get("cpuDutyCycle")
    if cpu_duty_cycle is not None:
        if (
            isinstance(cpu_duty_cycle, bool)
            or not isinstance(cpu_duty_cycle, (int, float))
            or cpu_duty_cycle < 0.0
            or cpu_duty_cycle > 1.0
        ):
            raise ValueError(
                f"test {name!r}: 'cpuDutyCycle' must be a float in [0.0, 1.0], got {cpu_duty_cycle!r}"
            )
        flags.extend(["--cpu-duty-cycle", str(float(cpu_duty_cycle))])

    if cpu_cores is not None or cpu_duty_cycle is not None:
        test["flags"] = flags


def build_image(commit: str) -> str:
    """Build & push the locust runner image. It must live in the same
    project as the test cluster so the runner Job can pull it."""
    return build_and_push(
        f"{os.environ['KO_DOCKER_REPO']}/locust-test:{commit}",
        "benchmarking/locust/Dockerfile",
    )


def pre_test(test: dict[str, Any]) -> None:
    """With a `controlPlane:` block, shape the control plane for the run."""
    cfg = control_plane_config(test)
    if cfg is None:
        return
    controlplane.shape(cfg)


def job_tmpl(manifests_dir: str) -> str:
    return os.path.join(manifests_dir, "runner-job.yaml.tmpl")


# Runner pod requests when a test sets none. Enough for the glutton and
# sweperf suites at their usual user counts; a boomer-driven class at
# thousands of users (agentsession marshals a 32Mi ingest payload per
# in-flight op) needs runnerCpu / runnerMemory raised in tests.yaml.
DEFAULT_RUNNER_CPU = "500m"
DEFAULT_RUNNER_MEMORY = "512Mi"


def job_subs(test: dict[str, Any]) -> dict[str, Any]:
    return {
        "TEST_FILE": test["file"],
        "DURATION": test["duration"],
        "USERS": test["users"],
        "RUNNER_CPU": test.get("runnerCpu", DEFAULT_RUNNER_CPU),
        "RUNNER_MEMORY": test.get("runnerMemory", DEFAULT_RUNNER_MEMORY),
    }
