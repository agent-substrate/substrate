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

"""The `type: locust` hooks (see the package docstring for the lifecycle).

The runner Job wraps benchmarking/locust/runner.py, which drives locust
(and boomer for glutton tests) and uploads its own results.

An optional `controlPlane:` block shapes ate-api-server and Postgres for a
capacity run (see controlplane.py); without it pre_test leaves the cluster
as installed.

The test's stub (the /app/tests/ entry in `file`) says which workloads the
test needs, through a workloads(test) function that returns (name, config)
pairs for workloads.install_all.
"""

import importlib.util
import os
from typing import Any

import controlplane
from util import build_and_push
from workloads import install_all

TEST_TYPE = "locust"

# The runner image's stub directory, and where the stubs are in the repo.
IMAGE_STUB_DIR = "/app/tests/"
REPO_STUB_DIR = "benchmarking/locust/tests"
# Set while the orchestrator loads a stub, so the stub skips its locust
# imports and User class. The orchestrator image has no locust.
NO_USERS_ENV = "LOCUST_NO_USERS"


def control_plane_config(test: dict[str, Any]) -> dict[str, Any] | None:
    """The test's `controlPlane:` block merged over its defaults, or None."""
    if "controlPlane" not in test:
        return None
    return controlplane.config(test["controlPlane"])


def stub_path(test: dict[str, Any]) -> str:
    """The repo path of the test's stub. Raises ValueError unless `file`
    has exactly one /app/tests/ entry."""
    stubs = [f for f in str(test["file"]).split(",") if f.startswith(IMAGE_STUB_DIR)]
    if len(stubs) != 1:
        raise ValueError(
            f"locust test {test.get('name')!r}: file needs one {IMAGE_STUB_DIR} entry, got {test['file']!r}"
        )
    return os.path.join(REPO_STUB_DIR, stubs[0].removeprefix(IMAGE_STUB_DIR))


def validate(test: dict[str, Any]) -> None:
    name = test.get("name")
    for field in ("file", "duration", "users"):
        if field not in test:
            raise ValueError(f"locust test {name!r} missing {field!r}")
    stub_path(test)
    if "controlPlane" not in test:
        return
    try:
        controlplane.validate(test["controlPlane"])
    except ValueError as e:
        raise ValueError(f"locust test {name!r}: {e}") from None


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


def workloads(test: dict[str, Any]) -> None:
    """Install the workloads the test's stub asks for. Runs from the repo
    root of the branch under test, so the stub comes from that branch."""
    path = stub_path(test)
    spec = importlib.util.spec_from_file_location("locust_stub", path)
    stub = importlib.util.module_from_spec(spec)
    prev = os.environ.get(NO_USERS_ENV)
    os.environ[NO_USERS_ENV] = "1"
    try:
        spec.loader.exec_module(stub)
    except ImportError as e:
        raise ValueError(f"{path} must load without locust when {NO_USERS_ENV}=1: {e}") from e
    finally:
        if prev is None:
            del os.environ[NO_USERS_ENV]
        else:
            os.environ[NO_USERS_ENV] = prev
    if not hasattr(stub, "workloads"):
        raise ValueError(f"{path} does not define workloads(test)")
    install_all(stub.workloads(test))


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
