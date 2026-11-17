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

"""One module per workload type. Workloads are shared across test types.

Each module implements:

  install(config)   installs the workload and waits until it is ready.

A test type's workloads(test) hook decides which workloads a test needs,
as (name, config) pairs, and passes them to install_all. Tests name a
workload by its WORKLOADS key, so the locust stubs, which live outside
benchmarking/automation, never import these modules.

Adding a workload = one new module here + one entry in WORKLOADS.
"""

from typing import Any, Iterable, Protocol

from workloads import deploy_sh


class Workload(Protocol):
    def install(self, config: dict[str, Any]) -> None: ...


WORKLOADS: dict[str, Workload] = {
    "deploy_sh": deploy_sh,
}


def install_all(entries: Iterable[tuple[str, dict[str, Any]]]) -> None:
    """Install each (name, config) pair in order. Raises ValueError on an
    unknown name, before anything is installed."""
    resolved = []
    for name, config in entries:
        if name not in WORKLOADS:
            raise ValueError(f"unknown workload {name!r} (want one of {list(WORKLOADS)})")
        resolved.append((WORKLOADS[name], config))
    for module, config in resolved:
        module.install(config)
