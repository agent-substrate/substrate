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

"""One module per tests.yaml `type`; orchestrator.py dispatches via TYPES.

Each module implements the TestType hooks, called in lifecycle order:

  validate(test)        preflight, before any cluster work. Raises
                        ValueError on a malformed entry; may normalize it
                        (nighthawk-ingress requires workerCount).
  build_image(commit)   builds + pushes the runner image; called lazily,
                        once per (target cluster, type).
  pre_test(test)        after deploy_substrate, before deploy_workloads —
                        the type's chance to shape the cluster (nighthawk-ingress
                        pins the router; locust needs nothing).
  job_tmpl(manifests_dir)
                        path of the type's own Job template.
  job_subs(test)        the type's ${...} substitutions for it. The test
                        itself is always the same shared machinery:
                        orchestrator renders the template, submits the
                        Job, waits, tails logs, deletes it.

No post_test hook: teardown is type-independent today. Name it here when
a type needs one.

Adding a test type = one new module here + one entry in TYPES.
"""

from typing import Any, Protocol

from testtypes import locust
from testtypes import nighthawk_ingress


class TestType(Protocol):
    TEST_TYPE: str

    def validate(self, test: dict[str, Any]) -> None: ...
    def build_image(self, commit: str) -> str: ...
    def pre_test(self, test: dict[str, Any]) -> None: ...
    def job_tmpl(self, manifests_dir: str) -> str: ...
    def job_subs(self, test: dict[str, Any]) -> dict[str, Any]: ...


TYPES: dict[str, TestType] = {
    locust.TEST_TYPE: locust,
    nighthawk_ingress.TEST_TYPE: nighthawk_ingress,
}

# The Protocol above only binds when a type checker runs, and CI runs none
# for Python — so enforce the contract here, where every entry point
# (orchestrator, run-dev.sh, the unit tests) fails at import instead of
# mid-benchmark.
for _name, _mod in TYPES.items():
    for _hook in ("validate", "build_image", "pre_test", "job_tmpl", "job_subs"):
        assert hasattr(_mod, _hook), f"test type {_name!r} missing hook {_hook!r}"
