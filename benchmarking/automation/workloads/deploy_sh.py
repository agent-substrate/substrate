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

"""The deploy_sh workload: benchmarking/workloads/deploy.sh --deploy.

deploy.sh sets up the WorkerPool and its default ActorTemplates and waits
for their golden snapshots. Its config is the test's whole tests.yaml
entry, because deploy.sh sizes the pool and the actors from the entry's
workerCount, sandboxClass, workerMemory, workerWaitTimeout, actorMemory,
actorCPU and isolate fields.
"""

from typing import Any

from util import isolate_workload_args, run


def install(test: dict[str, Any]) -> None:
    cmd = [
        "benchmarking/workloads/deploy.sh",
        "--deploy",
        "--worker-count",
        str(test.get("workerCount", 1)),
        "--sandbox-class",
        test.get("sandboxClass", "gvisor"),
    ]
    # Empty keeps the default in workloads/deploy.sh (256Mi); RAM-consuming
    # suites set actorMemory in tests.yaml.
    if actor_memory := test.get("actorMemory", ""):
        cmd += ["--actor-memory", actor_memory]
    # Empty keeps the default in workloads/deploy.sh (1 CPU). The scheduler
    # books the limit against worker CPU capacity, so dense suites set
    # actorCPU in tests.yaml to fit more actors per worker.
    # str() because a whole number of cores (actorCPU: 1) parses as an int.
    if actor_cpu := test.get("actorCPU", ""):
        cmd += ["--actor-cpu", str(actor_cpu)]
    # Empty leaves the worker pods unsized; set workerMemory in tests.yaml to
    # control how many workers the scheduler packs onto a node.
    if worker_memory := test.get("workerMemory", ""):
        cmd += ["--worker-memory", worker_memory]
    # Empty keeps deploy.sh's own default; large fleets set workerWaitTimeout
    # (whole seconds).
    wait_timeout_secs = test.get("workerWaitTimeout", "")
    if wait_timeout_secs != "":
        cmd += ["--wait-timeout", str(wait_timeout_secs)]
    # An isolate block's tuning of fake-atelet, on the fake data plane.
    cmd += isolate_workload_args(test)
    # deploy.sh itself blocks until every template's golden snapshot exists.
    run(cmd)
