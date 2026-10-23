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

"""The benchmark workers, shared by every test type.

A test's workers are the benchmark WorkerPool. How many, which sandbox
class and how big come from the tests.yaml entry (workerCount, sandboxClass,
workerMemory, workerWaitTimeout, and the isolate block's fake-atelet
tuning), not from the test's workloads:

  workers.config(test)     preflight, before any cluster work
  workers.install(test)    after deploy_substrate: fake-atelet tuning, the
                           WorkerPool, and a wait for its workers
  workers.teardown()       after the test, before teardown_substrate

The sandbox class's SandboxConfig comes from deploy_substrate, which passes
--sandbox-classes to ate-setup. The WorkerPool is rendered from
manifests/workloads.yaml.tmpl, the same file workloads/deploy.sh uses.
Commands run from the substrate repo root, like the rest of the orchestrator.
"""

from dataclasses import dataclass
from string import Template
from typing import Any

from util import isolate_tuning_env, run, run_no_check

POOL_MANIFEST = "benchmarking/workloads/manifests/workloads.yaml.tmpl"
NAMESPACE = "benchmark-workloads"
POOL_NAME = "benchmark-worker"
SANDBOX_CLASSES = ("gvisor", "microvm")
DEFAULT_WAIT_TIMEOUT_SECS = 300


@dataclass(frozen=True)
class Config:
    """A test's workers, resolved from its tests.yaml entry."""

    count: int = 1
    sandbox_class: str = "gvisor"
    # Worker pod memory, set as both request and limit. Empty leaves the pod
    # unsized.
    memory: str = ""
    wait_timeout_secs: int = DEFAULT_WAIT_TIMEOUT_SECS
    # FAKE_ATELET_<FLAG>=VALUE entries from the isolate block.
    fake_atelet_env: tuple[str, ...] = ()


def config(test: dict[str, Any]) -> Config:
    """The workers a tests.yaml entry asks for, with defaults filled in.
    Raises ValueError on a malformed entry."""
    name = test.get("name")
    count = test.get("workerCount", 1)
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError(f"test {name!r}: workerCount must be a whole number, got {count!r}")
    sandbox_class = test.get("sandboxClass", "gvisor")
    if sandbox_class not in SANDBOX_CLASSES:
        raise ValueError(
            f"test {name!r}: sandboxClass must be one of {list(SANDBOX_CLASSES)}, got {sandbox_class!r}"
        )
    memory = test.get("workerMemory") or ""
    if not isinstance(memory, str):
        raise ValueError(f"test {name!r}: workerMemory must be a quantity like 8Gi, got {memory!r}")
    timeout = test.get("workerWaitTimeout", DEFAULT_WAIT_TIMEOUT_SECS)
    # A whole number of seconds; YAML may load it as an int or a string.
    if isinstance(timeout, bool) or not str(timeout).isdigit():
        raise ValueError(
            f"test {name!r}: workerWaitTimeout must be a whole number of seconds, got {timeout!r}"
        )
    try:
        fake_atelet_env = isolate_tuning_env(test)
    except ValueError as e:
        raise ValueError(f"test {name!r}: {e}") from None
    return Config(
        count=count,
        sandbox_class=sandbox_class,
        memory=memory,
        wait_timeout_secs=int(timeout),
        fake_atelet_env=tuple(fake_atelet_env),
    )


def render_pool(cfg: Config) -> str:
    """The WorkerPool manifest for cfg, as deploy.sh substitutes it."""
    # One flow-style line, so an unsized pod leaves only a blank line.
    resources = ""
    if cfg.memory:
        resources = (
            f'resources: {{requests: {{memory: "{cfg.memory}"}}, '
            f'limits: {{memory: "{cfg.memory}"}}}}'
        )
    with open(POOL_MANIFEST) as f:
        return Template(f.read()).substitute(
            WORKER_COUNT=cfg.count,
            SANDBOX_CLASS=cfg.sandbox_class,
            WORKER_RESOURCES=resources,
        )


def install(test: dict[str, Any]) -> Config:
    """Set up the test's workers and wait until they are ready. Needs the
    substrate CRDs, so it runs after deploy_substrate."""
    cfg = config(test)
    fake = _fake_data_plane()
    # Before the pool, so its fake Workers are served by the tuned fake-atelet.
    if fake:
        _tune_fake_atelet(cfg)
    elif cfg.fake_atelet_env:
        raise RuntimeError("an isolate block needs substrate installed with --fake-data-plane")
    print(
        f"Installing workers (count={cfg.count}, sandbox_class={cfg.sandbox_class}, "
        f"memory={cfg.memory or 'unset'})...",
        flush=True,
    )
    # ko resolves the ko:// worker image before kubectl sees it.
    run(["hack/run-tool.sh", "ko", "apply", "-f", "-"], input=render_pool(cfg), text=True)
    _wait_ready(cfg, fake)
    return cfg


def teardown() -> None:
    """Remove the workers. Safe to run on a clean cluster. Runs after the
    test's ActorTemplates are deleted, and before teardown_substrate, which
    removes the CRDs these objects need."""
    run_no_check(
        ["kubectl", "delete", "workerpool", POOL_NAME, f"--namespace={NAMESPACE}", "--ignore-not-found"]
    )
    run_no_check(["kubectl", "delete", "namespace", NAMESPACE, "--ignore-not-found"])


def _wait_ready(cfg: Config, fake: bool) -> None:
    timeout = f"--timeout={cfg.wait_timeout_secs}s"
    if fake:
        # No Deployment is made for the pool; fake-workersync counts a fake
        # Worker ready once its capacity is reported, and writes that to the
        # pool's status.
        if cfg.count > 0:
            run(
                [
                    "kubectl", "wait",
                    f"--for=jsonpath={{.status.readyReplicas}}={cfg.count}",
                    f"workerpool/{POOL_NAME}", f"--namespace={NAMESPACE}", timeout,
                ]
            )
        return
    run(["kubectl", "wait", "--for=create", f"deployment/{POOL_NAME}", f"--namespace={NAMESPACE}", timeout])
    run(["kubectl", "rollout", "status", f"deployment/{POOL_NAME}", f"--namespace={NAMESPACE}", timeout])


def _kubectl_output(args: list[str]) -> str:
    return run(["kubectl", *args], capture_output=True, text=True).stdout.strip()


def _fake_data_plane() -> bool:
    """Whether substrate was installed with --fake-data-plane, which serves
    the WorkerPool with fake Workers instead of worker pods."""
    return bool(
        _kubectl_output(
            ["get", "deployment/fake-workersync", "--namespace=ate-system", "-o", "name", "--ignore-not-found"]
        )
    )


def _tune_fake_atelet(cfg: Config) -> None:
    """Write cfg's tuning, and nothing else, to the fake-atelet-tuning
    ConfigMap fake-atelet loads its flags from, and restart fake-atelet when
    that changes the ConfigMap."""
    get = ["get", "configmap", "fake-atelet-tuning", "--namespace=ate-system", "-o", "jsonpath={.data}"]
    before = _kubectl_output([*get, "--ignore-not-found"])
    manifest = _kubectl_output(
        [
            "create", "configmap", "fake-atelet-tuning", "--namespace=ate-system",
            *(f"--from-literal={e}" for e in cfg.fake_atelet_env),
            "--dry-run=client", "-o", "yaml",
        ]
    )
    run(["kubectl", "apply", "-f", "-"], input=manifest, text=True)
    if _kubectl_output(get) == before:
        return
    print(f"Restarting fake-atelet with its new tuning ({' '.join(cfg.fake_atelet_env) or 'defaults'})...", flush=True)
    # The DaemonSet name carries the version, so it is addressed by label.
    for ds in _kubectl_output(["get", "daemonset", "--namespace=ate-system", "-l", "app=atelet", "-o", "name"]).split():
        run(["kubectl", "rollout", "restart", "--namespace=ate-system", ds])
        run(["kubectl", "rollout", "status", "--namespace=ate-system", ds, f"--timeout={cfg.wait_timeout_secs}s"])
