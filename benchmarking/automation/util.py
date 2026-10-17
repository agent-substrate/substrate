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

"""Helpers shared by orchestrator.py and the per-test-type modules."""

import re
import shlex
import subprocess
import time


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(shlex.quote(c) for c in cmd)}", flush=True)
    start = time.monotonic()
    try:
        return subprocess.run(cmd, check=True, **kwargs)
    finally:
        print(f"  (took {time.monotonic() - start:.1f}s)", flush=True)


def run_no_check(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(shlex.quote(c) for c in cmd)}", flush=True)
    start = time.monotonic()
    try:
        return subprocess.run(cmd, check=False, **kwargs)
    finally:
        print(f"  (took {time.monotonic() - start:.1f}s)", flush=True)


def build_and_push(image: str, dockerfile: str) -> str:
    """docker build (linux/amd64, the cluster architecture) + push, from
    the repo root."""
    run(
        [
            "docker",
            "build",
            "--platform",
            "linux/amd64",
            "-t",
            image,
            "-f",
            dockerfile,
            ".",
        ]
    )
    run(["docker", "push", image])
    return image


def parse_duration_seconds(s: str) -> int:
    m = re.fullmatch(r"(\d+)\s*([smh]?)", s.strip())
    if not m:
        raise ValueError(f"unrecognized duration: {s}")
    n = int(m.group(1))
    unit = m.group(2) or "s"
    return n * {"s": 1, "m": 60, "h": 3600}[unit]


# The keys of a tests.yaml isolate block and the fake-atelet tuning
# variable each one sets, through deploy.sh --fake-atelet-env.
ISOLATE_TUNING = {
    "delay": "FAKE_ATELET_DELAY",
    "delayRun": "FAKE_ATELET_DELAY_RUN",
    "delayRestore": "FAKE_ATELET_DELAY_RESTORE",
    "delayCheckpoint": "FAKE_ATELET_DELAY_CHECKPOINT",
    "delayPromoteSnapshot": "FAKE_ATELET_DELAY_PROMOTE_SNAPSHOT",
    "delayTerminate": "FAKE_ATELET_DELAY_TERMINATE",
    "mintActorCertificate": "FAKE_ATELET_MINT_ACTOR_CERTIFICATE",
}


# A non-negative Go duration, as fake-atelet parses its delays.
ISOLATE_DELAY_PATTERN = re.compile(r"0|([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+")


def isolate_block(test: dict) -> dict | None:
    """Return a tests.yaml entry's isolate block, or None when the entry has
    none. A bare `isolate:` selects the mode with every default. Raises
    ValueError on an unknown key or a value fake-atelet would reject, so a bad
    entry fails before any cluster work rather than in a crash-looping
    fake-atelet."""
    if "isolate" not in test:
        return None
    # YAML loads a key with no value as None.
    block = test["isolate"]
    if block is None:
        block = {}
    if not isinstance(block, dict):
        raise ValueError(f"isolate must be a mapping, got {block!r}")
    unknown = sorted(set(block) - set(ISOLATE_TUNING))
    if unknown:
        raise ValueError(
            f"isolate has unknown keys {unknown} "
            f"(want some of {sorted(ISOLATE_TUNING)})"
        )
    for key, value in block.items():
        if key == "mintActorCertificate":
            if not isinstance(value, bool):
                raise ValueError(f"isolate.{key} must be true or false, got {value!r}")
        elif not isinstance(value, str) or not ISOLATE_DELAY_PATTERN.fullmatch(value):
            raise ValueError(f"isolate.{key} must be a duration string such as 500ms, got {value!r}")
    return block


def isolate_install_args(test: dict) -> list[str]:
    """Return the hack/install-ate.sh flags a tests.yaml entry's isolate block
    installs the fake data plane with, or none without a block. The installer
    only chooses the data plane; the block's tuning goes to deploy.sh."""
    return [] if isolate_block(test) is None else ["--fake-data-plane"]


def isolate_workload_args(test: dict) -> list[str]:
    """Return the benchmarking/workloads/deploy.sh flags that tune fake-atelet
    for a tests.yaml entry's isolate block, or none without a block."""
    block = isolate_block(test) or {}
    args = []
    for key, env in ISOLATE_TUNING.items():
        if key not in block:
            continue
        value = block[key]
        if isinstance(value, bool):
            value = "true" if value else "false"
        args += ["--fake-atelet-env", f"{env}={value}"]
    return args
