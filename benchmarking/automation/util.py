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


# The keys of a tests.yaml isolate block and the
# benchmarking/workloads/deploy.sh flag each one sets.
ISOLATE_FLAGS = {
    "nodes": "--isolate-nodes",
    "run": "--isolate-run",
    "delay": "--isolate-delay",
}


# What deploy.sh accepts for --isolate-run, and a Go duration for --isolate-delay,
# as fake-atelet parses it.
ISOLATE_RUN_PATTERN = re.compile(r"[a-z0-9]{1,8}")
ISOLATE_DELAY_PATTERN = re.compile(r"0|([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+")


def isolate_args(block: dict | None) -> list[str]:
    """Return the deploy.sh flags for a tests.yaml isolate block, or
    none when the entry has no block. Raises ValueError on an unknown key or
    a value deploy.sh would reject, so a bad entry fails before any cluster
    work rather than in deploy.sh."""
    if block is None:
        return []
    if not isinstance(block, dict):
        raise ValueError(f"isolate must be a mapping, got {block!r}")
    unknown = sorted(set(block) - set(ISOLATE_FLAGS))
    if unknown:
        raise ValueError(
            f"isolate has unknown keys {unknown} "
            f"(want some of {sorted(ISOLATE_FLAGS)})"
        )
    args = ["--isolate"]
    for key, flag in ISOLATE_FLAGS.items():
        if key not in block:
            continue
        value = block[key]
        if key == "nodes":
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"isolate.{key} must be a positive integer, got {value!r}")
        elif key == "run":
            if not isinstance(value, str) or not ISOLATE_RUN_PATTERN.fullmatch(value):
                raise ValueError(f"isolate.{key} must be 1 to 8 lowercase letters or digits, got {value!r}")
        elif key == "delay":
            if not isinstance(value, str) or not ISOLATE_DELAY_PATTERN.fullmatch(value):
                raise ValueError(f"isolate.{key} must be a duration string such as 500ms, got {value!r}")
        args += [flag, str(value)]
    return args
