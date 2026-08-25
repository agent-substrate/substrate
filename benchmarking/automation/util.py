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


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(shlex.quote(c) for c in cmd)}", flush=True)
    return subprocess.run(cmd, check=True, **kwargs)


def run_no_check(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(shlex.quote(c) for c in cmd)}", flush=True)
    return subprocess.run(cmd, check=False, **kwargs)


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
