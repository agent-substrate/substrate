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

"""The tests.yaml `gcsPrewarm` block: runs tools/gcs-prewarm in the
background while substrate and workloads deploy, so the snapshot bucket's
key ranges are scaled up before the test's real snapshot traffic starts. A
bucket sheds write bursts with 429s until its autoscaler splits the loaded
key ranges, which takes on the order of 20 minutes per doubling.

Every field is optional; a field left out takes the tool's own default
(see tools/gcs-prewarm/main.go), except bucket and prefix, defaulted below.
"""

import os
import signal
import subprocess
from typing import Any

from util import parse_duration_seconds

# The object prefix the glutton suites' snapshots are written under: the
# glutton ActorTemplate's storageLocation (benchmark-workloads/glutton, see
# workloads/manifests/glutton-template.yaml.tmpl) plus the actor prefix the
# atelet derives from it, atespaces/<atespace>/actors/<uid>. The random actor
# uid comes right after this prefix, which is what lets the prewarm's random
# keys land in the same key range as the real traffic.
GLUTTON_SNAPSHOT_PREFIX = "benchmark-workloads/glutton/atespaces/benchmark/actors"


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_duration(v: Any) -> bool:
    try:
        parse_duration_seconds(str(v))
    except ValueError:
        return False
    return True


# tests.yaml field -> (gcs-prewarm flag, validity check, what the check wants).
FIELDS = {
    "bucket": ("--bucket", lambda v: isinstance(v, str) and v, "a non-empty string"),
    "prefix": ("--prefix", lambda v: isinstance(v, str) and v, "a non-empty string"),
    "startRate": ("--start-rate", lambda v: _is_number(v) and v > 0, "a positive number"),
    "targetRate": ("--target-rate", lambda v: _is_number(v) and v > 0, "a positive number"),
    "doubleEvery": (
        "--double-every",
        lambda v: _is_duration(v) and parse_duration_seconds(str(v)) > 0,
        "a positive duration like 5m",
    ),
    "hold": ("--hold", _is_duration, "a duration like 5m"),
    "objectBytes": ("--object-bytes", lambda v: _is_int(v) and v > 0, "a positive integer"),
    "workers": ("--workers", lambda v: _is_int(v) and v > 0, "a positive integer"),
    "cleanup": ("--cleanup", lambda v: isinstance(v, bool), "true or false"),
}

DURATION_FIELDS = ("doubleEvery", "hold")


def validate(name: str, cfg: Any) -> None:
    """Raise ValueError if test `name`'s gcsPrewarm block is malformed."""
    if not isinstance(cfg, dict):
        raise ValueError(
            f"test {name!r} gcsPrewarm must be a mapping "
            f"(use `gcsPrewarm: {{}}` for all defaults)"
        )
    for field, value in cfg.items():
        if field not in FIELDS:
            raise ValueError(
                f"test {name!r} gcsPrewarm has unknown field {field!r} "
                f"(want one of {list(FIELDS)})"
            )
        _, ok, want = FIELDS[field]
        if not ok(value):
            raise ValueError(
                f"test {name!r} gcsPrewarm.{field} must be {want}, got {value!r}"
            )


def command(cfg: dict[str, Any], bucket_env: str) -> list[str]:
    """The gcs-prewarm argv for a validated cfg. bucket defaults to
    bucket_env (the target cluster's BUCKET_NAME, which the snapshots go
    to) and prefix to the glutton snapshot prefix."""
    cfg = {"prefix": GLUTTON_SNAPSHOT_PREFIX, **cfg}
    cfg.setdefault("bucket", bucket_env)
    if not cfg["bucket"]:
        raise RuntimeError(
            "gcsPrewarm has no bucket: set gcsPrewarm.bucket or BUCKET_NAME "
            "in the target cluster config"
        )
    cmd = ["go", "-C", "tools/gcs-prewarm", "run", "."]
    for field in FIELDS:
        if field not in cfg:
            continue
        value = cfg[field]
        if field in DURATION_FIELDS:
            value = f"{parse_duration_seconds(str(value))}s"
        elif isinstance(value, bool):
            value = str(value).lower()
        cmd.append(f"{FIELDS[field][0]}={value}")
    return cmd


def start(cfg: dict[str, Any]) -> subprocess.Popen:
    """Start the prewarm in the background. The tool ramps, holds, deletes
    its objects (unless cleanup is false), and exits on its own."""
    cmd = command(cfg, os.environ.get("BUCKET_NAME", ""))
    print(f"Starting gcs-prewarm in background: {' '.join(cmd)}", flush=True)
    # New session so kill() can killpg() the whole `go run` +
    # compiled-binary tree with one signal.
    return subprocess.Popen(cmd, start_new_session=True)


def wait(proc: subprocess.Popen | None) -> None:
    """Block until the prewarm exits; raise if it failed, since a test run
    against an unwarmed bucket measures GCS scaling rather than substrate."""
    if proc is None:
        return
    if proc.poll() is None:
        print("Waiting for gcs-prewarm to finish", flush=True)
    proc.wait()
    print(f"gcs-prewarm exited with code {proc.returncode}", flush=True)
    if proc.returncode != 0:
        raise RuntimeError(f"gcs-prewarm exited with code {proc.returncode}")


def kill(proc: subprocess.Popen | None) -> None:
    """SIGKILL the prewarm's process group if it is still running."""
    if proc is None or proc.poll() is not None:
        return
    print("SIGKILLing gcs-prewarm", flush=True)
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()
