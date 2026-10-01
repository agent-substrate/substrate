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

The prewarm runs only when the block sets `enabled: true`. An enabled block
also requires actorTemplate and atespace: together they say where the test's
actors write their snapshots. Every other field is optional and takes the
tool's own default (see tools/gcs-prewarm/main.go) when left out.
"""

import os
import signal
import subprocess
from pathlib import Path
from typing import Any

import yaml

from util import parse_duration_seconds

# Where benchmarking/workloads/deploy.sh reads the benchmark ActorTemplates
# from, one <name>-template.yaml.tmpl each, relative to the repo root.
TEMPLATES_DIR = "benchmarking/workloads/manifests"


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


def _is_name(v: Any) -> bool:
    return isinstance(v, str) and v != "" and "/" not in v


REQUIRED = ("actorTemplate", "atespace")

# The optional tests.yaml fields, which map one-to-one onto gcs-prewarm
# flags: field -> (flag, validity check, what the check wants).
FIELDS = {
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


def storage_location(template: str, templates_dir: str = TEMPLATES_DIR) -> str:
    """The unrendered snapshotConfig.storageLocation of benchmark
    ActorTemplate `template`, read from its manifest rather than the cluster
    because the prewarm starts before the template is deployed."""
    path = Path(templates_dir) / f"{template}-template.yaml.tmpl"
    if not path.exists():
        raise ValueError(f"no ActorTemplate manifest for {template!r} at {path}")
    location = (yaml.safe_load(path.read_text()).get("snapshotConfig") or {}).get(
        "storageLocation", ""
    )
    if not location.startswith("gs://"):
        raise ValueError(
            f"ActorTemplate {template!r} storageLocation {location!r} is not a "
            f"gs:// location"
        )
    return location


def enabled(cfg: Any) -> bool:
    """Whether a test's gcsPrewarm block (None when absent) turns the
    prewarm on. Anything but an explicit `enabled: true` leaves it off."""
    return isinstance(cfg, dict) and cfg.get("enabled") is True


def validate(name: str, cfg: Any, templates_dir: str = TEMPLATES_DIR) -> None:
    """Raise ValueError if test `name`'s gcsPrewarm block is malformed. An
    enabled block must also name an atespace and an ActorTemplate with a
    gs:// manifest; a disabled one is only checked for field names and
    types, so it can be switched off without being filled in."""
    if not isinstance(cfg, dict):
        raise ValueError(f"test {name!r} gcsPrewarm must be a mapping")
    if "enabled" in cfg and not isinstance(cfg["enabled"], bool):
        raise ValueError(
            f"test {name!r} gcsPrewarm.enabled must be true or false, "
            f"got {cfg['enabled']!r}"
        )
    for field in REQUIRED:
        if (field in cfg or enabled(cfg)) and not _is_name(cfg.get(field)):
            raise ValueError(
                f"test {name!r} gcsPrewarm.{field} must be a name"
                f"{' (required when enabled)' if enabled(cfg) else ''}, "
                f"got {cfg.get(field)!r}"
            )
    for field, value in cfg.items():
        if field == "enabled" or field in REQUIRED:
            continue
        if field not in FIELDS:
            raise ValueError(
                f"test {name!r} gcsPrewarm has unknown field {field!r} "
                f"(want one of {['enabled', *REQUIRED, *FIELDS]})"
            )
        _, ok, want = FIELDS[field]
        if not ok(value):
            raise ValueError(
                f"test {name!r} gcsPrewarm.{field} must be {want}, got {value!r}"
            )
    if enabled(cfg):
        storage_location(cfg["actorTemplate"], templates_dir)


def command(
    cfg: dict[str, Any], bucket_env: str, templates_dir: str = TEMPLATES_DIR
) -> list[str]:
    """The gcs-prewarm argv for a validated cfg. The bucket and prefix are
    the template's storageLocation, with ${BUCKET_NAME} rendered from
    bucket_env as deploy.sh does, plus the prefix the atelet puts actor
    snapshots under: atespaces/<atespace>/actors/<uid>. The random uid comes
    right after that prefix, so the tool's random keys land in the same key
    range as the real traffic."""
    location = storage_location(cfg["actorTemplate"], templates_dir)
    if "${BUCKET_NAME}" in location and not bucket_env:
        raise RuntimeError(
            f"ActorTemplate {cfg['actorTemplate']!r} storageLocation needs "
            f"BUCKET_NAME, which the target cluster config does not set"
        )
    bucket, _, base = (
        location.replace("${BUCKET_NAME}", bucket_env)
        .removeprefix("gs://")
        .partition("/")
    )
    prefix = "/".join(
        p for p in (base.strip("/"), "atespaces", cfg["atespace"], "actors") if p
    )
    cmd = [
        "go", "-C", "tools/gcs-prewarm", "run", ".",
        f"--bucket={bucket}",
        f"--prefix={prefix}",
    ]
    for field, (flag, _, _) in FIELDS.items():
        if field not in cfg:
            continue
        value = cfg[field]
        if field in DURATION_FIELDS:
            value = f"{parse_duration_seconds(str(value))}s"
        elif isinstance(value, bool):
            value = str(value).lower()
        cmd.append(f"{flag}={value}")
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
