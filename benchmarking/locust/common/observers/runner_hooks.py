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

"""Shared runner-side lifecycle, fail-open accounting, and artifact upload."""

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Callable

from .base import OBSERVERS, Observer, ObserverResult

_NAME_RE = re.compile(r"^[a-z0-9_]+$")
_ALLOWED_ENTRY_KEYS = frozenset({"name", "type", "config"})


@dataclass(frozen=True)
class StartedObservers:
    """Validation errors, started observer instances, and active output dirs."""

    status_map: dict[str, dict[str, Any]]
    started: list[tuple[str, str, Observer]]
    active_dirs: dict[str, Path]


def start_observers(
    entries: Any,
    work_dir: Path,
    log_fn: Callable[[str], None],
) -> StartedObservers:
    """Validate and start configured observers, isolating failures per entry."""
    status_map: dict[str, dict[str, Any]] = {}
    started: list[tuple[str, str, Observer]] = []
    active_dirs: dict[str, Path] = {}

    if entries is None or entries == []:
        return StartedObservers(
            status_map=status_map, started=started, active_dirs=active_dirs
        )

    if not isinstance(entries, list):
        err = f"observers must be a list, got {type(entries).__name__}"
        log_fn(f"Observer config error: {err}")
        status_map["observers"] = {"status": "error", "error": err}
        return StartedObservers(
            status_map=status_map, started=started, active_dirs=active_dirs
        )

    seen_names: set[str] = set()
    for idx, entry in enumerate(entries):
        fallback_key = f"observers[{idx}]"
        if not isinstance(entry, dict):
            err = (
                f"observer entry at index {idx} must be a mapping, "
                f"got {type(entry).__name__}"
            )
            log_fn(f"Observer config error ({fallback_key}): {err}")
            status_map[fallback_key] = {"status": "error", "error": err}
            continue

        raw_name = entry.get("name")
        if not isinstance(raw_name, str) or not _NAME_RE.fullmatch(raw_name):
            err = (
                f"invalid observer name {raw_name!r}; "
                "expected non-empty ^[a-z0-9_]+$"
            )
            log_fn(f"Observer config error ({fallback_key}): {err}")
            status_map[fallback_key] = {"status": "error", "error": err}
            continue
        if raw_name in seen_names:
            err = f"duplicate observer name {raw_name!r}"
            log_fn(f"Observer config error ({fallback_key}): {err}")
            status_map[fallback_key] = {"status": "error", "error": err}
            continue
        seen_names.add(raw_name)
        name = raw_name

        unknown = sorted(set(entry) - _ALLOWED_ENTRY_KEYS)
        if unknown:
            err = f"unknown observer entry keys: {', '.join(unknown)}"
            log_fn(f"Observer config error ({name}): {err}")
            status_map[name] = {"status": "error", "error": err}
            continue

        obs_type = entry.get("type")
        if not isinstance(obs_type, str) or obs_type not in OBSERVERS:
            err = f"unknown observer type {obs_type!r}"
            log_fn(f"Observer config error ({name}): {err}")
            status_map[name] = {"status": "error", "error": err}
            continue

        # YAML loads an empty `config:` block as None; treat it as empty mapping.
        cfg = entry.get("config")
        if cfg is None:
            cfg = {}
        if not isinstance(cfg, dict):
            err = (
                f"observer {name!r} config must be a mapping, "
                f"got {type(cfg).__name__}"
            )
            log_fn(f"Observer config error ({name}): {err}")
            status_map[name] = {"status": "error", "error": err}
            continue

        out_dir = work_dir / obs_type
        out_dir.mkdir(parents=True, exist_ok=True)
        active_dirs[obs_type] = out_dir

        try:
            obs = OBSERVERS[obs_type]()
            obs.start(name, cfg, out_dir)
        except Exception as exc:
            err = str(exc) or type(exc).__name__
            log_fn(f"Observer {name} ({obs_type}) failed to start: {err}")
            status_map[name] = {"status": "error", "error": err}
            continue

        started.append((name, obs_type, obs))

    return StartedObservers(
        status_map=status_map, started=started, active_dirs=active_dirs
    )


def finish_observers(
    run: StartedObservers,
    timestamp: str,
    tag: str,
    test_name: str,
    log_fn: Callable[[str], None],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Stop started observers and build status metadata plus stats.jsonl rows."""
    status_map = dict(run.status_map)
    stats_rows: list[dict[str, Any]] = []

    for name, obs_type, obs in run.started:
        try:
            res = obs.final_observation()
        except Exception as exc:
            err = str(exc) or type(exc).__name__
            log_fn(f"Observer {name} ({obs_type}) failed during final_observation: {err}")
            status_map[name] = {"status": "error", "error": err}
            continue

        if res.status == "error":
            err_msg = res.error or "unknown observer error"
            status_map[name] = {"status": "error", "error": err_msg}
            log_fn(f"Observer {name} ({obs_type}) reported error: {err_msg}")
        else:
            entry_status: dict[str, Any] = {"status": "ok"}
            if res.error is not None:
                entry_status["error"] = res.error
            status_map[name] = entry_status

        stats_rows.append(
            {
                "timestamp": timestamp,
                "tag": tag,
                "test_name": test_name,
                "metric": f"observer_{obs_type}_{name}",
                "measurements": {k: str(v) for k, v in res.counts.items()},
            }
        )

    return status_map, stats_rows


def upload_observer_artifacts(
    run: StartedObservers,
    dest_prefix: str,
    upload_fn: Callable[[Path, str], None],
    log_fn: Callable[[str], None],
) -> None:
    """Upload finalized observer artifacts without failing the benchmark run."""
    prefix = dest_prefix.rstrip("/")
    for obs_type, out_dir in sorted(run.active_dirs.items()):
        if not out_dir.is_dir():
            continue
        files = sorted(
            (p for p in out_dir.rglob("*") if p.is_file() and not p.name.endswith(".tmp")),
            key=lambda p: p.relative_to(out_dir).as_posix(),
        )
        for path in files:
            rel = path.relative_to(out_dir).as_posix()
            try:
                upload_fn(path, f"{prefix}/{obs_type}/{rel}")
            except Exception as exc:
                log_fn(f"Warning: failed to upload observer artifact {path}: {exc}")
