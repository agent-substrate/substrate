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

"""Pprof observer collecting Go runtime profiles from Kubernetes pods."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import http.client
import os
from pathlib import Path
import re
import socket
import tempfile
import threading
import time
from typing import Any, Callable, TypeVar

from .base import OBSERVERS, ObserverResult

WINDOWED_PROFILES: tuple[str, ...] = ("profile", "allocs", "mutex", "block")
INSTANTANEOUS_PROFILES: tuple[str, ...] = (
    "heap",
    "goroutine",
    "goroutineleak",
    "threadcreate",
)
ALLOWED_PROFILES: frozenset[str] = frozenset(WINDOWED_PROFILES) | frozenset(
    INSTANTANEOUS_PROFILES
)
DEFAULT_PROFILES: tuple[str, ...] = ("profile", "allocs", "heap", "goroutine")

ALLOWED_CONFIG_KEYS: frozenset[str] = frozenset(
    {"namespace", "label_selector", "profiles", "interval", "duration"}
)

DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_DURATION_SECONDS = 15

API_TIMEOUT_SECONDS = 5
CONNECT_TIMEOUT_SECONDS = 2.0
BASELINE_DEADLINE_SECONDS = 5.0
FINAL_DEADLINE_SECONDS = 15.0
READ_BUDGET_MARGIN_SECONDS = 10.0
MAX_WINDOWED_WORKERS = 64
MAX_INSTANT_WORKERS = 32
STREAM_CHUNK_BYTES = 65536
ERROR_BODY_LIMIT_BYTES = 65536

_DURATION_RE = re.compile(r"(\d+)\s*([smh]?)")
_DURATION_UNITS: dict[str, int] = {"": 1, "s": 1, "m": 60, "h": 3600}

# Coordinates /debug/pprof/profile across all PprofObserver instances in the process.
_CPU_LOCKS_MU = threading.Lock()
_CPU_LOCKS: dict[str, threading.Lock] = {}

_T = TypeVar("_T")


def _get_cpu_lock(pod_uid: str) -> threading.Lock:
    with _CPU_LOCKS_MU:
        lock = _CPU_LOCKS.get(pod_uid)
        if lock is None:
            lock = threading.Lock()
            _CPU_LOCKS[pod_uid] = lock
        return lock


def _run_pool(fn: Callable[[_T], None], items: list[_T], max_workers: int) -> None:
    if not items:
        return
    with ThreadPoolExecutor(max_workers=min(len(items), max_workers)) as pool:
        futures = [pool.submit(fn, item) for item in items]
        for fut in futures:
            fut.result()


def parse_duration_seconds(value: Any) -> int:
    """Parse an integer duration with optional [smh] suffix into whole seconds."""
    if isinstance(value, bool):
        raise ValueError(f"duration must be an integer or <int>[smh] string, got {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"duration must be non-negative, got {value!r}")
        return value
    if isinstance(value, str):
        match = _DURATION_RE.fullmatch(value.strip())
        if match is None:
            raise ValueError(f"invalid duration {value!r}; expected integer or <int>[smh]")
        amount = int(match.group(1))
        unit = _DURATION_UNITS[match.group(2)]
        return amount * unit
    raise ValueError(f"duration must be an integer or <int>[smh] string, got {value!r}")


def _default_core_v1_api() -> Any:
    from kubernetes import client, config as k8s_config

    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()
    return client.CoreV1Api()


def _utc_ms_timestamp() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%S')}.{now.microsecond // 1000:03d}Z"


@dataclass(frozen=True)
class _TargetPod:
    uid: str
    namespace: str
    name: str
    ip: str
    pprof_port: int
    pprof_container: str
    restart_count: int | None


class PprofObserver:
    """Discovers pods exposing a `pprof` container port and scrapes Go profiles."""

    OBSERVER_TYPE = "pprof"

    def __init__(self, core_v1_factory: Callable[[], Any] | None = None) -> None:
        self._core_v1_factory = core_v1_factory or _default_core_v1_api
        self._core_v1: Any = None
        self._name = ""
        self._out_dir = Path(".")
        self._log_path: Path | None = None
        self._namespace = ""
        self._label_selector = ""
        self._profiles: list[str] = list(DEFAULT_PROFILES)
        self._interval = DEFAULT_INTERVAL_SECONDS
        self._duration = DEFAULT_DURATION_SECONDS

        self._log_mu = threading.Lock()
        self._stats_mu = threading.Lock()
        self._conns_mu = threading.Lock()
        self._stop_event = threading.Event()
        self._active_conns: set[http.client.HTTPConnection] = set()
        self._thread: threading.Thread | None = None

        self._pods_discovered: set[str] = set()
        self._pods_profiled: set[str] = set()
        self._seen_restarts: dict[str, int] = {}
        self._scrapes_succeeded = 0
        self._scrapes_failed = 0
        self._scrapes_busy_skipped = 0
        self._scrapes_overlap_skipped = 0
        self._bytes_collected = 0
        self._last_discovery_error: str | None = None

    def _log(self, msg: str) -> None:
        if self._log_path is None:
            return
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        line = f"[{ts}] {msg}\n"
        with self._log_mu:
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(line)

    def _validate_and_configure(self, name: str, cfg: dict[str, Any], out_dir: Path) -> None:
        if not isinstance(cfg, dict):
            raise ValueError(f"config must be a mapping, got {type(cfg).__name__}")
        unknown = set(cfg) - ALLOWED_CONFIG_KEYS
        if unknown:
            raise ValueError(f"unknown config keys: {', '.join(sorted(unknown))}")

        namespace = cfg.get("namespace", "")
        if not isinstance(namespace, str):
            raise ValueError(f"namespace must be a string, got {type(namespace).__name__}")

        label_selector = cfg.get("label_selector", "")
        if not isinstance(label_selector, str):
            raise ValueError(
                f"label_selector must be a string, got {type(label_selector).__name__}"
            )

        if "profiles" in cfg:
            raw_profiles = cfg["profiles"]
            if not isinstance(raw_profiles, list) or not raw_profiles:
                raise ValueError("profiles must be a non-empty list")
            deduped: list[str] = []
            seen: set[str] = set()
            for prof in raw_profiles:
                if not isinstance(prof, str):
                    raise ValueError(
                        f"profile names must be strings, got {type(prof).__name__}"
                    )
                if prof == "trace":
                    raise ValueError("profile 'trace' is not supported in v1")
                if prof not in ALLOWED_PROFILES:
                    raise ValueError(f"unknown profile {prof!r}")
                if prof not in seen:
                    seen.add(prof)
                    deduped.append(prof)
            profiles = deduped
        else:
            profiles = list(DEFAULT_PROFILES)

        interval = parse_duration_seconds(cfg.get("interval", DEFAULT_INTERVAL_SECONDS))
        duration = parse_duration_seconds(cfg.get("duration", DEFAULT_DURATION_SECONDS))
        if duration < 1:
            raise ValueError(f"duration must be >= 1s, got {duration}s")
        if interval < 1:
            raise ValueError(f"interval must be >= 1s, got {interval}s")
        if duration > interval:
            raise ValueError(
                f"duration ({duration}s) must not exceed interval ({interval}s)"
            )

        self._name = name
        self._out_dir = out_dir
        self._namespace = namespace
        self._label_selector = label_selector
        self._profiles = profiles
        self._interval = interval
        self._duration = duration

    def start(self, name: str, config: dict[str, Any], out_dir: Path) -> None:
        self._validate_and_configure(name, config, out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        self._log_path = out_dir / f"{name}.log"
        self._core_v1 = self._core_v1_factory()

        self._log(
            f"Starting pprof observer {name!r}: namespace={self._namespace!r} "
            f"label_selector={self._label_selector!r} profiles={self._profiles} "
            f"interval={self._interval}s duration={self._duration}s"
        )

        # Capture pre-load baseline for instantaneous profiles before returning.
        pods = self._discover_pods()
        with self._stats_mu:
            discovery_error = self._last_discovery_error
        if not pods and discovery_error is None:
            self._log(
                "Warning: no Ready pods with a pprof port matched at start(); retrying periodically"
            )
        elif pods and self._instantaneous_profiles():
            deadline = time.monotonic() + BASELINE_DEADLINE_SECONDS
            self._scrape_instantaneous_phase(pods, final_gc=False, deadline=deadline)

        self._thread = threading.Thread(
            target=self._periodic_loop,
            name=f"pprof-observer-{name}",
            daemon=True,
        )
        self._thread.start()

    def final_observation(self) -> ObserverResult:
        self._stop_event.set()
        self._cancel_active_connections()
        if self._thread is not None:
            self._thread.join(timeout=CONNECT_TIMEOUT_SECONDS + API_TIMEOUT_SECONDS + 2.0)

        pods = self._discover_pods()
        if pods and self._instantaneous_profiles():
            deadline = time.monotonic() + FINAL_DEADLINE_SECONDS
            self._scrape_instantaneous_phase(pods, final_gc=True, deadline=deadline)

        if self._core_v1 is not None:
            self._core_v1.api_client.close()

        counts, last_err = self._snapshot_state()
        if counts["pods_profiled"] == 0:
            err = (
                f"no pods profiled (pod discovery failed: {last_err})"
                if last_err is not None
                else "no pods profiled"
            )
            self._log(f"Completed with 0 pods profiled: {err}")
            return ObserverResult(status="error", error=err, counts=counts)
        self._log(f"Completed cleanly: {counts}")
        return ObserverResult(status="ok", counts=counts)

    def _windowed_profiles(self) -> list[str]:
        return [p for p in self._profiles if p in WINDOWED_PROFILES]

    def _instantaneous_profiles(self) -> list[str]:
        return [p for p in self._profiles if p in INSTANTANEOUS_PROFILES]

    def _snapshot_state(self) -> tuple[dict[str, int], str | None]:
        with self._stats_mu:
            counts = {
                "pods_discovered": len(self._pods_discovered),
                "pods_profiled": len(self._pods_profiled),
                "scrapes_succeeded": self._scrapes_succeeded,
                "scrapes_failed": self._scrapes_failed,
                "scrapes_busy_skipped": self._scrapes_busy_skipped,
                "scrapes_overlap_skipped": self._scrapes_overlap_skipped,
                "bytes_collected": self._bytes_collected,
            }
            return counts, self._last_discovery_error

    def _cancel_active_connections(self) -> None:
        with self._conns_mu:
            conns = list(self._active_conns)
        for conn in conns:
            sock = conn.sock
            if sock is not None:
                try:
                    # Only shut down I/O across threads; conn.close() runs in the owning
                    # scrape thread's finally block to avoid FD-reuse races.
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def _discover_pods(self) -> list[_TargetPod]:
        if self._core_v1 is None:
            return []
        try:
            if self._namespace:
                resp = self._core_v1.list_namespaced_pod(
                    namespace=self._namespace,
                    label_selector=self._label_selector,
                    _request_timeout=API_TIMEOUT_SECONDS,
                )
            else:
                resp = self._core_v1.list_pod_for_all_namespaces(
                    label_selector=self._label_selector,
                    _request_timeout=API_TIMEOUT_SECONDS,
                )
        except Exception as exc:
            status = getattr(exc, "status", None)
            reason = getattr(exc, "reason", None)
            err_msg = f"{status} {reason}" if status and reason else str(exc)
            with self._stats_mu:
                self._last_discovery_error = err_msg
            self._log(f"Warning: pod discovery failed: {err_msg}")
            return []

        with self._stats_mu:
            self._last_discovery_error = None

        targets: list[_TargetPod] = []
        restart_warnings: list[str] = []
        for pod in resp.items or []:
            target = self._extract_target_pod(pod)
            if target is None:
                continue
            targets.append(target)
            with self._stats_mu:
                self._pods_discovered.add(target.uid)
                if target.restart_count is not None:
                    prev = self._seen_restarts.get(target.uid)
                    if prev is not None and target.restart_count > prev:
                        restart_warnings.append(
                            f"Warning: pod {target.namespace}/{target.name} container "
                            f"{target.pprof_container} restarted ({prev} -> {target.restart_count})"
                        )
                    self._seen_restarts[target.uid] = target.restart_count
        for warning in restart_warnings:
            self._log(warning)
        return targets

    def _extract_target_pod(self, pod: Any) -> _TargetPod | None:
        status = pod.status
        if status is None or status.phase != "Running":
            return None
        conditions = status.conditions or []
        if not any(c.type == "Ready" and c.status == "True" for c in conditions):
            return None
        pod_ip = status.pod_ip
        if not isinstance(pod_ip, str) or not pod_ip:
            return None

        spec = pod.spec
        if spec is None:
            return None
        pprof_container = ""
        pprof_port = 0
        for container in spec.containers or []:
            for port in container.ports or []:
                if port.name == "pprof":
                    cport = port.container_port
                    if isinstance(cport, int) and not isinstance(cport, bool) and cport > 0:
                        pprof_container = container.name or ""
                        pprof_port = cport
                        break
            if pprof_port:
                break
        if not pprof_port:
            return None

        meta = pod.metadata
        if meta is None:
            return None
        pod_ns = meta.namespace
        pod_name = meta.name
        if not pod_ns or not pod_name:
            return None
        pod_uid = meta.uid or f"{pod_ns}/{pod_name}"

        restart_count: int | None = None
        for cs in status.container_statuses or []:
            if cs.name == pprof_container:
                raw_rc = cs.restart_count
                if isinstance(raw_rc, int) and not isinstance(raw_rc, bool):
                    restart_count = raw_rc
                break

        return _TargetPod(
            uid=pod_uid,
            namespace=pod_ns,
            name=pod_name,
            ip=pod_ip,
            pprof_port=pprof_port,
            pprof_container=pprof_container,
            restart_count=restart_count,
        )

    def _periodic_loop(self) -> None:
        while not self._stop_event.is_set():
            tick_start = time.monotonic()
            try:
                self._run_periodic_tick()
            except Exception as exc:
                self._log(f"Warning: periodic tick failed: {exc}")
            elapsed = time.monotonic() - tick_start
            wait_s = max(0.0, self._interval - elapsed)
            if self._stop_event.wait(timeout=wait_s):
                break

    def _run_periodic_tick(self) -> None:
        if self._stop_event.is_set():
            return
        pods = self._discover_pods()
        if not pods or self._stop_event.is_set():
            return

        windowed = self._windowed_profiles()
        if windowed and not self._stop_event.is_set():
            _run_pool(
                lambda item: self._scrape_profile(
                    item[0],
                    item[1],
                    final_gc=False,
                    deadline=None,
                    cancel_event=self._stop_event,
                ),
                [(pod, prof) for pod in pods for prof in windowed],
                MAX_WINDOWED_WORKERS,
            )

        instant = self._instantaneous_profiles()
        if instant and not self._stop_event.is_set():
            _run_pool(
                lambda item: self._scrape_profile(
                    item[0],
                    item[1],
                    final_gc=False,
                    deadline=None,
                    cancel_event=self._stop_event,
                ),
                [(pod, prof) for pod in pods for prof in instant],
                MAX_INSTANT_WORKERS,
            )

    def _scrape_instantaneous_phase(
        self, pods: list[_TargetPod], *, final_gc: bool, deadline: float
    ) -> None:
        instant = self._instantaneous_profiles()
        if not instant:
            return
        _run_pool(
            lambda item: self._scrape_profile(
                item[0],
                item[1],
                final_gc=final_gc,
                deadline=deadline,
                cancel_event=None,
            ),
            [(pod, prof) for pod in pods for prof in instant],
            MAX_INSTANT_WORKERS,
        )

    def _profile_request(self, profile: str, *, final_gc: bool) -> tuple[str, str, float]:
        if profile == "profile":
            return (
                f"/debug/pprof/profile?seconds={self._duration}",
                "profile",
                self._duration + READ_BUDGET_MARGIN_SECONDS,
            )
        if profile in ("allocs", "mutex", "block"):
            return (
                f"/debug/pprof/{profile}?seconds={self._duration}&debug=0",
                profile,
                self._duration + READ_BUDGET_MARGIN_SECONDS,
            )
        if profile == "heap":
            if final_gc:
                return "/debug/pprof/heap?gc=1", "heap_gc", READ_BUDGET_MARGIN_SECONDS
            return "/debug/pprof/heap?gc=0", "heap", READ_BUDGET_MARGIN_SECONDS
        return f"/debug/pprof/{profile}?debug=0", profile, READ_BUDGET_MARGIN_SECONDS

    def _scrape_profile(
        self,
        pod: _TargetPod,
        profile: str,
        *,
        final_gc: bool,
        deadline: float | None,
        cancel_event: threading.Event | None,
    ) -> None:
        if cancel_event is not None and cancel_event.is_set():
            return

        cpu_lock: threading.Lock | None = None
        if profile == "profile":
            cpu_lock = _get_cpu_lock(pod.uid)
            if not cpu_lock.acquire(blocking=False):
                with self._stats_mu:
                    self._scrapes_overlap_skipped += 1
                self._log(
                    f"Skipping overlapping CPU profile for {pod.namespace}/{pod.name}"
                )
                return

        try:
            self._perform_http_scrape(
                pod,
                profile,
                final_gc=final_gc,
                deadline=deadline,
                cancel_event=cancel_event,
            )
        finally:
            if cpu_lock is not None:
                cpu_lock.release()

    def _perform_http_scrape(
        self,
        pod: _TargetPod,
        profile: str,
        *,
        final_gc: bool,
        deadline: float | None,
        cancel_event: threading.Event | None,
    ) -> None:
        path, filename, read_budget = self._profile_request(profile, final_gc=final_gc)
        pod_dir = self._out_dir / pod.namespace / pod.name
        final_path = pod_dir / f"{_utc_ms_timestamp()}.{filename}.pb.gz"
        tmp_path: Path | None = None

        max_deadline = time.monotonic() + CONNECT_TIMEOUT_SECONDS + read_budget
        scrape_deadline = max_deadline if deadline is None else min(deadline, max_deadline)

        conn = http.client.HTTPConnection(
            pod.ip, pod.pprof_port, timeout=CONNECT_TIMEOUT_SECONDS
        )
        conn.auto_open = 0
        registered = False
        try:
            pod_dir.mkdir(parents=True, exist_ok=True)
            remaining = scrape_deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"scrape deadline exceeded before connecting to {path}")
            conn.timeout = min(CONNECT_TIMEOUT_SECONDS, remaining)
            conn.connect()

            if cancel_event is not None:
                with self._conns_mu:
                    if cancel_event.is_set():
                        return
                    self._active_conns.add(conn)
                    registered = True

            if cancel_event is not None and cancel_event.is_set():
                return
            remaining = scrape_deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"scrape deadline exceeded before requesting {path}")
            if conn.sock is not None:
                conn.sock.settimeout(remaining)
            conn.request("GET", path)

            if cancel_event is not None and cancel_event.is_set():
                return
            remaining = scrape_deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"scrape deadline exceeded before response headers for {path}")
            if conn.sock is not None:
                conn.sock.settimeout(remaining)
            resp = conn.getresponse()

            if resp.status == 500:
                body = resp.read(ERROR_BODY_LIMIT_BYTES).decode("utf-8", errors="replace")
                if resp.getheader("X-Go-Pprof") == "1" and "already in use" in body.lower():
                    with self._stats_mu:
                        self._scrapes_busy_skipped += 1
                    self._log(
                        f"Skipping busy pprof target {pod.namespace}/{pod.name} "
                        f"({path}): {body.strip()}"
                    )
                    return
                raise http.client.HTTPException(
                    f"HTTP 500 from {pod.namespace}/{pod.name} ({path}): {body.strip()}"
                )

            if resp.status != 200:
                resp.read(ERROR_BODY_LIMIT_BYTES)
                raise http.client.HTTPException(
                    f"HTTP {resp.status} from {pod.namespace}/{pod.name} ({path})"
                )

            fd, raw_tmp = tempfile.mkstemp(
                dir=pod_dir, prefix=f".{final_path.name}.", suffix=".tmp"
            )
            tmp_path = Path(raw_tmp)
            written = 0
            with os.fdopen(fd, "wb") as out_f:
                while True:
                    if cancel_event is not None and cancel_event.is_set():
                        return
                    remaining = scrape_deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(f"scrape deadline exceeded while reading {path}")
                    if conn.sock is not None:
                        conn.sock.settimeout(remaining)
                    chunk = resp.read1(STREAM_CHUNK_BYTES)
                    if not chunk:
                        break
                    out_f.write(chunk)
                    written += len(chunk)

            # Detect truncated Content-Length responses where HTTPResponse.read1(n)
            # returns b"" on early EOF rather than raising IncompleteRead.
            if not resp.chunked and resp.length is not None and resp.length > 0:
                raise http.client.IncompleteRead(b"", resp.length)

            if cancel_event is not None and cancel_event.is_set():
                return
            os.replace(tmp_path, final_path)
            tmp_path = None
            with self._stats_mu:
                self._scrapes_succeeded += 1
                self._bytes_collected += written
                self._pods_profiled.add(pod.uid)
        except (OSError, http.client.HTTPException, TimeoutError, ValueError) as exc:
            if cancel_event is not None and cancel_event.is_set():
                return
            with self._stats_mu:
                self._scrapes_failed += 1
            self._log(
                f"Warning: failed scraping {pod.namespace}/{pod.name} {path}: {exc}"
            )
        finally:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)
            if registered:
                with self._conns_mu:
                    self._active_conns.discard(conn)
            try:
                conn.close()
            except OSError:
                pass


OBSERVERS["pprof"] = PprofObserver
