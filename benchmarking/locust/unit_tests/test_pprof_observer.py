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

"""Unit tests for common.observers.pprof.PprofObserver."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.observers import OBSERVERS, PprofObserver
from common.observers import pprof as pprof_mod


class _DaemonThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True


def _make_pod(
    *,
    name: str | None = "pod-a",
    namespace: str | None = "ate-system",
    uid: str = "uid-a",
    phase: str = "Running",
    ready: str | None = "True",
    pod_ip: str = "127.0.0.1",
    pprof_port: int | None = 6060,
    container_name: str = "app",
    restart_count: int = 0,
):
    ports = []
    if pprof_port is not None:
        ports.append(SimpleNamespace(name="pprof", container_port=pprof_port))
    conditions = (
        [] if ready is None else [SimpleNamespace(type="Ready", status=ready)]
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, namespace=namespace, uid=uid),
        status=SimpleNamespace(
            phase=phase,
            conditions=conditions,
            pod_ip=pod_ip,
            container_statuses=[
                SimpleNamespace(name=container_name, restart_count=restart_count)
            ],
        ),
        spec=SimpleNamespace(
            containers=[SimpleNamespace(name=container_name, ports=ports)]
        ),
    )


def _start_server(handler_cls):
    server = _DaemonThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, server.server_address[1], thread


def _stop_server(server, thread):
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


class PprofObserverConfigTest(unittest.TestCase):
    def tearDown(self):
        with pprof_mod._CPU_LOCKS_MU:
            pprof_mod._CPU_LOCKS.clear()

    def test_registered_in_observers_map(self):
        self.assertIs(OBSERVERS["pprof"], PprofObserver)

    def test_defaults_applied_without_pods(self):
        api = mock.Mock()
        api.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=[])
        with tempfile.TemporaryDirectory() as tmp:
            obs = PprofObserver(core_v1_factory=lambda: api)
            obs.start("cp_pprof", {}, Path(tmp))
            res = obs.final_observation()

            self.assertEqual(obs._namespace, "")
            self.assertEqual(obs._label_selector, "")
            self.assertEqual(obs._profiles, ["profile", "allocs", "heap", "goroutine"])
            self.assertEqual(obs._interval, 60)
            self.assertEqual(obs._duration, 15)
            self.assertEqual(res.status, "error")
            self.assertEqual(res.error, "no pods profiled")
            self.assertEqual(res.counts["pods_discovered"], 0)
            self.assertEqual(res.counts["pods_profiled"], 0)
            self.assertIn(
                "no Ready pods with a pprof port matched",
                (Path(tmp) / "cp_pprof.log").read_text(encoding="utf-8"),
            )

    def test_invalid_configs_rejected(self):
        bad_cases = [
            ("unknown key", {"bogus": 1}, "unknown config keys"),
            ("non-str namespace", {"namespace": 123}, "namespace must be a string"),
            ("non-str selector", {"label_selector": ["a"]}, "label_selector must be a string"),
            ("empty profiles", {"profiles": []}, "non-empty list"),
            ("string profiles", {"profiles": "heap"}, "non-empty list"),
            ("trace profile", {"profiles": ["heap", "trace"]}, "not supported in v1"),
            ("unknown profile", {"profiles": ["cpu"]}, "unknown profile"),
            ("duration zero", {"duration": "0s"}, ">= 1s"),
            ("duration negative", {"duration": -5}, "non-negative"),
            ("plus signed duration", {"duration": "+60s"}, "invalid duration"),
            ("minus signed duration", {"duration": "-5s"}, "invalid duration"),
            ("duration exceeds interval", {"duration": "30s", "interval": "15s"}, "must not exceed interval"),
            ("float duration", {"duration": 1.5}, "integer or <int>\\[smh\\]"),
            ("float string duration", {"duration": "1.5s"}, "invalid duration"),
            ("bool duration", {"duration": True}, "integer or <int>\\[smh\\]"),
            ("bad unit", {"interval": "5d"}, "invalid duration"),
        ]
        for label, cfg, pattern in bad_cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                api = mock.Mock()
                obs = PprofObserver(core_v1_factory=lambda: api)
                with self.assertRaisesRegex(ValueError, pattern):
                    obs.start("bad", cfg, Path(tmp))
                api.list_pod_for_all_namespaces.assert_not_called()
                api.list_namespaced_pod.assert_not_called()


class PprofObserverDiscoveryTest(unittest.TestCase):
    def tearDown(self):
        with pprof_mod._CPU_LOCKS_MU:
            pprof_mod._CPU_LOCKS.clear()

    def test_filters_ineligible_pods_and_tracks_restarts(self):
        ready_good = _make_pod(name="api-0", uid="uid-good", pprof_port=6060, restart_count=2)
        pending_pod = _make_pod(name="pending", uid="uid-p", phase="Pending")
        unready_pod = _make_pod(name="unready", uid="uid-u", ready="False")
        no_ip_pod = _make_pod(name="noip", uid="uid-ip", pod_ip="")
        postgres_pod = _make_pod(name="postgres", uid="uid-pg", pprof_port=None)
        empty_name_pod = _make_pod(name="", uid="uid-empty")

        restarted_good = _make_pod(name="api-0", uid="uid-good", pprof_port=6060, restart_count=3)

        api = mock.Mock()
        api.list_namespaced_pod.side_effect = [
            SimpleNamespace(
                items=[
                    ready_good,
                    pending_pod,
                    unready_pod,
                    no_ip_pod,
                    postgres_pod,
                    empty_name_pod,
                ]
            ),
            SimpleNamespace(items=[restarted_good]),
        ]
        api.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=[ready_good])

        with tempfile.TemporaryDirectory() as tmp:
            obs = PprofObserver(core_v1_factory=lambda: api)
            obs._validate_and_configure(
                "cp",
                {"namespace": "ate-system", "label_selector": "app=ate-api-server"},
                Path(tmp),
            )
            obs._log_path = Path(tmp) / "cp.log"
            obs._core_v1 = api

            # Baseline observation at restart_count=2 logs no restart warning.
            pods_first = obs._discover_pods()
            self.assertEqual([p.uid for p in pods_first], ["uid-good"])
            api.list_namespaced_pod.assert_called_with(
                namespace="ate-system",
                label_selector="app=ate-api-server",
                _request_timeout=5,
            )
            log_path = Path(tmp) / "cp.log"
            log_text_first = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
            self.assertNotIn("restarted", log_text_first)

            # Subsequent increase to restart_count=3 logs the restart warning.
            pods_second = obs._discover_pods()
            self.assertEqual([p.uid for p in pods_second], ["uid-good"])
            log_text = (Path(tmp) / "cp.log").read_text(encoding="utf-8")
            self.assertIn("restarted (2 -> 3)", log_text)
            self.assertIn("ate-system/api-0", log_text)

            # All-namespaces mode when namespace is empty.
            obs_all = PprofObserver(core_v1_factory=lambda: api)
            obs_all._validate_and_configure("all", {"label_selector": "tier=core"}, Path(tmp))
            obs_all._log_path = Path(tmp) / "all.log"
            obs_all._core_v1 = api
            pods_all = obs_all._discover_pods()
            self.assertEqual([p.uid for p in pods_all], ["uid-good"])
            api.list_pod_for_all_namespaces.assert_called_once_with(
                label_selector="tier=core",
                _request_timeout=5,
            )

    def test_persistent_discovery_error_surfaces_in_result_and_skips_empty_warning(self):
        class ForbiddenError(Exception):
            status = 403
            reason = "Forbidden"

        api = mock.Mock()
        api.list_pod_for_all_namespaces.side_effect = ForbiddenError("pods is forbidden")

        with tempfile.TemporaryDirectory() as tmp:
            obs = PprofObserver(core_v1_factory=lambda: api)
            obs.start("rbac_fail", {}, Path(tmp))
            res = obs.final_observation()

            api.list_pod_for_all_namespaces.assert_called_with(
                label_selector="",
                _request_timeout=5,
            )
            self.assertEqual(res.status, "error")
            self.assertEqual(res.error, "no pods profiled (pod discovery failed: 403 Forbidden)")
            log_text = (Path(tmp) / "rbac_fail.log").read_text(encoding="utf-8")
            self.assertIn("Warning: pod discovery failed: 403 Forbidden", log_text)
            self.assertNotIn("no Ready pods with a pprof port matched", log_text)


class PprofObserverHTTPScrapingTest(unittest.TestCase):
    def tearDown(self):
        with pprof_mod._CPU_LOCKS_MU:
            pprof_mod._CPU_LOCKS.clear()

    def test_baseline_periodic_final_lifecycle_writes_atomic_files(self):
        seen_paths: list[str] = []
        seen_mu = threading.Lock()
        heap_count = 0
        goroutine_count = 0
        tick_instant_responded = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                nonlocal heap_count, goroutine_count
                payload = f"pprof:{self.path}".encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                self.wfile.flush()
                with seen_mu:
                    seen_paths.append(self.path)
                    if self.path == "/debug/pprof/heap?gc=0":
                        heap_count += 1
                    elif self.path == "/debug/pprof/goroutine?debug=0":
                        goroutine_count += 1
                    # Baseline runs 1 heap?gc=0 + 1 goroutine; tick 1 finishes its
                    # windowed phase first and then runs 1 heap?gc=0 + 1 goroutine.
                    if heap_count >= 2 and goroutine_count >= 2:
                        tick_instant_responded.set()

            def log_message(self, *_args):
                return

        server, port, srv_thread = _start_server(Handler)
        try:
            pod = _make_pod(
                name="api-server-0",
                namespace="ate-system",
                uid="uid-lifecycle-test",
                pprof_port=port,
            )
            api = mock.Mock()
            api.list_namespaced_pod.return_value = SimpleNamespace(items=[pod])

            with tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp)
                obs = PprofObserver(core_v1_factory=lambda: api)
                obs.start(
                    "cp",
                    {
                        "namespace": "ate-system",
                        "profiles": ["profile", "allocs", "heap", "goroutine"],
                        "interval": "1s",
                        "duration": "1s",
                    },
                    out_dir,
                )
                self.assertTrue(
                    tick_instant_responded.wait(timeout=5),
                    "periodic scrape did not enter tick 1 instantaneous phase",
                )
                # Ensure tick 1's instantaneous scrape has finished writing and
                # accounting (2 baseline + 2 windowed + 2 instant = 6) before stop.
                deadline = time.monotonic() + 5.0
                while obs._snapshot_state()[0]["scrapes_succeeded"] < 6:
                    if time.monotonic() >= deadline:
                        self.fail("tick 1 instantaneous scrape did not finish cleanly")
                    time.sleep(0.01)

                res = obs.final_observation()

                self.assertEqual(res.status, "ok")
                self.assertEqual(res.counts["pods_discovered"], 1)
                self.assertEqual(res.counts["pods_profiled"], 1)
                self.assertEqual(res.counts["scrapes_failed"], 0)
                self.assertGreaterEqual(res.counts["scrapes_succeeded"], 8)
                self.assertGreater(res.counts["bytes_collected"], 0)

                self.assertIn("/debug/pprof/heap?gc=0", seen_paths)
                self.assertIn("/debug/pprof/goroutine?debug=0", seen_paths)
                self.assertIn("/debug/pprof/profile?seconds=1", seen_paths)
                self.assertIn("/debug/pprof/allocs?seconds=1&debug=0", seen_paths)
                self.assertIn("/debug/pprof/heap?gc=1", seen_paths)

                pod_dir = out_dir / "ate-system" / "api-server-0"
                files = [p.name for p in pod_dir.iterdir()]
                self.assertTrue(any(f.endswith(".heap.pb.gz") for f in files), files)
                self.assertTrue(any(f.endswith(".goroutine.pb.gz") for f in files), files)
                self.assertTrue(any(f.endswith(".profile.pb.gz") for f in files), files)
                self.assertTrue(any(f.endswith(".allocs.pb.gz") for f in files), files)
                self.assertTrue(any(f.endswith(".heap_gc.pb.gz") for f in files), files)
                self.assertEqual(list(out_dir.rglob("*.tmp")), [])
        finally:
            _stop_server(server, srv_thread)

    def test_module_cpu_lock_skips_overlapping_profile_across_instances(self):
        first_in_handler = threading.Event()
        release_first = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                first_in_handler.set()
                release_first.wait(timeout=5)
                payload = b"cpu-profile"
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                return

        server, port, srv_thread = _start_server(Handler)
        try:
            shared_uid = "uid-cpu-lock-shared"
            pod = _make_pod(name="router-0", uid=shared_uid, pprof_port=port)
            api = mock.Mock()
            api.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=[pod])

            with tempfile.TemporaryDirectory() as tmp1, tempfile.TemporaryDirectory() as tmp2:
                obs1 = PprofObserver(core_v1_factory=lambda: api)
                obs2 = PprofObserver(core_v1_factory=lambda: api)

                obs1._validate_and_configure(
                    "o1", {"profiles": ["profile"], "duration": "1s", "interval": "60s"}, Path(tmp1)
                )
                obs1._log_path = Path(tmp1) / "o1.log"
                obs1._core_v1 = api

                obs2._validate_and_configure(
                    "o2", {"profiles": ["profile"], "duration": "1s", "interval": "60s"}, Path(tmp2)
                )
                obs2._log_path = Path(tmp2) / "o2.log"
                obs2._core_v1 = api

                target = obs1._discover_pods()[0]
                t1 = threading.Thread(
                    target=obs1._scrape_profile,
                    args=(target, "profile"),
                    kwargs={"final_gc": False, "deadline": None, "cancel_event": None},
                )
                t1.start()
                self.assertTrue(first_in_handler.wait(timeout=5))

                # Second instance attempts CPU profile while first holds _CPU_LOCKS[uid].
                obs2._discover_pods()
                obs2._scrape_profile(
                    target, "profile", final_gc=False, deadline=None, cancel_event=None
                )

                release_first.set()
                t1.join(timeout=5)

                c1, _ = obs1._snapshot_state()
                c2, _ = obs2._snapshot_state()
                self.assertEqual(c1["scrapes_succeeded"], 1)
                self.assertEqual(c1["scrapes_overlap_skipped"], 0)
                self.assertEqual(c2["scrapes_succeeded"], 0)
                self.assertEqual(c2["scrapes_overlap_skipped"], 1)
        finally:
            release_first.set()
            _stop_server(server, srv_thread)

    def test_http_500_busy_header_vs_500_without_header_503_and_truncated_bodies(self):
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                if self.path.startswith("/debug/pprof/profile"):
                    body = b"Could not enable CPU profiling: cpu profiling already in use\n"
                    self.send_response(500)
                    self.send_header("X-Go-Pprof", "1")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif self.path.startswith("/debug/pprof/allocs"):
                    # Same busy text WITHOUT X-Go-Pprof: 1 must count as failure.
                    body = b"Could not enable CPU profiling: cpu profiling already in use\n"
                    self.send_response(500)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif self.path.startswith("/debug/pprof/goroutine"):
                    self.send_response(503)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                elif self.path.startswith("/debug/pprof/heap"):
                    # Declare 1024 bytes, write only a fragment, then drop TCP.
                    self.send_response(200)
                    self.send_header("Content-Length", "1024")
                    self.end_headers()
                    self.wfile.write(b"partial-data")
                    self.wfile.flush()
                    self.close_connection = True
                elif self.path.startswith("/debug/pprof/threadcreate"):
                    # Truncated chunked stream (missing terminating 0\r\n\r\n).
                    self.send_response(200)
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    self.wfile.write(b"5\r\nhello\r\n")
                    self.wfile.flush()
                    self.close_connection = True

            def log_message(self, *_args):
                return

        server, port, srv_thread = _start_server(Handler)
        try:
            pod = _make_pod(name="worker-0", uid="uid-failures-test", pprof_port=port)
            api = mock.Mock()
            api.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=[pod])

            with tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp)
                obs = PprofObserver(core_v1_factory=lambda: api)
                obs._validate_and_configure(
                    "errtest",
                    {
                        "profiles": ["profile", "allocs", "heap", "goroutine", "threadcreate"],
                        "duration": "1s",
                        "interval": "60s",
                    },
                    out_dir,
                )
                obs._log_path = out_dir / "errtest.log"
                obs._core_v1 = api
                target = obs._discover_pods()[0]

                obs._scrape_profile(target, "profile", final_gc=False, deadline=None, cancel_event=None)
                obs._scrape_profile(target, "allocs", final_gc=False, deadline=None, cancel_event=None)
                obs._scrape_profile(target, "goroutine", final_gc=False, deadline=None, cancel_event=None)
                obs._scrape_profile(target, "heap", final_gc=False, deadline=None, cancel_event=None)
                obs._scrape_profile(target, "threadcreate", final_gc=False, deadline=None, cancel_event=None)

                counts, _ = obs._snapshot_state()
                self.assertEqual(counts["scrapes_busy_skipped"], 1)
                self.assertEqual(counts["scrapes_failed"], 4)
                self.assertEqual(counts["scrapes_succeeded"], 0)
                self.assertEqual(counts["pods_discovered"], 1)
                self.assertEqual(counts["pods_profiled"], 0)
                self.assertEqual(list(out_dir.rglob("*.tmp")), [])
                self.assertEqual(list(out_dir.rglob("*.pb.gz")), [])
        finally:
            _stop_server(server, srv_thread)

    def test_slow_trickling_response_exceeding_deadline_fails_with_timeout(self):
        stop_trickle = threading.Event()
        client_disconnected = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                self.send_response(200)
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                safety_cap = time.monotonic() + 5.0
                try:
                    while not stop_trickle.is_set() and time.monotonic() < safety_cap:
                        self.wfile.write(b"1\r\nx\r\n")
                        self.wfile.flush()
                        if stop_trickle.wait(timeout=0.05):
                            return
                except (BrokenPipeError, ConnectionResetError, OSError):
                    client_disconnected.set()

            def log_message(self, *_args):
                return

        server, port, srv_thread = _start_server(Handler)
        try:
            pod = _make_pod(name="trickle-0", uid="uid-trickle", pprof_port=port)
            api = mock.Mock()
            api.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=[pod])

            with tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp)
                obs = PprofObserver(core_v1_factory=lambda: api)
                obs._validate_and_configure(
                    "trickle", {"profiles": ["heap"], "duration": "1s", "interval": "60s"}, out_dir
                )
                obs._log_path = out_dir / "trickle.log"
                obs._core_v1 = api
                target = obs._discover_pods()[0]

                obs._scrape_profile(
                    target,
                    "heap",
                    final_gc=False,
                    deadline=time.monotonic() + 0.15,
                    cancel_event=None,
                )

                self.assertTrue(
                    client_disconnected.wait(timeout=1.0),
                    "server handler did not observe client disconnect",
                )
                counts, _ = obs._snapshot_state()
                self.assertEqual(counts["scrapes_failed"], 1)
                self.assertEqual(counts["scrapes_succeeded"], 0)
                log_text = (out_dir / "trickle.log").read_text(encoding="utf-8")
                self.assertTrue(
                    "deadline exceeded" in log_text or "timed out" in log_text,
                    log_text,
                )
                self.assertEqual(list(out_dir.rglob("*.tmp")), [])
                self.assertEqual(list(out_dir.rglob("*.pb.gz")), [])
        finally:
            stop_trickle.set()
            _stop_server(server, srv_thread)

    def test_final_observation_cancels_slow_periodic_profile_without_counting_failure(self):
        first_chunk_sent = threading.Event()
        client_closed = threading.Event()
        paths: list[str] = []
        paths_mu = threading.Lock()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                with paths_mu:
                    paths.append(self.path)
                if self.path.startswith("/debug/pprof/profile?seconds=10"):
                    self.send_response(200)
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    chunk = b"10000\r\n" + (b"x" * 65536) + b"\r\n"
                    self.wfile.write(chunk)
                    self.wfile.flush()
                    first_chunk_sent.set()
                    try:
                        while True:
                            if not self.rfile.read(1):
                                break
                    except OSError:
                        pass
                    finally:
                        client_closed.set()
                    return
                payload = f"ok:{self.path}".encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                return

        server, port, srv_thread = _start_server(Handler)
        try:
            pod = _make_pod(name="api-slow", uid="uid-cancel-test", pprof_port=port)
            api = mock.Mock()
            api.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=[pod])

            with tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp)
                obs = PprofObserver(core_v1_factory=lambda: api)
                obs.start(
                    "cancel_obs",
                    {"profiles": ["profile", "heap"], "duration": "10s", "interval": "60s"},
                    out_dir,
                )
                self.assertTrue(
                    first_chunk_sent.wait(timeout=5),
                    "periodic scrape did not send initial slow profile chunk",
                )
                deadline = time.monotonic() + 5.0
                while not list(out_dir.rglob("*.tmp")):
                    if time.monotonic() >= deadline:
                        self.fail("slow profile scrape did not open a temp file before cancel")
                    time.sleep(0.005)

                res = obs.final_observation()
                self.assertTrue(
                    client_closed.wait(timeout=5),
                    "active profile socket was not closed during final_observation",
                )
                self.assertEqual(res.status, "ok")
                self.assertEqual(res.counts["scrapes_failed"], 0)
                self.assertEqual(res.counts["pods_profiled"], 1)
                self.assertIn("/debug/pprof/heap?gc=0", paths)
                self.assertIn("/debug/pprof/heap?gc=1", paths)
                self.assertEqual(list(out_dir.rglob("*.tmp")), [])
                self.assertTrue(
                    any(p.name.endswith(".heap_gc.pb.gz") for p in out_dir.rglob("*.pb.gz"))
                )
        finally:
            _stop_server(server, srv_thread)


if __name__ == "__main__":
    unittest.main()
