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

"""Unit tests for common.observers.runner_hooks, base, and package isolation."""

import ast
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.observers import (
    OBSERVERS,
    ObserverResult,
    finish_observers,
    start_observers,
    upload_observer_artifacts,
)
import common.observers as observers_pkg


class _RecordingObserver:
    OBSERVER_TYPE = "fake"

    def __init__(
        self,
        *,
        on_start=None,
        on_finish=None,
    ) -> None:
        self._on_start = on_start
        self._on_finish = on_finish
        self.started_with: tuple[str, dict, Path] | None = None

    def start(self, name: str, config: dict, out_dir: Path) -> None:
        self.started_with = (name, config, out_dir)
        if self._on_start is not None:
            self._on_start(name, config, out_dir)

    def final_observation(self) -> ObserverResult:
        if self._on_finish is not None:
            return self._on_finish()
        return ObserverResult(status="ok", counts={"scrapes_succeeded": 1})


class ObserverPackageIsolationTest(unittest.TestCase):
    def test_observers_package_never_imports_forbidden_modules(self):
        pkg_dir = Path(observers_pkg.__file__).resolve().parent
        for py_file in sorted(pkg_dir.glob("*.py")):
            tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self.assertFalse(
                            alias.name == "locust"
                            or alias.name.startswith("locust.")
                            or alias.name == "common.atespace"
                            or alias.name == "google.cloud"
                            or alias.name.startswith("google.cloud."),
                            f"{py_file.name} imports forbidden module {alias.name}",
                        )
                elif isinstance(node, ast.ImportFrom) and node.module:
                    self.assertFalse(
                        node.module == "locust"
                        or node.module.startswith("locust.")
                        or node.module == "common.atespace"
                        or node.module == "google.cloud"
                        or node.module.startswith("google.cloud."),
                        f"{py_file.name} imports from forbidden module {node.module}",
                    )


class ObserverRunnerHooksTest(unittest.TestCase):
    def test_empty_or_absent_entries_is_noop(self):
        for entries in (None, []):
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as tmp:
                logs: list[str] = []
                run = start_observers(entries, Path(tmp), logs.append)
                self.assertEqual(run.status_map, {})
                self.assertEqual(run.started, [])
                self.assertEqual(run.active_dirs, {})
                status_map, stats_rows = finish_observers(
                    run, "ts", "tag", "bench", logs.append
                )
                self.assertEqual(status_map, {})
                self.assertEqual(stats_rows, [])
                self.assertEqual(logs, [])

    def test_non_list_entries_fails_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            logs: list[str] = []
            run = start_observers({"name": "x"}, Path(tmp), logs.append)
            status_map, stats_rows = finish_observers(
                run, "ts", "tag", "bench", logs.append
            )
            self.assertEqual(status_map["observers"]["status"], "error")
            self.assertIn("list", status_map["observers"]["error"])
            self.assertEqual(stats_rows, [])
            self.assertTrue(logs)

    def test_validation_errors_unknown_keys_and_null_config(self):
        created: list[_RecordingObserver] = []

        def fake_factory():
            obs = _RecordingObserver()
            created.append(obs)
            return obs

        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.dict(OBSERVERS, {"fake": fake_factory}, clear=False),
        ):
            logs: list[str] = []
            entries = [
                "not-a-dict",
                {"type": "fake"},
                {"name": "Bad-Name", "type": "fake"},
                {"name": "good_one", "type": "fake", "config": {"k": "v"}},
                {"name": "good_one", "type": "fake"},
                {"name": "misindented", "type": "fake", "namespace": "x"},
                {"name": "bad_type", "type": "missing_type"},
                {"name": "bad_cfg", "type": "fake", "config": ["not", "dict"]},
                {"name": "empty_cfg", "type": "fake", "config": None},
            ]
            run = start_observers(entries, Path(tmp), logs.append)
            status_map, stats_rows = finish_observers(
                run, "2026-11-05T18:30:00Z", "sha", "bench", logs.append
            )

            self.assertEqual(status_map["observers[0]"]["status"], "error")
            self.assertEqual(status_map["observers[1]"]["status"], "error")
            self.assertEqual(status_map["observers[2]"]["status"], "error")
            self.assertEqual(status_map["observers[4]"]["status"], "error")
            self.assertIn("duplicate", status_map["observers[4]"]["error"])
            self.assertEqual(status_map["misindented"]["status"], "error")
            self.assertIn("unknown observer entry keys", status_map["misindented"]["error"])
            self.assertIn("namespace", status_map["misindented"]["error"])
            self.assertEqual(status_map["bad_type"]["status"], "error")
            self.assertEqual(status_map["bad_cfg"]["status"], "error")

            self.assertEqual(status_map["good_one"], {"status": "ok"})
            self.assertEqual(status_map["empty_cfg"], {"status": "ok"})
            self.assertEqual(len(created), 2)
            self.assertEqual(created[0].started_with[1], {"k": "v"})
            self.assertEqual(created[1].started_with[1], {})

            self.assertEqual(
                stats_rows,
                [
                    {
                        "timestamp": "2026-11-05T18:30:00Z",
                        "tag": "sha",
                        "test_name": "bench",
                        "metric": "observer_fake_good_one",
                        "measurements": {"scrapes_succeeded": "1"},
                    },
                    {
                        "timestamp": "2026-11-05T18:30:00Z",
                        "tag": "sha",
                        "test_name": "bench",
                        "metric": "observer_fake_empty_cfg",
                        "measurements": {"scrapes_succeeded": "1"},
                    },
                ],
            )
            self.assertTrue((Path(tmp) / "fake").is_dir())
            self.assertEqual(len(logs), 7)

    def test_start_exception_fails_open_and_sibling_still_runs(self):
        def failing_factory():
            def boom(*_args):
                raise RuntimeError("invalid config")

            return _RecordingObserver(on_start=boom)

        def good_factory():
            return _RecordingObserver(
                on_finish=lambda: ObserverResult(status="ok", counts={"bytes_collected": 42})
            )

        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.dict(
                OBSERVERS,
                {"failing": failing_factory, "good": good_factory},
                clear=False,
            ),
        ):
            logs: list[str] = []
            entries = [
                {"name": "obs_fail", "type": "failing"},
                {"name": "obs_ok", "type": "good"},
            ]
            run = start_observers(entries, Path(tmp), logs.append)
            status_map, stats_rows = finish_observers(
                run, "ts", "tag", "t", logs.append
            )

            self.assertEqual(
                status_map["obs_fail"],
                {"status": "error", "error": "invalid config"},
            )
            self.assertEqual(status_map["obs_ok"], {"status": "ok"})
            self.assertEqual(len(stats_rows), 1)
            self.assertEqual(stats_rows[0]["metric"], "observer_good_obs_ok")
            self.assertEqual(stats_rows[0]["measurements"], {"bytes_collected": "42"})
            self.assertTrue(any("obs_fail" in line for line in logs))

    def test_final_observation_exception_vs_zero_pods_profiled_result(self):
        def raise_final():
            raise RuntimeError("crash on finish")

        zero_counts = {
            "pods_discovered": 0,
            "pods_profiled": 0,
            "scrapes_succeeded": 0,
            "scrapes_failed": 0,
            "scrapes_busy_skipped": 0,
            "scrapes_overlap_skipped": 0,
            "bytes_collected": 0,
        }

        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.dict(
                OBSERVERS,
                {
                    "crash": lambda: _RecordingObserver(on_finish=raise_final),
                    "empty": lambda: _RecordingObserver(
                        on_finish=lambda: ObserverResult(
                            status="error",
                            error="no pods profiled",
                            counts=zero_counts,
                        )
                    ),
                },
                clear=False,
            ),
        ):
            logs: list[str] = []
            entries = [
                {"name": "obs_crash", "type": "crash"},
                {"name": "obs_empty", "type": "empty"},
            ]
            run = start_observers(entries, Path(tmp), logs.append)
            status_map, stats_rows = finish_observers(
                run, "ts", "tag", "bench", logs.append
            )

            self.assertEqual(
                status_map["obs_crash"],
                {"status": "error", "error": "crash on finish"},
            )
            self.assertEqual(
                status_map["obs_empty"],
                {"status": "error", "error": "no pods profiled"},
            )
            # obs_crash emits no stats row; obs_empty emits its 0-valued stats row.
            self.assertEqual(len(stats_rows), 1)
            self.assertEqual(stats_rows[0]["metric"], "observer_empty_obs_empty")
            self.assertEqual(
                stats_rows[0]["measurements"],
                {k: "0" for k in zero_counts},
            )
            self.assertTrue(any("obs_crash" in line for line in logs))
            self.assertTrue(any("obs_empty" in line for line in logs))

    def test_upload_artifacts_skips_tmp_and_is_fail_open(self):
        def writer_factory():
            def on_start(_name: str, _cfg: dict, out_dir: Path) -> None:
                nested = out_dir / "ate-system" / "ate-api-server-abc"
                nested.mkdir(parents=True, exist_ok=True)
                (out_dir / "cp.log").write_text("ok\n", encoding="utf-8")
                (nested / "20261105T183000.050Z.heap.pb.gz").write_bytes(b"gz1")
                (nested / ".20261105T183000.050Z.heap.pb.gz.abc.tmp").write_bytes(b"scratch")
                worker_dir = out_dir / "benchmark-workloads" / "worker-xyz"
                worker_dir.mkdir(parents=True, exist_ok=True)
                (worker_dir / "20261105T183000.100Z.profile.pb.gz").write_bytes(b"gz2")

            return _RecordingObserver(on_start=on_start)

        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.dict(OBSERVERS, {"pprof_fake": writer_factory}, clear=False),
        ):
            run = start_observers(
                [{"name": "cp", "type": "pprof_fake"}],
                Path(tmp),
                lambda _m: None,
            )

            uploaded: list[tuple[str, str]] = []
            upload_logs: list[str] = []

            def flaky_upload(src: Path, dest: str) -> None:
                if src.name == "20261105T183000.100Z.profile.pb.gz":
                    raise RuntimeError("transient upload failure")
                uploaded.append((src.name, dest))

            upload_observer_artifacts(
                run,
                "gs://bucket/runs/test/run_tag=sha/",
                flaky_upload,
                upload_logs.append,
            )

            self.assertEqual(
                uploaded,
                [
                    (
                        "20261105T183000.050Z.heap.pb.gz",
                        "gs://bucket/runs/test/run_tag=sha/pprof_fake/ate-system/ate-api-server-abc/20261105T183000.050Z.heap.pb.gz",
                    ),
                    (
                        "cp.log",
                        "gs://bucket/runs/test/run_tag=sha/pprof_fake/cp.log",
                    ),
                ],
            )
            self.assertEqual(len(upload_logs), 1)
            self.assertIn("20261105T183000.100Z.profile.pb.gz", upload_logs[0])


if __name__ == "__main__":
    unittest.main()
