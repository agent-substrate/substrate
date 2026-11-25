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

"""Unit tests for observer lifecycle wiring in benchmarking/locust/runner.py."""

from contextlib import ExitStack
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.observers import OBSERVERS, ObserverResult
import runner


def _fake_stats_to_jsonl(_csv, jsonl, *_args) -> int:
    jsonl.write_text('{"metric":"locust"}\n', encoding="utf-8")
    return 1


def _fake_runner_patches(
    *,
    stats_csv_content: str | None = None,
    events: list[str] | None = None,
    run_test_error: Exception | None = None,
):
    stack = ExitStack()
    stack.enter_context(mock.patch.object(runner, "log_run_config"))
    stack.enter_context(mock.patch.object(runner, "read_worker_pods", return_value=None))
    stack.enter_context(mock.patch.object(runner, "read_evictions", return_value=None))
    stack.enter_context(mock.patch.object(runner, "collect_cluster_facts", return_value={}))
    stack.enter_context(mock.patch.object(runner, "extract_and_record_server_telemetry"))
    stack.enter_context(mock.patch.object(runner, "append_ladder_steps", return_value=0))

    def fake_run_test(args, csv_prefix, logs, traces):
        if events is not None:
            events.append("run_test")
        if run_test_error is not None:
            raise run_test_error
        if stats_csv_content is not None:
            Path(f"{csv_prefix}_stats.csv").write_text(stats_csv_content, encoding="utf-8")
        return 0

    stack.enter_context(mock.patch.object(runner, "run_test", side_effect=fake_run_test))
    stack.enter_context(mock.patch.object(runner, "stats_to_jsonl", side_effect=_fake_stats_to_jsonl))
    return stack


class RunnerObserversWiringTest(unittest.TestCase):
    _FROZEN_NOW = datetime(2026, 11, 23, 12, 34, 56, tzinfo=timezone.utc)

    def _run_dir(self, dest: Path) -> Path:
        return (
            dest
            / "runs"
            / "bench"
            / "run_date=2026-11-23"
            / f"run_ts={int(self._FROZEN_NOW.timestamp())}"
            / "run_tag=deadbeef"
        )

    def _run_main(
        self,
        dest: Path,
        extra_args: list[str],
        *,
        stats_csv_content: str | None = "header\nrow\n",
        events: list[str] | None = None,
        run_test_error: Exception | None = None,
    ) -> dict:
        argv = [
            "runner.py",
            "-f",
            "tests/glutton.py",
            "-t",
            "1m",
            "-u",
            "1",
            "--tag",
            "deadbeef",
            "--name",
            "bench",
            "--dest",
            str(dest),
            "--no-cluster-facts",
            "--no-phase-breakdown",
            *extra_args,
        ]
        frozen_dt = mock.Mock()
        frozen_dt.now.return_value = self._FROZEN_NOW
        frozen_dt.fromtimestamp.side_effect = datetime.fromtimestamp
        with tempfile.TemporaryDirectory() as work_root:
            with (
                mock.patch.object(tempfile, "tempdir", work_root),
                mock.patch.object(sys, "argv", argv),
                mock.patch.object(runner, "datetime", frozen_dt),
                _fake_runner_patches(
                    stats_csv_content=stats_csv_content,
                    events=events,
                    run_test_error=run_test_error,
                ),
            ):
                runner.main()

        run_dir = self._run_dir(dest)
        return {
            "run_dir": run_dir,
            "status": json.loads((run_dir / "status.json").read_text(encoding="utf-8")),
            "jsonl": (run_dir / "stats.jsonl").read_text(encoding="utf-8").splitlines()
            if (run_dir / "stats.jsonl").exists()
            else [],
        }

    def test_observers_lifecycle_order_status_jsonl_and_recursive_upload(self):
        events: list[str] = []

        class RecordingObserver:
            def start(self, name: str, config: dict, out_dir: Path) -> None:
                events.append(f"start:{name}")
                nested = out_dir / "ate-system" / "api-0"
                nested.mkdir(parents=True, exist_ok=True)
                (out_dir / f"{name}.log").write_text("started\n", encoding="utf-8")
                (nested / "20261123T123456.000Z.heap.pb.gz").write_bytes(b"heap-gz")
                (nested / ".tmp.scratch.tmp").write_bytes(b"ignore")

            def final_observation(self) -> ObserverResult:
                events.append("finish")
                return ObserverResult(status="ok", counts={"pods_profiled": 1})

        test_entry = {
            "name": "bench",
            "observers": [{"name": "cp", "type": "rec", "config": {"interval": "10s"}}],
        }
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.dict(OBSERVERS, {"rec": RecordingObserver}, clear=False),
        ):
            out = self._run_main(
                Path(tmp),
                ["--test-entry", json.dumps(test_entry)],
                events=events,
            )

            self.assertEqual(events, ["start:cp", "run_test", "finish"])
            self.assertEqual(
                out["status"],
                {"locust_exit_code": 0, "stats_generated": True, "observers": {"cp": {"status": "ok"}}},
            )
            metrics = [json.loads(line) for line in out["jsonl"]]
            self.assertEqual(metrics[0], {"metric": "locust"})
            self.assertEqual(metrics[-1]["metric"], "observer_rec_cp")
            self.assertEqual(metrics[-1]["measurements"], {"pods_profiled": "1"})

            run_dir = out["run_dir"]
            self.assertEqual(
                (run_dir / "rec" / "cp.log").read_text(encoding="utf-8"),
                "started\n",
            )
            self.assertEqual(
                (run_dir / "rec" / "ate-system" / "api-0" / "20261123T123456.000Z.heap.pb.gz").read_bytes(),
                b"heap-gz",
            )
            self.assertEqual(list(run_dir.rglob("*.tmp")), [])

    def test_status_omits_observers_when_unconfigured(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = self._run_main(Path(tmp), [])
        self.assertEqual(out["status"], {"locust_exit_code": 0, "stats_generated": True})
        self.assertNotIn("observers", out["status"])

    def test_observer_stats_and_upload_before_missing_stats_exit(self):
        class OkObserver:
            def start(self, name: str, config: dict, out_dir: Path) -> None:
                (out_dir / "cp.log").write_text("captured\n", encoding="utf-8")

            def final_observation(self) -> ObserverResult:
                return ObserverResult(status="ok", counts={"scrapes_succeeded": 2})

        test_entry = {"observers": [{"name": "cp", "type": "ok_obs"}]}
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.dict(OBSERVERS, {"ok_obs": OkObserver}, clear=False),
        ):
            # Missing CSV + allow_empty_stats keeps stats_generated=False while
            # observer rows still land in stats.jsonl.
            out = self._run_main(
                Path(tmp) / "allow_empty",
                ["--allow-empty-stats", "--test-entry", json.dumps(test_entry)],
                stats_csv_content=None,
            )
            self.assertFalse(out["status"]["stats_generated"])
            self.assertEqual(len(out["jsonl"]), 1)
            self.assertEqual(json.loads(out["jsonl"][0])["metric"], "observer_ok_obs_cp")

            # Without --allow-empty-stats, missing CSV still uploads observer artifacts
            # before exiting 1.
            strict_dest = Path(tmp) / "strict"
            with self.assertRaises(SystemExit) as cm:
                self._run_main(
                    strict_dest,
                    ["--test-entry", json.dumps(test_entry)],
                    stats_csv_content=None,
                )
            self.assertEqual(cm.exception.code, 1)
            strict_run_dir = self._run_dir(strict_dest)
            self.assertEqual(
                (strict_run_dir / "ok_obs" / "cp.log").read_text(encoding="utf-8"),
                "captured\n",
            )

    def test_fail_open_on_start_finish_and_upload_errors(self):
        class BoomStartObserver:
            def start(self, name: str, config: dict, out_dir: Path) -> None:
                raise RuntimeError("boom start")

            def final_observation(self) -> ObserverResult:
                raise AssertionError("not called")

        class BoomFinishObserver:
            def start(self, name: str, config: dict, out_dir: Path) -> None:
                (out_dir / "keep.log").write_text("partial\n", encoding="utf-8")

            def final_observation(self) -> ObserverResult:
                raise RuntimeError("boom finish")

        test_entry = {
            "observers": [
                {"name": "bad_start", "type": "boom_start"},
                {"name": "bad_finish", "type": "boom_finish"},
            ]
        }

        real_upload = runner.upload

        def failing_obs_upload(src: Path, dest: str) -> None:
            if src.name == "keep.log":
                raise OSError("upload failed")
            real_upload(src, dest)

        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.dict(
                OBSERVERS,
                {"boom_start": BoomStartObserver, "boom_finish": BoomFinishObserver},
                clear=False,
            ),
            mock.patch.object(runner, "upload", side_effect=failing_obs_upload),
        ):
            out = self._run_main(Path(tmp), ["--test-entry", json.dumps(test_entry)])

        self.assertEqual(out["status"]["locust_exit_code"], 0)
        self.assertTrue(out["status"]["stats_generated"])
        self.assertEqual(
            out["status"]["observers"],
            {
                "bad_start": {"status": "error", "error": "boom start"},
                "bad_finish": {"status": "error", "error": "boom finish"},
            },
        )

    def test_gcs_client_reused_across_uploads(self):
        runner._gcs_client.cache_clear()
        self.addCleanup(runner._gcs_client.cache_clear)

        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch("google.cloud.storage.Client") as mock_client,
        ):
            f1 = Path(tmp) / "a.txt"
            f2 = Path(tmp) / "b.txt"
            f1.write_text("a", encoding="utf-8")
            f2.write_text("b", encoding="utf-8")

            runner.upload_to_gcs(f1, "gs://my-bucket/runs/a.txt")
            runner.upload_to_gcs(f2, "gs://my-bucket/runs/b.txt")

            mock_client.assert_called_once_with()
            mock_client.return_value.bucket.assert_has_calls(
                [
                    mock.call("my-bucket"),
                    mock.call().blob("runs/a.txt"),
                    mock.call().blob().upload_from_filename(str(f1)),
                    mock.call("my-bucket"),
                    mock.call().blob("runs/b.txt"),
                    mock.call().blob().upload_from_filename(str(f2)),
                ]
            )

    def test_exception_in_run_test_still_finishes_observers_once(self):
        finish_calls = 0

        class CountingObserver:
            def start(self, name: str, config: dict, out_dir: Path) -> None:
                return

            def final_observation(self) -> ObserverResult:
                nonlocal finish_calls
                finish_calls += 1
                return ObserverResult(status="ok")

        test_entry = {"observers": [{"name": "cp", "type": "counting"}]}
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.dict(OBSERVERS, {"counting": CountingObserver}, clear=False),
            self.assertRaisesRegex(RuntimeError, "boom"),
        ):
            self._run_main(
                Path(tmp),
                ["--test-entry", json.dumps(test_entry)],
                run_test_error=RuntimeError("boom"),
            )

        self.assertEqual(finish_calls, 1)


if __name__ == "__main__":
    unittest.main()
