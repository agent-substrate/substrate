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

"""Unit tests for density_runner.py."""

import argparse
from dataclasses import replace
import json
from pathlib import Path
import shutil
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import density_runner
from density_runner import StepResult


class NextStepUsersTest(unittest.TestCase):
    def test_step_up_and_bisection(self):
        def hist(passed, failed=()):
            return [StepResult(users=u, passed=True) for u in passed] + [
                StepResult(users=u, passed=False) for u in failed
            ]

        cases = [
            ([], 1024, 8),
            ([], 4, 4),
            (hist([], [8]), 1024, None),
            (hist([8, 16, 32]), 1024, 64),
            (hist([8, 16, 32, 64]), 100, 100),
            (hist([8, 16, 32, 64, 100]), 100, None),
            (hist([8, 16, 32], [64]), 1024, 48),
            (hist([8, 16, 32, 48], [64, 56]), 1024, None),
            (hist([8, 16, 32, 64, 128, 192], [256, 224, 208]), 1024, None),
        ]
        for steps, max_users, want in cases:
            with self.subTest(tested=[(s.users, s.passed) for s in steps], max_users=max_users):
                self.assertEqual(density_runner.next_step_users(steps, max_users), want)


class EvaluateStepTest(unittest.TestCase):
    def test_gates(self):
        base = StepResult(
            users=8,
            passed=True,
            live_actors=8,
            wake_p90_ms=120.0,
            suspend_p90_ms=150.0,
            failure_ratio=0.0,
        )
        slow_base = replace(base, wake_p90_ms=700.0, suspend_p90_ms=600.0)
        ok = StepResult(
            users=64,
            live_actors=64,
            replaced_actors=0,
            wake_p90_ms=650.0,
            suspend_p90_ms=800.0,
            failure_ratio=0.0,
        )
        cases = [
            ("clean_pass", ok, base, ""),
            ("no_wake", replace(ok, wake_p90_ms=None), base, "no wake measurements recorded"),
            ("wake_knee_floor", replace(ok, wake_p90_ms=1050.0), base, "wake P90 1050 ms > 1000 ms"),
            ("suspend_knee", replace(ok, suspend_p90_ms=1200.0), base, "suspend P90 1200 ms > 1000 ms"),
            ("multiplier_pass", replace(ok, wake_p90_ms=1350.0, suspend_p90_ms=1150.0), slow_base, ""),
            (
                "multiplier_fail",
                replace(ok, wake_p90_ms=1450.0, suspend_p90_ms=1150.0),
                slow_base,
                "wake P90 1450 ms > 1400 ms",
            ),
            ("eviction", replace(ok, oom_counts={"oom_events_evicted": 1}), base, "oom_events_evicted=1"),
            ("crash", replace(ok, crash_count=2), base, "actor_crashes=2"),
            ("step_failures", replace(ok, step_failures=3), base, "step_failures=3"),
            ("replaced", replace(ok, replaced_actors=2), base, "replaced_actors=2"),
            ("live_shortfall", replace(ok, live_actors=50), base, "live actors 50 < 58"),
            ("missing_live", replace(ok, live_actors=None), base, "live actors None < 58"),
            ("failure_ratio", replace(ok, failure_ratio=0.025), base, "failure ratio 0.0250 > 0.0100"),
        ]
        for name, step, ref_base, want in cases:
            with self.subTest(name=name):
                self.assertEqual(density_runner.evaluate_step(step, ref_base), want)


class ParseAndSweepTest(unittest.TestCase):
    def test_parse_summarize_and_build_jsonl(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "stats.jsonl"
            rows = [
                {
                    "metric": "summary_TimeToWake",
                    "measurements": {"request_count": "120", "p50": "112", "p90": "210", "requests_per_s": "2.5"},
                },
                {
                    "metric": "grpc_SuspendActorAsync",
                    "measurements": {"request_count": "120", "p50": "80", "p90": "140"},
                },
                {
                    "metric": "http_Step_01_morning_triage",
                    "measurements": {"request_count": "120", "failure_count": "2"},
                },
                {
                    "metric": "http_Step_02_code_review",
                    "measurements": {"request_count": "120", "failure_count": "1"},
                },
                {
                    "metric": "http_WakeFirstTouch",
                    "measurements": {"request_count": "125", "failure_count": "5"},
                },
                {
                    "metric": "trial_summary",
                    "measurements": {
                        "machine_type": "c4-standard-16",
                        "live_actors": "32",
                        "replaced_actors": "0",
                        "time_to_wake_failure_ratio": "0.005",
                        "suspend_actor_async_failure_ratio": "0.008",
                        "aggregate_failure_ratio": "0.08",
                    },
                },
                {
                    "metric": "server_summary",
                    "measurements": {
                        "active_actors_p50": "4.5",
                        "oom_events_worker_pods_lost": "0",
                        "working_set_node_p90_gb": "12.4",
                    },
                },
            ]
            p.write_text("\n".join(json.dumps(r) for r in rows) + "\n")

            step = density_runner.parse_step_stats(32, 0, p)
            self.assertEqual(step.live_actors, 32)
            self.assertEqual(step.replaced_actors, 0)
            self.assertEqual(step.step_failures, 3)
            self.assertEqual(step.wake_p90_ms, 210.0)
            self.assertEqual(step.suspend_p90_ms, 140.0)
            self.assertEqual(step.failure_ratio, 0.008)
            self.assertEqual(step.server_fields["working_set_node_p90_gb"], 12.4)
            self.assertEqual(step.oom_counts["oom_events_worker_pods_lost"], 0)
            step.passed = True

            base = StepResult(users=8, passed=True, wake_p90_ms=110.0, suspend_p90_ms=100.0)
            failed = StepResult(
                users=64,
                passed=False,
                failure_reason="wake P90 1200 ms > 1000 ms",
                wake_p90_ms=1200.0,
            )
            summary = density_runner.summarize_sweep([base, step, failed])
            self.assertEqual(summary["max_actors"], 32)
            self.assertTrue(summary["ceiling_reached"])
            self.assertEqual(summary["first_failed_users"], 64)

            records = density_runner.build_jsonl_records(
                [base, step, failed],
                summary,
                timestamp="2026-10-26T01:00:00Z",
                tag="abc",
                test_name="density_pa_light",
                run_start=1000,
                run_end=2000,
            )
            by_metric = {r["metric"]: r["measurements"] for r in records}
            self.assertEqual(by_metric["trial_summary"]["run_start"], "1000")
            self.assertEqual(by_metric["trial_summary"]["run_end"], "2000")
            self.assertEqual(by_metric["density_limit"]["max_actors"], "32")
            self.assertEqual(by_metric["density_step_0032"]["passed"], "true")
            self.assertEqual(by_metric["density_step_0064"]["passed"], "false")

    def test_build_step_cmd(self):
        args = argparse.Namespace(
            file="/app/tests/agentsession.py",
            duration="90m",
            density_step_duration="5m",
            users=1024,
            tag="t",
            name="n",
            dest="/tmp/out",
            prometheus_url="http://prom:9090",
            atelet_lag_s=70,
            cluster_facts=True,
            actors_per_user=None,
            metadata={"cluster_name": "c"},
            locust_extra=["--spawn-rate", "64", "--agentsession-phase-in", "90"],
        )
        cmd = density_runner.build_step_cmd(args, 64, Path("/tmp/s64"))
        self.assertIn("--no-phase-breakdown", cmd)
        self.assertEqual(cmd[-4:], ["--spawn-rate", "64", "--agentsession-phase-in", "90"])

        args.actors_per_user = 2
        with self.assertRaises(ValueError):
            density_runner.build_step_cmd(args, 8, Path("/tmp/s8"))

    def test_run_density_sweep_writes_status_json(self):
        def make_args(dest: Path) -> argparse.Namespace:
            return argparse.Namespace(
                file="/app/tests/agentsession.py",
                duration="90m",
                density_step_duration="1m",
                users=16,
                tag="abc123",
                name="density_pa_light_gvisor",
                dest=str(dest),
                prometheus_url="http://prom:9090",
                atelet_lag_s=0,
                cluster_facts=False,
                actors_per_user=None,
                metadata={},
                locust_extra=[],
            )

        def copy_upload(src: Path, dest: str) -> None:
            out = Path(dest)
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(src, out)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ok_dest, ok_work = root / "ok_out", root / "ok_work"
            ok_work.mkdir()

            def fake_ok_step(_args, users, _step_dest, _logs):
                return StepResult(
                    users=users,
                    live_actors=users,
                    wake_p50_ms=80.0,
                    wake_p90_ms=120.0 if users == 8 else 1500.0,
                    suspend_p50_ms=90.0,
                    suspend_p90_ms=130.0,
                    failure_ratio=0.0,
                    trial_summary={"machine_type": "c4-standard-16"},
                )

            with (
                mock.patch("density_runner.tempfile.mkdtemp", return_value=str(ok_work)),
                mock.patch("density_runner.run_step", side_effect=fake_ok_step),
                mock.patch("density_runner.wait_for_idle", return_value=True) as idle,
            ):
                density_runner.run_density_sweep(make_args(ok_dest), copy_upload)
            self.assertEqual(idle.call_count, 2)

            status_file = next(ok_dest.rglob("status.json"))
            self.assertEqual(
                json.loads(status_file.read_text(encoding="utf-8")),
                {"locust_exit_code": 0, "stats_generated": True},
            )

            fail_dest, fail_work = root / "fail_out", root / "fail_work"
            fail_work.mkdir()
            with (
                mock.patch("density_runner.tempfile.mkdtemp", return_value=str(fail_work)),
                mock.patch("density_runner.wait_for_idle", return_value=True),
                mock.patch("density_runner.run_step", return_value=StepResult(users=8, exit_code=2)),
                self.assertRaises(SystemExit) as cm,
            ):
                density_runner.run_density_sweep(make_args(fail_dest), copy_upload)

            self.assertEqual(cm.exception.code, 1)
            fail_status = next(fail_dest.rglob("status.json"))
            self.assertEqual(
                json.loads(fail_status.read_text(encoding="utf-8")),
                {"locust_exit_code": 1, "stats_generated": False},
            )

            stuck_dest, stuck_work = root / "stuck_out", root / "stuck_work"
            stuck_work.mkdir()
            with (
                mock.patch("density_runner.tempfile.mkdtemp", return_value=str(stuck_work)),
                mock.patch("density_runner.run_step", side_effect=fake_ok_step) as run_step,
                mock.patch("density_runner.wait_for_idle", return_value=False),
                self.assertRaises(SystemExit),
            ):
                density_runner.run_density_sweep(make_args(stuck_dest), copy_upload)
            run_step.assert_not_called()
            stuck_rows = {
                r["metric"]: r["measurements"]
                for r in map(
                    json.loads,
                    next(stuck_dest.rglob("stats.jsonl")).read_text(encoding="utf-8").splitlines(),
                )
            }
            self.assertEqual(stuck_rows["density_limit"]["abort_reason"], "cluster did not go idle")


class _FakeRpcError(density_runner.grpc.RpcError):
    def __init__(self, code=density_runner.grpc.StatusCode.UNAVAILABLE, details="unavailable"):
        super().__init__()
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


class WaitForIdleTest(unittest.TestCase):
    def test_cleanup_benchmark_actors_pages_and_ignores_not_found(self):
        deleted: list[str] = []

        class FakeStub:
            def __init__(self, _ch):
                self._pages = [
                    types.SimpleNamespace(
                        actors=[types.SimpleNamespace(metadata=types.SimpleNamespace(name="a1"))],
                        next_page_token="p2",
                    ),
                    types.SimpleNamespace(
                        actors=[types.SimpleNamespace(metadata=types.SimpleNamespace(name="a2"))],
                        next_page_token="",
                    ),
                ]

            def ListActors(self, _req, timeout=None):
                return self._pages.pop(0)

            def DeleteActor(self, req, timeout=None):
                deleted.append(req.actor.name)
                if req.actor.name == "a2":
                    raise _FakeRpcError(density_runner.grpc.StatusCode.NOT_FOUND, "gone")

        fake_pb2 = types.SimpleNamespace(
            ListActorsRequest=lambda **kw: types.SimpleNamespace(**kw),
            ListOptions=lambda **kw: types.SimpleNamespace(**kw),
            DeleteActorRequest=lambda **kw: types.SimpleNamespace(**kw),
            ObjectRef=lambda **kw: types.SimpleNamespace(**kw),
        )
        fake_pb2_grpc = types.SimpleNamespace(ControlStub=FakeStub)
        fake_atespace = types.SimpleNamespace(ATESPACE="benchmark")
        with (
            mock.patch("density_runner.ateapi_channel"),
            mock.patch.dict(
                sys.modules,
                {
                    "common": types.SimpleNamespace(
                        ateapi_pb2=fake_pb2, ateapi_pb2_grpc=fake_pb2_grpc
                    ),
                    "common.ateapi_pb2": fake_pb2,
                    "common.ateapi_pb2_grpc": fake_pb2_grpc,
                    "common.atespace": fake_atespace,
                },
            ),
        ):
            self.assertEqual(density_runner.cleanup_benchmark_actors(), 2)
        self.assertCountEqual(deleted, ["a1", "a2"])

    def test_waits_for_actors_and_pods(self):
        logs = mock.Mock()
        with (
            mock.patch("density_runner.cleanup_benchmark_actors", side_effect=[3, 0, 0]),
            mock.patch("density_runner.worker_pods_ready", side_effect=[True, False, True]),
            mock.patch("density_runner.time.sleep") as sleep,
            mock.patch("builtins.print"),
        ):
            self.assertTrue(density_runner.wait_for_idle(True, logs))
        self.assertEqual(sleep.call_count, 2)

    def test_skips_pod_check_without_cluster_facts(self):
        with (
            mock.patch("density_runner.cleanup_benchmark_actors", return_value=0),
            mock.patch("density_runner.worker_pods_ready") as ready,
        ):
            self.assertTrue(density_runner.wait_for_idle(False, mock.Mock()))
        ready.assert_not_called()

    def test_times_out_on_leftover_actors_or_list_errors(self):
        for name, count in (("leftover", {"return_value": 5}),
                            ("list_error", {"side_effect": _FakeRpcError()})):
            with (
                self.subTest(name=name),
                mock.patch("density_runner.SETTLE_TIMEOUT_S", 0),
                mock.patch("density_runner.cleanup_benchmark_actors", **count),
                mock.patch("density_runner.worker_pods_ready", return_value=True),
                mock.patch("builtins.print"),
            ):
                self.assertFalse(density_runner.wait_for_idle(True, mock.Mock()))


if __name__ == "__main__":
    unittest.main()
