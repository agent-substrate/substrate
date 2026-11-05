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

"""Unit tests for the CLI, `python -m evaluate`, on stored runs.

Runs are stand-in directories built by the tests; GCS is mocked.
"""

import contextlib
import io
import json
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evaluate import __main__ as cli  # noqa: E402
from evaluate import rundir  # noqa: E402

T0 = 1791700000  # 2026-10-11T07:06:40Z
TAG = "abc123"


def boomer(sec: float, msg: str, **kw) -> str:
    t = datetime.fromtimestamp(T0 + sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f000Z")
    return "[boomer] " + json.dumps({"time": t, "level": "INFO", "msg": msg, **kw})


def record(sec: float, actor: str, wall_ms: float = 8000.0) -> str:
    end = datetime.fromtimestamp(T0 + sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f000Z")
    return boomer(sec, "sweperf task record", v=1, user=1, actor=actor, end=end, cel_ms=6000.0, wall_ms=wall_ms,
                  resume_max_ms=200.0, ttfe_max_ms=210.0, retries=0, failed=False, fail_step="", fail_cycle=0)


def write_run(d: Path, records: list[str], cluster: str = "c1") -> Path:
    d.mkdir(parents=True, exist_ok=True)
    (d / "logs.txt").write_text("\n".join([*records, boomer(60, "sweperf shutdown started")]) + "\n")
    trial = {"timestamp": "2026-10-11T07:06:40Z", "tag": TAG, "test_name": d.name, "metric": "trial_summary",
             "measurements": {"node_count": "1", "allocatable_cores": "100", "allocatable_ram_gb": "400",
                              "aggregate_failure_ratio": "0.0005", "metadata_cluster_name": cluster}}
    (d / "stats.jsonl").write_text(json.dumps(trial) + "\n")
    (d / "server_summary.json").write_text(json.dumps({"metadata": {
        "start_ts": T0, "end_ts": T0 + 60, "steady_start_ts": T0, "steady_end_ts": T0 + 60}}))
    return d


def stored(root: Path, test: str, run_ts: int, records=None, cluster: str = "c1") -> Path:
    date = datetime.fromtimestamp(run_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    d = Path(rundir.run_uri(str(root), test, date, str(run_ts), TAG))
    return write_run(d, records or [record(10, "a1"), record(20, "a2", wall_ms=9000.0)], cluster=cluster)


def threshold(value: str, op: str, limit) -> dict:
    return {"value": value, "op": op, "limit": limit}


def evaluations(limit: float = 90) -> list[dict]:
    """goodput_strict against the run of t1 (the 1-user run), and an envelope check."""
    goodput_cfg = {"kind": "sweperf_tasks", "rules": [
        {"name": "slow_resume", "cel": "task.resume_max_ms <= 1500.0"},
        {"name": "slow_wall", "cel": "task.wall_ms <= 1.5 * baseline.wall_ms"}],
        "values": {"good_ratio_pct": "round(100.0 * good / records, 2)"}, "baseline_test": "t1"}
    return [
        {"name": "goodput_strict", "variables": [{"name": "goodput", "source": "cel", "source_cfg": goodput_cfg}],
         "conditions": [{"name": "good_ratio", "type": "threshold",
                         "check": threshold("goodput.good_ratio_pct", ">=", limit)}]},
        {"name": "envelope",
         "variables": [{"name": "trial", "source": "run_results",
                        "source_cfg": {"metric": "trial_summary", "measurements": ["aggregate_failure_ratio"]}}],
         "conditions": [{"name": "failure_ratio", "type": "threshold",
                         "check": threshold("trial.aggregate_failure_ratio", "<=", 0.001)}]},
    ]


def goodput(out: Path) -> dict:
    """The goodput variable of the evaluation.json in out."""
    return json.loads((out / "evaluation.json").read_text())["evaluations"][0]["variables"][0]


class CliTest(unittest.TestCase):
    def run_cli(self, *argv: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = cli.main(list(argv))
        return rc, out.getvalue(), err.getvalue()

    def test_evaluates_and_reevaluates_from_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            t1 = stored(root, "t1", T0)
            run = stored(root, "t150", T0 + 600, records=[record(10, "a1"), record(20, "a2", wall_ms=13000.0)])
            evs = root / "evs.json"
            evs.write_text(json.dumps(evaluations(limit=75)))
            out = root / "out"
            rc, printed, _ = self.run_cli(f"{run}/", "--evaluations", f"@{evs}", "--out", str(out))
            self.assertEqual(rc, 0)
            rows = [json.loads(line) for line in (out / "evaluation_rows.jsonl").read_text().splitlines()]
            self.assertEqual({r["timestamp"] for r in rows}, {"2026-10-11T07:06:40Z"})  # the run's, not today's
            self.assertEqual(len(rows), 3)
            # t1's run is the baseline: a2's 13 s is over 1.5 x its 8.5 s median.
            self.assertIn("evaluation goodput_strict: fail (failed: good_ratio)", printed)
            values = goodput(out)["values"]
            self.assertEqual((values["baseline_run"], values["bad_slow_wall"]), (str(t1), 1))
            report = json.loads((out / "evaluation.json").read_text())
            self.assertEqual((report["run"]["test_name"], report["run"]["run_ts"]), ("t150", str(T0 + 600)))
            self.assertFalse((run / "evaluation.json").exists())  # never writes into the stored run
            # The stored run's evaluation.json (as the runner uploads it) re-evaluates as recorded.
            (run / "evaluation.json").write_text((out / "evaluation.json").read_text())
            rc, printed, _ = self.run_cli(str(run), "--from-run", "--out", str(root / "o2"))
            self.assertEqual(rc, 0)
            self.assertEqual(json.loads((root / "o2" / "evaluation.json").read_text())["summary"], report["summary"])

    def test_paired_runs_need_the_runners_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            t1 = stored(root, "t1", T0)
            flat = write_run(root / "flat", [record(10, "a1")])
            rc, _, _ = self.run_cli(str(flat), "--evaluations", json.dumps(evaluations()), "--out", str(root / "o1"))
            self.assertEqual(rc, 0)
            self.assertEqual(goodput(root / "o1")["invalid_reason"], "rule 'slow_wall' reads baseline.wall_ms, which the "
                             "run lacks (baseline unavailable: no run of t1 to pair with)")
            # A symlink into a runs/ tree is stored there: its path gives the identity and the dest.
            link = Path(rundir.run_uri(tmp, "t150", "2026-10-11", str(T0 + 600), TAG))
            link.parent.mkdir(parents=True)
            link.symlink_to(flat)
            rc, _, _ = self.run_cli(str(link), "--evaluations", json.dumps(evaluations()), "--out", str(root / "o2"))
            self.assertEqual(rc, 0)
            report = json.loads((root / "o2" / "evaluation.json").read_text())
            self.assertEqual(report["run"]["run_ts"], str(T0 + 600))
            self.assertEqual(goodput(root / "o2")["values"]["baseline_run"], str(t1))

    def test_gcs_run(self):
        """A gs:// RUN is copied, and its paired run found and copied, under the same gs:// dest."""
        with tempfile.TemporaryDirectory() as tmp:
            bucket = Path(tmp) / "bucket"

            def gcs_list(prefix: str) -> list[str]:
                local = bucket / prefix[len("gs://b/"):]
                return [f"gs://b/{p.relative_to(bucket)}" for p in local.rglob("*") if p.is_file()]

            def gcs_copy(uri: str, dest: Path) -> bool:
                src = bucket / uri[len("gs://b/"):]
                return src.is_file() and bool(shutil.copy(src, dest))

            t1 = stored(bucket / "p", "t1", T0)
            run = stored(bucket / "p", "t150", T0 + 600)
            uri = f"gs://b/{run.relative_to(bucket)}"
            with mock.patch.object(rundir, "_gcs_list", side_effect=gcs_list), \
                 mock.patch.object(rundir, "_gcs_copy", side_effect=gcs_copy):
                rc, _, _ = self.run_cli(uri, "--evaluations", json.dumps(evaluations()), "--out", f"{tmp}/out")
            self.assertEqual(rc, 0)
            values = goodput(Path(tmp) / "out")["values"]
            self.assertEqual((values["baseline_run"], values["good"]), (f"gs://b/{t1.relative_to(bucket)}", 2))

    def test_exit_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp) / "run", [record(10, "a1")])
            self.assertEqual(self.run_cli(str(run), "--evaluations", '[{"name": 1}]')[0], 2)
            self.assertEqual(self.run_cli(str(run), "--from-run")[0], 2)  # no evaluation.json in the run
            self.assertEqual(self.run_cli(str(Path(tmp) / "missing"), "--evaluations",
                                          json.dumps(evaluations()))[0], 1)
            # A shape no check expects is a config error, not a traceback.
            bad_op = evaluations()
            bad_op[0]["conditions"][0]["check"]["op"] = ["<="]
            self.assertEqual(self.run_cli("--check-config", "--evaluations", json.dumps(bad_op))[0], 2)
            self.assertEqual(self.run_cli("--check-config", "--evaluations", json.dumps(evaluations()))[0], 0)
            with self.assertRaises(SystemExit):
                self.run_cli(str(run))  # no evaluations given

    def test_check_config_on_a_tests_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "tests.yaml"
            path.write_text(json.dumps({"tests": [{"name": "ok", "evaluations": evaluations()},
                                                  {"name": "bad", "evaluations": [{"name": "Bad"}]},
                                                  {"name": "none"}]}))
            rc, out, err = self.run_cli("--check-config", "--tests-file", str(path))
            self.assertEqual(rc, 2)
            self.assertIn("ok: ok (2 evaluations)", out)
            self.assertIn("bad: evaluations[0]: name 'Bad'", err)
            rc, out, _ = self.run_cli("--check-config", "--tests-file", str(path), "--test-name", "ok")
            self.assertEqual((rc, out), (0, "ok: ok (2 evaluations)\n"))


if __name__ == "__main__":
    unittest.main()
