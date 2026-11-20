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

"""Unit tests for the goodput script.

Run via: python3 tools/goodput/test_goodput.py
Never contacts GCS or a cluster; runs are stand-in directories built by the tests.
Stdlib only.
"""

import contextlib
import io
import json
import re
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import classify  # noqa: E402
import goodput  # noqa: E402
import metrics  # noqa: E402
import parse  # noqa: E402

CONFIG = json.loads((HERE / "config.json").read_text(encoding="utf-8"))
STRICT, RELAXED = CONFIG["modes"]["strict"], CONFIG["modes"]["relaxed"]

T0 = 1791700000.0  # an arbitrary run start (unix seconds)
A, B, C, D, E = (f"sb-{i}1111111-2222-3333-4444-555555555555" for i in range(5))
ONSTART = "sweperf on_start failed; goroutine will retry next iter"

# Real task log lines from sweperf runs: the ones the script reads,
# plus three it skips (a cycle start, the error-only resume line and a traced span).
REAL = {
    "start": '[boomer] {"time":"2026-10-11T06:57:35.684909393Z","level":"INFO","msg":"Starting sweperf cycle",'
             '"actor":"sb-818374a1-f288-4aa7-8acf-5c8f456f7883","cycle":1,"start_step":1,"end_step":6}',
    "resume_err": '[boomer] {"time":"2026-10-11T16:08:36.372117022Z","level":"ERROR","msg":"ResumeActor failed",'
                  '"actor":"sb-7fe17aa4-b5bb-490d-898d-2310443554fe","err":"rpc error: code = FailedPrecondition '
                  'desc = AssignWorker prerequisite not met for Actor"}',
    "resume_fail": '[boomer] {"time":"2026-10-11T07:02:29.268099898Z","level":"ERROR","msg":"ResumeActor failed in '
                   'step","actor":"sb-fd5fa02a-7b66-466d-8d60-e84758851b0d","cycle":3}',
    "exec_fail": '[boomer] {"time":"2026-10-11T16:08:36.157189554Z","level":"ERROR","msg":"execute steps failed",'
                 '"actor":"sb-7fe17aa4-b5bb-490d-898d-2310443554fe","cycle":3,"err":"timed out waiting for job '
                 '5db29190-c114-41cb-a45c-07bf33e4672d to complete in cycle 3"}',
    "suspend_fail": '[boomer] {"time":"2026-10-11T16:08:36.180424399Z","level":"ERROR","msg":"SuspendActor failed",'
                    '"actor":"sb-7fe17aa4-b5bb-490d-898d-2310443554fe","err":"rpc error: code = Internal desc = '
                    'internal server error: workflow failed at step CallAteletSuspend"}',
    "onstart_fail": '[boomer] {"time":"2026-10-18T02:54:02.431939215Z","level":"WARN","msg":"sweperf on_start '
                    'failed; goroutine will retry next iter","user":81,'
                    '"actor":"sb-a2b7fb28-ce3b-4f2b-a87a-a3f22949df7f","template":"sweperf-astropy-7336",'
                    '"err":"pollLiveness: failed to verify server liveness for actor '
                    'sb-a2b7fb28-ce3b-4f2b-a87a-a3f22949df7f"}',
    "quit": '[boomer] {"time":"2026-10-11T07:07:33.958613342Z","level":"INFO","msg":"Recv quit message from master, '
            'all the goroutines are stopped"}',
    "traced_span": '[boomer] {"time":"2026-10-11T06:57:36.1Z","level":"INFO","msg":"traced span",'
                   '"actor":"sb-818374a1-f288-4aa7-8acf-5c8f456f7883"}',
    "record_gvisor": '[boomer] {"time":"2026-10-18T02:29:42.61358281Z","level":"INFO","msg":"sweperf task record",'
                     '"v":1,"user":372,"actor":"sb-a597c5ed-9f70-47cb-96c6-67e267d7dd7a",'
                     '"template":"sweperf-astropy-7336","cycles":4,"steps":21,'
                     '"task_start":"2026-10-18T02:28:49.610791521Z","end":"2026-10-18T02:29:42.613576352Z",'
                     '"startup_ms":597.996826,"cel_ms":10301.58,"wall_ms":12584.251139,"think_ms":39819.169281,'
                     '"resume_max_ms":208.538,"retries":0,"failed":false,"fail_step":"","fail_cycle":0,'
                     '"ttfe_max_ms":216.560976}',
    "record_microvm": '[boomer] {"time":"2026-10-18T02:50:41.133817961Z","level":"INFO","msg":"sweperf task record",'
                      '"v":1,"user":463,"actor":"sb-ac4d080a-714d-4cb4-946b-3ca67692ef67",'
                      '"template":"sweperf-astropy-7336","cycles":4,"steps":21,'
                      '"task_start":"2026-10-18T02:48:19.086984726Z","end":"2026-10-18T02:50:41.133809403Z",'
                      '"startup_ms":2297.999567,"cel_ms":12719.2,"wall_ms":82314.394465,"think_ms":57432.57814,'
                      '"resume_max_ms":7232.807,"retries":0,"failed":false,"fail_step":"","fail_cycle":0,'
                      '"ttfe_max_ms":7240.629771}',
}


def ts(sec: float) -> str:
    """RFC 3339 for T0 + sec, with nanoseconds like the task log."""
    whole = int(T0 + sec)
    frac = round((T0 + sec - whole) * 1e9)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(whole)) + f".{frac:09d}Z"


def line(sec: float, msg: str, actor: str = "", **kw) -> str:
    j = {"time": ts(sec), "level": "ERROR" if "failed" in msg else "INFO", "msg": msg}
    if actor:
        j["actor"] = actor
    j.update(kw)
    return parse.LOG_PREFIX + json.dumps(j)


def record(sec: float, actor: str = A, **kw) -> str:
    """A task record logged and ending at T0 + sec: a clean 4-cycle task by default."""
    j = {"time": ts(sec), "level": "INFO", "msg": parse.MSG_TASK_RECORD, "v": 1, "user": 1, "actor": actor,
         "template": "sweperf-astropy-7336", "cycles": 4, "steps": 21, "task_start": ts(sec - 30),
         "end": ts(sec), "startup_ms": 600.0, "cel_ms": 6000.0, "wall_ms": 8000.0, "think_ms": 18000.0,
         "resume_max_ms": 200.0, "retries": 0, "failed": False, "fail_step": "", "fail_cycle": 0,
         "ttfe_max_ms": 210.0}
    j.update(kw)
    return parse.LOG_PREFIX + json.dumps({k: v for k, v in j.items() if v is not None})


def run_log(logs: list[str]) -> parse.RunLog:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "logs.txt"
        path.write_text("\n".join(logs) + "\n", encoding="utf-8")
        return parse.read_run(path)


def task(**kw) -> classify.Task:
    """A finished task from a record with the given fields."""
    rec = parse.parse_record(record(10, **kw))
    return classify.Task(actor=rec.actor, record=rec, end=rec.end)


def write_run(d: Path, logs: list[str], meta: dict = None, trial: dict = None, stats: list = None) -> Path:
    """A stand-in run directory: logs.txt, stats.jsonl (trial_summary plus stats) and server_summary.json."""
    d.mkdir(parents=True, exist_ok=True)
    (d / "logs.txt").write_text("\n".join(logs) + "\n", encoding="utf-8")
    if trial is None:
        trial = {"machine_type": "c3-highcpu-88", "node_count": "2", "allocatable_cores": "100",
                 "allocatable_ram_gb": "400"}
    rows = [{"timestamp": "2026-10-11T00:00:00Z", "tag": "abc", "test_name": "sweperf_test",
             "metric": "trial_summary", "measurements": trial}] + (stats or [])
    (d / "stats.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    if meta is not None:
        (d / "server_summary.json").write_text(json.dumps({"metadata": meta}), encoding="utf-8")
    return d


class ParseTest(unittest.TestCase):
    def test_real_lines(self):
        ev = {k: parse.parse_line(v) for k, v in REAL.items()}
        self.assertEqual((ev["resume_fail"].msg, ev["resume_fail"].cycle), (parse.MSG_RESUME_FAIL, 3))
        self.assertAlmostEqual(ev["resume_fail"].t, parse.parse_time("2026-10-11T07:02:29.268099898Z"))
        self.assertEqual((ev["exec_fail"].msg, ev["exec_fail"].cycle), (parse.MSG_EXEC_FAIL, 3))
        self.assertEqual((ev["suspend_fail"].msg, ev["suspend_fail"].cycle), (parse.MSG_SUSPEND_FAIL, None))
        self.assertEqual((ev["onstart_fail"].user, ev["onstart_fail"].actor),
                         (81, "sb-a2b7fb28-ce3b-4f2b-a87a-a3f22949df7f"))
        self.assertEqual(ev["quit"].msg, parse.MSG_BOOMER_QUIT)
        for skipped in ("start", "resume_err", "traced_span", "record_gvisor"):
            self.assertIsNone(ev[skipped], skipped)

    def test_real_records(self):
        g = parse.parse_record(REAL["record_gvisor"])
        self.assertEqual((g.actor, g.user, g.template, g.cycles, g.steps),
                         ("sb-a597c5ed-9f70-47cb-96c6-67e267d7dd7a", 372, "sweperf-astropy-7336", 4, 21))
        self.assertEqual((g.wall_ms, g.cel_ms, g.resume_max_ms, g.ttfe_max_ms),
                         (12584.251139, 10301.58, 208.538, 216.560976))
        self.assertEqual((g.failed, g.fail_step, g.fail_cycle, g.retries), (False, "", None, 0))
        self.assertAlmostEqual(g.t, parse.parse_time("2026-10-18T02:29:42.61358281Z"))
        self.assertAlmostEqual(g.end, parse.parse_time("2026-10-18T02:29:42.613576352Z"))
        self.assertAlmostEqual(g.task_start, parse.parse_time("2026-10-18T02:28:49.610791521Z"))
        m = parse.parse_record(REAL["record_microvm"])
        self.assertEqual((m.resume_max_ms, m.ttfe_max_ms), (7232.807, 7240.629771))

    def test_failed_record_without_ttfe(self):
        r = parse.parse_record(record(5, failed=True, fail_step="suspend", fail_cycle=3, retries=2, ttfe_max_ms=None))
        self.assertEqual((r.failed, r.fail_step, r.fail_cycle, r.retries, r.ttfe_max_ms), (True, "suspend", 3, 2, None))

    def test_missing_optional_fields_are_none(self):
        missing = ("cycles", "steps", "startup_ms", "think_ms", "retries", "ttfe_max_ms")
        r = parse.parse_record(record(5, **dict.fromkeys(missing)))
        self.assertEqual([getattr(r, f) for f in missing], [None] * len(missing))
        z = parse.parse_record(record(5, cycles=0, steps=0, startup_ms=0, think_ms=0, retries=0, ttfe_max_ms=0))
        self.assertEqual([getattr(z, f) for f in missing], [0] * len(missing))

    def test_unusable_records_raise(self):
        for bad in (record(5, v=2), record(5, actor=""), record(5, wall_ms="12s"), record(5, failed="no"),
                    record(5, end=None), record(5, time="yesterday")):
            with self.assertRaises(ValueError, msg=bad):
                parse.parse_record(bad)

    def test_read_run(self):
        run = run_log([REAL["start"], record(5), record(6, v=2), REAL["resume_fail"], REAL["onstart_fail"],
                       REAL["traced_span"], line(9, parse.MSG_SHUTDOWN), record(10, actor=B), REAL["quit"]])
        self.assertEqual([r.actor for r in run.records], [A, B])
        self.assertEqual(run.skipped_records, 1)
        self.assertEqual([e.msg for e in run.events],
                         [parse.MSG_RESUME_FAIL, ONSTART, parse.MSG_SHUTDOWN, parse.MSG_BOOMER_QUIT])

    def test_parse_time(self):
        self.assertAlmostEqual(parse.parse_time("2026-10-11T06:57:35.684909393Z"), 1791701855.684909393, places=6)
        self.assertEqual(parse.parse_time("2026-10-11T06:57:35Z"), 1791701855.0)
        with self.assertRaises(ValueError):
            parse.parse_time("2026-10-11T06:57:35+02:00")

    def test_rejects_other_lines(self):
        for bad in ("[locust-master] SuspendActor failed", "[boomer] {not json", "[boomer] [1, 2]",
                    '[boomer] {"msg": "SuspendActor failed"}',
                    '[boomer] {"time": 5, "msg": "sweperf shutdown started"}'):
            self.assertIsNone(parse.parse_line(bad), bad)

    def test_epoch(self):
        self.assertEqual(parse.epoch("1791700000"), 1791700000.0)
        self.assertEqual(parse.epoch("2026-10-11T00:00:00Z"), parse.parse_time("2026-10-11T00:00:00Z"))
        self.assertIsNone(parse.epoch(None))
        self.assertIsNone(parse.epoch("soon"))


class TaskTest(unittest.TestCase):
    def test_build_tasks(self):
        run = run_log([
            line(3, ONSTART, user=7),
            line(5, parse.MSG_RESUME_FAIL, B, cycle=2),  # B's resume fails and is retried; B finishes
            line(8, parse.MSG_RESUME_FAIL, C, cycle=2),  # C fails twice and never finishes
            line(9, parse.MSG_EXEC_FAIL, C, cycle=3),
            record(10, A),
            record(12, B, failed=True, fail_step="resume", fail_cycle=2, retries=1),
            line(20, parse.MSG_SHUTDOWN),
            record(20.5, E),  # last cycle done before the cut-off, record logged just after it
            line(22, ONSTART, user=8),
            line(25, parse.MSG_SUSPEND_FAIL, D),  # teardown
        ])
        cut, source = classify.cutoff(run.events, None, None)
        self.assertEqual((cut, source), (T0 + 20, "shutdown_marker"))
        tasks, startup = classify.build_tasks(run, cut)
        self.assertEqual([(t.actor, t.finished) for t in tasks], [(C, False), (A, True), (B, True), (E, True)])
        unfinished = tasks[0]
        self.assertEqual((unfinished.fail_step, unfinished.fail_cycle, unfinished.first_failure_t),
                         ("resume", 2, T0 + 8))
        self.assertEqual(classify.classify(unfinished, RELAXED, None), (classify.BAD, [classify.REASON_RESUME]))
        self.assertEqual((tasks[2].fail_step, tasks[2].fail_cycle, tasks[2].failures), ("resume", 2, []))
        self.assertEqual(tasks[3].end, T0 + 20)
        self.assertEqual([s.user for s in startup], [7])

    def test_startup_episodes(self):
        startup = [parse.Event(t=T0 + s, msg=ONSTART, user=u) for s, u in
                   ((1, 7), (2, 7), (3, 7), (4, 8), (5, None), (6, None), (12, 7))]
        records = [parse.parse_record(record(10, user=7))]
        self.assertEqual(classify.startup_episodes(startup, records), [T0 + 1, T0 + 4, T0 + 5, T0 + 6, T0 + 12])

    def test_cutoff_order(self):
        both = [parse.Event(t=T0 + 20, msg=parse.MSG_BOOMER_QUIT), parse.Event(t=T0 + 30, msg=parse.MSG_SHUTDOWN)]
        self.assertEqual(classify.cutoff(both, T0 + 5, T0 + 6), (T0 + 30, "shutdown_marker"))
        self.assertEqual(classify.cutoff(both[:1], T0 + 5, T0 + 6), (T0 + 20, "boomer_quit"))
        self.assertEqual(classify.cutoff([], T0 + 5, T0 + 6), (T0 + 5, "run_end"))
        self.assertEqual(classify.cutoff([], None, T0 + 6), (T0 + 6, "end_ts"))
        self.assertEqual(classify.cutoff([], None, None), (None, "none"))


class CheckTest(unittest.TestCase):
    REFS = {"cel_ms": 4000.0, "wall_ms": 5000.0}  # the default task: cel_ms 6000 (1.5x), wall_ms 8000 (1.6x)

    def test_g1_from_the_record(self):
        self.assertEqual(classify.classify(task(failed=True, fail_step="suspend", fail_cycle=3), RELAXED, None),
                         (classify.BAD, [classify.REASON_SUSPEND]))
        self.assertEqual(classify.classify(task(failed=True), RELAXED, None), (classify.BAD, [classify.REASON_FAILED]))
        slow_and_failed = task(failed=True, fail_step="execute", resume_max_ms=9000.0)
        self.assertEqual(classify.classify(slow_and_failed, STRICT, self.REFS),
                         (classify.BAD, [classify.REASON_EXECUTE]))

    def test_limit_checks(self):
        slow = task(resume_max_ms=1600.0, ttfe_max_ms=1100.0)
        self.assertEqual(classify.classify(slow, STRICT, None), (classify.BAD, ["slow_resume", "slow_ttfe"]))
        self.assertEqual(classify.classify(slow, RELAXED, None), (classify.GOOD, []))
        self.assertEqual(classify.classify(task(resume_max_ms=1500.0, ttfe_max_ms=None), STRICT, None),
                         (classify.GOOD, []))

    def test_ops(self):
        def status(op: str, retries: int) -> str:
            mode = {"checks": [{"field": "retries", "op": op, "limit": 2, "reason": "r"}]}
            return classify.classify(task(retries=retries), mode, None)[0]
        good, bad = classify.GOOD, classify.BAD
        want = {"<=": (good, good, bad), "<": (good, bad, bad), ">=": (bad, good, good), ">": (bad, bad, good)}
        for op, statuses in want.items():
            self.assertEqual(tuple(status(op, n) for n in (1, 2, 3)), statuses, op)

    def test_limit_of_zero(self):
        mode = {"checks": [{"field": "retries", "op": "<=", "limit": 0, "reason": "retried"}]}
        self.assertEqual(classify.classify(task(retries=0), mode, None), (classify.GOOD, []))
        self.assertEqual(classify.classify(task(retries=1), mode, None), (classify.BAD, ["retried"]))

    def test_baseline_ratio_checks(self):
        self.assertEqual(classify.classify(task(), STRICT, None), (classify.GOOD, []))
        self.assertEqual(classify.classify(task(), STRICT, self.REFS), (classify.BAD, ["slow_wall"]))
        self.assertEqual(classify.classify(task(), RELAXED, self.REFS), (classify.GOOD, []))
        self.assertEqual(classify.classify(task(cel_ms=6001.0), STRICT, self.REFS),
                         (classify.BAD, ["slow_cel", "slow_wall"]))

    def test_every_failing_check_is_a_reason(self):
        t = task(ttfe_max_ms=1200.0, cel_ms=7000.0)
        self.assertEqual(classify.classify(t, STRICT, self.REFS),
                         (classify.BAD, ["slow_ttfe", "slow_cel", "slow_wall"]))

    def test_baseline_ratio_checks_skip_without_a_usable_ref(self):
        for refs in (None, {}, {"cel_ms": 0, "wall_ms": -1}, {"retries": 5}):
            self.assertEqual(classify.classify(task(), STRICT, refs), (classify.GOOD, []), refs)
            self.assertEqual(classify.split_checks(STRICT, refs),
                             (["slow_resume", "slow_ttfe"], ["slow_cel", "slow_wall"]), refs)
            self.assertEqual(classify.ratio_fields(STRICT, refs), [], refs)
        self.assertEqual(classify.split_checks(STRICT, self.REFS),
                         (["slow_resume", "slow_ttfe", "slow_cel", "slow_wall"], []))
        self.assertEqual(classify.ratio_fields(STRICT, {"wall_ms": 5000.0}), ["wall_ms"])

    def test_limit_check_applies_when_no_task_has_the_value(self):
        mode = {"checks": [{"field": "ttfe_max_ms", "op": "<=", "limit": 1000, "reason": "slow_ttfe"}]}
        self.assertEqual(classify.classify(task(ttfe_max_ms=None), mode, None), (classify.GOOD, []))
        self.assertEqual(classify.split_checks(mode, None), (["slow_ttfe"], []))

    def test_failures_only_mode(self):
        mode = {"checks": []}
        self.assertEqual(classify.classify(task(resume_max_ms=9000.0), mode, None), (classify.GOOD, []))
        self.assertEqual(classify.classify(task(failed=True, fail_step="resume"), mode, None),
                         (classify.BAD, [classify.REASON_RESUME]))
        self.assertEqual(classify.split_checks(mode, None), ([], []))


class ConfigTest(unittest.TestCase):
    @staticmethod
    def config(*checks: dict, **top) -> dict:
        return {**top, "modes": {"m": {"checks": list(checks)}}}

    @staticmethod
    def check(**kw) -> dict:
        c = {"field": "cel_ms", "op": "<=", "limit": 1, "reason": "x"}
        c.update(kw)
        return {k: v for k, v in c.items() if v is not None}

    def test_valid(self):
        classify.validate_config(CONFIG)
        classify.validate_config(self.config())  # no version; failures only
        classify.validate_config(self.config(self.check(field="retries", limit=-1), version=2))

    def test_invalid(self):
        c = self.check
        cases = {
            "unknown keys": {"version": 1, "modes": {"strict": {"g2": True, "s_max": 1.5}}, "baselines": []},
            "version 1": self.config(c(), version=1),
            "version '2'": self.config(c(), version="2"),
            "'modes'": {"version": 2},
            "only 'checks'": {"modes": {"m": {"g2": True, "checks": []}}},
            "'checks' list": {"modes": {"m": {}}},
            "unknown keys ['extra']": self.config(c(extra=1)),
            "field 'fail_cycle'": self.config(c(field="fail_cycle")),
            "field 'failed'": self.config(c(field="failed")),
            "op '=='": self.config(c(op="==")),
            "exactly one": self.config(c(baseline_ratio=2)),
            "exactly one of": self.config(c(limit=None)),
            "limit must be a number": self.config(c(limit=True)),
            "limit must be": self.config(c(limit="1")),
            "baseline_ratio must be a positive number": self.config(c(limit=None, baseline_ratio=0)),
            "baseline_ratio must be": self.config(c(limit=None, baseline_ratio=True)),
            "needs a 'reason'": self.config(c(reason=None)),
            "own reason": self.config(c(), c(field="wall_ms")),
            "name failures": self.config(c(reason="resume_failed")),
        }
        for message, cfg in cases.items():
            with self.assertRaisesRegex(ValueError, re.escape(message), msg=message):
                classify.validate_config(cfg)

    def test_load_config_names_the_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "old.json"
            path.write_text(json.dumps({"version": 1, "modes": {"strict": {"g2": True}}}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, re.escape(str(path))):
                goodput.load_config(path)


class MetricsTest(unittest.TestCase):
    def test_percentile(self):
        self.assertIsNone(metrics.percentile([], 50))
        self.assertEqual(metrics.percentile([5.0], 99), 5.0)
        self.assertEqual(metrics.percentile([1.0, 2.0, 3.0, 4.0], 50), 2.5)

    def test_window_membership_and_rates(self):
        def row(status, end=None, ff=None, reasons=(), wall=1000.0):
            return {"status": status, "end": end, "first_failure_t": ff, "reasons": list(reasons), "wall_ms": wall}
        rows = [row(classify.GOOD, end=100), row(classify.GOOD, end=200), row(classify.GOOD, end=4000),
                row(classify.BAD, end=300, reasons=["slow_resume"]),
                row(classify.BAD, ff=400, reasons=[classify.REASON_EXECUTE, classify.REASON_SUSPEND]),
                row(classify.BAD, ff=5000, reasons=[classify.REASON_RESUME])]
        m = metrics.window_metrics(rows, [50, 60, 6000], [50, 6000], (0, 3600),
                                   {"node_count": 2, "allocatable_cores": 100, "allocatable_ram_gb": 400}, {}, [])
        self.assertEqual((m["tasks_done"], m["tasks_good"], m["tasks_bad"], m["tasks_unfinished_bad"]), (3, 2, 1, 1))
        # the unfinished task whose first failure is outside the window is not counted
        self.assertEqual(m["bad_reasons"], {"execute_failed": 1, "slow_resume": 1, "suspend_failed": 1})
        self.assertEqual(m["good_tasks_per_hr"], 2.0)
        self.assertEqual((m["good_tasks_per_node_hr"], m["good_tasks_per_vcpu_hr"], m["good_tasks_per_gb_ram_hr"]),
                         (1.0, 0.02, 0.005))
        self.assertEqual(m["goodput_ratio_pct"], 50.0)  # 2 good / (3 done + 1 unfinished bad)
        self.assertEqual((m["startup_failure_attempts"], m["startup_failure_episodes"]), (2, 1))
        self.assertNotIn("eta_infra", m)

    def test_baseline_ratios(self):
        rows = [{"status": classify.GOOD, "end": 10, "first_failure_t": None, "reasons": [], "wall_ms": w,
                 "values": {"wall_ms": w, "cel_ms": c}} for w, c in ((10000.0, 6000.0), (20000.0, None))]
        refs = {"wall_ms": 10000.0, "cel_ms": 7500.0}
        m = metrics.window_metrics(rows, [], [], (0, 100), {}, refs, ["wall_ms", "cel_ms"])
        self.assertEqual((m["wall_ms_ratio_p50"], m["cel_ms_ratio_p50"]), (1.5, 0.8))  # cel_ms: one task has it
        self.assertEqual(m["eta_infra"], 0.5)  # 7500 / 15000
        self.assertNotIn("resume_max_ms_ratio_p50", m)
        m = metrics.window_metrics(rows, [], [], (0, 100), {}, {"cel_ms": 0}, [])
        self.assertNotIn("eta_infra", m)
        self.assertNotIn("wall_ms_ratio_p50", m)

    def test_empty_window(self):
        m = metrics.window_metrics([], [], [], (10, 10), {}, {}, [])
        self.assertIsNone(m["good_tasks_per_hr"])
        self.assertIsNone(m["goodput_ratio_pct"])


class ComputeTest(unittest.TestCase):
    def test_steady_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            meta = {"start_ts": T0, "end_ts": T0 + 100, "steady_start_ts": T0 + 10, "steady_end_ts": T0 + 90}
            self.assertEqual(goodput.steady_window(meta, d, [], None), ((T0 + 10, T0 + 90), "server_summary"))
            self.assertEqual(goodput.steady_window(meta, d, [], T0 + 80), ((T0 + 10, T0 + 80), "server_summary"))
            self.assertEqual(goodput.steady_window(meta, d, [], T0 + 5), ((T0 + 10, T0 + 90), "server_summary"))
            hist = "Timestamp,User Count,Name\n" + "".join(
                f"{int(T0) + i},{u},Aggregated\n" for i, u in ((0, 1), (20, 9), (30, 10), (80, 10), (95, 2)))
            (d / "stats_history.csv").write_text(hist, encoding="utf-8")
            meta2 = {"start_ts": T0, "end_ts": T0 + 100}
            self.assertEqual(goodput.steady_window(meta2, d, [], None), ((T0 + 20, T0 + 80), "stats_history"))
            (d / "stats_history.csv").unlink()
            self.assertEqual(goodput.steady_window(meta2, d, [], T0 + 70), ((T0, T0 + 70), "run"))
            self.assertEqual(goodput.steady_window({}, d, [T0 + 3, T0 + 7, T0 + 50], T0 + 40),
                             ((T0 + 3, T0 + 7), "log"))

    def test_compute_end_to_end(self):
        """Three clean tasks, one with a slow resume, one that never finished, a startup
        failure, noise lines, and load stopping at the boomer quit line."""
        logs = ["[locust-master] a line the script ignores", REAL["traced_span"], line(3, ONSTART, user=9),
                record(11, A), record(11.5, B), record(15, C, resume_max_ms=2000.0), record(23, D),
                line(27.2, parse.MSG_RESUME_FAIL, E, cycle=2), line(30, parse.MSG_BOOMER_QUIT)]
        meta = {"test_name": "sweperf_test_uvm", "tag": "abc", "start_ts": T0, "end_ts": T0 + 40,
                "steady_start_ts": T0 + 5, "steady_end_ts": T0 + 30, "data_timestamp": "2026-10-11T00:00:00Z"}
        with tempfile.TemporaryDirectory() as tmp:
            r = goodput.compute(write_run(Path(tmp), logs, meta), CONFIG)
        self.assertEqual((r["run"]["runtime"], r["run"]["template"], r["run"]["node_count"]),
                         ("microvm", "sweperf-astropy-7336", 2.0))
        self.assertEqual(r["cutoff"], {"ts": T0 + 30, "source": "boomer_quit"})
        self.assertEqual(r["totals"], {"tasks_finished": 4, "tasks_unfinished_bad": 1, "startup_failure_attempts": 1,
                                       "startup_failure_episodes": 1, "skipped_records": 0, "actors": 5})
        strict, relaxed = r["modes"]["strict"], r["modes"]["relaxed"]
        self.assertEqual((r["version"], r["baseline"]),
                         (2, {"source": None, "identity": {}, "checked": [], "mismatch": {}, "refs": {}}))
        self.assertEqual((strict["checks_applied"], strict["checks_skipped"]),
                         (["slow_resume", "slow_ttfe"], ["slow_cel", "slow_wall"]))
        self.assertEqual((relaxed["checks_applied"], relaxed["checks_skipped"]), ([], ["slow_cel", "slow_wall"]))
        st = strict["steady"]
        self.assertEqual((st["tasks_done"], st["tasks_good"], st["tasks_bad"], st["tasks_unfinished_bad"]),
                         (4, 3, 1, 1))
        self.assertEqual(st["bad_reasons"], {"resume_failed": 1, "slow_resume": 1})
        self.assertEqual(st["goodput_ratio_pct"], 60.0)  # 3 good / (4 done + 1 unfinished bad)
        self.assertEqual(st["good_tasks_per_hr"], 432.0)  # 3 good in a 25 s window
        self.assertEqual(st["startup_failure_attempts"], 0)  # at 3 s, before the steady window
        self.assertEqual(strict["whole"]["startup_failure_episodes"], 1)
        self.assertEqual(relaxed["steady"]["goodput_ratio_pct"], 80.0)
        self.assertEqual(r["tasks"][-1]["status_by_mode"], {"relaxed": "bad", "strict": "bad"})
        rows = goodput.jsonl_rows(r)
        self.assertEqual([x["metric"] for x in rows], ["goodput_relaxed", "goodput_strict"])
        self.assertTrue(all(isinstance(v, str) for x in rows for v in x["measurements"].values()))
        self.assertEqual((rows[1]["measurements"]["tasks_good"], rows[1]["measurements"]["bad_slow_resume"],
                          rows[1]["measurements"]["whole_tasks_good"]), ("3", "1", "3"))
        self.assertEqual((rows[1]["measurements"]["checks_applied"], rows[1]["measurements"]["checks_skipped"],
                          rows[0]["measurements"]["checks_applied"], rows[1]["measurements"]["baseline_source"]),
                         ("slow_resume,slow_ttfe", "slow_cel,slow_wall", "", ""))

    def test_compute_needs_task_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "task record"):
                goodput.compute(write_run(Path(tmp), [REAL["start"], REAL["quit"]], {}), CONFIG)

    def test_compute_with_a_baseline(self):
        baseline = {"source": "file:base.json", "refs": {"wall_ms": 4000.0, "cel_ms": 6000.0},
                    "identity": {"template": "", "runtime": "", "machine_type": ""}}
        with tempfile.TemporaryDirectory() as tmp:
            r = goodput.compute(write_run(Path(tmp), [record(11), line(20, parse.MSG_BOOMER_QUIT)],
                                          {"start_ts": T0, "end_ts": T0 + 30}), CONFIG, baseline=baseline)
        strict, relaxed = r["modes"]["strict"], r["modes"]["relaxed"]
        self.assertEqual(r["baseline"], {**baseline, "checked": [], "mismatch": {}})
        self.assertEqual(strict["checks_skipped"], [])
        self.assertEqual(strict["whole"]["bad_reasons"], {"slow_wall": 1})  # wall 8000 / 4000 > 1.5; cel 1.0
        self.assertEqual((strict["whole"]["wall_ms_ratio_p50"], strict["whole"]["cel_ms_ratio_p50"]), (2.0, 1.0))
        self.assertEqual(relaxed["whole"]["tasks_good"], 1)  # 2.0 <= 2.0

    def test_identity_and_runtime(self):
        ident = goodput.identity_from_path(
            "gs://b/results/runs/sweperf_astropy_1_user/run_date=2026-10-11/run_ts=1791732523/run_tag=b73e/")
        self.assertEqual(ident, {"test_name": "sweperf_astropy_1_user", "run_date": "2026-10-11",
                                 "run_ts": "1791732523", "tag": "b73e"})
        self.assertEqual(goodput.identity_from_path("/tmp/x"), {})
        self.assertEqual(goodput.runtime_of("sweperf_astropy_1_user_uvm", {}), "microvm")
        self.assertEqual(goodput.runtime_of("sweperf_150u_microvm", {}), "microvm")
        self.assertEqual(goodput.runtime_of("sweperf_astropy_60_users", {}), "gvisor")
        self.assertEqual(goodput.runtime_of("x", {"sandbox_class": "microvm"}), "microvm")

    def test_cli_compute_writes_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp) / "run", [record(11)], {"start_ts": T0, "end_ts": T0 + 20})
            out = Path(tmp) / "out"
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(goodput.main(["compute", str(run), "--out", str(out)]), 0)
            self.assertEqual(json.loads((out / "goodput.json").read_text())["version"], 2)
            self.assertEqual(len((out / "goodput.jsonl").read_text().splitlines()), 2)

    def test_cli_baseline_file_and_baseline_run_agree(self):
        meta = {"start_ts": T0, "end_ts": T0 + 20}
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            run = write_run(d / "run", [record(11, A), record(12, B, wall_ms=6500.0)], meta)
            one_user = write_run(d / "one_user", [record(11, wall_ms=4000.0, cel_ms=6000.0)], meta)
            with contextlib.redirect_stdout(io.StringIO()) as printed:
                self.assertEqual(goodput.main(["baseline", str(one_user)]), 0)
            (d / "base.json").write_text(printed.getvalue(), encoding="utf-8")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(goodput.main(["compute", str(run), "--out", str(d / "a"),
                                               "--baseline-run", str(one_user)]), 0)
                self.assertEqual(goodput.main(["compute", str(run), "--out", str(d / "b"),
                                               "--baseline", str(d / "base.json")]), 0)
            a, b = (json.loads((d / x / "goodput.json").read_text()) for x in ("a", "b"))
        self.assertEqual((a["baseline"]["source"], b["baseline"]["source"]),
                         (f"run:{one_user}", f"file:{d / 'base.json'}"))
        self.assertEqual(a["baseline"]["refs"], b["baseline"]["refs"])
        self.assertEqual((a["modes"], a["tasks"]), (b["modes"], b["tasks"]))
        self.assertEqual([t["reasons_by_mode"]["strict"] for t in a["tasks"]], [["slow_wall"], ["slow_wall"]])
        self.assertEqual([t["status_by_mode"]["relaxed"] for t in a["tasks"]], ["good", "good"])

    def test_cli_baseline_flags(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp) / "run", [record(11)], {})
            err = io.StringIO()
            with contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as exit_:
                goodput.main(["compute", str(run), "--baseline", "b.json", "--baseline-run", str(run)])
            self.assertEqual(exit_.exception.code, 2)
            self.assertIn("not allowed with", err.getvalue())
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                self.assertEqual(goodput.main(["compute", str(run), "--baseline", str(Path(tmp) / "missing.json")]), 1)
            self.assertIn("baseline:", err.getvalue())


class BaselineTest(unittest.TestCase):
    def run_baseline(self, runs: list[Path]) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = goodput.main(["baseline", *map(str, runs)])
        return rc, out.getvalue(), err.getvalue()

    def test_refs_pool_the_clean_tasks_of_all_runs(self):
        meta = {"start_ts": T0, "end_ts": T0 + 100}
        failed = record(40, C, failed=True, fail_step="resume", fail_cycle=2, wall_ms=30000.0)
        task_cel = [{"timestamp": "", "tag": "abc", "test_name": "sweperf_test", "metric": "actor_TaskCEL",
                     "measurements": {"median_response_time": "9999", "request_count": "4"}}]
        with tempfile.TemporaryDirectory() as tmp:
            a = write_run(Path(tmp) / "a", [record(11, A), record(23, B, wall_ms=10000.0, ttfe_max_ms=None), failed],
                          meta, stats=task_cel)
            b = write_run(Path(tmp) / "b", [record(11, A, wall_ms=12000.0, startup_ms=None)], meta)
            rc, out, _ = self.run_baseline([a, b])
        self.assertEqual(rc, 0)
        j = json.loads(out)
        self.assertEqual((j["version"], j["runs"], j["tasks"], j["sources"]), (1, 2, 3, [str(a), str(b)]))
        refs = j["refs"]
        self.assertEqual(set(refs), set(classify.CHECK_FIELDS))
        self.assertEqual(refs["wall_ms"], 10000.0)  # median of 8000, 10000, 12000; the failed task is left out
        self.assertEqual(refs["cel_ms"], 6000.0)  # from the records, not actor_TaskCEL
        self.assertEqual((refs["ttfe_max_ms"], refs["startup_ms"]), (210.0, 600.0))  # tasks without it skipped
        self.assertEqual(refs["retries"], 0.0)

    def test_a_field_no_task_logged_is_left_out(self):
        with tempfile.TemporaryDirectory() as tmp:
            refs, n, _ = goodput.baseline_refs([(write_run(Path(tmp), [record(11, think_ms=None)], {}), {})])
        self.assertEqual(n, 1)
        self.assertNotIn("think_ms", refs)

    def test_tasks_are_built_like_compute(self):
        logs = [record(11, A), line(20, parse.MSG_SHUTDOWN), record(20.5, B, wall_ms=9000.0),
                line(25, parse.MSG_RESUME_FAIL, C, cycle=1)]
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), logs, {})
            refs, n, _ = goodput.baseline_refs([(run, {})])
            totals = goodput.compute(run, CONFIG)["totals"]
        self.assertEqual((n, totals["tasks_finished"], totals["tasks_unfinished_bad"]), (2, 2, 0))
        self.assertEqual(refs["wall_ms"], 8500.0)

    def test_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = write_run(Path(tmp) / "a", [record(11, failed=True, fail_step="execute", fail_cycle=1)], {})
            no_clean, missing = self.run_baseline([a]), self.run_baseline([Path(tmp) / "missing"])
        self.assertIn("no finished task", no_clean[2])
        for rc, _, err in (no_clean, missing):
            self.assertEqual(rc, 1)
            self.assertTrue(err.startswith("baseline: ") and err.count("\n") == 1, err)

    def test_load_baseline(self):
        ident = {"template": "t", "runtime": "microvm", "machine_type": ""}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "base.json"
            path.write_text(json.dumps({"version": 1, "refs": {"cel_ms": 5000, "wall_ms": 9000.5}, **ident}),
                            encoding="utf-8")
            self.assertEqual(goodput.load_baseline(path),
                             {"source": f"file:{path}", "refs": {"cel_ms": 5000, "wall_ms": 9000.5}, "identity": ident})
            for bad in ({"version": 2, "refs": {}, **ident}, {"version": 1, **ident},
                        {"version": 1, "refs": {"fail_cycle": 1}, **ident},
                        {"version": 1, "refs": {"cel_ms": True}, **ident},
                        {"version": 1, "refs": {"cel_ms": "5"}, **ident},
                        {"version": 1, "refs": {}, "template": "t", "runtime": "microvm"},
                        {"version": 1, "refs": {}, **ident, "machine_type": None}, []):
                path.write_text(json.dumps(bad), encoding="utf-8")
                with self.assertRaises(ValueError, msg=bad):
                    goodput.load_baseline(path)


GVISOR = {"machine_type": "c3-highcpu-88", "sandbox_class": "gvisor"}
MICROVM = {"machine_type": "c3-highcpu-88", "sandbox_class": "microvm"}
RUN_LAYOUT = "runs/{test}/run_date=2026-10-19/run_ts=1/run_tag=t"


class IdentityTest(unittest.TestCase):
    """The baseline records its runs' template, runtime and machine type, and is
    only used for a run that matches."""

    def baseline_file(self, d: Path, trial: dict) -> Path:
        one_user = write_run(d / "one_user", [record(11, wall_ms=4000.0)], {}, trial)
        with contextlib.redirect_stdout(io.StringIO()) as printed:
            self.assertEqual(goodput.main(["baseline", str(one_user)]), 0)
        (d / "base.json").write_text(printed.getvalue(), encoding="utf-8")
        return d / "base.json"

    def test_baseline_records_identity_and_round_trips(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.baseline_file(Path(tmp), MICROVM)
            j = json.loads(path.read_text())
            loaded = goodput.load_baseline(path)
        want = {"template": "sweperf-astropy-7336", "runtime": "microvm", "machine_type": "c3-highcpu-88"}
        self.assertEqual({k: j[k] for k in goodput.IDENTITY_KEYS}, want)
        self.assertEqual(loaded["identity"], want)

    def test_pooling_refuses_runs_that_differ(self):
        with tempfile.TemporaryDirectory() as tmp:
            g = write_run(Path(tmp) / "g", [record(11)], {}, GVISOR)
            m = write_run(Path(tmp) / "m", [record(11)], {}, {**MICROVM, "machine_type": "c3-highcpu-192-metal"})
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = goodput.main(["baseline", str(g), str(m)])
            out, err = out.getvalue(), err.getvalue()
        self.assertEqual((rc, out), (1, ""))
        self.assertEqual(err, "baseline: the baseline runs differ in runtime gvisor, microvm; "
                              "machine_type c3-highcpu-192-metal, c3-highcpu-88\n")

    def test_pooling_fills_unknown_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            known = write_run(Path(tmp) / "a", [record(11)], {}, MICROVM)
            unknown = write_run(Path(tmp) / "b", [record(11)], {}, {"sandbox_class": "microvm"})
            _, _, identity = goodput.baseline_refs([(known, {}), (unknown, {})])
        self.assertEqual(identity["machine_type"], "c3-highcpu-88")
        empty = {k: "" for k in goodput.IDENTITY_KEYS}
        self.assertEqual(goodput.pool_identity([empty, empty]), empty)

    def test_local_dir_and_gs_path_resolve_the_same_identity(self):
        # No sandbox_class, so the runtime comes from the test name in the path.
        test = "sweperf_astropy_1_user_uvm"
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp) / RUN_LAYOUT.format(test=test), [record(11)], {}, {"machine_type": "m"})
            local = goodput.baseline_from_runs([str(run)], Path(tmp))
            _, _, from_gs = goodput.baseline_refs([(run, goodput.identity_from_path(
                "gs://bucket/" + RUN_LAYOUT.format(test=test)))])
        self.assertEqual(local["identity"], from_gs)
        self.assertEqual(from_gs["runtime"], "microvm")

    def test_compute_exits_on_a_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            base = self.baseline_file(d, MICROVM)
            run = write_run(d / "run", [record(11)], {}, GVISOR)
            err = io.StringIO()
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
                rc = goodput.main(["compute", str(run), "--baseline", str(base), "--out", str(d / "out")])
            wrote = (d / "out").exists()
        self.assertEqual((rc, wrote), (1, False))
        self.assertTrue(err.getvalue().startswith("baseline: doesn't match the run (runtime:gvisor!=microvm)"))
        self.assertEqual(err.getvalue().count("\n"), 1)

    def test_backfill_keeps_a_mismatched_run_without_its_ratio_checks(self):
        gs_dir = "gs://bucket/" + RUN_LAYOUT.format(test="sweperf_150u_gvisor")
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            baseline = goodput.load_baseline(self.baseline_file(d, MICROVM))
            run = write_run(d / "run", [record(11)], {}, GVISOR)
            with mock.patch.object(goodput, "fetch_run", lambda gs, dest: run):
                row = goodput.backfill_one(gs_dir, CONFIG, d / "out", baseline)
            out = d / "out" / "sweperf_150u_gvisor" / "run_date=2026-10-19" / "run_ts=1" / "run_tag=t"
            result = json.loads((out / "goodput.json").read_text())
            jsonl = [json.loads(x) for x in (out / "goodput.jsonl").read_text().splitlines()]
        self.assertEqual((row["ok"], row["baseline_mismatch"]), (True, "runtime:gvisor!=microvm"))
        self.assertIn("strict (baseline skipped: runtime:gvisor!=microvm; slow_cel, slow_wall skipped): good",
                      row["line"])
        self.assertEqual(result["baseline"]["mismatch"], {"runtime": ["gvisor", "microvm"]})
        self.assertEqual(result["baseline"]["refs"], {})
        self.assertEqual(result["modes"]["strict"]["checks_skipped"], ["slow_cel", "slow_wall"])
        self.assertEqual({r["measurements"]["baseline_mismatch"] for r in jsonl}, {"runtime:gvisor!=microvm"})

    def test_values_one_side_lacks_are_not_compared(self):
        baseline = {"source": "test", "refs": {"wall_ms": 4000.0},
                    "identity": {"template": "sweperf-astropy-7336", "runtime": "gvisor", "machine_type": "c3"}}
        with tempfile.TemporaryDirectory() as tmp:
            r = goodput.compute(write_run(Path(tmp), [record(11)], {}, {"sandbox_class": "gvisor"}), CONFIG,
                                baseline=baseline)
        self.assertEqual((r["baseline"]["checked"], r["baseline"]["mismatch"]), (["template", "runtime"], {}))
        self.assertEqual(r["baseline"]["refs"], {"wall_ms": 4000.0})
        self.assertEqual(goodput.jsonl_rows(r)[0]["measurements"]["baseline_mismatch"], "")

    def test_a_mixed_runtime_does_not_match_a_single_one(self):
        baseline = {"source": "test", "refs": {"wall_ms": 4000.0},
                    "identity": {"template": "", "runtime": "microvm", "machine_type": ""}}
        with tempfile.TemporaryDirectory() as tmp:
            r = goodput.compute(write_run(Path(tmp), [record(11)], {}, {"sandbox_class": "gvisor,microvm"}), CONFIG,
                                baseline=baseline)
        self.assertEqual(r["baseline"]["mismatch"], {"runtime": ["gvisor,microvm", "microvm"]})

    def test_summary_line_names_skipped_checks(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            run = write_run(d / "run", [record(11)], {}, MICROVM)
            plain = goodput.summary_line(goodput.compute(run, CONFIG))
            matched = goodput.summary_line(goodput.compute(run, CONFIG, baseline=goodput.load_baseline(
                self.baseline_file(d, MICROVM))))
            partial = goodput.summary_line(goodput.compute(run, CONFIG, baseline={
                "source": "test", "refs": {"wall_ms": 4000.0}, "identity": {}}))
            no_refs = goodput.summary_line(goodput.compute(run, CONFIG, baseline={
                "source": "test", "refs": {}, "identity": {}}))
        self.assertIn("relaxed (no baseline; slow_cel, slow_wall skipped): good", plain)
        self.assertIn("strict (no baseline; slow_cel, slow_wall skipped): good", plain)
        self.assertIn("| relaxed: good", matched)
        self.assertIn("| strict: good", matched)
        self.assertIn("strict (no baseline value; slow_cel skipped): good", partial)
        self.assertIn("strict (no baseline value; slow_cel, slow_wall skipped): good", no_refs)


if __name__ == "__main__":
    unittest.main()
