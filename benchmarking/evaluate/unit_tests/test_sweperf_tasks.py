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

"""Unit tests for the cel source with sweperf_tasks records.

Never contacts GCS or a cluster; runs are stand-in directories built by the
tests, with copies of real task log lines.
"""

import dataclasses
import json
import re
import sys
import tempfile
import time
import unittest
import unittest.mock
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evaluate.sources import cel  # noqa: E402
from evaluate.sources.base import BenchmarkRunContext, SourceInvalid, valid_name  # noqa: E402
from evaluate.sources.cel import rules as cel_rules  # noqa: E402
from evaluate.sources.cel import sweperf  # noqa: E402
from evaluate.sources.cel.sweperf import metrics, parse, tasks  # noqa: E402


def rule(name: str, cel: str) -> dict:
    return {"name": name, "cel": cel}


STRICT = [
    rule("slow_resume", "task.resume_max_ms <= 1500.0"),
    rule("slow_ttfe", "task.ttfe_max_ms <= 1000.0"),
    rule("slow_cel", "task.cel_ms <= 1.5 * baseline.cel_ms"),
    rule("slow_wall", "task.wall_ms <= 1.5 * baseline.wall_ms"),
]
RELAXED = [
    rule("slow_cel", "task.cel_ms <= 2.0 * baseline.cel_ms"),
    rule("slow_wall", "task.wall_ms <= 2.0 * baseline.wall_ms"),
]
# The goodput values, as the README defines them.
VALUES = {
    "good_ratio_pct": "round(100.0 * good / records, 2)",
    "good_per_hour": "round(good / window_hours, 3)",
    "good_per_node_hour": "round(good / window_hours / nodes, 4)",
    "good_per_vcpu_hour": "round(good / window_hours / vcpus, 4)",
    "good_per_gb_ram_hour": "round(good / window_hours / gb_ram, 4)",
}

T0 = 1791700000.0  # an arbitrary run start (unix seconds)
A, B, C, D, E = (f"sb-{i}1111111-2222-3333-4444-555555555555" for i in range(5))
ONSTART = "sweperf on_start failed; goroutine will retry next iter"

# Real task log lines from sweperf runs: the ones the source reads, plus three
# it skips (a cycle start, the error-only resume line and a traced span).
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
    # The async lifecycle mode (the default) names its RPC.
    "suspend_async_fail": '[boomer] {"time":"2026-10-30T09:12:01.5Z","level":"ERROR","msg":"SuspendActorAsync '
                          'failed","actor":"sb-7fe17aa4-b5bb-490d-898d-2310443554fe","err":"rpc error: code = '
                          'Unavailable desc = connection refused"}',
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


def record(sec: float, actor: str = A, msg: str = parse.MSG_TASK_RECORD_SWEPERF, **kw) -> str:
    """A task record logged and ending at T0 + sec: a clean 4-cycle task by default."""
    j = {"time": ts(sec), "level": "INFO", "msg": msg, "v": 1, "user": 1, "actor": actor,
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


def task(**kw) -> tasks.Task:
    """A finished task from a record with the given fields."""
    rec = parse.parse_record(record(10, **kw))
    return tasks.Task(actor=rec.actor, record=rec, end=rec.end)


def compile_rules(rules: list[dict]) -> list[cel_rules.Rule]:
    return cel_rules.compile_rules(rules, sweperf.ENV, sweperf.SweperfTasks.FAILURE_REASONS)


def judge(t: tasks.Task, rules: list[dict], refs: Optional[dict] = None) -> list[str]:
    """Why t is bad under rules, with baseline refs; [] when it is good."""
    return cel.judge(sweperf.record(t), compile_rules(rules), sweperf.run_data(refs or {}))


def split(compiled: list[cel_rules.Rule], refs: Optional[dict]) -> tuple[list[str], list[str]]:
    """The names of the rules that apply with baseline refs, and of the skipped ones."""
    skipped = cel.skipped_rules(compiled, sweperf.SweperfTasks, sweperf.run_data(refs or {}))
    return [r.name for r in compiled if r.name not in skipped], skipped


def ratio_fields(compiled: list[cel_rules.Rule], refs: Optional[dict]) -> list[str]:
    """The ratio fields of the rules that apply with baseline refs."""
    applied = split(compiled, refs)[0]
    return sweperf.ratio_fields([r for r in compiled if r.name in applied])


def write_run(d: Path, logs: list[str], meta: dict = None, trial: dict = None, stats: list = None) -> BenchmarkRunContext:
    """A stand-in run: logs.txt, stats.jsonl (trial_summary plus stats) and server_summary.json."""
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
    return BenchmarkRunContext(files={name: d / name for name in sweperf.INPUTS}, label=str(d))


def stats_row(metric: str, **measurements) -> dict:
    return {"timestamp": "", "tag": "abc", "test_name": "sweperf_test", "metric": metric,
            "measurements": measurements}


def pair(run: BenchmarkRunContext, baseline: Optional[BenchmarkRunContext] = None, why: str = "no run of t1 with tag abc") -> BenchmarkRunContext:
    """run, paired with baseline whatever the test (with no run, and why, when baseline is None)."""
    def paired_run(test: str) -> BenchmarkRunContext:
        if baseline is None:
            raise LookupError(why)
        return baseline
    return dataclasses.replace(run, paired_run=paired_run)


def query(run: BenchmarkRunContext, cfg: dict) -> tuple[dict, Optional[str]]:
    """One sweperf_tasks query of run through the hooks, with VALUES unless cfg has its own: the values, and why
    the run is invalid (None when valid)."""
    cfg = {"records": "sweperf_tasks", "values": VALUES, **cfg}
    cel.Cel.validate(cfg)
    source = cel.Cel()
    source.init(run)
    try:
        return dict(source.query(cfg)), None
    except SourceInvalid as e:
        return e.values, e.reason


class ParseTest(unittest.TestCase):
    def test_real_lines(self):
        ev = {k: parse.parse_line(v) for k, v in REAL.items()}
        self.assertEqual((ev["resume_fail"].msg, ev["resume_fail"].cycle), (parse.MSG_RESUME_FAIL, 3))
        self.assertAlmostEqual(ev["resume_fail"].t, parse.parse_time("2026-10-11T07:02:29.268099898Z"))
        self.assertEqual((ev["exec_fail"].msg, ev["exec_fail"].cycle), (parse.MSG_EXEC_FAIL, 3))
        self.assertEqual((ev["suspend_fail"].msg, ev["suspend_fail"].cycle), (parse.MSG_SUSPEND_FAIL, None))
        self.assertEqual(ev["suspend_async_fail"].msg, parse.MSG_SUSPEND_ASYNC_FAIL)
        self.assertEqual((ev["onstart_fail"].user, ev["onstart_fail"].actor),
                         (81, "sb-a2b7fb28-ce3b-4f2b-a87a-a3f22949df7f"))
        self.assertEqual(ev["quit"].msg, parse.MSG_BOOMER_QUIT)
        for skipped in ("start", "resume_err", "traced_span", "record_gvisor"):
            self.assertIsNone(ev[skipped], skipped)

    def test_real_records(self):
        g = parse.parse_record(REAL["record_gvisor"])
        self.assertEqual((g.actor, g.user, g.template, g.cycles, g.steps, g.workload),
                         ("sb-a597c5ed-9f70-47cb-96c6-67e267d7dd7a", 372, "sweperf-astropy-7336", 4, 21, "sweperf"))
        self.assertEqual((g.wall_ms, g.cel_ms, g.resume_max_ms, g.ttfe_max_ms),
                         (12584.251139, 10301.58, 208.538, 216.560976))
        self.assertEqual((g.failed, g.fail_step, g.fail_cycle, g.retries), (False, "", None, 0))
        self.assertAlmostEqual(g.t, parse.parse_time("2026-10-18T02:29:42.61358281Z"))
        self.assertAlmostEqual(g.end, parse.parse_time("2026-10-18T02:29:42.613576352Z"))
        self.assertAlmostEqual(g.task_start, parse.parse_time("2026-10-18T02:28:49.610791521Z"))
        m = parse.parse_record(REAL["record_microvm"])
        self.assertEqual((m.resume_max_ms, m.ttfe_max_ms), (7232.807, 7240.629771))

    def test_neutral_record_names_its_workload(self):
        r = parse.parse_record(record(5, msg=parse.MSG_TASK_RECORD, workload="agentsession"))
        self.assertEqual((r.workload, r.wall_ms), ("agentsession", 8000.0))
        self.assertEqual(parse.parse_record(record(5, msg=parse.MSG_TASK_RECORD)).workload, "")
        # A sweperf task record is sweperf's, whatever else the line carries.
        self.assertEqual(parse.parse_record(record(5, workload="other")).workload, "sweperf")
        self.assertIsNone(parse.parse_record(record(5, msg="agent task record")))

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
                    record(5, end=None), record(5, time="yesterday"),
                    record(5, msg=parse.MSG_TASK_RECORD, workload="x", v=None)):
            with self.assertRaises(ValueError, msg=bad):
                parse.parse_record(bad)

    def test_read_run(self):
        run = run_log(["==== Run config ====", "  extra flags:", "    --lifecycle-mode sync",
                       REAL["start"], record(5), record(6, v=2), REAL["resume_fail"], REAL["onstart_fail"],
                       REAL["traced_span"], line(9, parse.MSG_SHUTDOWN),
                       record(10, actor=B, msg=parse.MSG_TASK_RECORD, workload="sweperf"), REAL["quit"]])
        self.assertEqual([r.actor for r in run.records], [A, B])
        self.assertEqual(run.skipped_records, 1)
        self.assertEqual(run.lifecycle_flag, "sync")
        self.assertEqual([e.msg for e in run.events],
                         [parse.MSG_RESUME_FAIL, ONSTART, parse.MSG_SHUTDOWN, parse.MSG_BOOMER_QUIT])

    def test_lifecycle_mode(self):
        rows = {m: {"metric": m} for m in ("grpc_SuspendActorAsync", "grpc_ResumeActor")}
        self.assertEqual(parse.lifecycle_mode(rows, "sync"), ("async", "stats"))  # what ran beats the flag
        self.assertEqual(parse.lifecycle_mode({"grpc_SuspendActor": {}}, ""), ("sync", "stats"))
        self.assertEqual(parse.lifecycle_mode({"grpc_SuspendActor": {}, "grpc_SuspendActorAsync": {}}, ""),
                         ("mixed", "stats"))
        self.assertEqual(parse.lifecycle_mode({}, "async"), ("async", "flag"))
        self.assertEqual(parse.lifecycle_mode({}, "none"), ("", "none"))  # resident actors: no suspend at all
        self.assertEqual(parse.lifecycle_mode({}, ""), ("", "none"))
        # Only the run config counts: the flag is read up to its end line.
        config = "==== Run config ====\n  extra flags:\n    --lifecycle-mode=sync\n====================\n"
        self.assertEqual(run_log([config + "    --lifecycle-mode async"]).lifecycle_flag, "sync")
        self.assertEqual(run_log(["==== Run config ====\n====================\n    --lifecycle-mode async"])
                         .lifecycle_flag, "")

    def test_non_string_msg_is_not_a_record(self):
        self.assertIsNone(parse.parse_record('[boomer] {"msg": ["task record"], "v": 1}'))
        self.assertIsNone(parse.parse_line('[boomer] {"msg": {"x": 1}, "time": "2026-10-11T00:00:00Z"}'))

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
            line(25, parse.MSG_SUSPEND_ASYNC_FAIL, D),  # teardown
        ])
        cut, source = tasks.cutoff(run.events, None, None)
        self.assertEqual((cut, source), (T0 + 20, "shutdown_marker"))
        built, startup = tasks.build_tasks(run, cut)
        self.assertEqual([(t.actor, t.finished) for t in built], [(C, False), (A, True), (B, True), (E, True)])
        unfinished = built[0]
        self.assertEqual((unfinished.fail_step, unfinished.fail_cycle, unfinished.first_failure_t),
                         ("resume", 2, T0 + 8))
        self.assertEqual(judge(unfinished, RELAXED), [tasks.REASON_RESUME])
        self.assertEqual((built[2].fail_step, built[2].fail_cycle, built[2].failures), ("resume", 2, []))
        self.assertEqual(built[3].end, T0 + 20)
        self.assertEqual([s.user for s in startup], [7])

    def test_async_suspend_failure_counts_in_a_query(self):
        logs = [record(10, A), line(12, parse.MSG_SUSPEND_ASYNC_FAIL, B), line(20, parse.MSG_SHUTDOWN)]
        with tempfile.TemporaryDirectory() as tmp:
            v, _ = query(write_run(Path(tmp), logs, {"start_ts": T0, "end_ts": T0 + 30}), {"rules": []})
        self.assertEqual((v["records"], v["unfinished"], v["bad_suspend_failed"], v["good_ratio_pct"]), (2, 1, 1, 50.0))

    def test_async_suspend_failure_is_an_unfinished_task(self):
        """An async-mode suspend failure without a record is a task that never finished."""
        run = run_log([record(10, A), line(12, parse.MSG_SUSPEND_ASYNC_FAIL, B), line(20, parse.MSG_SHUTDOWN)])
        built, _ = tasks.build_tasks(run, T0 + 20)
        self.assertEqual([(t.actor, t.finished) for t in built], [(A, True), (B, False)])
        self.assertEqual(judge(built[1], []), [tasks.REASON_SUSPEND])

    def test_startup_episodes(self):
        startup = [parse.Event(t=T0 + s, msg=ONSTART, user=u) for s, u in
                   ((1, 7), (2, 7), (3, 7), (4, 8), (5, None), (6, None), (12, 7))]
        records = [parse.parse_record(record(10, user=7))]
        self.assertEqual(tasks.startup_episodes(startup, records), [T0 + 1, T0 + 4, T0 + 5, T0 + 6, T0 + 12])

    def test_cutoff_order(self):
        both = [parse.Event(t=T0 + 20, msg=parse.MSG_BOOMER_QUIT), parse.Event(t=T0 + 30, msg=parse.MSG_SHUTDOWN)]
        self.assertEqual(tasks.cutoff(both, T0 + 5, T0 + 6), (T0 + 30, "shutdown_marker"))
        self.assertEqual(tasks.cutoff(both[:1], T0 + 5, T0 + 6), (T0 + 20, "boomer_quit"))
        self.assertEqual(tasks.cutoff([], T0 + 5, T0 + 6), (T0 + 5, "run_end"))
        self.assertEqual(tasks.cutoff([], None, T0 + 6), (T0 + 6, "end_ts"))
        self.assertEqual(tasks.cutoff([], None, None), (None, "none"))


class RuleTest(unittest.TestCase):
    REFS = {"cel_ms": 4000.0, "wall_ms": 5000.0}  # the default task: cel_ms 6000 (1.5x), wall_ms 8000 (1.6x)

    def test_g1_from_the_record(self):
        self.assertEqual(judge(task(failed=True, fail_step="suspend", fail_cycle=3), RELAXED),
                         [tasks.REASON_SUSPEND])
        self.assertEqual(judge(task(failed=True), RELAXED), [tasks.REASON_FAILED])
        slow_and_failed = task(failed=True, fail_step="execute", resume_max_ms=9000.0)
        self.assertEqual(judge(slow_and_failed, STRICT, self.REFS), [tasks.REASON_EXECUTE])

    def test_limit_rules(self):
        slow = task(resume_max_ms=1600.0, ttfe_max_ms=1100.0)
        self.assertEqual(judge(slow, STRICT), ["slow_resume", "slow_ttfe"])
        self.assertEqual(judge(slow, RELAXED), [])
        self.assertEqual(judge(task(resume_max_ms=1500.0), STRICT), [])

    def test_a_task_without_a_field_is_not_judged_by_its_rules(self):
        start = rule("slow_start", "task.startup_ms + task.ttfe_max_ms <= 700.0")  # 600 + 210 by default
        self.assertEqual(judge(task(), STRICT + [start]), ["slow_start"])
        self.assertEqual(judge(task(ttfe_max_ms=None), STRICT + [start]), [])

    def test_int_fields_are_doubles(self):
        rules = [rule("retried", "task.retries <= 0.0"), rule("long", "task.steps / task.cycles <= 5.0")]
        self.assertEqual(judge(task(retries=0), rules), ["long"])  # 21 steps in 4 cycles
        self.assertEqual(judge(task(retries=1, steps=20), rules), ["retried"])

    def test_baseline_ref_rules(self):
        self.assertEqual(judge(task(), STRICT), [])
        self.assertEqual(judge(task(), STRICT, self.REFS), ["slow_wall"])
        self.assertEqual(judge(task(), RELAXED, self.REFS), [])
        self.assertEqual(judge(task(cel_ms=6001.0), STRICT, self.REFS), ["slow_cel", "slow_wall"])

    def test_every_failing_rule_is_a_reason(self):
        t = task(ttfe_max_ms=1200.0, cel_ms=7000.0)
        self.assertEqual(judge(t, STRICT, self.REFS), ["slow_ttfe", "slow_cel", "slow_wall"])

    def test_baseline_ref_rules_skip_without_a_usable_ref(self):
        rules = compile_rules(STRICT)
        for refs in (None, {}, {"cel_ms": 0, "wall_ms": -1}, {"retries": 5}):
            self.assertEqual(judge(task(), STRICT, refs), [], refs)
            self.assertEqual(split(rules, refs),
                             (["slow_resume", "slow_ttfe"], ["slow_cel", "slow_wall"]), refs)
            self.assertEqual(ratio_fields(rules, refs), [], refs)
        self.assertEqual(split(rules, self.REFS),
                         (["slow_resume", "slow_ttfe", "slow_cel", "slow_wall"], []))
        self.assertEqual(ratio_fields(rules, {"wall_ms": 5000.0}), ["wall_ms"])

    def test_rules_over_several_values(self):
        floor = rule("slow_wall", "task.wall_ms <= 1.5 * baseline.wall_ms || task.wall_ms <= 9000.0")
        self.assertEqual(judge(task(), [floor], self.REFS), [])  # 8000: above 7500, below 9000
        own = rule("think_over_cel", "task.think_ms <= 2.0 * task.cel_ms")  # a task's own fields
        self.assertEqual(judge(task(), [own]), ["think_over_cel"])  # 18000 > 12000
        cross = rule("slow_think", "task.think_ms <= 4.0 * baseline.cel_ms")  # no field from both
        rules = compile_rules([own, floor, cross])
        self.assertEqual(split(rules, None), (["think_over_cel"], ["slow_wall", "slow_think"]))
        self.assertEqual(ratio_fields(rules, self.REFS), ["wall_ms"])

    def test_a_rule_reads_the_variables_the_checker_binds(self):
        commented = rule("slow_wall", "task.wall_ms <= 9000.0 // was 1.5 * baseline.wall_ms")
        parenthesized = rule("slow_cel", "task.cel_ms <= 1.5 * (baseline).cel_ms")
        rules = compile_rules([commented, parenthesized])
        self.assertEqual([r.reads for r in rules], [("task.wall_ms",), ("task.cel_ms", "baseline.cel_ms")])
        self.assertEqual(split(rules, None), (["slow_wall"], ["slow_cel"]))

    def test_rule_names(self):
        long_name = "slow_" + "x" * 56  # 61 characters: bad_<name> would pass 64
        with self.assertRaisesRegex(ValueError, "at most 60 characters"):
            compile_rules([rule(long_name, "task.cel_ms <= 1.0")])
        compile_rules([rule(long_name[:60], "task.cel_ms <= 1.0")])

    def test_failures_only(self):
        self.assertEqual(judge(task(resume_max_ms=9000.0), []), [])
        self.assertEqual(judge(task(failed=True, fail_step="resume"), []), [tasks.REASON_RESUME])
        self.assertEqual(split([], None), ([], []))


class ConfigTest(unittest.TestCase):
    @staticmethod
    def rule(**kw) -> dict:
        r = {"name": "x", "cel": "task.cel_ms <= 1.0"}
        r.update(kw)
        return {k: v for k, v in r.items() if v is not None}

    def test_valid(self):
        for cfg in ({"rules": STRICT}, {"rules": []}, {"rules": RELAXED, "window": "whole"},
                    {"rules": STRICT, "baseline_test": "sweperf_astropy_1_user", "require_baseline": True},
                    {"rules": [self.rule(cel="task.retries >= -1.0 && task.think_ms < 2.0 * task.cel_ms")]}):
            cel.Cel.validate({"records": "sweperf_tasks", **cfg})

    def test_invalid(self):
        r = self.rule
        threshold = {"value": "task.cel_ms", "op": "<=", "limit": 1}
        cases = {
            "unknown keys ['modes']": {"rules": [], "modes": {}},
            "unknown keys ['baseline']": {"rules": [], "baseline": {"paired_run": True}},
            "needs rules": {},
            "needs a list": {"rules": "strict"},
            "window": {"rules": [], "window": "peak"},
            "require_baseline": {"rules": [], "require_baseline": "yes"},
            "baseline_test: must be the name of a test": {"rules": [], "baseline_test": ""},
            "baseline_test: must be": {"rules": [], "baseline_test": ["sweperf_astropy_1_user"]},
            "rules[0]: not a mapping": {"rules": ["x"]},
            "unknown keys ['check', 'type']": {"rules": [r(cel=None, type="threshold", check=threshold)]},
            "name 'Slow'": {"rules": [r(name="Slow")]},
            "name 'a__b'": {"rules": [r(name="a__b")]},
            "name None": {"rules": [r(name=None)]},
            "cel: needs a CEL expression": {"rules": [r(cel=None)]},
            "Syntax error": {"rules": [r(cel="task.cel_ms <=")]},
            "undeclared reference to 'task.fail_cycle'": {"rules": [r(cel="task.fail_cycle <= 1.0")]},
            "undeclared reference to 'cel_ms'": {"rules": [r(cel="cel_ms <= 1.0")]},
            "undeclared reference to 'task'": {"rules": [r(cel='task["cel_ms"] <= 1.0')]},
            "'_<=_' applied to '(double, int)'": {"rules": [r(cel="task.resume_max_ms <= 1500")]},
            "'_*_' applied to '(double, int)'": {"rules": [r(cel="task.cel_ms <= baseline.cel_ms * 2")]},
            "cel: must give a bool, gives DOUBLE": {"rules": [r(cel="task.cel_ms * 2.0")]},
            "own name": {"rules": [r(), r()]},
            "are reasons the records give": {"rules": [r(name="resume_failed")]},
        }
        for message, cfg in cases.items():
            with self.assertRaisesRegex(ValueError, re.escape(message), msg=message):
                cel.Cel.validate({"records": "sweperf_tasks", **cfg})


class MetricsTest(unittest.TestCase):
    def test_percentile(self):
        self.assertIsNone(metrics.percentile([], 50))
        self.assertEqual(metrics.percentile([5.0], 99), 5.0)
        self.assertEqual(metrics.percentile([1.0, 2.0, 3.0, 4.0], 50), 2.5)

    def test_window_values(self):
        t = [task(wall_ms=w) for w in (1000.0, 3000.0, 2000.0)] + [tasks.Task(actor=E, failures=[])]
        m = metrics.window_values(t, [True, True, False, False], [50, 60, 6000], [50, 6000], (0, 3600), {}, [])
        self.assertEqual((m["unfinished"], m["window_s"]), (1, 3600.0))
        self.assertEqual((m["good_wall_ms_p50"], m["good_wall_ms_p99"]), (2000.0, 2980.0))  # the good tasks only
        self.assertEqual((m["startup_failure_attempts"], m["startup_failure_episodes"]), (2, 1))
        self.assertEqual(set(m), metrics.VALUES)

    def test_baseline_ratios(self):
        t = [task(wall_ms=10000.0, cel_ms=6000.0), task(wall_ms=20000.0, cel_ms=6000.0)]
        t[1].record = dataclasses.replace(t[1].record, cel_ms=None)
        m = metrics.window_values(t, [True, True], [], [], (0, 100), {"wall_ms": 10000.0, "cel_ms": 7500.0},
                                  ["wall_ms", "cel_ms"])
        self.assertEqual((m["wall_ms_ratio_p50"], m["cel_ms_ratio_p50"]), (1.5, 0.8))  # cel_ms: one task has it
        self.assertNotIn("resume_max_ms_ratio_p50", m)

    def test_empty_window(self):
        m = metrics.window_values([], [], [], [], (10, 10), {}, [])
        self.assertEqual((m["window_s"], m["good_wall_ms_p50"]), (0.0, None))


class ComputeTest(unittest.TestCase):
    def test_steady_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            hist_path = Path(tmp) / "stats_history.csv"
            meta = {"start_ts": T0, "end_ts": T0 + 100, "steady_start_ts": T0 + 10, "steady_end_ts": T0 + 90}
            self.assertEqual(sweperf.steady_window(meta, None, [], None), ((T0 + 10, T0 + 90), "server_summary"))
            self.assertEqual(sweperf.steady_window(meta, None, [], T0 + 80), ((T0 + 10, T0 + 80), "server_summary"))
            self.assertEqual(sweperf.steady_window(meta, None, [], T0 + 5), ((T0 + 10, T0 + 90), "server_summary"))
            hist = "Timestamp,User Count,Name\n" + "".join(
                f"{int(T0) + i},{u},Aggregated\n" for i, u in ((0, 1), (20, 9), (30, 10), (80, 10), (95, 2)))
            hist_path.write_text(hist, encoding="utf-8")
            meta2 = {"start_ts": T0, "end_ts": T0 + 100}
            self.assertEqual(sweperf.steady_window(meta2, hist_path, [], None), ((T0 + 20, T0 + 80), "stats_history"))
            hist_path.unlink()
            self.assertEqual(sweperf.steady_window(meta2, hist_path, [], T0 + 70), ((T0, T0 + 70), "run"))
            self.assertEqual(sweperf.steady_window({}, None, [T0 + 3, T0 + 7, T0 + 50], T0 + 40),
                             ((T0 + 3, T0 + 7), "log"))

    def test_query_end_to_end(self):
        """Three clean tasks, one with a slow resume, one that never finished, a startup
        failure, noise lines, and load stopping at the boomer quit line."""
        logs = ["[locust-master] a line the source ignores", REAL["traced_span"], line(3, ONSTART, user=9),
                record(11, A), record(11.5, B), record(15, C, resume_max_ms=2000.0), record(23, D),
                line(27.2, parse.MSG_RESUME_FAIL, E, cycle=2), line(30, parse.MSG_BOOMER_QUIT)]
        meta = {"start_ts": T0, "end_ts": T0 + 40, "steady_start_ts": T0 + 5, "steady_end_ts": T0 + 30}
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), logs, meta, stats=[stats_row("grpc_SuspendActorAsync")])
            v, invalid = query(run, {"rules": STRICT})
            relaxed, _ = query(run, {"rules": RELAXED})
            whole, _ = query(run, {"rules": STRICT, "window": "whole"})
            finished, _ = query(run, {"rules": [], "values": {"finished": "records - unfinished"}})
        self.assertIsNone(invalid)
        self.assertEqual((v["records"], v["good"], v["bad"], v["unfinished"]), (5, 3, 2, 1))
        self.assertEqual((v["bad_resume_failed"], v["bad_slow_resume"]), (1, 1))
        # Every rule and failure reason has a key: 0 when no task had it, null when skipped.
        self.assertEqual((v["bad_execute_failed"], v["bad_suspend_failed"], v["bad_failed"], v["bad_slow_ttfe"]),
                         (0, 0, 0, 0))
        self.assertEqual((v["bad_slow_cel"], v["bad_slow_wall"]), (None, None))
        self.assertEqual(v["good_ratio_pct"], 60.0)  # 3 good of 5 tasks, one of them unfinished
        self.assertEqual(v["good_per_hour"], 432.0)  # 3 good in a 25 s window
        self.assertEqual((v["good_per_node_hour"], v["good_per_vcpu_hour"], v["good_per_gb_ram_hour"]),
                         (216.0, 4.32, 1.08))  # 2 nodes, 100 allocatable cores, 400 GB
        self.assertEqual(v["startup_failure_attempts"], 0)  # at 3 s, before the steady window
        self.assertEqual((v["rules_applied"], v["rules_skipped"]), ("slow_resume,slow_ttfe", "slow_cel,slow_wall"))
        self.assertEqual((v["baseline_run"], v["baseline_tasks"], v["baseline_cel_ms"], v["baseline_wall_ms"]),
                         ("", 0, None, None))
        self.assertEqual(v["baseline_note"], "baseline unavailable: no baseline_test in source_cfg")
        self.assertEqual((v["lifecycle_mode"], v["cutoff_source"], v["window_source"], v["skipped_records"]),
                         ("async", "boomer_quit", "server_summary", 0))
        self.assertEqual((relaxed["good_ratio_pct"], relaxed["rules_applied"]), (80.0, ""))
        self.assertEqual(finished["finished"], 4.0)
        # The whole window, run start to the cut-off, has every task and the startup failure.
        self.assertEqual((whole["records"], whole["unfinished"], whole["startup_failure_attempts"],
                          whole["startup_failure_episodes"], whole["window_source"]), (5, 1, 1, 1, "run"))
        self.assertTrue(all(valid_name(k) for k in v), sorted(v))
        own = set(v) - set(VALUES) - {"records", "good", "bad", "rules_applied", "rules_skipped"}
        self.assertEqual({k for k in own if not k.startswith("bad_")},
                         sweperf.SweperfTasks.value_names(compile_rules(STRICT)))

    def test_keys_do_not_change_from_run_to_run(self):
        """Dashboards read <variable>__<value> columns: a run without a baseline, capacity
        or good tasks has the same keys, null where there is nothing to measure."""
        meta = {"start_ts": T0, "end_ts": T0 + 30}
        cfg = {"rules": STRICT, "baseline_test": "t1"}
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            full = write_run(d / "full", [record(11, A), record(12, B)], meta)
            paired, _ = query(pair(full, write_run(d / "one", [record(11)], meta)), cfg)
            unpaired, _ = query(full, cfg)
            unasked, _ = query(full, {"rules": STRICT})
            bare, _ = query(write_run(d / "bare", [record(11, failed=True, fail_step="execute")], meta, trial={}), cfg)
        self.assertEqual(set(paired), set(unpaired))
        self.assertEqual(set(paired), set(unasked))
        self.assertEqual(set(paired), set(bare))
        self.assertEqual((unpaired["wall_ms_ratio_p50"], unpaired["bad_slow_wall"]), (None, None))
        self.assertEqual(unpaired["baseline_note"], "baseline unavailable: no run of t1 to pair with")
        self.assertEqual((bare["good_per_vcpu_hour"], bare["good_wall_ms_p50"]), (None, None))
        self.assertEqual(bare["bad_execute_failed"], 1)

    def test_baseline_in_another_lifecycle_mode_is_unavailable(self):
        meta = {"start_ts": T0, "end_ts": T0 + 30}
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            run = write_run(d / "run", [record(11, A)], meta, stats=[stats_row("grpc_SuspendActorAsync")])
            sync = write_run(d / "sync", [record(11, wall_ms=1000.0)], meta, stats=[stats_row("grpc_SuspendActor")])
            v, invalid = query(pair(run, sync), {"rules": STRICT, "baseline_test": "t1", "require_baseline": True})
        self.assertEqual(invalid, "baseline unavailable: lifecycle mode differs (async vs sync)")
        self.assertEqual((v["rules_skipped"], v["baseline_tasks"], v["baseline_run"]), ("slow_cel,slow_wall", 0, ""))

    def test_invalid_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            no_records = write_run(d / "a", [REAL["start"], REAL["quit"]], {})
            no_logs = BenchmarkRunContext(files={"logs.txt": d / "missing" / "logs.txt"})
            late = write_run(d / "b", [record(50)], {"start_ts": T0, "end_ts": T0 + 100, "steady_start_ts": T0 + 1,
                                                     "steady_end_ts": T0 + 20})
            self.assertIn("no task records in logs.txt", query(no_records, {"rules": []})[1])
            self.assertEqual(query(no_logs, {"rules": []}), ({}, "logs.txt absent"))
            values, invalid = query(late, {"rules": []})
            self.assertEqual(invalid, "no tasks in the steady window")
            self.assertEqual(values["records"], 0)  # the values stay
            # One record and no window anywhere: the run start, end and steady window are all unknown.
            point = write_run(d / "c", [record(50)], {})
            self.assertEqual(query(point, {"rules": []})[1], "empty steady window")

    def test_a_rule_that_fails_to_evaluate_makes_the_run_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), [record(11, A)], {"start_ts": T0, "end_ts": T0 + 30})
            values, invalid = query(run, {"rules": [rule("too_big", "int(task.wall_ms * 1e300) > 0")]})
        self.assertEqual(invalid, f"rule 'too_big' on {A}: OUT_OF_RANGE: double out of int64 range")
        self.assertEqual(values, {})

    def test_paired_run_baseline(self):
        meta = {"start_ts": T0, "end_ts": T0 + 30}
        strict = {"rules": STRICT, "baseline_test": "t1"}
        required = {**strict, "require_baseline": True}
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            run = write_run(d / "run", [record(11, A), record(12, B, wall_ms=6500.0)], meta)
            one_user = write_run(d / "one_user", [record(11, wall_ms=4000.0, cel_ms=6000.0),
                                                  record(12, B, failed=True, fail_step="execute", wall_ms=1.0)], meta)
            r, invalid = query(pair(run, one_user), strict)
            missing, missing_invalid = query(pair(run), strict)
            req, req_invalid = query(pair(run), required)
            broken, _ = query(pair(run, write_run(d / "empty", [REAL["quit"]], meta)), strict)
            no_cel, no_cel_invalid = query(pair(run, write_run(d / "no_cel", [record(11, cel_ms=0.0)], meta)), required)
            unasked, unasked_invalid = query(pair(run, one_user), {"rules": STRICT, "require_baseline": True})
        self.assertIsNone(invalid)
        # wall 8000 and 6500 against 4000 (the failed task stays out of the baseline) both break 1.5x.
        self.assertEqual((r["good"], r["bad_slow_wall"], r["bad_slow_cel"]), (0, 2, 0))
        self.assertEqual((r["baseline_run"], r["baseline_note"], r["baseline_tasks"], r["rules_skipped"]),
                         (str(d / "one_user"), "", 1, ""))
        self.assertEqual((r["baseline_wall_ms"], r["baseline_cel_ms"]), (4000.0, 6000.0))
        self.assertEqual((r["wall_ms_ratio_p50"], r["cel_ms_ratio_p50"]), (1.812, 1.0))
        self.assertEqual((missing_invalid, missing["rules_skipped"]), (None, "slow_cel,slow_wall"))
        self.assertEqual(missing["baseline_note"], "baseline unavailable: no run of t1 with tag abc")
        self.assertEqual(req_invalid, "baseline unavailable: no run of t1 with tag abc")
        self.assertEqual(req, missing)  # the numbers without the skipped rules
        self.assertEqual((no_cel_invalid, no_cel["baseline_cel_ms"]),
                         ("baseline unavailable: no baseline value for slow_cel", 0.0))
        self.assertEqual(broken["baseline_note"], f"baseline unavailable: t1 run {d / 'empty'}: no task records in "
                         "logs.txt: no task finished, or the workload logs none")
        self.assertEqual(broken["rules_skipped"], "slow_cel,slow_wall")
        # Rules that read the baseline, without a baseline_test: skipped, and invalid when required.
        self.assertEqual(unasked_invalid, "baseline unavailable: no baseline_test in source_cfg")
        self.assertEqual(unasked["baseline_run"], "")

    def test_reads_each_run_once(self):
        """However many queries: one read of the run, and one lookup and read per baseline_test."""
        meta = {"start_ts": T0, "end_ts": T0 + 30}
        asked = []
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            one_user = write_run(d / "one_user", [record(11, wall_ms=4000.0)], meta)

            def paired_run(test: str) -> BenchmarkRunContext:
                asked.append(test)
                return one_user

            run = dataclasses.replace(write_run(d / "run", [record(11, A)], meta), paired_run=paired_run)
            source = cel.Cel()
            source.init(run)
            with unittest.mock.patch.object(sweperf.parse, "read_run", wraps=parse.read_run) as read_run:
                for cfg in ({"rules": STRICT, "baseline_test": "t1"},
                            {"rules": RELAXED, "baseline_test": "t1"},
                            {"rules": STRICT, "baseline_test": "t1", "window": "whole"},
                            {"rules": STRICT, "baseline_test": "t2"}, {"rules": []}):
                    source.query({"records": "sweperf_tasks", **cfg})
        self.assertEqual(asked, ["t1", "t2"])
        self.assertEqual([c.args[0] for c in read_run.call_args_list],
                         [d / "run" / "logs.txt", d / "one_user" / "logs.txt", d / "one_user" / "logs.txt"])

    def test_init_reads_nothing(self):
        """The runner inits sources when the run starts, before its files exist."""
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp) / "run"
            source = cel.Cel()
            source.init(BenchmarkRunContext(files={name: d / name for name in sweperf.INPUTS}))
            write_run(d, [record(11, A)], {"start_ts": T0, "end_ts": T0 + 30})
            self.assertEqual(source.query({"records": "sweperf_tasks", "rules": []})["good"], 1)

    def test_baseline_refs_use_clean_tasks(self):
        meta = {"start_ts": T0, "end_ts": T0 + 100}
        failed = record(40, C, failed=True, fail_step="resume", fail_cycle=2, wall_ms=30000.0)
        with tempfile.TemporaryDirectory() as tmp:
            a = write_run(Path(tmp) / "a", [record(11, A), record(23, B, wall_ms=10000.0, ttfe_max_ms=None,
                                                                    startup_ms=None), failed], meta)
            refs, n = sweperf.baseline_refs(sweperf.load_run(a))
        self.assertEqual(n, 2)
        self.assertEqual(refs["wall_ms"], 9000.0)  # median of 8000 and 10000; the failed task is left out
        self.assertEqual((refs["ttfe_max_ms"], refs["startup_ms"], refs["think_ms"]), (210.0, 600.0, 18000.0))
        self.assertEqual(refs["retries"], 0.0)
        with tempfile.TemporaryDirectory() as tmp:
            only_failed = write_run(Path(tmp), [failed], meta)
            with self.assertRaisesRegex(ValueError, "no finished task"):
                sweperf.baseline_refs(sweperf.load_run(only_failed))

    def test_a_field_no_task_logged_is_left_out(self):
        with tempfile.TemporaryDirectory() as tmp:
            refs, n = sweperf.baseline_refs(sweperf.load_run(write_run(Path(tmp), [record(11, think_ms=None)], {})))
        self.assertEqual(n, 1)
        self.assertNotIn("think_ms", refs)

    def test_baseline_tasks_are_built_like_any_run(self):
        """The baseline uses the tasks a query would count: a record just after the
        cut-off counts, a failure line after it (teardown) does not."""
        logs = [record(11, A), line(20, parse.MSG_SHUTDOWN), record(20.5, B, wall_ms=9000.0),
                line(25, parse.MSG_RESUME_FAIL, C, cycle=1)]
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), logs, {})
            refs, n = sweperf.baseline_refs(sweperf.load_run(run))
            whole, _ = query(run, {"rules": [], "window": "whole"})
        self.assertEqual((n, whole["records"], whole["unfinished"]), (2, 2, 0))
        self.assertEqual(refs["wall_ms"], 8500.0)


if __name__ == "__main__":
    unittest.main()
