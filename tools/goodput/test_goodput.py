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
import csv
import io
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import classify  # noqa: E402
import goodput  # noqa: E402
import metrics  # noqa: E402
import parse  # noqa: E402
import summarize  # noqa: E402

CONFIG = json.loads((HERE / "config.json").read_text(encoding="utf-8"))
STRICT, RELAXED = CONFIG["modes"]["strict"], CONFIG["modes"]["relaxed"]

T0 = 1791700000.0  # an arbitrary run start (unix seconds)
ACTOR = "sb-11111111-2222-3333-4444-555555555555"
OTHER = "sb-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
FINISHED_FRESH = "Actor finished all cycles and was deleted; a new actor will run the trajectory next"
ONSTART = "sweperf on_start failed; goroutine will retry next iter"

# Real task log lines from Prow sweperf runs: one per message type the script reads,
# plus two it skips (the error-only resume line and a traced span).
REAL = {
    "start": '[boomer] {"time":"2026-10-11T06:57:35.684909393Z","level":"INFO","msg":"Starting sweperf cycle",'
             '"actor":"sb-818374a1-f288-4aa7-8acf-5c8f456f7883","cycle":1,"start_step":1,"end_step":6}',
    "done": '[boomer] {"time":"2026-10-11T06:57:43.643723243Z","level":"INFO","msg":"Completed sweperf cycle",'
            '"actor":"sb-e25a3518-d040-498b-bb73-e02f0186788b","cycle":1}',
    "finished_reused": '[boomer] {"time":"2026-10-11T06:58:59.75120248Z","level":"INFO","msg":"Actor finished all '
                       'cycles and reset session back to cycle 1","actor":"sb-7161258b-ba3c-483d-9aa6-baf3cc46b589",'
                       '"cycles":4}',
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
    "onstart_fail": '[boomer] {"time":"2026-10-11T07:01:04.957981322Z","level":"WARN","msg":"sweperf on_start '
                    'failed; goroutine will retry next iter","err":"pollLiveness: failed to verify server liveness '
                    'for actor sb-2891de09-68fb-4997-a54a-c4eb15b62fca"}',
    "quit": '[boomer] {"time":"2026-10-11T07:07:33.958613342Z","level":"INFO","msg":"Recv quit message from master, '
            'all the goroutines are stopped"}',
    "traced_span": '[boomer] {"time":"2026-10-11T06:57:36.1Z","level":"INFO","msg":"traced span",'
                   '"actor":"sb-818374a1-f288-4aa7-8acf-5c8f456f7883"}',
}

# A task record line in the benchmark's layout (field order and number formats as slog writes them).
RECORD_LINE = ('[boomer] {"time":"2026-10-18T19:02:11.524115321Z","level":"INFO","msg":"sweperf task record","v":1,'
               '"user":42,"actor":"sb-0c3de0a1-7f6e-4b8a-9d2c-1e5f00a1b2c3","template":"sweperf-astropy-7336",'
               '"cycles":4,"steps":60,"task_start":"2026-10-18T19:01:30.1Z","end":"2026-10-18T19:02:11.524107Z",'
               '"startup_ms":2311.4,"cel_ms":8612.25,"wall_ms":15890.7,"think_ms":21003.9,"resume_max_ms":712.3,'
               '"retries":0,"failed":false,"fail_step":"","fail_cycle":0,"ttfe_max_ms":801.6}')
# A newer startup failure line, which names the user and the actor.
ONSTART_USER_LINE = ('[boomer] {"time":"2026-10-14T03:00:01.5Z","level":"WARN","msg":"sweperf on_start failed; '
                     'goroutine will retry next iter","user":12,"actor":"sb-0c3de0a1-7f6e-4b8a-9d2c-1e5f00a1b2c4",'
                     '"template":"sweperf-astropy-7336","err":"ensureAtespace: rpc error: code = Unavailable"}')


def ts(sec: float) -> str:
    """RFC 3339 for T0 + sec, with nanoseconds like the task log."""
    whole = int(T0 + sec)
    frac = round((T0 + sec - whole) * 1e9)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(whole)) + f".{frac:09d}Z"


def line(sec: float, msg: str, actor: str = ACTOR, **kw) -> str:
    j = {"time": ts(sec), "level": "ERROR" if "failed" in msg else "INFO", "msg": msg}
    if actor:
        j["actor"] = actor
    j.update(kw)
    return parse.LOG_PREFIX + json.dumps(j)


def cycle(sec: float, n: int, dur: float, actor: str = ACTOR) -> list[str]:
    return [line(sec, parse.MSG_START, actor, cycle=n), line(sec + dur, parse.MSG_DONE, actor, cycle=n)]


def task(sec: float, actor: str = ACTOR, finished: str = parse.MSG_FINISHED_REUSED) -> list[str]:
    """A clean 4-cycle task: 2 s cycles with 1 s think time between them; it ends at sec + 11."""
    out = []
    for n in range(1, 5):
        out += cycle(sec + 3 * (n - 1), n, 2.0, actor)
    out.append(line(sec + 11, finished, actor, cycles=4))
    return out


def sb(n: int) -> str:
    """An actor name."""
    return f"sb-{n:08d}-0000-0000-0000-000000000000"


def record(sec: float, who: str = ACTOR, cel: float = 8000.0, wall: float = 12000.0, resume: float = 600.0,
           ttfe: float = None, fail_step: str = "", template: str = "tmpl-a") -> str:
    """A task record line, laid out like the benchmark's, for a task that ends at sec.

    fail_step makes it a failed task, failing in cycle 2.
    """
    j = {"v": 1, "user": 7, "template": template, "cycles": 4, "steps": 60, "task_start": ts(sec - 30),
         "end": ts(sec), "startup_ms": 1500.5, "cel_ms": cel, "wall_ms": wall, "think_ms": 9000.0,
         "resume_max_ms": resume, "retries": 1 if fail_step == "resume" else 0, "failed": bool(fail_step),
         "fail_step": fail_step, "fail_cycle": 2 if fail_step else 0}
    if ttfe is not None:
        j["ttfe_max_ms"] = ttfe
    return line(sec, parse.MSG_TASK_RECORD, who, **j)


def record_task(**kw) -> classify.Task:
    """A finished Task built from a record line (see record)."""
    tasks, _ = classify.build_record_tasks([parse.parse_line(record(10, **kw))], None)
    return tasks[0]


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


def build(logs: list[str], cut=None):
    return classify.build_tasks([e for e in (parse.parse_line(x) for x in logs) if e], cut)


class ParseTest(unittest.TestCase):
    def test_real_lines(self):
        ev = {k: parse.parse_line(v) for k, v in REAL.items()}
        self.assertEqual((ev["start"].msg, ev["start"].actor, ev["start"].cycle),
                         (parse.MSG_START, "sb-818374a1-f288-4aa7-8acf-5c8f456f7883", 1))
        self.assertAlmostEqual(ev["start"].t, parse.parse_time("2026-10-11T06:57:35.684909393Z"))
        self.assertEqual((ev["done"].msg, ev["done"].cycle), (parse.MSG_DONE, 1))
        self.assertEqual(ev["finished_reused"].actor, "sb-7161258b-ba3c-483d-9aa6-baf3cc46b589")
        self.assertEqual((ev["resume_fail"].msg, ev["resume_fail"].cycle), (parse.MSG_RESUME_FAIL, 3))
        self.assertEqual((ev["exec_fail"].msg, ev["exec_fail"].cycle), (parse.MSG_EXEC_FAIL, 3))
        self.assertEqual((ev["suspend_fail"].msg, ev["suspend_fail"].cycle), (parse.MSG_SUSPEND_FAIL, None))
        self.assertEqual(ev["onstart_fail"].actor, "sb-2891de09-68fb-4997-a54a-c4eb15b62fca")
        self.assertEqual(ev["quit"].msg, parse.MSG_BOOMER_QUIT)
        # the error-only resume line (counted through its "in step" line) and other lines are skipped
        self.assertIsNone(ev["resume_err"])
        self.assertIsNone(ev["traced_span"])

    def test_both_task_end_wordings(self):
        reused = parse.parse_line(REAL["finished_reused"])
        fresh = parse.parse_line(line(1, FINISHED_FRESH, cycles=4))
        self.assertEqual(parse.log_format([reused]), parse.FORMAT_REUSED_ACTOR)
        self.assertEqual(parse.log_format([fresh]), parse.FORMAT_FRESH_ACTOR)
        self.assertEqual(parse.log_format([]), parse.FORMAT_UNKNOWN)

    def test_parse_time(self):
        self.assertAlmostEqual(parse.parse_time("2026-10-11T06:57:35.684909393Z"), 1791701855.684909393, places=6)
        self.assertEqual(parse.parse_time("2026-10-11T06:57:35Z"), 1791701855.0)
        with self.assertRaises(ValueError):
            parse.parse_time("2026-10-11T06:57:35+02:00")

    def test_rejects_other_lines(self):
        for bad in ("[locust-master] not a task log line", "[boomer] {not json", "[boomer] [1, 2]",
                    '[boomer] {"msg": "Starting sweperf cycle"}', '[boomer] {"time": 5, "msg": "shutdown"}'):
            self.assertIsNone(parse.parse_line(bad), bad)

    def test_epoch(self):
        self.assertEqual(parse.epoch("1791700000"), 1791700000.0)
        self.assertEqual(parse.epoch("2026-10-11T00:00:00Z"), parse.parse_time("2026-10-11T00:00:00Z"))
        self.assertIsNone(parse.epoch(None))
        self.assertIsNone(parse.epoch("soon"))


class TaskTest(unittest.TestCase):
    def test_one_actor_many_tasks_retry_and_cutoff(self):
        """One actor serves 4 tasks; cycle pairing never crosses a task boundary."""
        logs = task(0)  # task 0: clean
        # task 1: cycle 2's resume fails once and the cycle is retried
        logs += cycle(12, 1, 2.0)
        logs += [line(15, parse.MSG_START, cycle=2), line(15.1, parse.MSG_RESUME_FAIL, cycle=2)]
        logs += cycle(16, 2, 2.5) + cycle(19, 3, 2.0) + cycle(22, 4, 2.0)
        logs.append(line(24.5, parse.MSG_FINISHED_REUSED, cycles=4))
        logs += task(25)  # task 2: clean
        logs += cycle(37, 1, 2.0) + [line(40, parse.MSG_START, cycle=2)]  # task 3: cut off mid-cycle
        logs.append(line(41, parse.MSG_BOOMER_QUIT, actor=""))
        logs.append(line(42, parse.MSG_RESUME_FAIL, cycle=2))  # teardown, after the cut-off
        events = [e for e in (parse.parse_line(x) for x in logs) if e]
        cut, source = classify.cutoff(events, None, None)
        self.assertEqual((cut, source), (T0 + 41, "boomer_quit"))
        tasks, _ = classify.build_tasks(events, cut)
        self.assertEqual([(t.actor, t.index) for t in tasks], [(ACTOR, i) for i in range(4)])
        t0, t1, t2, t3 = tasks
        self.assertTrue(t0.finished and t1.finished and t2.finished)
        self.assertEqual((t0.wall_ms, t0.retries), (8000.0, 0))
        # the retried cycle counts only its last attempt: Starting 16.0 .. Completed 18.5
        self.assertEqual((t1.cycle_ms[2], t1.retries, t1.wall_ms), (2500.0, 1, 8500.0))
        self.assertEqual([(f.reason, f.cycle) for f in t1.failures], [(classify.REASON_RESUME, 2)])
        self.assertEqual(classify.classify(t1, STRICT, None), (classify.BAD, [classify.REASON_RESUME]))
        self.assertEqual(classify.classify(t0, STRICT, None), (classify.GOOD, []))
        # task 3 was cut off; its failure after the cut-off is teardown and ignored
        self.assertFalse(t3.finished)
        self.assertEqual((t3.failures, t3.cycle_ms), ([], {1: 2000.0}))
        self.assertEqual(classify.classify(t3, STRICT, None), (classify.UNFINISHED, []))

    def test_unfinished_with_failure_is_bad(self):
        logs = task(0) + cycle(12, 1, 2.0) + [line(15, parse.MSG_START, cycle=2),
                                             line(15.1, parse.MSG_RESUME_FAIL, cycle=2)]
        tasks, _ = build(logs, T0 + 20)
        self.assertEqual(classify.classify(tasks[1], RELAXED, None), (classify.BAD, [classify.REASON_RESUME]))

    def test_crashed_actor_is_one_bad_task(self):
        """Execute and suspend fail in cycle 3, then every retry of cycle 4 fails until the run stops."""
        logs = task(0) + cycle(12, 1, 2.0) + cycle(15, 2, 2.0)
        logs += [line(18, parse.MSG_START, cycle=3), line(140, parse.MSG_EXEC_FAIL, cycle=3, err="timed out"),
                 line(140, parse.MSG_SUSPEND_FAIL, err="CallAteletSuspend"), line(140, parse.MSG_DONE, cycle=3)]
        for i in range(200):
            logs += [line(141 + 0.5 * i, parse.MSG_START, cycle=4),
                     line(141 + 0.5 * i, parse.MSG_RESUME_FAIL, cycle=4)]
        tasks, _ = build(logs, T0 + 300)
        self.assertEqual(len(tasks), 2)
        crashed = tasks[1]
        self.assertFalse(crashed.finished)
        self.assertEqual((len(crashed.failures), crashed.retries), (202, 199))
        self.assertEqual(classify.classify(crashed, STRICT, None),
                         (classify.BAD, [classify.REASON_EXECUTE, classify.REASON_SUSPEND, classify.REASON_RESUME]))

    def test_actors_are_independent(self):
        logs = []
        for x, y in zip(task(0, ACTOR), task(0.5, OTHER)):  # interleaved like concurrent users
            logs += [x, y]
        tasks, _ = build(logs)
        self.assertEqual(sorted((t.actor, t.wall_ms) for t in tasks), [(ACTOR, 8000.0), (OTHER, 8000.0)])

    def test_fresh_actor_wording_ends_tasks_too(self):
        tasks, _ = build(task(0, ACTOR, FINISHED_FRESH) + task(0, OTHER, FINISHED_FRESH))
        self.assertTrue(all(t.finished for t in tasks))

    def test_completed_without_starting_is_ignored(self):
        tasks, _ = build([line(1, parse.MSG_DONE, cycle=1), line(2, parse.MSG_FINISHED_REUSED)])
        self.assertEqual(tasks[0].cycle_ms, {})

    def test_startup_failures_before_cutoff(self):
        logs = [line(1, ONSTART, actor="", err=f"liveness for actor {ACTOR}"), line(9, ONSTART, actor="", err="x")]
        _, startup = build(logs, T0 + 5)
        self.assertEqual([(s.actor, s.t) for s in startup], [(ACTOR, T0 + 1)])


class CutoffTest(unittest.TestCase):
    def test_order(self):
        both = [parse.Event(t=T0 + 20, msg=parse.MSG_BOOMER_QUIT), parse.Event(t=T0 + 30, msg=parse.MSG_SHUTDOWN)]
        self.assertEqual(classify.cutoff(both, T0 + 5, T0 + 6), (T0 + 30, "shutdown_marker"))
        self.assertEqual(classify.cutoff(both[:1], T0 + 5, T0 + 6), (T0 + 20, "boomer_quit"))
        self.assertEqual(classify.cutoff([], T0 + 5, T0 + 6), (T0 + 5, "run_end"))
        self.assertEqual(classify.cutoff([], None, T0 + 6), (T0 + 6, "end_ts"))
        self.assertEqual(classify.cutoff([], None, None), (None, "none"))


class GateTest(unittest.TestCase):
    BASE = {"runtime": "gvisor", "machine_type": "m", "format": parse.FORMAT_REUSED_ACTOR, "wall_ref_ms": 10000}

    def test_g3_wall_with_baseline_only(self):
        t = classify.Task(actor=ACTOR, index=0, start=T0, end=T0 + 30, cycle_ms={1: 17000.0})
        self.assertEqual(classify.classify(t, STRICT, None), (classify.GOOD, []))
        self.assertEqual(classify.classify(t, STRICT, self.BASE), (classify.BAD, [classify.REASON_SLOW]))
        self.assertEqual(classify.classify(t, RELAXED, self.BASE), (classify.GOOD, []))

    def test_slow_only_without_other_failures(self):
        t = classify.Task(actor=ACTOR, index=0, start=T0, end=T0 + 30, cycle_ms={1: 30000.0},
                          failures=[classify.Failure(reason=classify.REASON_RESUME, t=T0 + 1, cycle=1)])
        self.assertEqual(classify.classify(t, STRICT, self.BASE), (classify.BAD, [classify.REASON_RESUME]))

    def test_match_baseline(self):
        reused, fresh = parse.FORMAT_REUSED_ACTOR, parse.FORMAT_FRESH_ACTOR
        bs = [{"runtime": "gvisor", "machine_type": "a", "template": "t1", "format": reused},
              {"runtime": "microvm", "machine_type": "a", "format": reused},
              {"runtime": "gvisor", "machine_type": "a", "format": fresh}]
        self.assertIs(classify.match_baseline(bs, "gvisor", "a", "t1", reused), bs[0])
        self.assertIsNone(classify.match_baseline(bs, "gvisor", "a", "t2", reused))
        # a baseline for one template doesn't apply to a run that names none
        self.assertIsNone(classify.match_baseline(bs, "gvisor", "a", "", reused))
        self.assertIs(classify.match_baseline(bs, "microvm", "a", "anything", reused), bs[1])
        self.assertIsNone(classify.match_baseline(bs, "gvisor", "b", "t1", reused))

    def test_match_baseline_prefers_the_same_template(self):
        rec = parse.FORMAT_TASK_RECORD
        any_t = {"runtime": "gvisor", "machine_type": "a", "format": rec}
        t1 = {"runtime": "gvisor", "machine_type": "a", "format": rec, "template": "t1"}
        self.assertIs(classify.match_baseline([any_t, t1], "gvisor", "a", "t1", rec), t1)
        self.assertIs(classify.match_baseline([any_t, t1], "gvisor", "a", "t2", rec), any_t)
        self.assertIs(classify.match_baseline([any_t, t1], "gvisor", "a", "", rec), any_t)

    def test_match_baseline_needs_the_same_log_format(self):
        reused, fresh = parse.FORMAT_REUSED_ACTOR, parse.FORMAT_FRESH_ACTOR
        bs = [{"runtime": "microvm", "machine_type": "a", "format": reused}]
        self.assertIsNone(classify.match_baseline(bs, "microvm", "a", "", fresh))
        self.assertIsNone(classify.match_baseline([{"runtime": "microvm", "machine_type": "a"}],
                                                  "microvm", "a", "", reused))


class MetricsTest(unittest.TestCase):
    def test_percentile(self):
        self.assertIsNone(metrics.percentile([], 50))
        self.assertEqual(metrics.percentile([5.0], 99), 5.0)
        self.assertEqual(metrics.percentile([1.0, 2.0, 3.0, 4.0], 50), 2.5)

    def test_window_membership_and_rates(self):
        def row(status, end=None, ff=None, reasons=(), wall=1000.0):
            return {"status": status, "end": end, "first_failure_t": ff, "reasons": list(reasons), "wall_ms": wall}
        rows = [row(classify.GOOD, end=100), row(classify.GOOD, end=200), row(classify.GOOD, end=4000),
                row(classify.BAD, end=300, reasons=[classify.REASON_RESUME]),
                row(classify.BAD, ff=400, reasons=[classify.REASON_EXECUTE, classify.REASON_SUSPEND]),
                row(classify.BAD, ff=5000, reasons=[classify.REASON_RESUME]),
                row(classify.UNFINISHED)]
        m = metrics.window_metrics(rows, [50, 6000], (0, 3600),
                                   {"node_count": 2, "allocatable_cores": 100, "allocatable_ram_gb": 400}, None)
        self.assertEqual((m["tasks_done"], m["tasks_good"], m["tasks_bad"], m["tasks_unfinished_bad"]), (3, 2, 1, 1))
        # the unfinished task whose first failure is outside the window is not counted
        self.assertEqual(m["bad_reasons"], {"execute_failed": 1, "resume_failed": 1, "suspend_failed": 1})
        self.assertEqual(m["good_tasks_per_hr"], 2.0)
        self.assertEqual((m["good_tasks_per_node_hr"], m["good_tasks_per_vcpu_hr"], m["good_tasks_per_gb_ram_hr"]),
                         (1.0, 0.02, 0.005))
        self.assertEqual(m["goodput_ratio_pct"], 50.0)  # 2 good / (3 done + 1 unfinished bad)
        self.assertEqual((m["startup_failure_attempts"], m["startup_failure_episodes"]), (1, None))

    def test_baseline_ratios(self):
        rows = [{"status": classify.GOOD, "end": 10, "first_failure_t": None, "reasons": [], "wall_ms": w}
                for w in (10000.0, 20000.0)]
        m = metrics.window_metrics(rows, [], (0, 100), {}, {"wall_ref_ms": 10000, "task_cel_ref_ms": 7500})
        self.assertEqual((m["wall_ratio_p50"], m["eta_infra"]), (1.5, 0.5))

    def test_empty_window(self):
        m = metrics.window_metrics([], [], (10, 10), {}, None)
        self.assertIsNone(m["good_tasks_per_hr"])
        self.assertIsNone(m["goodput_ratio_pct"])


class ComputeTest(unittest.TestCase):
    def test_steady_window_fallbacks(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            meta = {"start_ts": T0, "end_ts": T0 + 100, "steady_start_ts": T0 + 10, "steady_end_ts": T0 + 90}
            self.assertEqual(goodput.steady_window(meta, d, []), ((T0 + 10, T0 + 90), "server_summary"))
            hist = "Timestamp,User Count,Name\n" + "".join(
                f"{int(T0) + i},{u},Aggregated\n" for i, u in ((0, 1), (20, 9), (30, 10), (80, 10), (95, 2)))
            (d / "stats_history.csv").write_text(hist, encoding="utf-8")
            meta2 = {"start_ts": T0, "end_ts": T0 + 100}
            self.assertEqual(goodput.steady_window(meta2, d, []), ((T0 + 20, T0 + 80), "stats_history"))
            (d / "stats_history.csv").unlink()
            self.assertEqual(goodput.steady_window(meta2, d, []), ((T0, T0 + 100), "run"))
            evs = [parse.Event(t=T0 + 3, msg="x"), parse.Event(t=T0 + 7, msg="y")]
            self.assertEqual(goodput.steady_window({}, d, evs), ((T0 + 3, T0 + 7), "log"))

    def test_compute_end_to_end(self):
        """A run with two actors, one reused for 3 tasks, a failed unfinished task, a startup failure,
        a noise line, and load stopping at the boomer quit line."""
        logs = ["[locust-master] a line the script ignores", REAL["traced_span"]]
        logs += task(0) + task(12)  # ACTOR: 2 clean tasks, ending at 11 and 23
        logs += task(0.5, OTHER)  # OTHER: clean, ending at 11.5
        logs += cycle(24, 1, 2.0) + [line(27, parse.MSG_START, cycle=2), line(27.2, parse.MSG_RESUME_FAIL, cycle=2)]
        logs.append(line(3, ONSTART, actor="", err=f"liveness for actor {OTHER}"))
        logs.append(line(30, parse.MSG_BOOMER_QUIT, actor=""))
        meta = {"test_name": "sweperf_test_uvm", "tag": "abc", "start_ts": T0, "end_ts": T0 + 40,
                "steady_start_ts": T0 + 5, "steady_end_ts": T0 + 30, "data_timestamp": "2026-10-11T00:00:00Z"}
        with tempfile.TemporaryDirectory() as tmp:
            r = goodput.compute(write_run(Path(tmp), logs, meta), CONFIG)
        self.assertEqual((r["run"]["runtime"], r["run"]["log_format"], r["run"]["node_count"]),
                         ("microvm", parse.FORMAT_REUSED_ACTOR, 2.0))
        self.assertEqual(r["cutoff"], {"ts": T0 + 30, "source": "boomer_quit"})
        self.assertEqual(r["steady_window"]["source"], "server_summary")
        self.assertEqual(r["totals"], {"tasks_finished": 3, "tasks_unfinished": 1, "startup_failure_attempts": 1,
                                       "actors": 2})
        st = r["modes"]["strict"]["steady"]
        self.assertEqual((st["tasks_done"], st["tasks_good"], st["tasks_bad"], st["tasks_unfinished_bad"]),
                         (3, 3, 0, 1))
        self.assertEqual(st["bad_reasons"], {"resume_failed": 1})
        self.assertEqual(st["goodput_ratio_pct"], 75.0)  # 3 good / (3 done + 1 unfinished bad)
        self.assertEqual(st["good_tasks_per_hr"], 432.0)  # 3 good in a 25 s window
        self.assertEqual(st["good_tasks_per_node_hr"], 216.0)
        self.assertEqual(st["startup_failure_attempts"], 0)  # at 3 s, before the steady window
        self.assertEqual(r["modes"]["strict"]["whole"]["startup_failure_attempts"], 1)
        self.assertEqual((r["modes"]["strict"]["g2"], r["modes"]["strict"]["g3_basis"]), ("n/a", "skipped"))
        self.assertEqual(r["tasks"][-1]["status_by_mode"], {"relaxed": "bad", "strict": "bad"})
        rows = goodput.jsonl_rows(r)
        self.assertEqual([x["metric"] for x in rows], ["goodput_relaxed", "goodput_strict"])
        self.assertTrue(all(isinstance(v, str) for x in rows for v in x["measurements"].values()))
        self.assertEqual((rows[1]["measurements"]["tasks_good"], rows[1]["measurements"]["bad_resume_failed"]),
                         ("3", "1"))
        self.assertEqual(rows[1]["measurements"]["whole_tasks_good"], "3")

    def test_compute_uses_a_matching_baseline(self):
        logs = task(0) + [line(20, parse.MSG_BOOMER_QUIT, actor="")]
        base = {"runtime": "gvisor", "machine_type": "c3-highcpu-88", "format": parse.FORMAT_REUSED_ACTOR,
                "wall_ref_ms": 4000}
        with tempfile.TemporaryDirectory() as tmp:
            r = goodput.compute(write_run(Path(tmp), logs, {"start_ts": T0, "end_ts": T0 + 30}),
                                dict(CONFIG, baselines=[base]))
        self.assertEqual(r["modes"]["strict"]["g3_basis"], "wall")
        self.assertEqual(r["modes"]["strict"]["whole"]["bad_reasons"], {"slow": 1})  # 8000 / 4000 > 1.5
        self.assertEqual(r["modes"]["relaxed"]["whole"]["tasks_good"], 1)  # 2.0 <= 2.0

    def test_identity_and_runtime(self):
        ident = goodput.identity_from_path(
            "gs://b/from_prow/runs/sweperf_astropy_1_user/run_date=2026-10-11/run_ts=1791732523/run_tag=b73e/")
        self.assertEqual(ident, {"test_name": "sweperf_astropy_1_user", "run_date": "2026-10-11",
                                 "run_ts": "1791732523", "tag": "b73e"})
        self.assertEqual(goodput.identity_from_path("/tmp/x"), {})
        self.assertEqual(goodput.runtime_of("sweperf_astropy_1_user_uvm", {}), "microvm")
        self.assertEqual(goodput.runtime_of("sweperf_150u_c3hm192metal_microvm", {}), "microvm")
        self.assertEqual(goodput.runtime_of("sweperf_astropy_60_users", {}), "gvisor")
        self.assertEqual(goodput.runtime_of("x", {"sandbox_class": "microvm"}), "microvm")

    def test_cli_compute_writes_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp) / "run", task(0), {"start_ts": T0, "end_ts": T0 + 20})
            out = Path(tmp) / "out"
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(goodput.main(["compute", str(run), "--out", str(out)]), 0)
            self.assertEqual(json.loads((out / "goodput.json").read_text())["version"], 1)
            self.assertEqual(len((out / "goodput.jsonl").read_text().splitlines()), 2)


class BaselineTest(unittest.TestCase):
    def run_baseline(self, runs: list[Path], *extra: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = goodput.main(["baseline", *map(str, runs), *extra])
        return rc, out.getvalue(), err.getvalue()

    def test_entry_from_two_runs(self):
        def task_cel(median_ms: str, count: str) -> list[dict]:
            return [{"timestamp": "", "tag": "abc", "test_name": "sweperf_test", "metric": "actor_TaskCEL",
                     "measurements": {"median_response_time": median_ms, "request_count": count}}]
        meta = {"start_ts": T0, "end_ts": T0 + 100}
        failed = cycle(40, 1, 2.0) + [line(43, parse.MSG_START, cycle=2), line(43.5, parse.MSG_RESUME_FAIL, cycle=2)]
        failed += cycle(44, 2, 30.0) + cycle(80, 3, 1.0) + cycle(82, 4, 1.0) + [line(84, parse.MSG_FINISHED_REUSED)]
        with tempfile.TemporaryDirectory() as tmp:
            a = write_run(Path(tmp) / "a", task(0) + task(12) + failed, meta, stats=task_cel("6000", "1"))
            b = write_run(Path(tmp) / "b", task(0), meta, stats=task_cel("9000", "3"))
            rc, out, _ = self.run_baseline([a, b])
        self.assertEqual(rc, 0)
        entry = json.loads(out)
        # 3 clean tasks of 8000 ms; the task with a failed resume is left out
        self.assertEqual(entry, {"runs": 2, "tasks": 3, "runtime": "gvisor", "machine_type": "c3-highcpu-88",
                                 "template": "", "format": parse.FORMAT_REUSED_ACTOR, "wall_ref_ms": 8000.0,
                                 "task_cel_ref_ms": 8250.0})  # (6000*1 + 9000*3) / 4

    def test_runs_that_disagree(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = write_run(Path(tmp) / "a", task(0), {})
            b = write_run(Path(tmp) / "b", task(0), {}, trial={"machine_type": "other"})
            rc, _, err = self.run_baseline([a, b])
            self.assertEqual(rc, 1)
            self.assertIn("machine_type", err)
            rc, out, _ = self.run_baseline([a, b], "--machine-type", "c3-highcpu-88")
            self.assertEqual((rc, json.loads(out)["machine_type"]), (0, "c3-highcpu-88"))

    def test_no_clean_tasks(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = write_run(Path(tmp) / "a", cycle(0, 1, 1.0), {})
            rc, _, err = self.run_baseline([a])
        self.assertEqual(rc, 1)
        self.assertIn("no finished task", err)


def cli(*argv: str) -> tuple[int, str, str]:
    """goodput.main's exit code, stdout and stderr."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = goodput.main(list(argv))
    return rc, out.getvalue(), err.getvalue()


class RecordParseTest(unittest.TestCase):
    def test_record_line(self):
        ev = parse.parse_line(RECORD_LINE)
        self.assertEqual((ev.msg, ev.actor, ev.user),
                         (parse.MSG_TASK_RECORD, "sb-0c3de0a1-7f6e-4b8a-9d2c-1e5f00a1b2c3", 42))
        rec = ev.record
        self.assertEqual((rec.template, rec.cycles, rec.steps, rec.retries, rec.failed, rec.fail_step, rec.fail_cycle),
                         ("sweperf-astropy-7336", 4, 60, 0, False, "", None))
        self.assertEqual((rec.cel_ms, rec.wall_ms, rec.resume_max_ms, rec.ttfe_max_ms, rec.startup_ms, rec.think_ms),
                         (8612.25, 15890.7, 712.3, 801.6, 2311.4, 21003.9))
        self.assertAlmostEqual(rec.end, parse.parse_time("2026-10-18T19:02:11.524107Z"))
        self.assertAlmostEqual(rec.task_start, parse.parse_time("2026-10-18T19:01:30.1Z"))
        self.assertEqual(parse.log_format([parse.parse_line(REAL["finished_reused"]), ev]), parse.FORMAT_TASK_RECORD)

    def test_failed_record_without_ttfe(self):
        rec = parse.parse_line(record(5, fail_step="execute")).record
        self.assertIsNone(rec.ttfe_max_ms)
        self.assertEqual((rec.failed, rec.fail_step, rec.fail_cycle), (True, "execute", 2))

    def test_unreadable_end_falls_back_to_the_line_time(self):
        j = json.loads(record(5)[len(parse.LOG_PREFIX):])
        j["end"] = "2026-10-18T19:02:11+02:00"
        ev = parse.parse_line(parse.LOG_PREFIX + json.dumps(j))
        self.assertEqual(ev.record.end, ev.t)

    def test_startup_failure_line_with_user(self):
        ev = parse.parse_line(ONSTART_USER_LINE)
        self.assertEqual((ev.actor, ev.user), ("sb-0c3de0a1-7f6e-4b8a-9d2c-1e5f00a1b2c4", 12))


class RecordTaskTest(unittest.TestCase):
    def test_tasks_from_records_and_failure_lines(self):
        logs = cycle(1, 1, 2.0, sb(1))  # cycle lines aren't paired in a run with task records
        logs.append(record(10, sb(1)))  # finished
        logs += [line(11, parse.MSG_EXEC_FAIL, sb(2), cycle=2, err="timed out"), record(12, sb(2), fail_step="execute")]
        logs.append(line(13, parse.MSG_RESUME_FAIL, sb(3), cycle=3))  # never finishes
        logs += cycle(14, 1, 2.0, sb(4))  # in flight at the cut-off without a failure: no task
        logs.append(line(15, parse.MSG_SUSPEND_FAIL, sb(5), err="x"))  # its record comes after the cut-off
        logs.append(line(16, ONSTART, actor=sb(6), user=3, err="pollLiveness: failed"))
        logs.append(line(20, parse.MSG_SHUTDOWN, actor=""))
        logs += [record(21, sb(5), fail_step="suspend"), line(22, parse.MSG_RESUME_FAIL, sb(7), cycle=1),
                 record(23, sb(8)), line(24, ONSTART, actor=sb(9), user=4, err="canceled")]
        events = [e for e in (parse.parse_line(x) for x in logs) if e]
        cut, source = classify.cutoff(events, None, None)
        self.assertEqual((cut, source), (T0 + 20, "shutdown_marker"))
        tasks, startup = classify.build_record_tasks(events, cut)
        self.assertEqual([(t.actor, t.index, t.finished) for t in tasks],
                         [(sb(1), 0, True), (sb(2), 0, True), (sb(3), 0, False), (sb(5), 0, False)])
        self.assertEqual([(s.actor, s.t, s.user) for s in startup], [(sb(6), T0 + 16, 3)])
        self.assertEqual((tasks[0].wall_ms, tasks[0].retries, tasks[0].start, tasks[0].end),
                         (12000.0, 0, T0 - 20, T0 + 10))
        self.assertEqual([classify.classify(t, STRICT, None) for t in tasks],
                         [(classify.GOOD, []), (classify.BAD, [classify.REASON_EXECUTE]),
                          (classify.BAD, [classify.REASON_RESUME]), (classify.BAD, [classify.REASON_SUSPEND])])


class RecordGateTest(unittest.TestCase):
    BASE = {"task_cel_ref_ms": 8000, "wall_ref_ms": 10000}
    GOOD, BAD = classify.GOOD, classify.BAD

    def test_g1_from_the_record(self):
        self.assertEqual(classify.classify(record_task(fail_step="execute"), RELAXED, None),
                         (self.BAD, [classify.REASON_EXECUTE]))
        self.assertEqual(classify.classify(record_task(fail_step="resume"), RELAXED, None),
                         (self.BAD, [classify.REASON_RESUME]))
        t = record_task()
        t.record.failed = True  # failed without naming the step
        self.assertEqual(classify.classify(t, RELAXED, None), (self.BAD, [classify.REASON_FAILED]))

    def test_g2_resume_and_ttfe(self):
        self.assertEqual(classify.classify(record_task(resume=1500.0, ttfe=1000.0), STRICT, None), (self.GOOD, []))
        self.assertEqual(classify.classify(record_task(resume=1500.5), STRICT, None),
                         (self.BAD, [classify.REASON_SLOW_RESUME]))
        self.assertEqual(classify.classify(record_task(ttfe=1000.5), STRICT, None),
                         (self.BAD, [classify.REASON_SLOW_TTFE]))
        self.assertEqual(classify.classify(record_task(resume=9000.0, ttfe=9000.0), RELAXED, None), (self.GOOD, []))
        # a record without a TTFE isn't checked for it
        self.assertEqual(classify.classify(record_task(resume=100.0), STRICT, None), (self.GOOD, []))

    def test_g2_resume_bound_on_local_ssd(self):
        lssd = classify.resolve_mode(STRICT, "c4-highmem-32-lssd")
        self.assertEqual((lssd["resume_max_s"], STRICT["resume_max_s"]), (0.5, 1.5))
        self.assertIs(classify.resolve_mode(STRICT, "c4-highmem-32"), STRICT)
        self.assertIs(classify.resolve_mode(RELAXED, "c4-highmem-32-lssd"), RELAXED)
        self.assertEqual(classify.classify(record_task(resume=700.0), lssd, None),
                         (self.BAD, [classify.REASON_SLOW_RESUME]))
        self.assertEqual(classify.classify(record_task(resume=700.0), STRICT, None), (self.GOOD, []))

    def test_g3_on_cel_not_wall(self):
        slow_cel = record_task(cel=12800.0, wall=10000.0)  # 1.6x the reference CEL
        slow_wall = record_task(cel=8000.0, wall=50000.0)  # 5x the reference wall, the reference CEL
        self.assertEqual(classify.classify(slow_cel, STRICT, self.BASE), (self.BAD, [classify.REASON_SLOW]))
        self.assertEqual(classify.classify(slow_cel, RELAXED, self.BASE), (self.GOOD, []))
        self.assertEqual(classify.classify(slow_wall, STRICT, self.BASE), (self.GOOD, []))
        self.assertEqual(classify.classify(slow_cel, STRICT, {"wall_ref_ms": 1000}), (self.GOOD, []))
        self.assertEqual([classify.g3_basis(True, b) for b in (self.BASE, {"wall_ref_ms": 1000}, None)],
                         ["cel", "skipped", "skipped"])
        self.assertEqual(classify.g3_basis(False, self.BASE), "wall")

    def test_g2_and_g3_apply_after_g1_and_together(self):
        self.assertEqual(classify.classify(record_task(cel=20000.0, resume=3000.0), STRICT, self.BASE),
                         (self.BAD, [classify.REASON_SLOW_RESUME, classify.REASON_SLOW]))
        self.assertEqual(classify.classify(record_task(cel=20000.0, resume=3000.0, fail_step="suspend"), STRICT,
                                           self.BASE), (self.BAD, [classify.REASON_SUSPEND]))

    def test_optional_wall_check(self):
        wall_mode = dict(STRICT, g3_wall=True)
        slow_wall = record_task(cel=8000.0, wall=50000.0)  # the reference CEL, 5x the reference wall
        self.assertEqual(classify.classify(slow_wall, STRICT, self.BASE), (self.GOOD, []))
        self.assertEqual(classify.classify(slow_wall, wall_mode, self.BASE), (self.BAD, [classify.REASON_SLOW_WALL]))
        self.assertEqual(classify.classify(record_task(cel=12800.0, wall=50000.0), wall_mode, self.BASE),
                         (self.BAD, [classify.REASON_SLOW, classify.REASON_SLOW_WALL]))
        self.assertEqual(classify.classify(record_task(wall=15000.0), wall_mode, self.BASE), (self.GOOD, []))  # 1.5x
        self.assertEqual(classify.classify(record_task(wall=50000.0, fail_step="execute"), wall_mode, self.BASE),
                         (self.BAD, [classify.REASON_EXECUTE]))
        # without task records G3 compares wall time already, so there is no second wall reason
        t = classify.Task(actor=ACTOR, index=0, start=T0, end=T0 + 30, cycle_ms={1: 50000.0})
        self.assertEqual(classify.classify(t, wall_mode, self.BASE), (self.BAD, [classify.REASON_SLOW]))
        self.assertEqual([classify.g3_wall_basis(m, rec, b) for m, rec, b in (
            (STRICT, True, self.BASE), (wall_mode, True, self.BASE), (wall_mode, True, {"task_cel_ref_ms": 1}),
            (wall_mode, False, self.BASE))], ["off", "on", "skipped", "n/a"])


class RecordMetricsTest(unittest.TestCase):
    def test_eta_infra_uses_every_finished_task(self):
        rows = [{"status": s, "end": 10, "first_failure_t": None, "reasons": [], "wall_ms": w}
                for s, w in ((classify.GOOD, 10000.0), (classify.BAD, 40000.0), (classify.BAD, 50000.0))]
        m = metrics.window_metrics(rows, [], (0, 100), {}, {"task_cel_ref_ms": 8000})
        self.assertEqual((m["good_wall_ms_p50"], m["eta_infra"]), (10000.0, 0.2))  # 8000 / median wall 40000

    def test_ratios_use_each_rows_own_baseline(self):
        def row(wall, cel, wall_ref, cel_ref, fail_step=""):
            return {"status": classify.BAD if fail_step else classify.GOOD, "end": 10, "first_failure_t": None,
                    "reasons": [fail_step] if fail_step else [], "wall_ms": wall, "cel_ms": cel,
                    "fail_step": fail_step, "wall_ref_ms": wall_ref, "task_cel_ref_ms": cel_ref}
        rows = [row(20000.0, 9000.0, 10000, 6000), row(30000.0, 12000.0, 10000, 6000),
                row(5000.0, 1000.0, 10000, 6000, classify.REASON_EXECUTE)]
        m = metrics.window_metrics(rows, [], (0, 100), {}, {"wall_ref_ms": 1, "task_cel_ref_ms": 1})
        # the wall ratio covers every finished task, the CEL slowdown only those that passed G1
        self.assertEqual((m["wall_ratio_p50"], m["cel_slowdown_p50"], m["cel_slowdown_p90"]), (2.0, 1.75, 1.95))
        self.assertEqual(m["eta_infra"], 0.3)  # 6000 / median wall 20000
        rows.append(row(10000.0, 7000.0, None, None))  # a task with no baseline for its template
        self.assertIsNone(metrics.window_metrics(rows, [], (0, 100), {}, None)["eta_infra"])


class RecordComputeTest(unittest.TestCase):
    BASE_ANY = {"runtime": "gvisor", "machine_type": "c3-highcpu-88", "format": parse.FORMAT_TASK_RECORD,
                "task_cel_ref_ms": 1000, "wall_ref_ms": 1000}
    BASE_A = dict(BASE_ANY, template="tmpl-a", task_cel_ref_ms=8000, wall_ref_ms=10000)
    REF = {"name": "ref", "runtime": "gvisor", "machine_type": "n2d-standard-64", "format": parse.FORMAT_TASK_RECORD,
           "template": "tmpl-a", "task_cel_ref_ms": 4000}
    META = {"test_name": "sweperf_proto", "start_ts": T0, "end_ts": T0 + 60, "steady_start_ts": T0,
            "steady_end_ts": T0 + 30}

    def test_record_run_end_to_end(self):
        logs = [line(2, ONSTART, actor=sb(7), user=9, err="pollLiveness: failed")]
        logs += cycle(3, 1, 2.0, sb(1))
        logs.append(record(10, sb(1), cel=8000.0, wall=12000.0, resume=600.0, ttfe=700.0))  # good
        logs.append(record(12, sb(2), cel=12800.0, wall=20000.0, resume=1600.0))  # strict: slow_resume, slow
        logs += [line(13, parse.MSG_EXEC_FAIL, sb(3), cycle=2, err="timed out"),
                 record(14, sb(3), cel=4000.0, wall=9000.0, resume=500.0, fail_step="execute")]
        logs.append(line(16, parse.MSG_RESUME_FAIL, sb(4), cycle=3))  # never finishes
        logs.append(record(18, sb(5), cel=8000.0, wall=14000.0, resume=0.0))  # every resume was a no-op
        logs.append(line(20, parse.MSG_SHUTDOWN, actor=""))
        logs += [line(20.5, parse.MSG_RESUME_FAIL, sb(6), cycle=2), record(21, sb(6))]  # teardown
        config = dict(CONFIG, baselines=[self.BASE_ANY, self.BASE_A, self.REF], reference_baseline="ref")
        with tempfile.TemporaryDirectory() as tmp:
            r = goodput.compute(write_run(Path(tmp), logs, self.META), config)
        self.assertEqual((r["run"]["log_format"], r["run"]["template"], r["valid"], r["invalid_reason"]),
                         (parse.FORMAT_TASK_RECORD, "tmpl-a", True, ""))
        self.assertEqual(r["cutoff"], {"ts": T0 + 20, "source": "shutdown_marker"})
        self.assertEqual(r["baseline"], self.BASE_A)  # the template's own baseline wins
        self.assertEqual((r["reference_baseline"], r["abs_speed_ratio"]), ("ref", 2.0))  # 8000 / 4000
        self.assertEqual(r["totals"], {"tasks_finished": 4, "tasks_unfinished": 1, "startup_failure_attempts": 1,
                                       "actors": 5})
        strict, relaxed = r["modes"]["strict"], r["modes"]["relaxed"]
        self.assertEqual((strict["g2"], strict["g2_resume_max_s"], strict["g2_ttfe_max_s"], strict["g3_basis"]),
                         ("on", 1.5, 1.0, "cel"))
        self.assertEqual((relaxed["g2"], relaxed["g3_basis"]), ("off", "cel"))
        st = strict["steady"]
        self.assertEqual((st["tasks_done"], st["tasks_good"], st["tasks_bad"], st["tasks_unfinished_bad"]),
                         (4, 2, 2, 1))
        self.assertEqual(st["bad_reasons"], {"execute_failed": 1, "resume_failed": 1, "slow": 1, "slow_resume": 1})
        self.assertEqual(st["goodput_ratio_pct"], 40.0)  # 2 good / (4 done + 1 unfinished bad)
        self.assertEqual((st["good_tasks_per_hr"], st["done_tasks_per_hr"], st["good_tasks_per_vcpu_hr"]),
                         (240.0, 480.0, 2.4))
        self.assertEqual((st["cel_slowdown_p50"], st["cel_slowdown_p90"]), (1.0, 1.48))  # without the failed task
        self.assertEqual(st["wall_ratio_p50"], 1.3)  # every finished task
        self.assertEqual(st["eta_infra"], 0.6154)  # 8000 / median wall 13000
        self.assertEqual((st["resume_max_ms_p50"], st["resume_max_ms_p90"], st["ttfe_max_ms_p50"]),
                         (600.0, 1400.0, 700.0))
        rl = relaxed["steady"]
        self.assertEqual((rl["tasks_good"], rl["goodput_ratio_pct"], rl["bad_reasons"]),
                         (3, 60.0, {"execute_failed": 1, "resume_failed": 1}))
        row = next(t for t in r["tasks"] if t["actor"] == sb(3))
        self.assertEqual((row["fail_step"], row["fail_cycle"], row["cel_ms"], row["template"], row["status_by_mode"]),
                         ("execute_failed", 2, 4000.0, "tmpl-a", {"relaxed": "bad", "strict": "bad"}))
        meas = goodput.jsonl_rows(r)[1]["measurements"]
        self.assertEqual((meas["g2"], meas["valid"], meas["abs_speed_ratio"], meas["g2_resume_max_s"]),
                         ("on", "true", "2.0", "1.5"))

    def test_mode_with_the_optional_wall_gate(self):
        logs = [record(10, sb(1), cel=8000.0, wall=12000.0), record(12, sb(2), cel=8000.0, wall=150000.0),
                line(20, parse.MSG_SHUTDOWN, actor="")]
        modes = dict(CONFIG["modes"], strict_wall=dict(STRICT, g3_wall=True))
        with tempfile.TemporaryDirectory() as tmp:
            r = goodput.compute(write_run(Path(tmp), logs, self.META), dict(CONFIG, baselines=[self.BASE_A],
                                                                            modes=modes))
        strict, wall = r["modes"]["strict"], r["modes"]["strict_wall"]
        self.assertEqual((strict["g3_wall"], wall["g3_wall"], wall["g3_basis"]), ("off", "on", "cel"))
        self.assertEqual((strict["steady"]["tasks_good"], wall["steady"]["tasks_good"], wall["steady"]["bad_reasons"]),
                         (2, 1, {"slow_wall": 1}))  # 150000 / 10000 > 1.5
        self.assertEqual([(x["metric"], x["measurements"]["g3_wall"]) for x in goodput.jsonl_rows(r)],
                         [("goodput_relaxed", "off"), ("goodput_strict", "off"), ("goodput_strict_wall", "on")])
        rows, _ = summarize.summarize([r])
        per_run = summarize.run_rows([r])
        self.assertEqual((rows[0]["strict_wall_tasks_good_median"], rows[0]["strict_wall_bad_slow_wall_max"],
                          per_run[0]["strict_wall_goodput_ratio_pct"], per_run[0]["strict_bad_slow_wall"]),
                         (1.0, 1.0, 50.0, 0))

    def test_local_ssd_machine_type_tightens_the_resume_bound(self):
        trial = {"machine_type": "c4-highmem-32-lssd", "allocatable_cores": "32"}
        logs = [record(10, resume=700.0), line(20, parse.MSG_SHUTDOWN, actor="")]
        with tempfile.TemporaryDirectory() as tmp:
            r = goodput.compute(write_run(Path(tmp), logs, self.META, trial=trial), CONFIG)
        strict = r["modes"]["strict"]
        self.assertEqual((strict["g2_resume_max_s"], strict["g3_basis"], strict["steady"]["bad_reasons"]),
                         (0.5, "skipped", {"slow_resume": 1}))
        self.assertEqual(r["modes"]["relaxed"]["steady"]["tasks_good"], 1)

    def test_each_task_uses_the_baseline_for_its_template(self):
        logs = [record(10, sb(1)), record(11, sb(2), cel=12800.0), record(12, sb(3), cel=99999.0, template="tmpl-b"),
                line(20, parse.MSG_SHUTDOWN, actor="")]
        with tempfile.TemporaryDirectory() as tmp:
            r = goodput.compute(write_run(Path(tmp), logs, self.META), dict(CONFIG, baselines=[self.BASE_A]))
        self.assertEqual((r["run"]["template"], r["run"]["templates"]), ("tmpl-a", {"tmpl-a": 2, "tmpl-b": 1}))
        self.assertEqual(r["baselines_by_template"], {"tmpl-a": self.BASE_A, "tmpl-b": None})
        strict = r["modes"]["strict"]
        self.assertEqual((strict["g3_basis"], strict["steady"]["bad_reasons"], strict["steady"]["tasks_good"]),
                         ("cel+skipped", {"slow": 1}, 2))
        self.assertIsNone(strict["steady"]["eta_infra"])  # the tasks don't share one reference


class ValidityTest(unittest.TestCase):
    def test_rule(self):
        self.assertEqual(classify.run_validity(0, 0), (False, "no finished tasks"))
        self.assertEqual(classify.run_validity(3, 30), (True, ""))
        self.assertEqual(classify.run_validity(3, 31), (False, "31 startup failures for 3 finished tasks"))
        self.assertEqual(classify.run_validity(3, 7, 2), (False, "7 startup failures for 3 finished tasks"))

    def test_run_that_never_started_a_task(self):
        """Like a run whose every start timed out reaching the API, so no task ran."""
        logs = [line(i, ONSTART, actor=sb(i), user=1, err="ensureAtespace: context deadline exceeded")
                for i in range(1, 40)]
        logs.append(line(50, parse.MSG_SHUTDOWN, actor=""))
        with tempfile.TemporaryDirectory() as tmp:
            r = goodput.compute(write_run(Path(tmp), logs, {"start_ts": T0, "end_ts": T0 + 60}), CONFIG)
        self.assertEqual((r["valid"], r["invalid_reason"], r["totals"]["startup_failure_attempts"]),
                         (False, "no finished tasks", 39))
        self.assertIn("INVALID: no finished tasks", goodput.summary_line(r))
        self.assertEqual(goodput.jsonl_rows(r)[0]["measurements"]["valid"], "false")

    def test_mostly_failed_starts(self):
        onstart = [line(1 + 0.1 * i, ONSTART, actor=sb(i), user=1, err="x") for i in range(11)]
        with tempfile.TemporaryDirectory() as tmp:
            many = goodput.compute(write_run(Path(tmp) / "a", onstart + [record(10)], {}), CONFIG)
            some = goodput.compute(write_run(Path(tmp) / "b", onstart[:10] + [record(10)], {}), CONFIG)
        self.assertEqual((many["valid"], many["invalid_reason"]), (False, "11 startup failures for 1 finished tasks"))
        self.assertTrue(some["valid"])

    def test_log_without_task_lines_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(Path(tmp), ["[locust-master] not a task log line", REAL["traced_span"], REAL["quit"]], {})
            with self.assertRaisesRegex(ValueError, "no sweperf task log lines"):
                goodput.compute(run, CONFIG)


class RecordBaselineTest(unittest.TestCase):
    def test_entry_from_clean_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = write_run(Path(tmp) / "a", [record(10, sb(1), cel=8000.0, wall=12000.0, resume=600.0, ttfe=700.0),
                                            record(20, sb(2), cel=9000.0, wall=13000.0, resume=800.0),
                                            record(30, sb(3), cel=10.0, wall=99999.0, fail_step="resume")], {})
            b = write_run(Path(tmp) / "b", [record(10, sb(4), cel=10000.0, wall=15000.0, resume=1000.0,
                                                   ttfe=900.0)], {})
            rc, out, _ = cli("baseline", str(a), str(b), "--name", "gvisor-c3-a")
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(out), {
            "runs": 2, "tasks": 3, "name": "gvisor-c3-a", "runtime": "gvisor", "machine_type": "c3-highcpu-88",
            "template": "tmpl-a", "format": parse.FORMAT_TASK_RECORD, "task_cel_ref_ms": 9000.0,
            "wall_ref_ms": 13000.0, "resume_max_ref_ms": 800.0, "ttfe_max_ref_ms": 800.0})

    def test_template_from_the_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = write_run(Path(tmp) / "a", [record(10, sb(1)), record(20, sb(2), cel=5000.0, template="tmpl-b")], {})
            rc, _, err = cli("baseline", str(a))
            self.assertEqual(rc, 1)
            self.assertIn("template", err)
            rc, out, _ = cli("baseline", str(a), "--template", "tmpl-b")
        entry = json.loads(out)
        self.assertEqual((rc, entry["template"], entry["tasks"], entry["task_cel_ref_ms"]), (0, "tmpl-b", 1, 5000.0))


class SummaryTest(unittest.TestCase):
    def test_summary_of_runs_in_a_tree(self):
        meta = {"start_ts": T0, "end_ts": T0 + 60, "steady_start_ts": T0, "steady_end_ts": T0 + 36}
        runs = {  # test name -> each run's log lines
            "sweperf_a": [[record(10, sb(1)), record(12, sb(2))],
                          [record(10, sb(1)), record(12, sb(2), resume=2000.0), record(14, sb(3)), record(16, sb(4))],
                          [line(1, ONSTART, actor=sb(9), user=1, err="ensureAtespace: timeout")]],
            "sweperf_b": [task(0)],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root, out = Path(tmp) / "results", Path(tmp) / "summary"
            for test, logs_per_run in runs.items():
                for i, logs in enumerate(logs_per_run):
                    run = write_run(Path(tmp) / "runs" / test / str(i), logs, meta)
                    ident = {"test_name": test, "run_date": f"2026-10-1{i}"}
                    goodput.write_outputs(goodput.compute(run, CONFIG, ident), root / test / str(i))
            rc, stdout, _ = cli("summary", str(root), "--out", str(out))
            self.assertEqual(rc, 0)
            self.assertIn("4 runs in 2 tests", stdout)
            with open(out / summarize.CSV_NAME, encoding="utf-8") as f:
                rows = {r["test_name"]: r for r in csv.DictReader(f)}
            with open(out / summarize.RUNS_CSV_NAME, encoding="utf-8") as f:
                per_run = list(csv.DictReader(f))
            md = (out / summarize.MD_NAME).read_text(encoding="utf-8")
        a, b = rows["sweperf_a"], rows["sweperf_b"]
        self.assertEqual((a["runs"], a["runs_valid"], a["runs_invalid"]), ("3", "2", "1"))
        self.assertEqual((a["first_run_date"], a["last_run_date"], a["log_format"], a["template"]),
                         ("2026-10-10", "2026-10-12", parse.FORMAT_TASK_RECORD, "tmpl-a"))
        # strict: 2 of 2 and 3 of 4 good (one slow resume); relaxed: all good
        self.assertEqual((a["strict_tasks_good_median"], a["strict_tasks_good_min"], a["strict_tasks_good_max"]),
                         ("2.5", "2.0", "3.0"))
        self.assertEqual((a["strict_goodput_ratio_pct_median"], a["relaxed_goodput_ratio_pct_median"]),
                         ("87.5", "100.0"))
        self.assertEqual(a["strict_good_tasks_per_hr_median"], "250.0")  # 2 and 3 good tasks in a 36 s window
        self.assertEqual((a["strict_bad_slow_resume_max"], b["strict_bad_slow_resume_max"]), ("1.0", "0.0"))
        self.assertEqual((a["strict_resume_max_ms_p50_median"], b["strict_resume_max_ms_p50_median"]), ("600.0", ""))
        labels = {"test_name", "first_run_date", "last_run_date", *summarize.IDENTITY}
        for r in rows.values():  # every other column is a number or empty
            for k, v in r.items():
                if k not in labels and v:
                    float(v)
        self.assertIn("| sweperf_a | 2 / 1 |", md)
        self.assertIn("## Invalid runs", md)
        self.assertIn("| sweperf_a | 2026-10-12 |", md)
        # one row per run, valid or not, with the same metrics unaggregated
        self.assertEqual([(r["test_name"], r["run_date"], r["valid"]) for r in per_run],
                         [("sweperf_a", "2026-10-10", "1"), ("sweperf_a", "2026-10-11", "1"),
                          ("sweperf_a", "2026-10-12", "0"), ("sweperf_b", "2026-10-10", "1")])
        self.assertEqual((per_run[1]["strict_tasks_good"], per_run[1]["strict_bad_slow_resume"],
                          per_run[1]["relaxed_good_tasks_per_hr"], per_run[2]["invalid_reason"]),
                         ("3", "1", "400.0", "no finished tasks"))
        self.assertEqual({k[:-len("_median")] for k in a if k.endswith("_median")},
                         {k for k in per_run[0] if k.startswith(("strict_", "relaxed_", "abs_speed_ratio"))})

    def test_goodput_json_without_a_valid_flag(self):
        old = {"run": {"test_name": "t", "run_date": "2026-10-01"}, "modes": {},
               "totals": {"tasks_finished": 0, "startup_failure_attempts": 9}}
        rows, invalid = summarize.summarize([old])
        self.assertEqual((rows[0]["runs_invalid"], invalid[0]["reason"]), (1, "no finished tasks"))


if __name__ == "__main__":
    unittest.main()
