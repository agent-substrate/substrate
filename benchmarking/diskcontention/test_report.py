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

"""Tests for report.py. No cluster needed:

    cd benchmarking/diskcontention && python3 -m pytest -q .
"""

import contextlib
import datetime
import io
import json
import os
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import report  # noqa: E402

START = 1_800_000_000
WARMUP = 180
END = START + 900
TRACE_HEADER = "time\tname\tduration_ms\tlatency_source\ttrace_id\terr\n"


def iso(t: float) -> str:
    """Formats unix seconds the way kubectl and the runner do: nanoseconds, Z."""
    dt = datetime.datetime.fromtimestamp(t, datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond:06d}123Z"


def trace(t: float, name: str, ms: float, err: str = "") -> str:
    return f"{iso(t)}\t{name}\t{ms}\tclient\t{'0' * 32}\t{err}\n"


def write_run(root: str, arm: str, rows: list[str], flags: str = "", users: int = 3,
              tag: str = "", collect: dict | None = None, stats: list[dict] | None = None,
              runtime: str = "microvm", active_before: str = "0") -> str:
    """Writes one run_arm.sh result directory and returns its path."""
    tag = tag or f"dc-{arm}"
    d = os.path.join(root, f"dc_{runtime}_{arm}", tag)
    os.makedirs(d)
    meta = {"name": f"dc_{runtime}_{arm}", "tag": tag, "runtime": runtime, "arm": arm,
            "users": users, "duration": "15m", "start": START, "end": END, "warmup": WARMUP,
            "active_actors_before": active_before, "flags": flags}
    with open(os.path.join(d, "meta.json"), "w") as f:
        json.dump(meta, f)
    with open(os.path.join(d, "traces.txt"), "w") as f:
        f.write(TRACE_HEADER)
        f.writelines(rows)
    if collect is not None:
        with open(os.path.join(d, "collect.json"), "w") as f:
            json.dump(collect, f)
    if stats is not None:
        with open(os.path.join(d, "stats.jsonl"), "w") as f:
            f.writelines(json.dumps(s) + "\n" for s in stats)
    return d


def probe_rows(n: int, resume_ms: float, suspend_ms: float) -> list[str]:
    """n resume and n suspend rows in the window, spread over [x, 2x)."""
    rows = []
    for i in range(n):
        t = START + WARMUP + 1 + i
        rows.append(trace(t, "ProbeResumeActor", resume_ms * (1 + i / n)))
        rows.append(trace(t, "ProbeSuspendActor", suspend_ms * (1 + i / n)))
    return rows


class ArmSortKeyTest(unittest.TestCase):
    def test_orders_baseline_caps_uncapped_nofsync_then_sweperf(self):
        arms = ["C64", "X", "Bmaxnf", "B25", "C", "A", "B100", "Bmax", "B25nf", "C32"]
        self.assertEqual(
            sorted(arms, key=report.arm_sort_key),
            ["A", "B25", "B100", "Bmax", "B25nf", "Bmaxnf", "C", "C32", "C64", "X"],
        )


class PctTest(unittest.TestCase):
    def test_empty(self):
        self.assertIsNone(report.pct([], 0.5))

    def test_interpolates_between_ranks(self):
        self.assertEqual(report.pct([4, 1, 3, 2], 0.5), 2.5)
        self.assertAlmostEqual(report.pct(list(range(1, 101)), 0.95), 95.05)

    def test_ends(self):
        xs = [5.0, 1.0, 9.0]
        self.assertEqual(report.pct(xs, 0), 1.0)
        self.assertEqual(report.pct(xs, 1), 9.0)
        self.assertEqual(report.pct([7.0], 0.99), 7.0)


class ParseTsTest(unittest.TestCase):
    def test_nanoseconds_are_trimmed(self):
        self.assertAlmostEqual(report.parse_ts("2027-01-15T08:00:00.123456789Z"),
                               datetime.datetime(2027, 1, 15, 8, 0, 0, 123456,
                                                 datetime.timezone.utc).timestamp())

    def test_without_fraction_and_with_offset(self):
        want = datetime.datetime(2027, 1, 15, 8, 0, tzinfo=datetime.timezone.utc).timestamp()
        self.assertEqual(report.parse_ts("2027-01-15T08:00:00Z"), want)
        self.assertEqual(report.parse_ts("2027-01-15T09:00:00.5+01:00"), want + 0.5)

    def test_trailing_zeros_trimmed(self):
        # RFC 3339 with nanoseconds drops trailing zeros: .5, .12345, .1234567.
        base = report.parse_ts("2027-01-15T08:00:00Z")
        self.assertAlmostEqual(report.parse_ts("2027-01-15T08:00:00.5Z"), base + 0.5)
        self.assertAlmostEqual(report.parse_ts("2027-01-15T08:00:00.12345Z"), base + 0.12345)
        self.assertAlmostEqual(report.parse_ts("2027-01-15T08:00:00.1234567Z"), base + 0.123456)

    def test_independent_of_the_local_time_zone(self):
        want = report.parse_ts("2027-01-15T08:00:00.123456789Z")
        old = os.environ.get("TZ")
        try:
            os.environ["TZ"] = "America/Los_Angeles"
            time.tzset()
            self.assertEqual(report.parse_ts("2027-01-15T08:00:00.123456789Z"), want)
        finally:
            if old is None:
                del os.environ["TZ"]
            else:
                os.environ["TZ"] = old
            time.tzset()

    def test_bad_input(self):
        self.assertIsNone(report.parse_ts(""))
        self.assertIsNone(report.parse_ts("yesterday"))


class FlagValueTest(unittest.TestCase):
    def test_lookup(self):
        flags = "--dc-probe-users 3 --dc-noise-mode disk --dc-noise-rate-mbps"
        self.assertEqual(report.flag_value(flags, "--dc-noise-mode"), "disk")
        self.assertEqual(report.flag_value(flags, "--mem-target", "512Mi"), "512Mi")
        # A flag at the end with no value falls back to the default.
        self.assertIsNone(report.flag_value(flags, "--dc-noise-rate-mbps"))


class LifecycleModeTest(unittest.TestCase):
    def test_ops_and_sections(self):
        suspend = {"runtime": "microvm", "flags": "--dc-probe-users 3"}
        pause = {"runtime": "microvm", "flags": "--dc-probe-users 3 --lifecycle-mode pause"}
        none = {"runtime": "gvisor", "flags": "--lifecycle-mode none"}
        self.assertEqual(report.probe_ops(suspend), ("ProbeResumeActor", "ProbeSuspendActor"))
        self.assertEqual(report.probe_ops(pause), ("ProbeResumeActor", "ProbePauseActor"))
        self.assertEqual(report.probe_ops(none), ("ProbeResumeActor",))
        self.assertEqual(report.section(suspend), "microvm")
        self.assertEqual(report.section(pause), "microvm (pause)")
        self.assertEqual(report.section(none), "gvisor (none)")


class LoadRunTest(unittest.TestCase):
    def test_window_failures_noise_and_inputs(self):
        ws, we = START + WARMUP, END - report.SHUTDOWN_TAIL_S
        rows = [
            trace(ws - 1, "ProbeResumeActor", 99999),  # warm-up
            trace(we + 1, "ProbeResumeActor", 99999),  # shutdown tail
            trace(ws + 10, "ProbeResumeActor", 100),
            trace(ws + 20, "ProbeResumeActor", 300),
            trace(ws + 30, "ProbeResumeActor", 200),
            trace(ws + 40, "ProbeSuspendActor", 50, err="rpc error: code = Unavailable"),
            trace(ws + 50, "NoiseWriteDisk", 80),
            trace(ws + 51, "NoiseWriteDisk", 90),
            trace(ws + 52, "NoiseWriteDisk", 70, err="short write"),
            trace(ws + 60, "ExecCommand", 10),
            trace(ws + 61, "GluttonPing", 1),
            "truncated\tline\n",
        ]
        with tempfile.TemporaryDirectory() as root:
            d = write_run(root, "B50", rows,
                          flags="--dc-noise-mode disk --dc-noise-write-size-bytes 1000000",
                          collect={"node": {"util_frac": 0.5}},
                          stats=[{"metric": "grpc_ProbeResumeActor", "measurements": {"request_count": 7}},
                                 {"metric": "grpc_Other", "measurements": {"request_count": 1}}])
            run = report.load_run(d)

        self.assertEqual(run["window_s"], we - ws)
        resume = run["ops"]["ProbeResumeActor"]
        self.assertEqual((resume["n"], resume["fail"]), (3, 0))
        self.assertEqual(resume["p50"], 200)
        suspend = run["ops"]["ProbeSuspendActor"]
        self.assertEqual((suspend["n"], suspend["fail"]), (0, 1))
        self.assertIsNone(suspend["p95"])
        self.assertEqual(suspend["errors"], ["rpc error: code = Unavailable"])
        self.assertEqual(run["noise"]["writes"], 2)
        self.assertEqual(run["noise"]["write_fail"], 1)
        self.assertAlmostEqual(run["noise"]["app_MBps"], 2 * 1.0 / (we - ws))
        # Rows that are neither probe, noise, nor glutton are the sweperf replay.
        self.assertEqual(run["noise"]["sweperf_ops"], 1)
        self.assertEqual(run["trace_rows_in_window"], 9)
        self.assertEqual(run["stats_counts"], {"ProbeResumeActor": 7})
        self.assertEqual(run["collect"], {"node": {"util_frac": 0.5}})

    def test_missing_meta(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(report.load_run(d))


class BootstrapRatioTest(unittest.TestCase):
    def test_too_few_samples(self):
        self.assertEqual(report.bootstrap_ratio([1.0] * 9, [1.0] * 50, 0.95), (None, None))

    def test_ci_brackets_the_ratio_and_is_repeatable(self):
        a = [float(x) for x in range(100, 200)]
        b = [2 * x for x in a]
        lo, hi = report.bootstrap_ratio(a, b, 0.95, iters=500)
        self.assertLessEqual(lo, 2.0)
        self.assertGreaterEqual(hi, 2.0)
        self.assertLess(hi - lo, 0.5)
        self.assertEqual(report.bootstrap_ratio(a, b, 0.95, iters=500), (lo, hi))


class MainTest(unittest.TestCase):
    def run_report(self, root: str, *extra: str) -> str:
        out = io.StringIO()
        with mock.patch.object(sys, "argv", ["report.py", root, *extra]), contextlib.redirect_stdout(out):
            self.assertEqual(report.main(), 0)
        return out.getvalue()

    def test_compares_arms_with_the_baseline(self):
        collect = {"node": {"write_MBps_avg": 120.0, "util_frac": 0.4},
                   "checkpoint_phases": {"worker_checkpoint": {"p95_s": 1.5}}}
        with tempfile.TemporaryDirectory() as root:
            write_run(root, "A", probe_rows(40, 1000, 2000), collect=collect)
            write_run(root, "B50", probe_rows(40, 2000, 4000), collect=collect, users=11,
                      flags="--dc-probe-users 3 --dc-noise-mode disk --dc-noise-rate-mbps 10 "
                            "--dc-noise-write-size-bytes 1000000")
            md = self.run_report(root, "--json", os.path.join(root, "summary.json"))
            with open(os.path.join(root, "summary.json")) as f:
                summary = json.load(f)

        self.assertIn("## microvm", md)
        lines = md.splitlines()
        b50 = next(line for line in lines if line.startswith("| B50 | 1 | ResumeActor |"))
        # The B50 resume p95 is twice the baseline's.
        self.assertIn("| 2.00 (", b50)
        self.assertIn("| B50 | 0.0 | 120.0 / – | 40% |", md)
        self.assertIn("| A | 1.50 | – |", md)
        # No noise writes, so the target of 10 MB/s for each of the 8 noise
        # VUs is missed.
        self.assertIn("microvm/B50/dc-B50: noise 0 MB/s vs target 80 MB/s", md)
        self.assertEqual(len(summary), 2)
        self.assertNotIn("samples", summary[0]["ops"]["ProbeResumeActor"])
        self.assertNotIn("Snapshot disk", md)

    def test_snapshot_disk_table(self):
        snap = {"device": "nvme0n4", "write_MBps_avg": 90.0, "util_frac": 0.25}
        with tempfile.TemporaryDirectory() as root:
            write_run(root, "A", probe_rows(40, 1000, 2000), collect={"node": {}})
            write_run(root, "C32", probe_rows(40, 1000, 2000), tag="dc-1",
                      collect={"node": {}, "snapshots_node": snap})
            write_run(root, "C32", probe_rows(40, 1000, 2000), tag="dc-2",
                      collect={"node": {}, "snapshots_node": {**snap, "write_MBps_avg": 110.0}})
            md = self.run_report(root)

        self.assertIn("### Snapshot disk, /var/lib/ate/snapshots", md)
        self.assertIn("| C32 | nvme0n4 | 100.0 / – | – | 25% | – | – |", md)
        self.assertNotIn("| A | nvme0n4", md)

    def test_quality_checks(self):
        with tempfile.TemporaryDirectory() as root:
            # A disk arm without noise VUs must not divide by zero.
            write_run(root, "B25", probe_rows(5, 1000, 2000), users=3, active_before="2",
                      flags="--dc-probe-users 3 --dc-noise-mode disk --dc-noise-rate-mbps 10")
            write_run(root, "C", probe_rows(5, 1000, 2000), tag="dc-c",
                      flags="--dc-noise-mode sweperf",
                      collect={"node": {"rootfs_free_frac_min": 0.12}})
            md = self.run_report(root)

        for want in (
            "microvm/B25/dc-B25: only 5 ProbeResumeActor samples in window (<30)",
            "microvm/B25/dc-B25: 2 actors active at start",
            "microvm/B25/dc-B25: collect.json missing",
            "microvm/C/dc-c: no sweperf operations recorded in window",
            "microvm/C/dc-c: boot disk free fell to 12%",
        ):
            self.assertIn(want, md)
        self.assertNotIn("vs target", md)

    def test_pause_runs_have_their_own_section(self):
        pause_flags = "--dc-probe-users 3 --lifecycle-mode pause --dc-noise-mode none"
        pause_rows = []
        for i in range(40):
            t = START + WARMUP + 1 + i
            pause_rows.append(trace(t, "ProbeResumeActor", 400 + i))
            pause_rows.append(trace(t, "ProbePauseActor", 800 + i))
        with tempfile.TemporaryDirectory() as root:
            write_run(root, "A", probe_rows(40, 1000, 2000), collect={"node": {}})
            write_run(root, "A", pause_rows, tag="dc-pause", flags=pause_flags, collect={"node": {}})
            md = self.run_report(root)

        self.assertIn("## microvm\n", md)
        self.assertIn("## microvm (pause)\n", md)
        pause_section = md.split("## microvm (pause)")[1]
        self.assertIn("| A | 1 | PauseActor | 40 |", pause_section)
        self.assertNotIn("SuspendActor", pause_section.split("## Quality checks")[0])
        # Each section's baseline is its own A run: suspend resumes take 1-2 s,
        # pause resumes about 0.4 s, and neither is pooled with the other.
        suspend_section = md.split("## microvm\n")[1].split("## microvm (pause)")[0]
        self.assertIn("| A | 1 | ResumeActor | 40 |", suspend_section)
        self.assertNotIn("PauseActor", suspend_section)
        self.assertNotIn("only 0", md)

    def test_no_runs(self):
        with tempfile.TemporaryDirectory() as root, \
                mock.patch.object(sys, "argv", ["report.py", root]), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(report.main(), 1)


if __name__ == "__main__":
    unittest.main()
