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

"""Parser and aggregation tests for phase_report. No cluster needed:

    python3 -m unittest discover -s benchmarking/analysis
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import phase_report  # noqa: E402


def parse(*records):
    out = phase_report.Parsed()
    for rec in records:
        phase_report.parse_line(json.loads(json.dumps(rec)), out)
    return out


def render(fn, *args, **kwargs):
    buf = io.StringIO()
    fn(*args, writer=lambda line: buf.write(line + "\n"), **kwargs)
    return buf.getvalue()


ATELET_RESTORE = {
    "time": "2026-09-23T10:00:05.500000000Z", "level": "INFO",
    "msg": "Restore timing breakdown",
    "ate.actor.uid": "uid-1", "ate.template.name": "swebench-astropy-7336",
    "ate.snapshot.scope": "full", "ate.snapshot.kind": "latest",
    "ate.actor.restore.duration.download": 2.4,
    "ate.actor.restore.duration.ateom_restore": 1.1,
    "ate.actor.restore.duration.total": 3.9,
}

ATEOM_RESTORE = {
    "time": "2026-09-23T10:00:05.400000000Z", "level": "INFO",
    "msg": "Restore timing breakdown",
    "ate.actor.uid": "uid-1", "ate.template.name": "swebench-astropy-7336",
    "ate.snapshot.scope": "full",
    "ateom.actor.restore.duration.vm_restore": 0.8,
    "ateom.actor.restore.duration.wakeup_probe": 0.2,
    "ateom.actor.restore.duration.total": 1.05,
}

ATELET_CHECKPOINT = {
    "time": "2026-09-23T10:01:00.000000000Z", "level": "INFO",
    "msg": "Checkpoint timing breakdown",
    "ate.actor.uid": "uid-1", "ate.template.name": "swebench-astropy-7336",
    "ate.snapshot.scope": "full", "ate.snapshot.kind": "latest",
    "ate.actor.checkpoint.duration.sandbox_assets": 0.01,
    "ate.actor.checkpoint.duration.ateom_checkpoint": 1.14,
    "ate.actor.checkpoint.duration.persist": 4.48,
    "ate.actor.checkpoint.duration.total": 6.0,
}

ATEOM_CHECKPOINT = {
    "time": "2026-09-23T10:00:55.000000000Z", "level": "INFO",
    "msg": "Checkpoint timing breakdown",
    "ate.actor.uid": "uid-1", "ate.template.name": "swebench-astropy-7336",
    "ate.snapshot.scope": "full",
    "ateom.actor.checkpoint.duration.prep": 0.04,
    "ateom.actor.checkpoint.duration.pause": 0.01,
    "ateom.actor.checkpoint.duration.snapshot": 0.5,
    "ateom.actor.checkpoint.duration.rootfs_upper": 0.9,
    "ateom.actor.checkpoint.duration.teardown": 0.2,
    "ateom.actor.checkpoint.duration.total": 1.2,
}

# The same actor's ateom restore record from an hour earlier: a different
# cycle, whose own atelet partner is missing from the dump.
ATEOM_RESTORE_STALE = dict(ATEOM_RESTORE, time="2026-09-23T09:00:05.400000000Z")

# One entry as `gcloud logging read --format json` writes it, trimmed to the
# fields that matter: the record under jsonPayload, slog's time promoted to
# the entry's timestamp, dotted keys kept as they are.
CLOUD_LOGGING_ENTRY = {
    "insertId": "m9xljnzmzppup1vj",
    "jsonPayload": {
        "ate.actor.checkpoint.duration.ateom_checkpoint": 1.024788329,
        "ate.actor.checkpoint.duration.persist": 4.18451256,
        "ate.actor.checkpoint.duration.sandbox_assets": 2.4358e-05,
        "ate.actor.checkpoint.duration.total": 5.385861508,
        "ate.actor.name": "sb-d790f7ed", "ate.actor.uid": "cc7a2ac7",
        "ate.atespace": "benchmark", "ate.sandbox.class": "microvm",
        "ate.snapshot.kind": "latest", "ate.snapshot.scope": "full",
        "ate.template.atespace": "benchmark-workloads", "ate.template.name": "glutton",
        "level": "INFO", "msg": "Checkpoint timing breakdown",
    },
    "logName": "projects/p/logs/stdout",
    "resource": {"labels": {"container_name": "atelet", "namespace_name": "ate-system"},
                 "type": "k8s_container"},
    "severity": "INFO",
    "timestamp": "2026-09-28T17:49:47.665836237Z",
}

FAILED = {
    "time": "2026-09-23T10:02:00Z", "level": "INFO",
    "msg": "Restore timing breakdown",
    "ate.actor.uid": "uid-2", "ate.snapshot.scope": "full",
    "error.type": "DeadlineExceeded",
    "ate.actor.restore.duration.download": 30.0,
    "ate.actor.restore.duration.total": 30.0,
}


class ParseTest(unittest.TestCase):
    def test_parses_both_layers_of_the_same_msg(self):
        out = parse(ATELET_RESTORE, ATEOM_RESTORE)
        self.assertEqual([(b.source, b.op) for b in out.breakdowns],
                         [("atelet", "restore"), ("ateom", "restore")])
        self.assertEqual(out.breakdowns[0].phases["download"], 2.4)
        self.assertEqual(out.breakdowns[1].phases["vm_restore"], 0.8)
        self.assertEqual(out.breakdowns[1].actor_uid, "uid-1")

    def test_nanosecond_timestamps_parse(self):
        out = parse(ATELET_RESTORE, FAILED)
        self.assertIsNotNone(out.breakdowns[0].ts)
        self.assertIsNotNone(out.breakdowns[1].ts)
        self.assertAlmostEqual(out.breakdowns[0].ts % 1, 0.5, places=6)
        self.assertIsNone(phase_report.parse_time("yesterday"))

    def test_ateom_checkpoint_parses(self):
        b = parse(ATEOM_CHECKPOINT).breakdowns[0]
        self.assertEqual((b.source, b.op), ("ateom", "checkpoint"))
        self.assertEqual(b.phases["rootfs_upper"], 0.9)

    def test_cloud_logging_entry_parses_with_the_entry_timestamp(self):
        b = parse(CLOUD_LOGGING_ENTRY).breakdowns[0]
        self.assertEqual((b.source, b.op), ("atelet", "checkpoint"))
        self.assertEqual(b.time, "2026-09-28T17:49:47.665836237Z")
        self.assertIsNotNone(b.ts)
        self.assertEqual((b.sandbox_class, b.kind, b.scope), ("microvm", "latest", "full"))
        self.assertAlmostEqual(b.phases["persist"], 4.18451256)

    def test_cloud_logging_export_file_is_a_json_array(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "export.json")
            second = dict(CLOUD_LOGGING_ENTRY, timestamp="2026-09-28T17:50:47.000000000Z")
            with open(path, "w") as f:
                json.dump([CLOUD_LOGGING_ENTRY, second], f, indent=2)
            out = phase_report.parse_files([path])
        self.assertEqual((out.lines_seen, len(out.breakdowns)), (2, 2))

    def test_ateom_records_take_class_and_kind_from_the_paired_atelet_record(self):
        atelet = dict(ATELET_RESTORE, **{"ate.sandbox.class": "gvisor"})
        out = parse(ATEOM_RESTORE, atelet)
        self.assertEqual(out.breakdowns[0].sandbox_class, "")  # not on the ateom record
        phase_report.join_layers(out.breakdowns)
        self.assertEqual((out.breakdowns[0].sandbox_class, out.breakdowns[0].kind), ("gvisor", "latest"))

    def test_an_unpaired_ateom_record_keeps_no_class(self):
        out = parse(ATEOM_RESTORE)
        phase_report.join_layers(out.breakdowns)
        self.assertEqual(out.breakdowns[0].sandbox_class, "")
        rows = phase_report.report_phases(out.breakdowns, lambda _: None)
        self.assertEqual({r["class"] for r in rows}, {"-"})
    def test_non_numeric_duration_is_skipped_not_fatal(self):
        rec = dict(ATELET_RESTORE, **{"ate.actor.restore.duration.download": None,
                                      "ate.actor.restore.duration.oci_unpack": "fast"})
        out = parse(rec)
        self.assertEqual(out.bad_values, 2)
        self.assertEqual(out.breakdowns[0].phases["total"], 3.9)
        self.assertNotIn("download", out.breakdowns[0].phases)

    def test_malformed_export_warns_instead_of_vanishing(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "export.json")
            with open(path, "w") as f:
                f.write('[{"jsonPayload": {"msg": "Checkpoint timing breakdown"')  # truncated
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                out = phase_report.parse_files([path])
        self.assertEqual(out.breakdowns, [])
        self.assertIn("export.json", err.getvalue())
        self.assertIn("not a JSON array", err.getvalue())

    def test_bracket_prefixed_dump_is_read_line_by_line(self):
        # A dump whose lines start with "[INFO]" also starts with "[", but
        # it is not an export; its records must not be lost.
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "pod.log")
            with open(path, "w") as f:
                f.write("[INFO] " + json.dumps(ATELET_RESTORE) + "\n"
                        "[2026-09-23 10:00:05] " + json.dumps(ATEOM_RESTORE) + "\n")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                out = phase_report.parse_files([path])
        self.assertEqual(len(out.breakdowns), 2)
        self.assertIn("reading it line by line", err.getvalue())

    def test_parse_lines_feeds_an_existing_parse(self):
        out = phase_report.parse_lines([json.dumps(ATELET_RESTORE), "not json", ""])
        self.assertEqual((out.lines_seen, len(out.breakdowns)), (1, 1))
        phase_report.parse_lines(["pod/x " + json.dumps(ATEOM_RESTORE)], out)
        self.assertEqual(len(out.breakdowns), 2)

    def test_prefixed_kubectl_lines_still_parse(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "pod.log")
            with open(path, "w") as f:
                f.write("pod/atelet-abc " + json.dumps(ATELET_RESTORE) + "\nnot json\n{\"msg\": \"other\"}\n")
            out = phase_report.parse_files([path])
        self.assertEqual(len(out.breakdowns), 1)
        self.assertEqual(out.lines_seen, 2)


class UnattributedTest(unittest.TestCase):
    def test_atelet_checkpoint_residual_is_total_minus_sequential_phases(self):
        out = parse(ATELET_CHECKPOINT)
        phase_report.join_layers(out.breakdowns)
        self.assertAlmostEqual(out.breakdowns[0].phases["unattributed"], 6.0 - (0.01 + 1.14 + 4.48))
    def test_microvm_ateom_checkpoint_counts_the_concurrent_captures_once(self):
        # prep + pause + max(snapshot, rootfs_upper) + teardown; the two
        # captures overlapped, so only the slower one is spent wall time.
        phases = {k[len("ateom.actor.checkpoint.duration."):]: v
                  for k, v in ATEOM_CHECKPOINT.items() if k.startswith("ateom.actor.checkpoint.duration.")}
        got = phase_report.unattributed("ateom", "checkpoint", "microvm", phases)
        self.assertAlmostEqual(got, 1.2 - (0.04 + 0.01 + 0.9 + 0.2))

    def test_gvisor_ateom_checkpoint_phases_are_sequential(self):
        phases = {"prep": 0.01, "pause": 0.02, "checkpoint": 0.15, "durable_dir": 0.03,
                  "resume": 0.02, "teardown": 0.05, "total": 0.30}
        got = phase_report.unattributed("ateom", "checkpoint", "gvisor", phases)
        self.assertAlmostEqual(got, 0.30 - (0.01 + 0.02 + 0.15 + 0.03 + 0.02 + 0.05))

    def test_ateom_checkpoint_of_unknown_runtime_gets_no_residual(self):
        out = parse(ATEOM_CHECKPOINT)  # no atelet partner, so no class
        phase_report.join_layers(out.breakdowns)
        self.assertNotIn("unattributed", out.breakdowns[0].phases)

    def test_gvisor_pair_splits_both_layers_by_class(self):
        atelet = dict(ATELET_CHECKPOINT, **{"ate.sandbox.class": "gvisor"})
        ateom = {
            "time": "2026-09-23T10:00:55.000000000Z", "msg": "Checkpoint timing breakdown",
            "ate.actor.uid": "uid-1", "ate.template.name": "swebench-astropy-7336", "ate.snapshot.scope": "full",
            "ateom.actor.checkpoint.duration.pause": 0.02,
            "ateom.actor.checkpoint.duration.checkpoint": 0.15,
            "ateom.actor.checkpoint.duration.teardown": 0.05,
            "ateom.actor.checkpoint.duration.total": 0.25,
        }
        out = parse(ateom, atelet)
        phase_report.join_layers(out.breakdowns)
        rows = phase_report.report_phases(out.breakdowns, lambda _: None)
        ateom_rows = {r["phase"]: r for r in rows if r["layer"] == "ateom"}
        self.assertEqual({r["class"] for r in ateom_rows.values()}, {"gvisor"})
        self.assertEqual({r["kind"] for r in ateom_rows.values()}, {"latest"})
        self.assertAlmostEqual(ateom_rows["unattributed"]["p50_ms"], (0.25 - 0.22) * 1000)
        # gVisor's phases sort in their own order, before the shared tail.
        order = [r["phase"] for r in rows if r["layer"] == "ateom"]
        self.assertEqual(order, ["pause", "checkpoint", "teardown", "unattributed", "total"])
    def test_restore_layers_get_no_residual(self):
        out = parse(ATELET_RESTORE, ATEOM_RESTORE)
        phase_report.join_layers(out.breakdowns)
        for b in out.breakdowns:
            self.assertNotIn("unattributed", b.phases)
    def test_negative_residual_is_not_clamped(self):
        # Cannot happen on a well-formed record; if it does, it must show.
        phases = {"sandbox_assets": 0.01, "ateom_checkpoint": 1.14, "persist": 4.48, "total": 1.0}
        self.assertAlmostEqual(phase_report.unattributed("atelet", "checkpoint", "microvm", phases),
                               1.0 - (0.01 + 1.14 + 4.48))

class ReportTest(unittest.TestCase):
    def test_failed_records_are_flagged_and_excluded_from_percentiles(self):
        out = parse(ATELET_RESTORE, FAILED)
        self.assertEqual([b.failed for b in out.breakdowns], [False, True])
        rows = phase_report.report_phases(out.breakdowns, lambda _: None)
        downloads = [r for r in rows if r["phase"] == "download"]
        self.assertEqual(len(downloads), 1)
        self.assertEqual(downloads[0]["count"], 1)  # the failed 30s never entered

    def test_percentiles_split_by_sandbox_class_and_kind(self):
        golden = dict(ATELET_RESTORE, **{"time": "2026-09-23T09:59:00.000000000Z",
                                         "ate.snapshot.kind": "golden", "ate.sandbox.class": "gvisor",
                                         "ate.actor.restore.duration.download": 9.0,
                                         "ate.actor.restore.duration.total": 10.0})
        latest = dict(ATELET_RESTORE, **{"ate.sandbox.class": "gvisor"})
        rows = phase_report.report_phases(parse(golden, latest).breakdowns, lambda _: None)
        downloads = {(r["class"], r["kind"]): r["p50_ms"] for r in rows if r["phase"] == "download"}
        self.assertEqual(downloads, {("gvisor", "golden"): 9000.0, ("gvisor", "latest"): 2400.0})

    def test_paired_ateom_record_inherits_the_atelet_kind(self):
        out = parse(ATEOM_RESTORE, ATELET_RESTORE)
        pairs = phase_report.pair_records(out.breakdowns)
        self.assertEqual(len(pairs), 1)
        self.assertEqual(out.breakdowns[0].kind, "latest")
        rows = phase_report.report_phases(out.breakdowns, lambda _: None)
        self.assertEqual({r["kind"] for r in rows if r["layer"] == "ateom"}, {"latest"})

    def test_checkpoint_pairing_window_ends_where_persist_starts(self):
        # The ateom record is written when ateom_checkpoint ends, before the
        # 4.48 s persist; a record from inside the persist window belongs to
        # a later cycle of a rapidly cycling actor.
        during_persist = dict(ATEOM_CHECKPOINT, time="2026-09-23T10:00:59.000000000Z")
        out = parse(ATEOM_CHECKPOINT, during_persist, ATELET_CHECKPOINT)
        pairs = phase_report.pair_records(out.breakdowns)
        atelet = next(b for b in out.breakdowns if b.source == "atelet")
        self.assertIs(pairs[id(atelet)], out.breakdowns[0])

    def test_an_ateom_record_pairs_with_at_most_one_operation(self):
        second = dict(ATELET_RESTORE, time="2026-09-23T10:00:05.900000000Z")
        out = parse(ATEOM_RESTORE, ATELET_RESTORE, second)
        pairs = phase_report.pair_records(out.breakdowns)
        self.assertEqual(len(pairs), 1)
        first = next(b for b in out.breakdowns if b.source == "atelet")
        self.assertIn(id(first), pairs)

    def test_waterfall_ignores_an_ateom_record_from_another_cycle(self):
        out = parse(ATEOM_RESTORE_STALE, ATELET_RESTORE)
        text = render(phase_report.report_waterfalls, out.breakdowns, slowest=1)
        self.assertIn("atelet ateom_restore", text)
        self.assertNotIn("ateom  vm_restore", text)
        self.assertNotIn("(gap)", text)

    def test_stats_rows_take_the_runner_jsonl_shape(self):
        rows = phase_report.report_phases(parse(ATELET_RESTORE).breakdowns, lambda _: None)
        entries = phase_report.stats_rows(rows, "2026-09-23T10:00:00Z", "abc123", "glutton_mem_1gi")
        download = next(e for e in entries if e["metric"].endswith("_download"))
        self.assertEqual(download["metric"], "phase_atelet_restore_none_latest_full_download")
        self.assertEqual((download["timestamp"], download["tag"], download["test_name"]),
                         ("2026-09-23T10:00:00Z", "abc123", "glutton_mem_1gi"))
        # String values and the dimensions as fields, like the runner's rows.
        self.assertEqual(download["measurements"], {
            "layer": "atelet", "op": "restore", "class": "none", "kind": "latest",
            "scope": "full", "phase": "download",
            "count": "1", "p50_ms": "2400.0", "p90_ms": "2400.0", "p95_ms": "2400.0", "max_ms": "2400.0"})

    def test_waterfall_nests_ateom_and_gap_under_atelet(self):
        out = parse(ATEOM_RESTORE, ATELET_RESTORE)
        text = render(phase_report.report_waterfalls, out.breakdowns, slowest=1)
        lines = [line.strip() for line in text.splitlines()]
        self.assertTrue(any(line.startswith("atelet ateom_restore") for line in lines), text)
        self.assertTrue(any(line.startswith("ateom  vm_restore") for line in lines), text)
        gap = [line for line in lines if line.startswith("(gap)")]
        self.assertEqual(len(gap), 1, text)
        self.assertIn("50.0", gap[0])  # 1.1s atelet ateom_restore - 1.05s ateom total


if __name__ == "__main__":
    unittest.main()
