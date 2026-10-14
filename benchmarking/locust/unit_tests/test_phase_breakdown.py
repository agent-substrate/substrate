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

"""Unit tests for phase_breakdown.py.

Run via: python3 benchmarking/locust/unit_tests/test_phase_breakdown.py
Never contacts a cluster: pod logs come from a mocked CoreV1Api. Needs the
kubernetes client: pip install -r benchmarking/locust/requirements.txt
"""

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from kubernetes.client.rest import ApiException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import phase_breakdown

ATELET = json.dumps({
    "time": "2026-09-23T10:00:05.500000000Z", "msg": "Restore timing breakdown",
    "ate.actor.uid": "uid-1", "ate.template.name": "glutton", "ate.sandbox.class": "microvm",
    "ate.snapshot.scope": "full", "ate.snapshot.kind": "latest",
    "ate.actor.restore.duration.download": 2.4,
    "ate.actor.restore.duration.ateom_restore": 1.1,
    "ate.actor.restore.duration.total": 3.9,
})
ATEOM = json.dumps({
    "time": "2026-09-23T10:00:05.400000000Z", "msg": "Restore timing breakdown",
    "ate.actor.uid": "uid-1", "ate.template.name": "glutton", "ate.snapshot.scope": "full",
    "ateom.actor.restore.duration.vm_restore": 0.8,
    "ateom.actor.restore.duration.total": 1.05,
})
STATS_CSV = ("Type,Name,Request Count,Failure Count\n"
             "grpc,SuspendActor,0,0\ngrpc,ResumeActor,1,0\n,Aggregated,1,0\n")


def fake_api(logs_by_namespace, broken_pod=None, restarted=False, previous_logs=None,
             previous_missing=False):
    """Stand-in CoreV1Api: one pod per namespace, its log from the mapping."""
    api = mock.Mock()
    api.list_namespaced_pod.side_effect = lambda namespace, **_: SimpleNamespace(
        items=[SimpleNamespace(
            metadata=SimpleNamespace(name=f"{namespace}-pod"),
            status=SimpleNamespace(container_statuses=[SimpleNamespace(restart_count=1 if restarted else 0)]))])

    def read_log(name, namespace, previous=False, **_):
        if name == broken_pod:
            raise ApiException(status=400, reason="container is being created")
        if previous and previous_missing:
            raise ApiException(status=400, reason="previous terminated container not found")
        source = (previous_logs or {}) if previous else logs_by_namespace
        # The raw (un-preloaded) response the code asks for: bytes in .data.
        return SimpleNamespace(data=source.get(namespace, "").encode())
    api.read_namespaced_pod_log.side_effect = read_log
    return api


def append(api, stats=STATS_CSV):
    with tempfile.TemporaryDirectory() as d:
        jsonl, stats_csv = Path(d) / "stats.jsonl", Path(d) / "stats.csv"
        jsonl.write_text('{"metric": "grpc_ResumeActor"}\n')
        stats_csv.write_text(stats)
        out = io.StringIO()
        with mock.patch.object(phase_breakdown, "_load_kube_config", return_value=True), \
             mock.patch.object(phase_breakdown.client, "CoreV1Api", return_value=api), \
             contextlib.redirect_stdout(out):
            n = phase_breakdown.append_phase_breakdown(
                jsonl, stats_csv, 600, "2026-09-23T10:01:00Z", "abc123", "unit-run")
        return n, [json.loads(line) for line in jsonl.read_text().splitlines()], out.getvalue()


class PhaseBreakdownTest(unittest.TestCase):
    def test_rows_are_appended_after_the_locust_rows(self):
        api = fake_api({"ate-system": ATELET, "benchmark-workloads": ATEOM})
        n, entries, _ = append(api)
        self.assertEqual(entries[0]["metric"], "grpc_ResumeActor")  # untouched
        metrics = {e["metric"] for e in entries[1:]}
        self.assertIn("phase_atelet_restore_microvm_latest_full_download", metrics)
        # The ateom row took its kind from the paired atelet record.
        self.assertIn("phase_ateom_restore_microvm_latest_full_vm_restore", metrics)
        self.assertEqual(n, len(entries) - 1)
        for e in entries[1:]:
            self.assertEqual((e["timestamp"], e["tag"], e["test_name"]),
                             ("2026-09-23T10:01:00Z", "abc123", "unit-run"))
        summary = next(e for e in entries if e["metric"] == "phase_breakdown_summary")
        self.assertEqual(summary["measurements"]["atelet_restore_records"], "1")
        self.assertEqual(summary["measurements"]["locust_ResumeActor"], "1")
        self.assertEqual((summary["measurements"]["pods_read"], summary["measurements"]["pods_failed"]), ("2", "0"))

    def test_fewer_records_than_successful_requests_is_warned(self):
        api = fake_api({"ate-system": ATELET, "benchmark-workloads": ATEOM})
        _, entries, output = append(api, stats=STATS_CSV.replace("ResumeActor,1,0", "ResumeActor,5,0"))
        self.assertIn("Warning: locust made 5 successful ResumeActor requests but the atelet logs hold 1", output)
        self.assertIn("a node log was rotated", output)
        summary = next(e for e in entries if e["metric"] == "phase_breakdown_summary")
        self.assertEqual(summary["measurements"]["locust_ResumeActor_failures"], "0")

    def test_requests_that_never_reached_atelet_are_not_owed_a_record(self):
        # 5 requests, 4 failed client-side: only 1 successful one reached atelet.
        api = fake_api({"ate-system": ATELET, "benchmark-workloads": ATEOM})
        _, _, output = append(api, stats=STATS_CSV.replace("ResumeActor,1,0", "ResumeActor,5,4"))
        self.assertNotIn("Warning: locust made", output)

    def test_missing_records_blame_unreadable_pods_when_there_are_any(self):
        api = fake_api({"ate-system": ATELET, "benchmark-workloads": ATEOM}, broken_pod="ate-system-pod")
        _, _, output = append(api)
        self.assertIn("1 pod(s) could not be read", output)
        self.assertNotIn("rotated", output)

    def test_a_missing_previous_log_keeps_the_current_one(self):
        api = fake_api({"ate-system": ATELET, "benchmark-workloads": ATEOM}, restarted=True, previous_missing=True)
        _, entries, output = append(api)
        self.assertIn("no previous log for", output)
        self.assertTrue(any(e["metric"].startswith("phase_atelet_restore") for e in entries))
        summary = next(e for e in entries if e["metric"] == "phase_breakdown_summary")
        self.assertEqual((summary["measurements"]["pods_read"], summary["measurements"]["pods_failed"]), ("2", "0"))

    def test_an_unreadable_pod_is_skipped_not_fatal(self):
        api = fake_api({"ate-system": ATELET, "benchmark-workloads": ATEOM},
                       broken_pod="benchmark-workloads-pod")
        n, entries, output = append(api)
        self.assertIn("could not read logs of benchmark-workloads/benchmark-workloads-pod", output)
        self.assertTrue(any(e["metric"].startswith("phase_atelet_") for e in entries))
        self.assertFalse(any(e["metric"].startswith("phase_ateom_") for e in entries))

    def test_no_records_still_writes_the_summary_row(self):
        # A failed configuration must not look like an idle run.
        n, entries, output = append(fake_api({}))
        self.assertEqual((n, len(entries)), (1, 2))
        self.assertEqual(entries[1]["metric"], "phase_breakdown_summary")
        self.assertEqual(entries[1]["measurements"]["records"], "0")
        self.assertIn("Warning: no timing breakdown records", output)

    def test_a_restarted_container_is_read_twice(self):
        api = fake_api({"ate-system": "", "benchmark-workloads": ATEOM}, restarted=True,
                       previous_logs={"ate-system": ATELET})
        _, entries, _ = append(api)
        self.assertTrue(any(e["metric"].startswith("phase_atelet_restore") for e in entries))
        kwargs = [c.kwargs for c in api.read_namespaced_pod_log.call_args_list]
        self.assertEqual(sum(1 for k in kwargs if k.get("previous")), 2)

    def test_failed_requests_do_not_trigger_the_rotation_warning(self):
        failed = ATELET.replace('"ate.snapshot.kind": "latest",', '"ate.snapshot.kind": "latest", "error.type": "DeadlineExceeded",')
        api = fake_api({"ate-system": failed, "benchmark-workloads": ATEOM})
        _, _, output = append(api)
        self.assertNotIn("Warning: locust made", output)


if __name__ == "__main__":
    unittest.main()
