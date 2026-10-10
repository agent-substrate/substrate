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

"""Suspend/resume phase percentiles for stats.jsonl, read from the node logs.

atelet and ateom-microvm write one `Checkpoint timing breakdown` / `Restore
timing breakdown` record per operation (see benchmarking/analysis/README.md).
The orchestrator deletes the worker and ate-system pods right after a test,
so the runner reads those pod logs itself, through the Kubernetes API it
already uses for cluster facts, while the pods still exist, and appends the
percentiles to stats.jsonl next to the SuspendActor / ResumeActor rows.

Like the cluster facts this is additive: a failure here must never cost the
locust measurements. Pass --no-phase-breakdown to runner.py to skip it.
"""

import csv
import json
import sys
from pathlib import Path
from typing import Any, TextIO

from kubernetes import client

from cluster_facts import (
    API_TIMEOUT_SECONDS,
    WORKER_POOL_LABEL,
    WORKER_POOL_NAMESPACE,
    _load_kube_config,
    _log,
)

# In the repository phase_report.py lives in benchmarking/analysis/; the
# locust image copies it next to this file (benchmarking/locust/Dockerfile).
_ANALYSIS_DIR = Path(__file__).resolve().parent.parent / "analysis"
if _ANALYSIS_DIR.is_dir():
    sys.path.insert(0, str(_ANALYSIS_DIR))
import phase_report  # noqa: E402

ATELET_NAMESPACE = "ate-system"
ATELET_LABEL = "app=atelet"

# A log body is a download, not a cached list; give it longer than the
# API_TIMEOUT_SECONDS the list calls use.
LOG_READ_TIMEOUT_SECONDS = 60

# The locust request names whose counts the atelet records must reach. More
# records than requests is normal: boomer suspends the actors it created when
# it shuts down, and those checkpoints are measured like any other.
LOCUST_OP_NAMES = {"checkpoint": "SuspendActor", "restore": "ResumeActor"}


class PodLogs:
    """The lines read from one namespace's pods, and how the reads went."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.pods_read = 0
        self.pods_failed = 0


def _read_log(v1: client.CoreV1Api, name: str, namespace: str,
              since_seconds: int, previous: bool = False) -> str:
    # The raw response: with preloading, the client renders a text/plain body
    # as the str() of bytes (one line of b'...').
    resp = v1.read_namespaced_pod_log(
        name, namespace, since_seconds=since_seconds, previous=previous,
        _request_timeout=LOG_READ_TIMEOUT_SECONDS, _preload_content=False,
    )
    return resp.data.decode("utf-8", errors="replace")


def read_pod_logs(v1: client.CoreV1Api, namespace: str, label: str,
                  since_seconds: int, logs: TextIO | None = None) -> PodLogs:
    """The log lines of every pod matching label in namespace, from
    since_seconds ago. A container that restarted is read twice, its previous
    log too, so a mid-run restart does not drop the records before it. A pod
    that cannot be read is reported, counted and skipped."""
    out = PodLogs()
    try:
        pods = v1.list_namespaced_pod(
            namespace=namespace, label_selector=label,
            resource_version="0", _request_timeout=API_TIMEOUT_SECONDS,
        ).items
    except Exception as e:
        _log(logs, f"Notice: could not list pods in {namespace} ({label}): "
                   f"{getattr(e, 'reason', e)}")
        out.pods_failed += 1
        return out
    for pod in pods:
        name = pod.metadata.name
        statuses = pod.status.container_statuses or []
        restarted = any(getattr(c, "restart_count", 0) for c in statuses)
        try:
            text = _read_log(v1, name, namespace, since_seconds)
        except Exception as e:
            _log(logs, f"Notice: could not read logs of {namespace}/{name}: "
                       f"{getattr(e, 'reason', e)}")
            out.pods_failed += 1
            continue
        if restarted:
            # kubelet may have discarded the previous container's log already;
            # that loses the records before the restart, not the current log.
            try:
                text += "\n" + _read_log(v1, name, namespace, since_seconds, previous=True)
            except Exception as e:
                _log(logs, f"Notice: no previous log for {namespace}/{name}: "
                           f"{getattr(e, 'reason', e)}")
        out.pods_read += 1
        out.lines.extend(text.splitlines())
    return out


def locust_request_counts(stats_csv: Path) -> dict[str, tuple[int, int]]:
    """SuspendActor / ResumeActor (requests, failures) from locust's stats.csv."""
    counts: dict[str, tuple[int, int]] = {}
    with open(stats_csv) as f:
        for row in csv.DictReader(f):
            if row.get("Name") in LOCUST_OP_NAMES.values():
                counts[row["Name"]] = (int(row.get("Request Count") or 0),
                                       int(row.get("Failure Count") or 0))
    return counts


def collect_phase_breakdown(v1: client.CoreV1Api, since_seconds: int,
                            logs: TextIO | None = None) -> tuple[list[dict], Any, PodLogs]:
    """Reads the atelet and worker pod logs and aggregates the records.
    Returns the percentile rows, the parse result and the read tallies."""
    parsed = phase_report.Parsed()
    reads = PodLogs()
    for namespace, label in ((ATELET_NAMESPACE, ATELET_LABEL),
                             (WORKER_POOL_NAMESPACE, WORKER_POOL_LABEL)):
        got = read_pod_logs(v1, namespace, label, since_seconds, logs)
        reads.pods_read += got.pods_read
        reads.pods_failed += got.pods_failed
        phase_report.parse_lines(got.lines, parsed)
    phase_report.join_layers(parsed.breakdowns)
    rows = phase_report.report_phases(parsed.breakdowns, lambda _: None)
    return rows, parsed, reads


def append_phase_breakdown(jsonl_path: Path, stats_csv: Path, since_seconds: int,
                           timestamp: str, tag: str, test_name: str,
                           logs: TextIO | None = None) -> int:
    """Appends the phase percentile rows to jsonl_path and returns how many.

    A phase_breakdown_summary row is always written once the API was
    reachable, so a run with no records is distinguishable from a run where
    the reads failed: it carries the pods read and failed, and the atelet
    record counts next to locust's SuspendActor / ResumeActor request and
    failure counts. A successful request must have reached atelet, so fewer
    atelet records than successful requests means records were lost: to pods
    that could not be read if there were any, else to a node log rotated
    during the run. The log says which.
    """
    if not _load_kube_config(logs):
        return 0
    v1 = client.CoreV1Api()
    rows, parsed, reads = collect_phase_breakdown(v1, since_seconds, logs)

    expected = locust_request_counts(stats_csv) if stats_csv.exists() else {}
    summary: dict[str, Any] = {"records": parsed.lines_matched,
                               "pods_read": reads.pods_read, "pods_failed": reads.pods_failed}
    for op, request_name in LOCUST_OP_NAMES.items():
        got = sum(1 for b in parsed.breakdowns if b.source == "atelet" and b.op == op)
        summary[f"atelet_{op}_records"] = got
        if request_name in expected:
            requests, failures = expected[request_name]
            summary[f"locust_{request_name}"] = requests
            summary[f"locust_{request_name}_failures"] = failures
            # A request that failed before reaching atelet leaves no record,
            # so only the successful ones are owed one.
            if got < requests - failures:
                cause = (f"{reads.pods_failed} pod(s) could not be read" if reads.pods_failed
                         else "a node log was rotated during the run")
                _log(logs, f"Warning: locust made {requests - failures} successful {request_name} "
                           f"requests but the atelet logs hold {got} {op} records; {cause}")
    if not rows:
        _log(logs, f"Warning: no timing breakdown records in the node logs "
                   f"({reads.pods_read} pods read, {reads.pods_failed} failed); "
                   f"check the runner's pods/log access if the run made requests")

    entries = phase_report.stats_rows(rows, timestamp, tag, test_name)
    entries.append({"timestamp": timestamp, "tag": tag, "test_name": test_name,
                    "metric": "phase_breakdown_summary",
                    "measurements": {k: str(v) for k, v in summary.items()}})
    with open(jsonl_path, "a") as out:
        for entry in entries:
            out.write(json.dumps(entry) + "\n")
    _log(logs, f"Appended {len(entries)} phase breakdown rows from "
               f"{parsed.lines_matched} records")
    return len(entries)
