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

"""Unit tests for server_telemetry.py.

Run via: python3 benchmarking/locust/unit_tests/test_server_telemetry.py
"""

import contextlib
import csv
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import runner
import server_telemetry

NO_PERCENTILES = {"min": None, "p50": None, "p90": None, "p95": None,
                  "p99": None, "max": None, "avg": None}

WINDOW = {"prom_url": "http://localhost:9090", "start_ts": 100,
          "end_ts": 105, "steady_start_ts": 100}


def ranges(packing, node=(), pod=()):
    """A query_prometheus_range fake answering packing, node PSI and pod PSI.

    Pod PSI only answers the pod-slice selector, so a query that loses the id
    regex (and would also sum the pause container) gets nothing back.
    """
    pod_slice = ('{pod=~"benchmark-ateom.*", container="", '
                 'id=~".*[/-]pod[^/]+(\\\\.slice)?"}')

    def fake(_url, query, *_args, **_kwargs):
        if "ate_workerpool_workers" in query:
            return packing
        if 'container="node"' in query:
            return list(node)
        return list(pod) if pod_slice in query else []
    return fake

ARGV = ["runner.py", "-f", "tests/glutton.py", "-t", "1m", "-u", "10",
        "--tag", "unit", "--name", "unit-run", "--dest", "/tmp"]


def harvest(worker_pod_count=5):
    return server_telemetry.harvest_server_telemetry(
        worker_pod_count=worker_pod_count, **WINDOW
    )


def snapshots(lag_s=0):
    """The snapshot section alone, which issues no range queries."""
    return server_telemetry._harvest_snapshots(
        WINDOW["prom_url"], WINDOW["steady_start_ts"], WINDOW["end_ts"], lag_s
    )


def parse(*extra):
    with mock.patch.object(sys, "argv", ARGV + list(extra)):
        return runner.parse_args()


MB = 1024 * 1024

# Each cumulative sum _harvest_snapshots reads, by a substring of its query.
SUMS = {
    "count": 'atelet_snapshot_size_bytes_count{file_name="pages.img"}',
    "size": 'atelet_snapshot_size_bytes_sum{file_name="pages.img"}',
    "bytes": "sum(atelet_snapshot_size_bytes_sum)",
    "seconds": "ate_actor_checkpoint_duration_seconds_sum",
    "restore_sum": 'seconds_sum{rpc_method="atelet.AteomHerder/Restore"}',
    "restore_count": 'seconds_count{rpc_method="atelet.AteomHerder/Restore"}',
    "checkpoint_sum": 'seconds_sum{rpc_method="atelet.AteomHerder/Checkpoint"}',
    "checkpoint_count":
        'seconds_count{rpc_method="atelet.AteomHerder/Checkpoint"}',
}


def snapshot_prom(quantile=None, start=None, end=None, at=(100, 105)):
    """A query_prometheus_instant fake answering by query and timestamp.

    Every quantile read at the window end returns `quantile`; `start` and `end`
    map a SUMS name to its value at the two instants in `at`. Anything else,
    including a read at the wrong time, returns no series.
    """
    by_time = {at[0]: start or {}, at[1]: end or {}}

    def fake(_url, query, time_ts=None):
        if query.startswith("histogram_quantile"):
            val = quantile if time_ts == at[1] else None
        else:
            name = next((n for n, s in SUMS.items() if s in query), None)
            val = by_time.get(time_ts, {}).get(name)
        return [{"value": [time_ts, val]}] if val is not None else []
    return fake


def history_csv(rows):
    """A stats_history.csv built from (timestamp, user count) pairs."""
    f = tempfile.NamedTemporaryFile(
        "w", delete=False, suffix=".csv", encoding="utf-8"
    )
    writer = csv.DictWriter(f, fieldnames=["Timestamp", "Name", "User Count"])
    writer.writeheader()
    for ts, users in rows:
        writer.writerow({"Timestamp": ts, "Name": "Aggregated", "User Count": users})
    f.close()
    return Path(f.name)


class ServerTelemetryTest(unittest.TestCase):
    def test_compute_percentiles(self):
        res = server_telemetry.compute_percentiles([float(i) for i in range(1, 201)])
        self.assertEqual(
            (res["min"], res["p50"], res["p90"], res["p95"], res["p99"],
             res["max"], res["avg"]),
            (1.0, 101.0, 181.0, 191.0, 199.0, 200.0, 100.5))

        res = server_telemetry.compute_percentiles([7.5])
        self.assertEqual((res["p50"], res["p90"], res["p95"], res["p99"]),
                         (7.5, 7.5, 7.5, 7.5))

        res = server_telemetry.compute_percentiles(
            [1.0, float("nan"), 2.0, float("inf"), float("-inf"), 3.0]
        )
        self.assertEqual((res["min"], res["p50"], res["max"], res["avg"]),
                         (1.0, 2.0, 3.0, 2.0))

        self.assertEqual(server_telemetry.compute_percentiles([]), NO_PERCENTILES)
        self.assertEqual(server_telemetry.compute_percentiles([float("nan")]),
                         NO_PERCENTILES)

    def test_steady_state_window(self):
        # 90% of peak (7) is 6.3; the drop at 140 is teardown and left out.
        path = history_csv([("100", "2"), ("110", "4"), ("120", "6.3"),
                            ("130", "7"), ("140", "3")])
        try:
            self.assertEqual(
                server_telemetry.get_steady_state_window(
                    path, start_ts=100, end_ts=150),
                (120, 130),
            )
        finally:
            path.unlink()

        # Uses observed peak (15) rather than requested -u (5).
        path = history_csv([("100", "5"), ("110", "10"), ("120", "15"),
                            ("130", "15")])
        try:
            self.assertEqual(
                server_telemetry.get_steady_state_window(
                    path, start_ts=100, end_ts=150),
                (120, 130),
            )
        finally:
            path.unlink()

        # Zero users falls back to [start_ts, end_ts].
        path = history_csv([("120", "0")])
        try:
            self.assertEqual(
                server_telemetry.get_steady_state_window(
                    path, start_ts=100, end_ts=150),
                (100, 150),
            )
        finally:
            path.unlink()

    @mock.patch("urllib.request.urlopen")
    def test_range_query_window_guard(self, mock_urlopen):
        # Zero-length window [100, 100] is widened to [100, 101].
        resp = mock.MagicMock()
        resp.read.return_value = json.dumps({
            "status": "success",
            "data": {"result": [{"metric": {}, "values": [[100, "1.0"]]}]},
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = resp

        res = server_telemetry.query_prometheus_range(
            "http://localhost:9090", "up", 100, 100)
        url = mock_urlopen.call_args[0][0].full_url
        self.assertIn("start=100", url)
        self.assertIn("end=101", url)
        self.assertEqual(len(res), 1)

        server_telemetry.query_prometheus_range(
            "http://localhost:9090", "up", 100, 160)
        url = mock_urlopen.call_args[0][0].full_url
        self.assertIn("start=100", url)
        self.assertIn("end=160", url)

    def test_malformed_instant_response(self):
        for bad in ([{"value": None}], [{"value": []}], [{"value": [100, None]}]):
            self.assertIsNone(server_telemetry._parse_instant_float(bad))

    @mock.patch("server_telemetry.query_prometheus_range")
    @mock.patch("server_telemetry.query_prometheus_instant")
    def test_packing_and_checkpoint_math(self, mock_instant, mock_range):
        # Inf samples are filtered out before JSON serialization.
        assigned = {"metric": {"ate_worker_state": "assigned"},
                    "values": [[100, "4.0"], [105, "4.0"], [110, "Inf"]]}
        quiet = [{"values": [[100, "0.0"], [105, "0.0"]]}]
        pods = [{"metric": {"pod": f"benchmark-ateom-{p}"},
                 "values": [[100, "0.5"], [105, "0.5"]]} for p in "ab"]
        mock_range.side_effect = ranges([assigned], node=quiet, pod=pods)
        # The default 15s lag reads the snapshot window at 107 and 112.
        mock_instant.side_effect = snapshot_prom(
            "11.5",
            start={"count": "100", "size": "0", "restore_sum": "0",
                   "restore_count": "0", "checkpoint_sum": "0",
                   "checkpoint_count": "0"},
            end={"count": "150", "size": str(100 * MB), "bytes": str(100 * MB),
                 "seconds": "50", "restore_sum": "4", "restore_count": "8",
                 "checkpoint_sum": "100", "checkpoint_count": "50"},
            at=(107, 112))

        with tempfile.TemporaryDirectory() as td:
            out_json = Path(td) / "server_summary.json"
            out_jsonl = Path(td) / "stats.jsonl"
            with contextlib.redirect_stdout(io.StringIO()), \
                 mock.patch("time.time", return_value=110), \
                 mock.patch("time.sleep") as sleep:
                server_telemetry.extract_and_record_server_telemetry(
                    prom_url="http://localhost:9090",
                    start_ts=100,
                    end_ts=105,
                    stats_history_csv=Path(td) / "missing.csv",
                    worker_pod_count=5,
                    output_json_path=out_json,
                    jsonl_path=out_jsonl,
                    data_ts="2026-01-01",
                    tag="unit",
                    test_name="unit-run",
                )
            summary = json.loads(out_json.read_text())
            row = json.loads(out_jsonl.read_text().splitlines()[0])

        sleep.assert_called_once_with(10)  # until end 105 + 15, from 110
        self.assertEqual(summary["metadata"]["atelet_lag_s"], 15)
        # Packing and PSI are not shifted.
        self.assertEqual({c.args[2:4] for c in mock_range.call_args_list},
                         {(100, 105)})
        self.assertEqual(
            set(summary),
            {"metadata", "cluster_packing", "node_psi", "pod_psi", "snapshots"},
        )
        packing = summary["cluster_packing"]
        self.assertEqual(packing["summary"]["p50"], 0.8)  # 4 assigned / 5 pods
        self.assertEqual(packing["timeseries"][0]["total_workers"], 5.0)
        self.assertEqual(len(packing["timeseries"]), 2)
        self.assertNotIn("Infinity", json.dumps(summary))

        self.assertEqual(summary["pod_psi"]["pods"], 2)
        self.assertEqual(summary["pod_psi"]["cpu_stall_pct"]["p99"], 0.5)
        self.assertEqual(summary["node_psi"]["cpu_stall_pct"]["p99"], 0.0)

        snapshots = summary["snapshots"]
        self.assertEqual(snapshots["checkpoints_in_window"], 50)  # 150 - 100
        self.assertEqual(snapshots["checkpoints_cumulative"], 150)

        self.assertEqual(row["metric"], "server_summary")
        m = row["measurements"]
        # Every source answered, so a key wired to a wrong name would read None.
        self.assertEqual(len(m), 45)
        self.assertEqual([k for k, v in m.items() if v is None], [])
        # Every value a string, so one row's types match every other row's.
        self.assertTrue(all(isinstance(v, str) for v in m.values()))
        self.assertEqual(m["checkpoint_mb_s"], "2.0")
        self.assertEqual(m["pod_psi_cpu_stall_p99"], "0.5")
        self.assertEqual(m["psi_cpu_stall_p90"], "0.0")
        self.assertEqual(m["cluster_packing_p95"], "0.8")
        self.assertEqual(m["snapshot_size_p95_mb"], "11.5")
        self.assertEqual(m["restore_mean_s"], "0.5")
        self.assertEqual(m["checkpoint_mean_s"], "2.0")

    @mock.patch("server_telemetry.query_prometheus_range")
    @mock.patch("server_telemetry.query_prometheus_instant")
    def test_unknown_pod_count_uses_observed_workers(self, mock_instant, mock_range):
        # Falls back to observed worker sum when worker_pod_count is None.
        mock_range.side_effect = ranges([
            {"metric": {"ate_worker_state": "assigned"}, "values": [[100, "4.0"]]},
            {"metric": {"ate_worker_state": "idle"}, "values": [[100, "16.0"]]},
        ])
        mock_instant.return_value = []

        point = harvest(worker_pod_count=None)["cluster_packing"]["timeseries"][0]
        self.assertEqual(point["total_workers"], 20.0)  # 4 + 16 observed
        self.assertEqual(point["packing_ratio"], 0.2)

    @mock.patch("server_telemetry.query_prometheus_range")
    @mock.patch("server_telemetry.query_prometheus_instant")
    def test_missing_denominator_is_null(self, mock_instant, mock_range):
        # Zero total workers yields packing_ratio=None.
        mock_range.side_effect = ranges(
            [{"metric": {"ate_worker_state": "idle"}, "values": [[100, "0.0"]]}])
        mock_instant.return_value = []
        point = harvest(worker_pod_count=None)["cluster_packing"]["timeseries"][0]
        self.assertEqual(point["total_workers"], 0.0)
        self.assertIsNone(point["packing_ratio"])

        # Zero-duration window yields checkpoint_mb_s=None.
        mock_range.side_effect = ranges([])
        mock_instant.return_value = [{"value": [100, "5"]}]
        snaps = server_telemetry.harvest_server_telemetry(
            prom_url="http://localhost:9090", start_ts=100, end_ts=100,
            steady_start_ts=100, worker_pod_count=5)["snapshots"]
        self.assertEqual(snaps["checkpoints_in_window"], 0)
        self.assertIsNone(snaps["checkpoint_mb_s"])

    @mock.patch("server_telemetry.query_prometheus_instant")
    def test_snapshot_fields_are_null_not_zero(self, mock_instant):
        # Empty response -> all fields None.
        mock_instant.return_value = []
        snaps = snapshots()
        self.assertIsNone(snaps["checkpoints_in_window"])
        self.assertIsNone(snaps["checkpoints_cumulative"])
        self.assertIsNone(snaps["checkpoint_mb_s"])
        self.assertIsNone(snaps["size_p95_mb"])
        self.assertIsNone(snaps["size_avg_mb"])
        self.assertIsNone(snaps["restore_mean_s"])

        # 0 bytes written over 4s -> measured 0.0.
        mock_instant.side_effect = snapshot_prom(
            "0.0", start={"count": "100"},
            end={"count": "150", "bytes": "0", "seconds": "4"})
        snaps = snapshots()
        self.assertEqual(snaps["size_p50_mb"], 0.0)
        self.assertEqual(snaps["checkpoints_in_window"], 50)
        self.assertEqual(snaps["checkpoint_mb_s"], 0.0)

        # Counter reset (end < start) -> window delta None.
        mock_instant.side_effect = snapshot_prom(
            "1.0", start={"count": "900"}, end={"count": "150"})
        snaps = snapshots()
        self.assertIsNone(snaps["checkpoints_in_window"])
        self.assertEqual(snaps["checkpoints_cumulative"], 150)

        # Series born inside the window counts from zero.
        mock_instant.side_effect = snapshot_prom("1.0", end={"count": "6"})
        self.assertEqual(snapshots()["checkpoints_in_window"], 6)

        # 100 MiB across 50 checkpoints = 2.0 MB; 4s over 8 restores = 0.5s.
        mock_instant.side_effect = snapshot_prom(
            "1.75",
            start={"count": "100", "size": str(300 * MB),
                   "restore_sum": "10", "restore_count": "20"},
            end={"count": "150", "size": str(400 * MB),
                 "restore_sum": "14", "restore_count": "28"},
        )
        snaps = snapshots()
        self.assertEqual(snaps["size_p99_mb"], 1.75)
        self.assertEqual(snaps["size_avg_mb"], 2.0)
        self.assertEqual(snaps["restore_mean_s"], 0.5)
        # Percentiles diff buckets from the window start, never a fixed 5m.
        queries = [c.args[1] for c in mock_instant.call_args_list]
        self.assertFalse(any("[5m]" in q for q in queries))
        self.assertTrue(any("@ 100)" in q for q in queries))

        # Both edges read half the lag late: 107 and 112.
        mock_instant.side_effect = snapshot_prom(
            "1.0", start={"count": "100"}, end={"count": "150"}, at=(107, 112))
        mock_instant.reset_mock()
        self.assertEqual(snapshots(lag_s=15)["checkpoints_in_window"], 50)
        queries = [c.args[1] for c in mock_instant.call_args_list]
        self.assertTrue(any("@ 107)" in q for q in queries))

        # Sum reset or zero count -> size_avg_mb None.
        mock_instant.side_effect = snapshot_prom(
            "1.0", start={"count": "100", "size": str(400 * MB)},
            end={"count": "150", "size": str(300 * MB)})
        self.assertIsNone(snapshots()["size_avg_mb"])

        mock_instant.side_effect = snapshot_prom(
            "1.0", start={"count": "0", "size": "0"},
            end={"count": "0", "size": "0"})
        self.assertIsNone(snapshots()["size_avg_mb"])

    @mock.patch("server_telemetry.query_prometheus_instant")
    def test_checkpoint_throughput_from_counter_sums(self, mock_instant):
        # 100 MiB / 50s = 2.0 MB/s.
        mock_instant.side_effect = snapshot_prom(
            "9.0", end={"bytes": str(100 * MB), "seconds": "50"})
        self.assertEqual(snapshots()["checkpoint_mb_s"], 2.0)

        # Zero duration -> None.
        mock_instant.side_effect = snapshot_prom(
            "9.0", end={"bytes": str(8 * MB), "seconds": "0"})
        self.assertIsNone(snapshots()["checkpoint_mb_s"])

        # Duration query must filter by ate_snapshot_phase="total".
        spent = [c.args[1] for c in mock_instant.call_args_list
                 if "ate_actor_checkpoint_duration_seconds_sum" in c.args[1]]
        self.assertIn('ate_snapshot_phase="total"', spent[0])

    def test_prometheus_url_flag(self):
        self.assertEqual(parse().prometheus_url, runner.DEFAULT_PROMETHEUS_URL)
        self.assertEqual(parse("--prometheus-url", "http://x:9090").prometheus_url,
                         "http://x:9090")
        extra = parse("--prometheus-url", "http://x:9090", "--max-wait-time", "1.0")
        self.assertNotIn("--prometheus-url", extra.locust_extra)
        self.assertEqual(extra.locust_extra, ["--max-wait-time", "1.0"])

        self.assertEqual(parse().atelet_lag_s, 15)
        lag = parse("--atelet-lag-s", "70")
        self.assertEqual((lag.atelet_lag_s, lag.locust_extra), (70, []))

    @mock.patch.object(runner, "extract_and_record_server_telemetry")
    @mock.patch.object(runner, "upload")
    @mock.patch.object(runner, "run_test", return_value=1)
    def test_telemetry_survives_a_missing_stats_csv(self, _run, _up, telemetry):
        with mock.patch.object(sys, "argv", ARGV + ["--no-cluster-facts",
                                                    "--allow-empty-stats",
                                                    "--atelet-lag-s", "70"]), \
             contextlib.redirect_stdout(io.StringIO()):
            runner.main()
        self.assertTrue(telemetry.called)
        self.assertEqual(telemetry.call_args.kwargs["lag_s"], 70)


if __name__ == "__main__":
    unittest.main()
