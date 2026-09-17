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

"""Unit tests for server_telemetry.py: python3 benchmarking/locust/unit_tests/test_server_telemetry.py"""

import csv
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import server_telemetry

NO_PERCENTILES = {"min": None, "p50": None, "p90": None,
                  "p99": None, "max": None, "avg": None}

# Every harvest test shares this five-second window; only the pod count varies.
WINDOW = {"prom_url": "http://localhost:9090", "start_ts": 100,
          "end_ts": 105, "steady_start_ts": 100}

# packing, CPU PSI, memory PSI, IO PSI, CFS throttling.
EMPTY_RANGES = [[], [], [], [], []]


def harvest(worker_pod_count=5):
    return server_telemetry.harvest_server_telemetry(
        worker_pod_count=worker_pod_count, **WINDOW
    )


def snapshot_instants(size_p50, size_p90, c_start="100", c_end="150",
                      size_p95="0", size_sum_start=None, size_sum_end=None):
    """The eleven instant queries the snapshot block issues, in order.

    Order matters: these are consumed as a mock side_effect.
    """
    return [
        [{"value": [105, size_p50]}],  # snapshot size p50
        [{"value": [105, size_p90]}],  # snapshot size p90
        [{"value": [105, size_p95]}],  # snapshot size p95
        [{"value": [100, c_start]}],   # checkpoint count at window start
        [{"value": [105, c_end]}],     # checkpoint count at window end
        [{"value": [100, size_sum_start]}] if size_sum_start is not None else [],
        [{"value": [105, size_sum_end]}] if size_sum_end is not None else [],
        [{"value": [105, "0.08"]}],    # restore p50
        [{"value": [105, "0.15"]}],    # restore p95
        [{"value": [105, "0.12"]}],    # checkpoint p50
        [{"value": [105, "0.22"]}],    # checkpoint p95
    ]


def history_csv(rows):
    """A stats_history.csv built from (timestamp, user count) pairs."""
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv")
    writer = csv.DictWriter(f, fieldnames=["Timestamp", "Name", "User Count"])
    writer.writeheader()
    for ts, users in rows:
        writer.writerow({"Timestamp": ts, "Name": "Aggregated", "User Count": users})
    f.close()
    return Path(f.name)


class ServerTelemetryTest(unittest.TestCase):
    def test_compute_percentiles(self):
        # 200 distinct samples, so an off-by-one in the indexing shows up.
        res = server_telemetry.compute_percentiles([float(i) for i in range(1, 201)])
        self.assertEqual(
            (res["min"], res["p50"], res["p90"], res["p99"], res["max"], res["avg"]),
            (1.0, 101.0, 181.0, 199.0, 200.0, 100.5))

        # With a single sample, every percentile is that sample.
        res = server_telemetry.compute_percentiles([7.5])
        self.assertEqual((res["p50"], res["p90"], res["p99"]), (7.5, 7.5, 7.5))

        res = server_telemetry.compute_percentiles(
            [1.0, float("nan"), 2.0, float("inf"), float("-inf"), 3.0]
        )
        self.assertEqual((res["min"], res["p50"], res["max"], res["avg"]),
                         (1.0, 2.0, 3.0, 2.0))

        # Nothing left to measure is None, not zero.
        self.assertEqual(server_telemetry.compute_percentiles([]), NO_PERCENTILES)
        self.assertEqual(server_telemetry.compute_percentiles([float("nan")]),
                         NO_PERCENTILES)

    def test_steady_state_window(self):
        # Steady starts at the first sample at or above 90% of target (6.3).
        path = history_csv([("100", "2"), ("110", "4"), ("120", "7"), ("130", "7")])
        try:
            self.assertEqual(
                server_telemetry.get_steady_state_window(
                    path, active_users=7, start_ts=100, end_ts=150),
                (120, 150),
            )
        finally:
            path.unlink()

        # Target never reached, so the whole run is used rather than nothing.
        path = history_csv([("100", "2")])
        try:
            self.assertEqual(
                server_telemetry.get_steady_state_window(
                    path, active_users=10, start_ts=100, end_ts=150),
                (100, 150),
            )
        finally:
            path.unlink()

    @mock.patch("urllib.request.urlopen")
    def test_range_query_window_guard(self, mock_urlopen):
        # Prometheus answers 400 when end is not after start, so a zero-length
        # window is widened by a second. Assert on the URL actually sent.
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

    @mock.patch("server_telemetry.query_prometheus_instant")
    def test_quantile_fallback(self, mock_instant):
        # A rate query over a quiet window yields NaN: fall back to cumulative.
        mock_instant.side_effect = [
            [{"value": [100, "NaN"]}],
            [{"value": [100, "11.625"]}],
        ]
        val = server_telemetry._query_quantile_with_fallback(
            "http://localhost:9090", 0.50, "rate(bucket[5m])", "bucket", end_ts=100)
        self.assertEqual(val, 11.625)

        # A malformed response costs one field, it must not raise.
        for bad in ([{"value": None}], [{"value": []}], [{"value": [100, None]}]):
            self.assertIsNone(server_telemetry._parse_instant_float(bad))

    @mock.patch("server_telemetry.query_prometheus_range")
    @mock.patch("server_telemetry.query_prometheus_instant")
    def test_packing_and_checkpoint_math(self, mock_instant, mock_range):
        # The Inf sample must be dropped: json.dumps would write a bare
        # Infinity that strict parsers reject.
        assigned = {"metric": {"ate_worker_state": "assigned"},
                    "values": [[100, "4.0"], [105, "4.0"], [110, "Inf"]]}
        quiet = [{"values": [[100, "0.0"], [105, "0.0"]]}]
        mock_range.side_effect = [[assigned], quiet, quiet, quiet, quiet]
        mock_instant.side_effect = snapshot_instants("11.5", "12.0")

        summary = harvest(worker_pod_count=5)

        packing = summary["cluster_packing"]
        self.assertEqual(packing["summary"]["p50"], 0.8)  # 4 assigned / 5 pods
        self.assertEqual(packing["timeseries"][0]["total_workers"], 5.0)
        self.assertEqual(len(packing["timeseries"]), 2)   # the Inf point is gone
        self.assertNotIn("Infinity", json.dumps(summary))

        snapshots = summary["snapshots"]
        self.assertEqual(snapshots["checkpoints_in_window"], 50)  # 150 - 100
        self.assertEqual(snapshots["checkpoints_cumulative"], 150)

    @mock.patch("server_telemetry.query_prometheus_range")
    @mock.patch("server_telemetry.query_prometheus_instant")
    def test_unknown_pod_count_uses_observed_workers(self, mock_instant, mock_range):
        # cluster_facts returns None rather than guessing, so the denominator
        # is what Prometheus reports.
        mock_range.side_effect = [
            [
                {"metric": {"ate_worker_state": "assigned"},
                 "values": [[100, "4.0"]]},
                {"metric": {"ate_worker_state": "idle"},
                 "values": [[100, "16.0"]]},
            ],
            [{"values": [[100, "0.0"]]}], [{"values": [[100, "0.0"]]}],
            [{"values": [[100, "0.0"]]}], [{"values": [[100, "0.0"]]}],
        ]
        mock_instant.return_value = []

        point = harvest(worker_pod_count=None)["cluster_packing"]["timeseries"][0]
        self.assertEqual(point["total_workers"], 20.0)  # 4 + 16 observed
        self.assertEqual(point["packing_ratio"], 0.2)

    @mock.patch("server_telemetry.query_prometheus_range")
    @mock.patch("server_telemetry.query_prometheus_instant")
    def test_snapshot_fields_are_null_not_zero(self, mock_instant, mock_range):
        # "no checkpoints happened" and "we could not find out" must differ.
        mock_range.side_effect = EMPTY_RANGES
        mock_instant.return_value = []
        snaps = harvest()["snapshots"]
        self.assertIsNone(snaps["checkpoints_in_window"])
        self.assertIsNone(snaps["checkpoints_cumulative"])
        self.assertIsNone(snaps["throughput_mb_s"])
        self.assertIsNone(snaps["size_p95_mb"])
        self.assertIsNone(snaps["size_avg_mb"])

        # A 0.0 median is falsy but real, and must still produce throughput.
        mock_range.side_effect = EMPTY_RANGES
        mock_instant.side_effect = snapshot_instants("0.0", "0.0")
        snaps = harvest()["snapshots"]
        self.assertEqual(snaps["size_p50_mb"], 0.0)
        self.assertEqual(snaps["checkpoints_in_window"], 50)
        self.assertEqual(snaps["throughput_mb_s"], 0.0)

        # Counter went backwards, so an atelet restarted and the delta is
        # unknowable. 0 would read as "nothing was checkpointed".
        mock_range.side_effect = EMPTY_RANGES
        mock_instant.side_effect = snapshot_instants("1.0", "1.0",
                                                     c_start="900", c_end="150")
        snaps = harvest()["snapshots"]
        self.assertIsNone(snaps["checkpoints_in_window"])
        self.assertIsNone(snaps["throughput_mb_s"])
        self.assertEqual(snaps["checkpoints_cumulative"], 150)

        # Windowed delta over windowed count: 100 MiB across 50 checkpoints is
        # 2.0 MB. A lifetime mean would read 400/150 = 2.667 instead.
        mock_range.side_effect = EMPTY_RANGES
        mock_instant.side_effect = snapshot_instants(
            "1.0", "1.5", size_p95="1.75",
            size_sum_start=str(300 * 1024 * 1024),
            size_sum_end=str(400 * 1024 * 1024),
        )
        snaps = harvest()["snapshots"]
        self.assertEqual(snaps["size_p95_mb"], 1.75)
        self.assertEqual(snaps["size_avg_mb"], 2.0)

        # The mock replies by position, so only the query text proves p95 was
        # asked for. restore_p95 also uses 0.95, hence the metric name too.
        queries = [c.args[1] for c in mock_instant.call_args_list]
        self.assertTrue(any("histogram_quantile(0.95" in q
                            and "atelet_snapshot_size_bytes" in q
                            for q in queries))

        # Likewise time_ts, or both sum endpoints could read the same instant.
        sum_times = {c.kwargs.get("time_ts") for c in mock_instant.call_args_list
                     if c.args[1] == "sum(atelet_snapshot_size_bytes_sum)"}
        self.assertEqual(len(sum_times), 2)

        # Sum went backwards even though the counts rose, so a restart again.
        mock_range.side_effect = EMPTY_RANGES
        mock_instant.side_effect = snapshot_instants(
            "1.0", "1.0",
            size_sum_start=str(400 * 1024 * 1024),
            size_sum_end=str(300 * 1024 * 1024),
        )
        self.assertIsNone(harvest()["snapshots"]["size_avg_mb"])

        # A zero count means nothing was snapshotted: unknown, not 0 MB.
        mock_range.side_effect = EMPTY_RANGES
        mock_instant.side_effect = snapshot_instants(
            "1.0", "1.0", c_start="0", c_end="0",
            size_sum_start="0", size_sum_end="0",
        )
        self.assertIsNone(harvest()["snapshots"]["size_avg_mb"])


if __name__ == "__main__":
    unittest.main()
