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
import math
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

# Ensure benchmarking/locust is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import server_telemetry


class ComputePercentilesTest(unittest.TestCase):
    def test_normal_distribution(self):
        vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        res = server_telemetry.compute_percentiles(vals)
        self.assertEqual(res["min"], 1.0)
        self.assertEqual(res["max"], 10.0)
        self.assertEqual(res["p50"], 6.0)
        self.assertEqual(res["avg"], 5.5)

    def test_nan_and_inf_filtering(self):
        vals = [1.0, float("nan"), 2.0, float("inf"), float("-inf"), 3.0]
        res = server_telemetry.compute_percentiles(vals)
        self.assertEqual(res["min"], 1.0)
        self.assertEqual(res["max"], 3.0)
        self.assertEqual(res["p50"], 2.0)
        self.assertEqual(res["avg"], 2.0)

    def test_empty_or_all_nan(self):
        self.assertEqual(
            server_telemetry.compute_percentiles([]),
            {"min": None, "p50": None, "p90": None, "p99": None, "max": None, "avg": None},
        )
        self.assertEqual(
            server_telemetry.compute_percentiles([float("nan")]),
            {"min": None, "p50": None, "p90": None, "p99": None, "max": None, "avg": None},
        )


class SteadyStateWindowTest(unittest.TestCase):
    def test_window_detection(self):
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as f:
            writer = csv.DictWriter(
                f, fieldnames=["Timestamp", "Name", "User Count"]
            )
            writer.writeheader()
            writer.writerow({"Timestamp": "100", "Name": "Aggregated", "User Count": "2"})
            writer.writerow({"Timestamp": "110", "Name": "Aggregated", "User Count": "4"})
            writer.writerow({"Timestamp": "120", "Name": "Aggregated", "User Count": "7"})
            writer.writerow({"Timestamp": "130", "Name": "Aggregated", "User Count": "7"})
            csv_path = Path(f.name)

        try:
            start_ts, end_ts = 100, 150
            steady_start, steady_end = server_telemetry.get_steady_state_window(
                csv_path, active_users=7, start_ts=start_ts, end_ts=end_ts
            )
            # 7 >= 0.9 * 7 (6.3) at timestamp 120
            self.assertEqual(steady_start, 120)
            self.assertEqual(steady_end, 150)
        finally:
            csv_path.unlink()

    def test_fallback_when_threshold_not_reached(self):
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as f:
            writer = csv.DictWriter(
                f, fieldnames=["Timestamp", "Name", "User Count"]
            )
            writer.writeheader()
            writer.writerow({"Timestamp": "100", "Name": "Aggregated", "User Count": "2"})
            csv_path = Path(f.name)

        try:
            steady_start, steady_end = server_telemetry.get_steady_state_window(
                csv_path, active_users=10, start_ts=100, end_ts=150
            )
            self.assertEqual(steady_start, 100)
            self.assertEqual(steady_end, 150)
        finally:
            csv_path.unlink()


class PrometheusQueryTest(unittest.TestCase):
    @mock.patch("urllib.request.urlopen")
    def test_query_prometheus_range_guard(self, mock_urlopen):
        mock_resp = mock.MagicMock()
        mock_resp.read.return_value = json.dumps({
            "status": "success",
            "data": {"result": [{"metric": {}, "values": [[100, "1.0"]]}]}
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        # start_ts == end_ts should be automatically guarded to end_ts = start_ts + 1
        res = server_telemetry.query_prometheus_range("http://localhost:9090", "up", 100, 100)
        self.assertEqual(len(res), 1)

    @mock.patch("server_telemetry.query_prometheus_instant")
    def test_quantile_fallback(self, mock_instant):
        # First call (rate query) returns empty or NaN
        # Second call (cumulative query) returns valid 11.6 MB
        mock_instant.side_effect = [
            [{"value": [100, "NaN"]}],
            [{"value": [100, "11.625"]}],
        ]
        val = server_telemetry._query_quantile_with_fallback(
            "http://localhost:9090",
            0.50,
            "rate(bucket[5m])",
            "bucket",
            end_ts=100,
        )
        self.assertEqual(val, 11.625)
        self.assertEqual(mock_instant.call_count, 2)


class HarvestServerTelemetryTest(unittest.TestCase):
    @mock.patch("server_telemetry.query_prometheus_range")
    @mock.patch("server_telemetry.query_prometheus_instant")
    def test_harvest_packing_math(self, mock_instant, mock_range):
        # Mock packing timeseries: assigned=4 with worker_pod_count=5
        mock_range.side_effect = [
            # Packing query
            [
                {
                    "metric": {"ate_worker_state": "assigned"},
                    "values": [[100, "4.0"], [105, "4.0"]],
                }
            ],
            # CPU PSI
            [{"values": [[100, "1.5"], [105, "1.8"]]}],
            # Memory PSI
            [{"values": [[100, "0.0"], [105, "0.0"]]}],
            # IO PSI
            [{"values": [[100, "0.0"], [105, "0.0"]]}],
            # CFS Throttled
            [{"values": [[100, "0.01"], [105, "0.02"]]}],
        ]

        # Instant queries: snap size, count start, count end, restore p50, restore p95, ckpt p50, ckpt p95
        mock_instant.side_effect = [
            [{"value": [105, "11.5"]}],  # snap size p50
            [{"value": [105, "12.0"]}],  # snap size p90
            [{"value": [100, "100"]}],   # count start
            [{"value": [105, "150"]}],   # count end
            [{"value": [105, "0.08"]}],  # restore p50
            [{"value": [105, "0.15"]}],  # restore p95
            [{"value": [105, "0.12"]}],  # ckpt p50
            [{"value": [105, "0.22"]}],  # ckpt p95
        ]

        summary = server_telemetry.harvest_server_telemetry(
            prom_url="http://localhost:9090",
            start_ts=100,
            end_ts=105,
            steady_start_ts=100,
            worker_pod_count=5,
        )

        packing = summary["cluster_packing"]
        # 4 assigned / 5 total worker pods = 0.80
        self.assertEqual(packing["summary"]["p50"], 0.8)
        self.assertEqual(packing["timeseries"][0]["packing_ratio"], 0.8)
        self.assertEqual(packing["timeseries"][0]["total_workers"], 5.0)

        snapshots = summary["snapshots"]
        self.assertEqual(snapshots["checkpoints_in_window"], 50)
        self.assertEqual(snapshots["checkpoints_cumulative"], 150)
        self.assertEqual(snapshots["size_p50_mb"], 11.5)
        self.assertIsNotNone(snapshots["throughput_mb_s"])


if __name__ == "__main__":
    unittest.main()
