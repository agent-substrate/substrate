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

"""Unit tests for AdaptiveLoadSessionOutput -> rows/records/summary.

Uses a canned dict in the exact shape json_format.MessageToDict(
preserving_proto_field_name=True) produces from a real session output
(uint64 counters as strings, Durations as '30s' strings)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import output as output_mod


def benchmark_result(
    rps: int, http_2xx: int, http_5xx: int = 0, failed_metric: str | None = None
) -> dict:
    total = http_2xx + http_5xx
    return {
        "nighthawk_service_output": {
            "options": {"requests_per_second": rps, "concurrency": "4"},
            "results": [
                {
                    "name": "global",
                    "execution_duration": "10s",
                    "counters": [
                        {"name": "benchmark.http_2xx", "value": str(http_2xx)},
                        {"name": "benchmark.http_5xx", "value": str(http_5xx)},
                        {"name": "upstream_rq_total", "value": str(total)},
                    ],
                    "statistics": [
                        {
                            "id": output_mod.LATENCY_STAT_ID,
                            "count": str(total),
                            "mean": "0.004s",
                            "min": "0.001s",
                            "max": "0.100s",
                            "percentiles": [
                                {"percentile": 0.5, "duration": "0.003s"},
                                {"percentile": 0.95, "duration": "0.008s"},
                                {"percentile": 0.990625, "duration": "0.020s"},
                            ],
                        }
                    ],
                }
            ],
        },
        "start_time": "2026-08-14T00:00:00Z",
        "end_time": "2026-08-14T00:00:10Z",
        "metric_evaluations": [
            {
                "metric_id": "nighthawk.builtin/success-rate",
                "metric_value": http_2xx / total if total else 0.0,
                "threshold_score": 1.0,
            },
            # informational metrics: no threshold_score field at all
            {
                "metric_id": "nighthawk.builtin/attempted-rps",
                "metric_value": float(rps * 4),
            },
            {
                "metric_id": "nighthawk.builtin/achieved-rps",
                "metric_value": total / 10.0,
            },
            {
                "metric_id": "nighthawk.builtin/send-rate",
                "metric_value": (total / 10.0) / (rps * 4) if rps else 0.0,
            },
        ] + ([
            {
                "metric_id": f"nighthawk.builtin/{failed_metric}",
                "metric_value": 999.0,
                "threshold_score": -1.0,
            }
        ] if failed_metric else []),
    }


SESSION = {
    "session_status": {},
    "adjusting_stage_results": [
        benchmark_result(rps=125, http_2xx=5000),
        benchmark_result(rps=250, http_2xx=9990, http_5xx=10),
    ],
    "testing_stage_result": benchmark_result(rps=200, http_2xx=8000),
}


def test_stage_rows():
    rows = output_mod.stage_rows(SESSION)
    assert [r["stage"] for r in rows] == [
        "adjusting_000",
        "adjusting_001",
        "testing",
    ]
    first = rows[0]
    assert first["metric_nighthawk.builtin_attempted_rps"] == 500.0
    assert first["metric_nighthawk.builtin_achieved_rps"] == 500.0
    assert first["metric_nighthawk.builtin_success_rate"] == 1.0
    assert first["metric_nighthawk.builtin_send_rate"] == 1.0
    assert first["p50_ms"] == 3.0
    assert first["p95_ms"] == 8.0
    # 0.990625 is the first HdrHistogram point at/above 0.99.
    assert first["p99_ms"] == 20.0
    assert first["latency_mean_ms"] == 4.0

    degraded = rows[1]
    assert degraded["metric_nighthawk.builtin_attempted_rps"] == 1000.0
    assert degraded["metric_nighthawk.builtin_achieved_rps"] == 1000.0  # sent counts errors too
    assert degraded["http_5xx"] == 10
    assert 0.998 < degraded["metric_nighthawk.builtin_success_rate"] < 1.0


def test_stats_records_shape():
    rows = output_mod.stage_rows(SESSION)
    records = output_mod.stats_records(rows, tag="abc123", test_name="cap2")
    assert len(records) == 3
    rec = records[-1]
    assert rec["metric"] == "nighthawk_testing"
    assert rec["tag"] == "abc123"
    assert rec["test_name"] == "cap2"
    assert rec["timestamp"] == "2026-08-14T00:00:00Z"
    assert rec["measurements"]["metric_nighthawk.builtin_attempted_rps"] == 800.0
    assert "stage" not in rec["measurements"]


def test_capacity_summary_converged():
    summary = output_mod.capacity_summary(
        SESSION, envoy_cpu=4, actors=100,
        client_concurrency=16, tail_latency_slo_ms=25,
    )
    assert summary["converged"] is True
    assert summary["envoy_cpu"] == 4
    assert summary["client_concurrency"] == 16
    assert summary["tail_latency_slo_ms"] == 25
    assert summary["adjusting_stages"] == 2
    assert "max_total_rps" not in summary
    assert summary["metric_nighthawk.builtin_attempted_rps"] == 800.0
    assert summary["metric_nighthawk.builtin_achieved_rps"] == 800.0
    assert summary["p99_ms"] == 20.0
    # slo_max = achieved-rps of the highest-attempted clean stage
    # (adjusting_001: 10000 sent / 10s).
    assert summary["binding_threshold"] == []
    assert summary["slo_max_stage"] == "adjusting_001"
    assert summary["slo_max_rps"] == 1000.0


def test_failed_thresholds_and_binding():
    session = {
        "session_status": {},
        "adjusting_stage_results": [
            benchmark_result(rps=125, http_2xx=5000),
            benchmark_result(
                rps=500, http_2xx=9000,
                failed_metric="latency-ns-mean-plus-2stdev",
            ),
            benchmark_result(rps=250, http_2xx=9000, failed_metric="send-rate"),
        ],
        "testing_stage_result": benchmark_result(rps=200, http_2xx=8000),
    }
    rows = output_mod.stage_rows(session)
    assert rows[0]["failed_thresholds"] == []
    assert rows[1]["failed_thresholds"] == ["latency-ns-mean-plus-2stdev"]
    # Threshold fields use short native names, not measurement keys.
    assert rows[2]["failed_thresholds"] == ["send-rate"]
    summary = output_mod.capacity_summary(session, envoy_cpu=2, actors=100)
    assert summary["binding_threshold"] == [
        "latency-ns-mean-plus-2stdev", "send-rate",
    ]
    # Failing stages are skipped; testing (800 attempted) is clean.
    assert summary["slo_max_stage"] == "testing"
    assert summary["slo_max_rps"] == 800.0


def test_capacity_summary_no_testing_stage():
    session = {
        "session_status": {"code": 4, "message": "convergence deadline"},
        "adjusting_stage_results": [benchmark_result(rps=125, http_2xx=100)],
    }
    summary = output_mod.capacity_summary(session, envoy_cpu=2, actors=10)
    assert summary["converged"] is False
    assert "max_total_rps" not in summary


if __name__ == "__main__":
    for fn_name, fn in sorted(dict(globals()).items()):
        if fn_name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {fn_name}")
