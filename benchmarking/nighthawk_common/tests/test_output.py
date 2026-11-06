# Copyright 2026 The Agent Substrate Authors
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

"""Unit tests for the stage-row fields nighthawk_common.output derives."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from nighthawk_common import output as output_mod


def statistic(stat_id: str, mean: str, pstdev: str, max_: str, points, count="0") -> dict:
    return {
        "id": stat_id,
        "count": count,
        "mean": mean,
        "pstdev": pstdev,
        "max": max_,
        "percentiles": [{"percentile": p, "duration": d} for p, d in points],
    }


def latency_statistic() -> dict:
    return statistic(
        output_mod.LATENCY_STAT_ID, "0.004s", "0.002s", "0.100s",
        [(0.5, "0.003s"), (0.990625, "0.020s")],
    )


def stage_result(*, statistics=(), duration="10s", rps=25, sent=1000, evaluations=()) -> dict:
    return {
        "nighthawk_service_output": {
            "options": {"requests_per_second": rps, "concurrency": "4"},
            "results": [
                {
                    "name": "global",
                    "execution_duration": duration,
                    "counters": [
                        {"name": "benchmark.http_2xx", "value": str(sent)},
                        {"name": "upstream_rq_total", "value": str(sent)},
                    ],
                    "statistics": list(statistics),
                }
            ],
        },
        "metric_evaluations": list(evaluations),
    }


def stage_row(stage: dict) -> dict:
    return output_mod.stage_rows({"adjusting_stage_results": [stage]})[0]


def test_subsecond_duration_recomputes_exact_rps_and_send_rate():
    # Nighthawk C++ metrics_plugin_impl.cc truncates 19.990s -> 19s via DurationToSeconds.
    stage = stage_result(
        duration="19.990s", rps=25, sent=1999,
        evaluations=[
            {"metric_id": "nighthawk.builtin/achieved-rps", "metric_value": 1999.0 / 19.0},
            {"metric_id": "nighthawk.builtin/send-rate", "metric_value": (1999.0 / 19.0) / 100.0},
        ],
    )
    row = stage_row(stage)
    assert abs(row["metric_nighthawk.builtin_achieved_rps"] - 100.0) < 1e-6
    assert abs(row["metric_nighthawk.builtin_send_rate"] - 1.0) < 1e-6
    assert abs(row["metric_nighthawk.builtin_achieved_rps_cpp"] - (1999.0 / 19.0)) < 1e-3
    assert abs(row["metric_nighthawk.builtin_send_rate_cpp"] - ((1999.0 / 19.0) / 100.0)) < 1e-3


def test_stage_row_connect_and_blocking_telemetry():
    stage = stage_result(statistics=[
        latency_statistic(),
        statistic(
            output_mod.CONNECT_STAT_ID, "0.0005s", "0.0001s", "0.002s",
            [(0.5, "0.0004s"), (0.95, "0.001s"), (0.990625, "0.0015s")], count="4000",
        ),
        statistic(
            output_mod.SEQUENCER_BLOCKING_STAT_ID, "0.010s", "0.002s", "0.050s",
            [(0.5, "0.008s"), (0.990625, "0.040s")], count="12",
        ),
    ])
    row = stage_row(stage)
    assert row["latency_pstdev_ms"] == 2.0
    assert row["queue_to_connect_count"] == 4000
    assert row["queue_to_connect_mean_ms"] == 0.5
    assert row["queue_to_connect_pstdev_ms"] == 0.1
    assert row["queue_to_connect_max_ms"] == 2.0
    assert row["queue_to_connect_p50_ms"] == 0.4
    assert row["queue_to_connect_p99_ms"] == 1.5
    assert row["sequencer_blocking_p99_ms"] == 40.0
    assert "sequencer_blocking_p50_ms" not in row

    row = stage_row(stage_result(statistics=[latency_statistic()]))
    got = [k for k in row if k.startswith(("queue_to_connect_", "sequencer_blocking_"))]
    assert got == []
