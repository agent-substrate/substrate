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

"""Unit tests for benchmarking/nighthawk-egress/output.py."""

from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import output as egress_output

# Excerpt from a real GKE gateway_after.prom snapshot (atenet-egress:15090/stats/prometheus).
PROM_BEFORE = """
# TYPE envoy_cluster_upstream_rq_total counter
envoy_cluster_upstream_rq_total{envoy_cluster_name="envoy_admin"} 66
envoy_cluster_upstream_rq_total{envoy_cluster_name="ext_proc_server"} 10
# TYPE envoy_http_downstream_cx_total counter
envoy_http_downstream_cx_total{envoy_http_conn_manager_prefix="admin_metrics"} 66
envoy_http_downstream_cx_total{envoy_http_conn_manager_prefix="egress_connect"} 2
# TYPE envoy_http_downstream_rq_xx counter
envoy_http_downstream_rq_xx{envoy_response_code_class="2",envoy_http_conn_manager_prefix="admin_metrics"} 65
envoy_http_downstream_rq_xx{envoy_response_code_class="2",envoy_http_conn_manager_prefix="egress_connect"} 10
# TYPE envoy_http_ext_proc_egress_policy_cleartext_streams_started counter
envoy_http_ext_proc_egress_policy_cleartext_streams_started{envoy_http_conn_manager_prefix="mitm_cleartext"} 0
# TYPE envoy_http_ext_proc_egress_policy_mitm_streams_started counter
envoy_http_ext_proc_egress_policy_mitm_streams_started{envoy_http_conn_manager_prefix="mitm_http"} 0
# TYPE envoy_listener_downstream_cx_total counter
envoy_listener_downstream_cx_total{envoy_listener_address="[__]_443"} 2
envoy_listener_downstream_cx_total{envoy_listener_address="mitm"} 10
# TYPE envoy_listener_ssl_handshake counter
envoy_listener_ssl_handshake{envoy_listener_address="[__]_443"} 2
envoy_listener_ssl_handshake{envoy_listener_address="mitm"} 0
# TYPE envoy_tcp_downstream_cx_total counter
envoy_tcp_downstream_cx_total{envoy_tcp_prefix="passthrough"} 0
"""

PROM_AFTER_MITM = """
# TYPE envoy_cluster_upstream_rq_total counter
envoy_cluster_upstream_rq_total{envoy_cluster_name="envoy_admin"} 68
envoy_cluster_upstream_rq_total{envoy_cluster_name="ext_proc_server"} 845376
# TYPE envoy_http_downstream_cx_total counter
envoy_http_downstream_cx_total{envoy_http_conn_manager_prefix="admin_metrics"} 68
envoy_http_downstream_cx_total{envoy_http_conn_manager_prefix="egress_connect"} 12
# TYPE envoy_http_downstream_rq_xx counter
envoy_http_downstream_rq_xx{envoy_response_code_class="2",envoy_http_conn_manager_prefix="admin_metrics"} 67
envoy_http_downstream_rq_xx{envoy_response_code_class="2",envoy_http_conn_manager_prefix="egress_connect"} 6374
# TYPE envoy_http_ext_proc_egress_policy_cleartext_streams_started counter
envoy_http_ext_proc_egress_policy_cleartext_streams_started{envoy_http_conn_manager_prefix="mitm_cleartext"} 0
# TYPE envoy_http_ext_proc_egress_policy_mitm_streams_started counter
envoy_http_ext_proc_egress_policy_mitm_streams_started{envoy_http_conn_manager_prefix="mitm_http"} 839002
# TYPE envoy_listener_downstream_cx_total counter
envoy_listener_downstream_cx_total{envoy_listener_address="[__]_443"} 12
envoy_listener_downstream_cx_total{envoy_listener_address="mitm"} 6374
# TYPE envoy_listener_ssl_handshake counter
envoy_listener_ssl_handshake{envoy_listener_address="[__]_443"} 12
envoy_listener_ssl_handshake{envoy_listener_address="mitm"} 6364
# TYPE envoy_tcp_downstream_cx_total counter
envoy_tcp_downstream_cx_total{envoy_tcp_prefix="passthrough"} 0
"""


def _stage(rps: int, sent: int, ok: int, p99_s: str, failed: str | None = None) -> dict:
    evals = [
        {"metric_id": "nighthawk.builtin/attempted-rps", "metric_value": float(rps * 4)},
        {"metric_id": "nighthawk.builtin/achieved-rps", "metric_value": sent / 20.0},
        {"metric_id": "nighthawk.builtin/send-rate", "metric_value": (sent / 20.0) / (rps * 4)},
        {"metric_id": "nighthawk.builtin/success-rate", "metric_value": ok / sent, "threshold_score": 1.0},
    ]
    if failed:
        evals.append({"metric_id": f"nighthawk.builtin/{failed}", "metric_value": 1.0, "threshold_score": -1.0})
    return {
        "start_time": "2026-11-02T00:00:00Z",
        "end_time": "2026-11-02T00:00:20Z",
        "metric_evaluations": evals,
        "nighthawk_service_output": {
            "options": {"requests_per_second": rps, "concurrency": "4", "connections": 16},
            "results": [{
                "name": "global",
                "execution_duration": "20s",
                "counters": [
                    {"name": "upstream_rq_total", "value": str(sent)},
                    {"name": "benchmark.http_2xx", "value": str(ok)},
                    {"name": "benchmark.http_5xx", "value": str(sent - ok)},
                    {"name": "ssl.handshake", "value": "64"},
                    {"name": "upstream_cx_total", "value": "64"},
                    {"name": "upstream_cx_connect_fail", "value": "0"},
                ],
                "statistics": [
                    {
                        "id": egress_output.LATENCY_STAT_ID,
                        "mean": "0.005s",
                        "pstdev": "0.002s",
                        "min": "0.001s",
                        "max": "0.050s",
                        "percentiles": [
                            {"percentile": 0.5, "duration": "0.004s"},
                            {"percentile": 0.9, "duration": "0.010s"},
                            {"percentile": 0.95, "duration": "0.015s"},
                            {"percentile": 0.99, "duration": p99_s},
                        ],
                    },
                    {
                        "id": egress_output.CONNECT_STAT_ID,
                        "count": "64",
                        "mean": "0.001s",
                        "pstdev": "0.0005s",
                        "max": "0.005s",
                        "percentiles": [
                            {"percentile": 0.5, "duration": "0.001s"},
                            {"percentile": 0.99, "duration": "0.004s"},
                        ],
                    },
                    {
                        "id": egress_output.SEQUENCER_BLOCKING_STAT_ID,
                        "mean": "0.0001s",
                        "max": "0.001s",
                        "percentiles": [
                            {"percentile": 0.99, "duration": "0.0008s"},
                        ],
                    },
                ],
            }],
        },
    }


SAMPLE_OUTPUT = {
    "session_status": {"code": 0},
    "adjusting_stage_results": [
        _stage(100, 8000, 8000, "0.018s"),
        _stage(200, 15000, 14985, "0.024s"),
        _stage(400, 22000, 20000, "0.080s", failed="latency-ns-mean-plus-2stdev"),
    ],
    "testing_stage_result": _stage(200, 44940, 44910, "0.023s"),
}


class TestEgressOutput(unittest.TestCase):
    def test_stage_rows_and_stats_records(self):
        rows = egress_output.stage_rows(SAMPLE_OUTPUT)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["stage"], "adjusting_000")
        self.assertAlmostEqual(rows[0]["metric_nighthawk.builtin_achieved_rps"], 400.0)
        self.assertEqual(rows[0]["ssl.handshake"], 64)
        self.assertEqual(rows[0]["upstream_cx_total"], 64)
        self.assertAlmostEqual(rows[0]["requests_per_connection"], 125.0)
        self.assertAlmostEqual(rows[0]["cx_churn_ratio"], 1.0)
        self.assertAlmostEqual(rows[0]["error_rate"], 0.0)
        self.assertAlmostEqual(rows[0]["latency_pstdev_ms"], 2.0)
        self.assertEqual(rows[0]["queue_to_connect_count"], 64)
        self.assertAlmostEqual(rows[0]["queue_to_connect_mean_ms"], 1.0)
        self.assertAlmostEqual(rows[0]["queue_to_connect_pstdev_ms"], 0.5)
        self.assertAlmostEqual(rows[0]["queue_to_connect_p99_ms"], 4.0)
        self.assertAlmostEqual(rows[0]["sequencer_blocking_p99_ms"], 0.8)
        self.assertEqual(rows[2]["failed_thresholds"], ["latency-ns-mean-plus-2stdev"])
        self.assertEqual(rows[3]["stage"], "testing")
        self.assertAlmostEqual(rows[3]["p99_ms"], 23.0)

        recs = egress_output.stats_records(rows, tag="abc1234", test_name="egress_mitm")
        self.assertEqual(len(recs), 4)
        self.assertEqual(recs[-1]["metric"], "nighthawk_testing")
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "stats.jsonl"
            egress_output.write_jsonl(recs, p)
            self.assertEqual(len(p.read_text().strip().splitlines()), 4)

    def test_prometheus_gateway_counter_deltas(self):
        before = egress_output.parse_gateway_prometheus(PROM_BEFORE)
        after = egress_output.parse_gateway_prometheus(PROM_AFTER_MITM)
        deltas = egress_output.gateway_counter_deltas(before, after)
        self.assertEqual(
            deltas,
            {
                "egress_connect_2xx": 6364,
                "egress_downstream_cx_total": 10,
                "mitm_downstream_cx_total": 6364,
                "mitm_ssl_handshake": 6364,
                "cleartext_ext_proc_streams": 0,
                "mitm_ext_proc_streams": 839002,
                "passthrough_cx_total": 0,
                "ext_proc_upstream_rq_total": 845366,
                "forward_proxy_cx_overflow": 0,
                "forward_proxy_rq_pending_overflow": 0,
            },
        )

    def test_capacity_summary(self):
        deltas = egress_output.gateway_counter_deltas(
            egress_output.parse_gateway_prometheus(PROM_BEFORE),
            egress_output.parse_gateway_prometheus(PROM_AFTER_MITM),
        )
        cap = egress_output.capacity_summary(
            SAMPLE_OUTPUT,
            mode="mitm",
            envoy_cpu=2,
            client_concurrency=4,
            connections=16,
            tail_latency_slo_ms=25.0,
            gateway_counters=deltas,
        )
        self.assertTrue(cap["converged"])
        self.assertTrue(cap["testing_stage_completed"])
        self.assertTrue(cap["testing_passed"])
        self.assertAlmostEqual(cap["tunnels_per_stage"], 1591.0)
        self.assertEqual(cap["slo_max_stage"], "adjusting_001")
        self.assertAlmostEqual(cap["slo_max_rps"], 750.0)
        self.assertEqual(cap["client_pool_bound"], False)
        self.assertEqual(cap["ext_proc_invariant_ok"], True)
        self.assertEqual(cap["mode_exclusivity_ok"], True)
        self.assertEqual(cap["forward_proxy_overflow_ok"], True)
        self.assertEqual(cap["gateway_counter_reset"], False)
        self.assertEqual(cap["binding_threshold"], ["latency-ns-mean-plus-2stdev"])
        self.assertEqual(cap["gateway_counters"]["mitm_ssl_handshake"], 6364)
        self.assertAlmostEqual(cap["requests_per_connection"], 234.14)
        self.assertAlmostEqual(cap["cx_churn_ratio"], 1.0)
        self.assertAlmostEqual(cap["latency_pstdev_ms"], 2.0)
        self.assertEqual(cap["queue_to_connect_count"], 64)
        self.assertAlmostEqual(cap["queue_to_connect_mean_ms"], 1.0)
        self.assertAlmostEqual(cap["queue_to_connect_pstdev_ms"], 0.5)
        self.assertAlmostEqual(cap["queue_to_connect_p99_ms"], 4.0)
        self.assertAlmostEqual(cap["sequencer_blocking_p99_ms"], 0.8)

    def test_mode_exclusivity_and_forward_proxy_overflow(self):
        prom_overflow = PROM_AFTER_MITM + (
            '\nenvoy_cluster_upstream_cx_overflow{envoy_cluster_name="egress_forward_proxy_passthrough"} 3\n'
            'envoy_cluster_upstream_rq_pending_overflow{envoy_cluster_name="egress_forward_proxy"} 2\n'
        )
        parsed = egress_output.parse_gateway_prometheus(prom_overflow)
        self.assertEqual(parsed["forward_proxy_cx_overflow"], 3)
        self.assertEqual(parsed["forward_proxy_rq_pending_overflow"], 2)

        base_counters = {k: 0 for k in egress_output.GATEWAY_COUNTER_KEYS}
        cases = [
            ("http clean", "http", {"cleartext_ext_proc_streams": 100}, True, True),
            ("http with passthrough leak", "http", {"cleartext_ext_proc_streams": 100, "passthrough_cx_total": 1}, False, True),
            ("http with mitm handshake leak", "http", {"cleartext_ext_proc_streams": 100, "mitm_ssl_handshake": 1}, False, True),
            ("mitm clean", "mitm", {"mitm_ssl_handshake": 10, "mitm_ext_proc_streams": 100}, True, True),
            ("mitm with cleartext leak", "mitm", {"mitm_ssl_handshake": 10, "cleartext_ext_proc_streams": 1}, False, True),
            ("mitm with passthrough leak", "mitm", {"mitm_ssl_handshake": 10, "passthrough_cx_total": 1}, False, True),
            ("passthrough clean", "passthrough", {"passthrough_cx_total": 64}, True, True),
            ("passthrough with mitm handshake leak", "passthrough", {"passthrough_cx_total": 64, "mitm_ssl_handshake": 1}, False, True),
            ("passthrough with cleartext leak", "passthrough", {"passthrough_cx_total": 64, "cleartext_ext_proc_streams": 1}, False, True),
            ("cx overflow", "passthrough", {"passthrough_cx_total": 64, "forward_proxy_cx_overflow": 1}, True, False),
            ("rq pending overflow", "http", {"cleartext_ext_proc_streams": 100, "forward_proxy_rq_pending_overflow": 5}, True, False),
        ]
        for label, mode, overrides, want_excl, want_ovf in cases:
            with self.subTest(label=label):
                cap = egress_output.capacity_summary(
                    SAMPLE_OUTPUT,
                    mode=mode,
                    envoy_cpu=1,
                    gateway_counters={**base_counters, **overrides},
                )
                self.assertEqual(cap["mode_exclusivity_ok"], want_excl)
                self.assertEqual(cap["forward_proxy_overflow_ok"], want_ovf)

    def test_non_converged_summary(self):
        non_conv = {
            "session_status": {"code": 8, "message": "Max RPS below min"},
            "adjusting_stage_results": [_stage(100, 5000, 4000, "0.100s", failed="success-rate")],
        }
        cap = egress_output.capacity_summary(non_conv, mode="http", envoy_cpu=2)
        self.assertFalse(cap["converged"])
        self.assertFalse(cap["testing_stage_completed"])
        self.assertFalse(cap["testing_passed"])
        self.assertIsNone(cap["slo_max_rps"])
        self.assertEqual(cap["binding_threshold"], ["success-rate"])

    def test_knife_edge_testing_stage_completed_not_converged(self):
        # Testing stage ran and completed, but session failed with deadline (code != 0)
        knife_edge = {
            "session_status": {"code": 4, "message": "convergence deadline"},
            "adjusting_stage_results": [
                _stage(100, 8000, 8000, "0.018s"),
                _stage(200, 15000, 14985, "0.024s"),
            ],
            "testing_stage_result": _stage(250, 20000, 18000, "0.035s", failed="latency-ns-mean-plus-2stdev"),
        }
        cap = egress_output.capacity_summary(knife_edge, mode="mitm", envoy_cpu=2)
        self.assertFalse(cap["converged"])
        self.assertTrue(cap["testing_stage_completed"])
        self.assertFalse(cap["testing_passed"])
        # slo_max_rps is populated from the highest clean adjusting stage (adjusting_001)
        self.assertEqual(cap["slo_max_stage"], "adjusting_001")
        self.assertAlmostEqual(cap["slo_max_rps"], 750.0)
        self.assertEqual(cap["client_pool_bound"], False)
        self.assertIsNone(cap["ext_proc_invariant_ok"])
        self.assertIsNone(cap["mode_exclusivity_ok"])
        self.assertIsNone(cap["forward_proxy_overflow_ok"])
        self.assertEqual(cap["gateway_counter_reset"], False)
        self.assertEqual(cap["binding_threshold"], ["latency-ns-mean-plus-2stdev"])


if __name__ == "__main__":
    unittest.main()
