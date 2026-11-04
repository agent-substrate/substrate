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

TESTDATA = Path(__file__).resolve().parent / "testdata"

# Deltas stored in each golden run's capacity.json gateway_counters.
GOLDEN_DELTAS = {
    "http": {
        "egress_connect_2xx": 4053,
        "egress_downstream_cx_total": 5,
        "mitm_downstream_cx_total": 4053,
        "mitm_ssl_handshake": 0,
        "cleartext_ext_proc_streams": 2055885,
        "mitm_ext_proc_streams": 0,
        "passthrough_cx_total": 0,
        "ext_proc_upstream_rq_total": 2059938,
        "forward_proxy_cx_overflow": 0,
        "forward_proxy_rq_pending_overflow": 0,
    },
    "mitm": {
        "egress_connect_2xx": 3954,
        "egress_downstream_cx_total": 5,
        "mitm_downstream_cx_total": 3954,
        "mitm_ssl_handshake": 3954,
        "cleartext_ext_proc_streams": 0,
        "mitm_ext_proc_streams": 2104792,
        "passthrough_cx_total": 0,
        "ext_proc_upstream_rq_total": 2108746,
        "forward_proxy_cx_overflow": 0,
        "forward_proxy_rq_pending_overflow": 0,
    },
    "passthrough": {
        "egress_connect_2xx": 6697,
        "egress_downstream_cx_total": 7,
        "mitm_downstream_cx_total": 6697,
        "mitm_ssl_handshake": 0,
        "cleartext_ext_proc_streams": 0,
        "mitm_ext_proc_streams": 0,
        "passthrough_cx_total": 6697,
        "ext_proc_upstream_rq_total": 6697,
        "forward_proxy_cx_overflow": 0,
        "forward_proxy_rq_pending_overflow": 0,
    },
}


def _golden(name: str) -> str:
    return (TESTDATA / f"{name}.prom").read_text()


def _golden_deltas(mode: str) -> dict[str, int]:
    return egress_output.gateway_counter_deltas(
        egress_output.parse_gateway_prometheus(_golden(f"{mode}_before")),
        egress_output.parse_gateway_prometheus(_golden(f"{mode}_after")),
    )


def _stage(
    rps: int, sent: int, ok: int, p99_s: str, failed: str | None = None, q2c_p50_s: str = "0.001s"
) -> dict:
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
                            {"percentile": 0.5, "duration": q2c_p50_s},
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
        # Zero lines for the other three clusters follow the egress_forward_proxy
        # line, so the overflow row fails if the parser keeps only the last line.
        overflow_after = _golden("http_after").replace(
            'envoy_cluster_upstream_cx_overflow{envoy_cluster_name="egress_forward_proxy"} 0',
            'envoy_cluster_upstream_cx_overflow{envoy_cluster_name="egress_forward_proxy"} 3',
        ).replace(
            'envoy_cluster_upstream_rq_pending_overflow{envoy_cluster_name="egress_forward_proxy"} 0',
            'envoy_cluster_upstream_rq_pending_overflow{envoy_cluster_name="egress_forward_proxy"} 2',
        )
        cases = [
            ("http golden", _golden("http_before"), _golden("http_after"), GOLDEN_DELTAS["http"]),
            ("mitm golden", _golden("mitm_before"), _golden("mitm_after"), GOLDEN_DELTAS["mitm"]),
            ("passthrough golden", _golden("passthrough_before"), _golden("passthrough_after"),
             GOLDEN_DELTAS["passthrough"]),
            ("http with one forward proxy cluster overflowing", _golden("http_before"), overflow_after,
             {**GOLDEN_DELTAS["http"], "forward_proxy_cx_overflow": 3, "forward_proxy_rq_pending_overflow": 2}),
        ]
        for label, before, after, want in cases:
            with self.subTest(label=label):
                got = egress_output.gateway_counter_deltas(
                    egress_output.parse_gateway_prometheus(before),
                    egress_output.parse_gateway_prometheus(after),
                )
                self.assertEqual(got, want)

    def test_capacity_summary(self):
        cap = egress_output.capacity_summary(
            SAMPLE_OUTPUT,
            mode="mitm",
            envoy_cpu=2,
            client_concurrency=4,
            connections=16,
            tail_latency_slo_ms=25.0,
            gateway_counters=_golden_deltas("mitm"),
        )
        self.assertTrue(cap["converged"])
        self.assertTrue(cap["testing_stage_completed"])
        self.assertTrue(cap["testing_passed"])
        self.assertAlmostEqual(cap["tunnels_per_stage"], 988.5)
        self.assertEqual(cap["slo_max_stage"], "adjusting_001")
        self.assertAlmostEqual(cap["slo_max_rps"], 750.0)
        self.assertEqual(cap["client_pool_bound"], False)
        self.assertEqual(cap["ext_proc_invariant_ok"], True)
        self.assertEqual(cap["mode_exclusivity_ok"], True)
        self.assertEqual(cap["forward_proxy_overflow_ok"], True)
        self.assertEqual(cap["gateway_counter_reset"], False)
        self.assertEqual(cap["binding_threshold"], ["latency-ns-mean-plus-2stdev"])
        self.assertEqual(cap["gateway_counters"]["mitm_ssl_handshake"], 3954)
        self.assertAlmostEqual(cap["cx_churn_ratio"], 1.0)
        self.assertAlmostEqual(cap["latency_pstdev_ms"], 2.0)
        self.assertAlmostEqual(cap["queue_to_connect_p99_ms"], 4.0)
        self.assertAlmostEqual(cap["sequencer_blocking_p99_ms"], 0.8)

        without = egress_output.capacity_summary(SAMPLE_OUTPUT, mode="mitm", envoy_cpu=2)
        self.assertIsNone(without["tunnels_per_stage"])

    def test_mode_exclusivity_and_forward_proxy_overflow(self):
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

    def test_session_shapes(self):
        cases = [
            (
                "failed session without a testing stage",
                {
                    "session_status": {"code": 8, "message": "Max RPS below min"},
                    "adjusting_stage_results": [_stage(100, 5000, 4000, "0.100s", failed="success-rate")],
                },
                {
                    "converged": False,
                    "testing_stage_completed": False,
                    "testing_passed": False,
                    "slo_max_rps": None,
                    "binding_threshold": ["success-rate"],
                },
            ),
            (
                "deadline after a failed testing stage",
                {
                    "session_status": {"code": 4, "message": "convergence deadline"},
                    "adjusting_stage_results": [
                        _stage(100, 8000, 8000, "0.018s"),
                        _stage(200, 15000, 14985, "0.024s"),
                    ],
                    "testing_stage_result": _stage(
                        250, 20000, 18000, "0.035s", failed="latency-ns-mean-plus-2stdev"
                    ),
                },
                {
                    "converged": False,
                    "testing_stage_completed": True,
                    "testing_passed": False,
                    "slo_max_stage": "adjusting_001",
                    "slo_max_rps": 750.0,
                    "client_pool_bound": False,
                    "ext_proc_invariant_ok": None,
                    "mode_exclusivity_ok": None,
                    "forward_proxy_overflow_ok": None,
                    "gateway_counter_reset": False,
                    "binding_threshold": ["latency-ns-mean-plus-2stdev"],
                },
            ),
            (
                "ok session that ended before the testing stage",
                {
                    "session_status": {"code": 0},
                    "adjusting_stage_results": [
                        _stage(100, 8000, 8000, "0.018s"),
                        _stage(200, 15000, 14985, "0.024s"),
                    ],
                },
                {"converged": False, "testing_stage_completed": False},
            ),
            (
                "client waits 5 ms at p50 for a connection",
                {
                    **SAMPLE_OUTPUT,
                    "adjusting_stage_results": [
                        _stage(100, 8000, 8000, "0.018s"),
                        _stage(200, 15000, 14985, "0.024s", q2c_p50_s="0.005s"),
                    ],
                },
                {"slo_max_stage": "adjusting_001", "client_pool_bound": True},
            ),
        ]
        for label, output_dict, want in cases:
            with self.subTest(label=label):
                cap = egress_output.capacity_summary(output_dict, mode="mitm", envoy_cpu=2)
                self.assertEqual({k: cap[k] for k in want}, want)


if __name__ == "__main__":
    unittest.main()
