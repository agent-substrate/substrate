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

"""Unit tests for benchmarking/nighthawk-egress/spec.py."""

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import spec as egress_spec

FAKE_SERVICEDNS_CA = "-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n"


class TestEgressSpec(unittest.TestCase):
    def test_modes_uris_and_tls_contexts(self):
        self.assertEqual(egress_spec.build_egress_session_spec("http")["nighthawk_traffic_template"]["connections"], 16)
        tpls = {}
        for mode, uri in (
            ("http", "http://nighthawk-server.nighthawk.svc:80/"),
            ("mitm", "https://nighthawk-server.nighthawk.svc:443/"),
            ("passthrough", "https://nighthawk-server.nighthawk.svc:8443/"),
        ):
            with self.subTest(mode=mode):
                tpl = egress_spec.build_egress_session_spec(
                    mode, concurrency=4, connections=32, servicedns_ca_pem=FAKE_SERVICEDNS_CA
                )["nighthawk_traffic_template"]
                self.assertEqual(tpl["uri"], uri)
                self.assertEqual(tpl["concurrency"], "4")
                self.assertEqual(tpl["connections"], 32)
                self.assertEqual(tpl["max_pending_requests"], 32)
                self.assertTrue(tpl["open_loop"])
                self.assertEqual(tpl["failure_predicates"]["benchmark.pool_overflow"], "1000000000")
                tpls[mode] = tpl
        self.assertNotIn("transport_socket", tpls["http"])
        self.assertEqual(tpls["mitm"]["transport_socket"], {
            "name": "envoy.transport_sockets.tls",
            "typed_config": {
                "@type": "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext",
                "sni": "nighthawk-server.nighthawk.svc",
                "common_tls_context": {
                    "alpn_protocols": ["http/1.1"],
                    "validation_context": {
                        "trusted_ca": {"filename": egress_spec.ACTOR_TRUST_BUNDLE},
                        "match_typed_subject_alt_names": [
                            {"san_type": "DNS", "matcher": {"exact": "nighthawk-server.nighthawk.svc"}},
                        ],
                    },
                },
            },
        })
        passthrough_ctx = tpls["passthrough"]["transport_socket"]["typed_config"]["common_tls_context"]
        self.assertEqual(passthrough_ctx["validation_context"]["trusted_ca"], {"inline_string": FAKE_SERVICEDNS_CA})

    def test_thresholds_and_step_controller(self):
        s = egress_spec.build_egress_session_spec("mitm", concurrency=8, initial_rps=800, tail_latency_slo_ms=25.0)
        sc = s["step_controller_config"]["typed_config"]
        self.assertAlmostEqual(sc["initial_value"], 100.0)
        by_name = {t["metric_spec"]["metric_name"]: t["threshold_spec"]["scoring_function"]["typed_config"] for t in s["metric_thresholds"]}
        self.assertEqual(set(by_name), {"success-rate", "send-rate", "latency-ns-mean-plus-2stdev"})
        self.assertAlmostEqual(by_name["latency-ns-mean-plus-2stdev"]["upper_threshold"], 25_000_000.0)


if __name__ == "__main__":
    unittest.main()
