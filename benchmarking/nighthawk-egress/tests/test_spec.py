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

import os
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import spec as egress_spec

FAKE_SERVICEDNS_CA = "-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n"


class TestEgressSpec(unittest.TestCase):
    def test_modes_uris_and_tls_contexts(self):
        self.assertEqual(
            egress_spec.MODE_URIS,
            {
                "http": "http://nighthawk-server.nighthawk.svc:80/",
                "mitm": "https://nighthawk-server.nighthawk.svc:443/",
                "passthrough": "https://nighthawk-server.nighthawk.svc:8443/",
            },
        )
        self.assertEqual(egress_spec.DEFAULT_CONNECTIONS, 16)
        def_spec = egress_spec.build_egress_session_spec("http")
        self.assertEqual(def_spec["nighthawk_traffic_template"]["connections"], 16)
        for mode, uri in egress_spec.MODE_URIS.items():
            s = egress_spec.build_egress_session_spec(
                mode,
                concurrency=4,
                connections=32,
                initial_rps=400,
                servicedns_ca_pem=FAKE_SERVICEDNS_CA if mode == "passthrough" else None,
            )
            tpl = s["nighthawk_traffic_template"]
            self.assertEqual(tpl["uri"], uri)
            self.assertEqual(tpl["concurrency"], "4")
            self.assertEqual(tpl["connections"], 32)
            self.assertEqual(tpl["max_pending_requests"], egress_spec.DEFAULT_MAX_PENDING)
            self.assertTrue(tpl["open_loop"])
            self.assertEqual(tpl["failure_predicates"], egress_spec.EGRESS_FAILURE_PREDICATES)
            self.assertNotIn("tls_context", tpl)
            if mode == "http":
                self.assertNotIn("transport_socket", tpl)
            else:
                expected_ca = (
                    {"filename": egress_spec.ACTOR_TRUST_BUNDLE}
                    if mode == "mitm"
                    else {"inline_string": FAKE_SERVICEDNS_CA}
                )
                self.assertEqual(
                    tpl["transport_socket"],
                    {
                        "name": "envoy.transport_sockets.tls",
                        "typed_config": {
                            "@type": egress_spec.UPSTREAM_TLS_TYPE_URL,
                            "sni": egress_spec.UPSTREAM_SNI,
                            "common_tls_context": {
                                "alpn_protocols": ["http/1.1"],
                                "validation_context": {
                                    "trusted_ca": expected_ca,
                                    "match_typed_subject_alt_names": [
                                        {
                                            "san_type": "DNS",
                                            "matcher": {"exact": egress_spec.UPSTREAM_SNI},
                                        },
                                    ],
                                },
                            },
                        },
                    },
                )

    def test_thresholds_and_step_controller(self):
        s = egress_spec.build_egress_session_spec("mitm", concurrency=8, initial_rps=800, tail_latency_slo_ms=25.0)
        sc = s["step_controller_config"]["typed_config"]
        self.assertAlmostEqual(sc["initial_value"], 100.0)
        by_name = {t["metric_spec"]["metric_name"]: t["threshold_spec"]["scoring_function"]["typed_config"] for t in s["metric_thresholds"]}
        self.assertEqual(set(by_name), {"success-rate", "send-rate", "latency-ns-mean-plus-2stdev"})
        self.assertAlmostEqual(by_name["latency-ns-mean-plus-2stdev"]["upper_threshold"], 25_000_000.0)

        s_no_slo = egress_spec.build_egress_session_spec("http", tail_latency_slo_ms=0)
        self.assertEqual(len(s_no_slo["metric_thresholds"]), 2)

    def test_invalid_inputs_rejected(self):
        with self.assertRaises(ValueError):
            egress_spec.build_egress_session_spec("grpc")
        with self.assertRaises(ValueError):
            egress_spec.build_egress_session_spec("http", concurrency=0)
        with self.assertRaisesRegex(ValueError, "servicedns_ca_pem"):
            egress_spec.build_egress_session_spec("passthrough")
        with self.assertRaisesRegex(ValueError, "servicedns_ca_pem"):
            egress_spec.build_egress_session_spec("passthrough", servicedns_ca_pem="   ")

    def test_spec_round_trips_through_real_protos(self):
        desc = os.environ.get("NIGHTHAWK_DESC", "/app/nighthawk.desc")
        if not os.path.exists(desc):
            self.skipTest(f"{desc} not present")
        from google.protobuf import text_format
        from nighthawk_common import spec as common_spec

        pool = common_spec.load_pool(desc)
        for mode in egress_spec.MODE_URIS:
            s = egress_spec.build_egress_session_spec(
                mode,
                servicedns_ca_pem=FAKE_SERVICEDNS_CA if mode == "passthrough" else None,
            )
            text = egress_spec.spec_dict_to_textproto(s, desc)
            msg = common_spec.message_class(pool, common_spec.SPEC_MESSAGE)()
            text_format.Parse(text, msg, descriptor_pool=pool)
            self.assertEqual(msg.nighthawk_traffic_template.concurrency.value, "16")


if __name__ == "__main__":
    unittest.main()
