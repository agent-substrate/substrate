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

"""AdaptiveLoadSessionSpec builder for the Nighthawk egress gateway benchmark."""

from typing import Any

from nighthawk_common.spec import (
    PERMISSIVE_FAILURE_PREDICATES,
    build_session_spec,
    spec_dict_to_textproto,
)

UPSTREAM_SNI = "nighthawk-server.nighthawk.svc"
ACTOR_TRUST_BUNDLE = "/run/ate/egress-trust-bundle/trust-bundle.pem"
UPSTREAM_TLS_TYPE_URL = (
    "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext"
)
MODE_URIS: dict[str, str] = {
    "http": f"http://{UPSTREAM_SNI}:80/",
    "mitm": f"https://{UPSTREAM_SNI}:443/",
    "passthrough": f"https://{UPSTREAM_SNI}:8443/",
}

DEFAULT_CONCURRENCY = 16
DEFAULT_CONNECTIONS = 16
DEFAULT_MAX_PENDING = 32
DEFAULT_INITIAL_RPS = 500
DEFAULT_EXP_FACTOR = 2.0
DEFAULT_MEASURING_PERIOD_S = 20
DEFAULT_CONVERGENCE_DEADLINE_S = 600
DEFAULT_TESTING_STAGE_S = 60
DEFAULT_COOLDOWN_S = 5
DEFAULT_SUCCESS_RATE_THRESHOLD = 0.998
DEFAULT_SEND_RATE_THRESHOLD = 0.9
DEFAULT_TAIL_LATENCY_SLO_MS = 25.0

EGRESS_FAILURE_PREDICATES = {
    **PERMISSIVE_FAILURE_PREDICATES,
    "benchmark.pool_overflow": "1000000000",
}


def build_egress_session_spec(
    mode: str,
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    connections: int = DEFAULT_CONNECTIONS,
    max_pending: int = DEFAULT_MAX_PENDING,
    initial_rps: int = DEFAULT_INITIAL_RPS,
    exp_factor: float = DEFAULT_EXP_FACTOR,
    measuring_period_s: int = DEFAULT_MEASURING_PERIOD_S,
    convergence_deadline_s: int = DEFAULT_CONVERGENCE_DEADLINE_S,
    testing_stage_s: int = DEFAULT_TESTING_STAGE_S,
    cooldown_s: int = DEFAULT_COOLDOWN_S,
    success_rate_threshold: float = DEFAULT_SUCCESS_RATE_THRESHOLD,
    send_rate_threshold: float | None = DEFAULT_SEND_RATE_THRESHOLD,
    tail_latency_slo_ms: float | None = DEFAULT_TAIL_LATENCY_SLO_MS,
    servicedns_ca_pem: str | None = None,
) -> dict[str, Any]:
    if mode not in MODE_URIS:
        raise ValueError(f"unsupported egress mode {mode!r}; expected one of {sorted(MODE_URIS)}")
    if concurrency < 1 or connections < 1 or initial_rps < 1 or exp_factor <= 1.0:
        raise ValueError("concurrency, connections, initial_rps must be >= 1 and exp_factor > 1.0")

    target_uri = MODE_URIS[mode]
    traffic_template: dict[str, Any] = {
        "uri": target_uri,
        "concurrency": str(concurrency),
        "connections": connections,
        "max_pending_requests": max_pending,
        "open_loop": True,
        "request_options": {"request_method": "GET", "request_headers": []},
        "failure_predicates": dict(EGRESS_FAILURE_PREDICATES),
    }
    if target_uri.startswith("https://"):
        if mode == "mitm":
            trusted_ca: dict[str, str] = {"filename": ACTOR_TRUST_BUNDLE}
        else:
            if not servicedns_ca_pem or not servicedns_ca_pem.strip():
                raise ValueError("passthrough mode requires non-empty servicedns_ca_pem")
            trusted_ca = {"inline_string": servicedns_ca_pem}
        traffic_template["transport_socket"] = {
            "name": "envoy.transport_sockets.tls",
            "typed_config": {
                "@type": UPSTREAM_TLS_TYPE_URL,
                "sni": UPSTREAM_SNI,
                "common_tls_context": {
                    "alpn_protocols": ["http/1.1"],
                    "validation_context": {
                        "trusted_ca": trusted_ca,
                        "match_typed_subject_alt_names": [
                            {
                                "san_type": "DNS",
                                "matcher": {"exact": UPSTREAM_SNI},
                            },
                        ],
                    },
                },
            },
        }

    return build_session_spec(
        traffic_template,
        client_concurrency=concurrency,
        initial_total_rps=initial_rps,
        exponential_factor=exp_factor,
        measuring_period_s=measuring_period_s,
        convergence_deadline_s=convergence_deadline_s,
        testing_stage_duration_s=testing_stage_s,
        success_rate_threshold=success_rate_threshold,
        send_rate_threshold=send_rate_threshold,
        tail_latency_slo_ms=tail_latency_slo_ms,
        benchmark_cooldown_s=cooldown_s,
    )
