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

"""AdaptiveLoadSessionSpec builder for the ingress router capacity benchmark.

Specs are plain dicts, validated and rendered to textproto through the
FileDescriptorSet built by protogen/generate_descriptor.sh — field names
are checked against the real Nighthawk protos at spec-build time.
"""

from nighthawk_common.spec import (
    PERMISSIVE_FAILURE_PREDICATES,
    REQUEST_SOURCE_PLUGIN,
    REQUEST_SOURCE_TYPE_URL,
    build_session_spec,
)

ROUTER_SNI = "atenet-router.ate-system.svc"
CA_FILE = "/run/servicedns-ca/ca.crt"
CRED_BUNDLE = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"


def build_spec_dict(
    *,
    uri: str,
    actor_names: list[str],
    atespace: str,
    client_concurrency: int,
    connections: int,
    max_pending_requests: int,
    initial_total_rps: int,
    exponential_factor: float,
    measuring_period_s: int,
    convergence_deadline_s: int,
    testing_stage_duration_s: int,
    success_rate_threshold: float,
    send_rate_threshold: float | None = 0.9,
    tail_latency_slo_ms: float | None = None,
    benchmark_cooldown_s: int = 5,
) -> dict:
    """Assemble the AdaptiveLoadSessionSpec as a JSON-shaped dict."""
    if not uri.startswith("https://"):
        raise ValueError(f"router URI must use https://, got {uri!r}")

    request_variants = [
        {
            "request_method": "POST",
            "request_headers": [
                {
                    "header": {
                        "key": "ate-target-actor",
                        "value": f"{atespace}/{actor_name}",
                    },
                    "append_action": "OVERWRITE_IF_EXISTS_OR_ADD",
                },
            ],
        }
        for actor_name in actor_names
    ]

    traffic_template: dict = {
        "uri": uri,
        "concurrency": str(client_concurrency),
        "connections": connections,
        "max_pending_requests": max_pending_requests,
        "open_loop": True,
        "failure_predicates": dict(PERMISSIVE_FAILURE_PREDICATES),
        # request_options shares a oneof with this field: method and
        # headers must come from the per-variant RequestOptions.
        "request_source_plugin_config": {
            "name": REQUEST_SOURCE_PLUGIN,
            "typed_config": {
                "@type": REQUEST_SOURCE_TYPE_URL,
                "options_list": {"options": request_variants},
                # 0 = cycle through options_list indefinitely.
                "num_requests": 0,
            },
        },
        "tls_context": {
            "sni": ROUTER_SNI,
            "common_tls_context": {
                "tls_certificates": [
                    {
                        "certificate_chain": {"filename": CRED_BUNDLE},
                        "private_key": {"filename": CRED_BUNDLE},
                    },
                ],
                "validation_context": {
                    "trusted_ca": {"filename": CA_FILE},
                    "match_typed_subject_alt_names": [
                        {
                            "san_type": "DNS",
                            "matcher": {"exact": ROUTER_SNI},
                        },
                    ],
                },
            },
        },
    }
    return build_session_spec(
        traffic_template,
        client_concurrency=client_concurrency,
        initial_total_rps=initial_total_rps,
        exponential_factor=exponential_factor,
        measuring_period_s=measuring_period_s,
        convergence_deadline_s=convergence_deadline_s,
        testing_stage_duration_s=testing_stage_duration_s,
        success_rate_threshold=success_rate_threshold,
        send_rate_threshold=send_rate_threshold,
        tail_latency_slo_ms=tail_latency_slo_ms,
        benchmark_cooldown_s=benchmark_cooldown_s,
    )
