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

"""Output parser and atenet-egress Prometheus counter verifier for nighthawk-egress."""

import re
from typing import Any

import nighthawk_common.output

from nighthawk_common.output import (
    CONNECT_STAT_ID,
    LATENCY_STAT_ID,
    SEQUENCER_BLOCKING_STAT_ID,
    builtin_key,
    counters,
    global_result,
    parse_output_textproto,
    session_succeeded,
    stage_rows as _common_stage_rows,
    stats_records,
    write_jsonl,
)

EGRESS_EXTRA_COUNTERS = (
    "upstream_cx_total",
    "upstream_cx_http1_total",
    "upstream_cx_connect_fail",
    "upstream_cx_connect_timeout",
    "upstream_rq_pending_overflow",
    "ssl.handshake",
    "ssl.connection_error",
    "client.ssl_ciphers.ECDHE-RSA-AES128-GCM-SHA256",
)

GATEWAY_COUNTER_KEYS = (
    "egress_connect_2xx",
    "egress_downstream_cx_total",
    "mitm_downstream_cx_total",
    "mitm_ssl_handshake",
    "cleartext_ext_proc_streams",
    "mitm_ext_proc_streams",
    "passthrough_cx_total",
    "ext_proc_upstream_rq_total",
    "forward_proxy_cx_overflow",
    "forward_proxy_rq_pending_overflow",
)

_FORWARD_PROXY_CLUSTERS = (
    "egress_forward_proxy",
    "egress_forward_proxy_cleartext",
    "egress_forward_proxy_passthrough",
    "egress_forward_proxy_grpc",
)

# Exact Prometheus metric + label shapes emitted by atenet-egress:15090/stats/prometheus.
_GATEWAY_PROMETHEUS_MATCHERS: dict[tuple[str, frozenset[tuple[str, str]]], str] = {
    (
        "envoy_http_downstream_rq_xx",
        frozenset({("envoy_response_code_class", "2"), ("envoy_http_conn_manager_prefix", "egress_connect")}),
    ): "egress_connect_2xx",
    (
        "envoy_http_downstream_cx_total",
        frozenset({("envoy_http_conn_manager_prefix", "egress_connect")}),
    ): "egress_downstream_cx_total",
    (
        "envoy_listener_downstream_cx_total",
        frozenset({("envoy_listener_address", "mitm")}),
    ): "mitm_downstream_cx_total",
    (
        "envoy_listener_ssl_handshake",
        frozenset({("envoy_listener_address", "mitm")}),
    ): "mitm_ssl_handshake",
    (
        "envoy_http_ext_proc_egress_policy_cleartext_streams_started",
        frozenset({("envoy_http_conn_manager_prefix", "mitm_cleartext")}),
    ): "cleartext_ext_proc_streams",
    (
        "envoy_http_ext_proc_egress_policy_mitm_streams_started",
        frozenset({("envoy_http_conn_manager_prefix", "mitm_http")}),
    ): "mitm_ext_proc_streams",
    (
        "envoy_tcp_downstream_cx_total",
        frozenset({("envoy_tcp_prefix", "passthrough")}),
    ): "passthrough_cx_total",
    (
        "envoy_cluster_upstream_rq_total",
        frozenset({("envoy_cluster_name", "ext_proc_server")}),
    ): "ext_proc_upstream_rq_total",
    **{
        (
            "envoy_cluster_upstream_cx_overflow",
            frozenset({("envoy_cluster_name", cluster)}),
        ): "forward_proxy_cx_overflow"
        for cluster in _FORWARD_PROXY_CLUSTERS
    },
    **{
        (
            "envoy_cluster_upstream_rq_pending_overflow",
            frozenset({("envoy_cluster_name", cluster)}),
        ): "forward_proxy_rq_pending_overflow"
        for cluster in _FORWARD_PROXY_CLUSTERS
    },
}

_PROM_LINE_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([0-9.eE+-]+)")
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"')

__all__ = [
    "CONNECT_STAT_ID",
    "EGRESS_EXTRA_COUNTERS",
    "GATEWAY_COUNTER_KEYS",
    "LATENCY_STAT_ID",
    "SEQUENCER_BLOCKING_STAT_ID",
    "capacity_summary",
    "gateway_counter_deltas",
    "parse_gateway_prometheus",
    "parse_output_textproto",
    "session_succeeded",
    "stage_rows",
    "stats_records",
    "write_jsonl",
]


def stage_rows(output_dict: dict[str, Any]) -> list[dict[str, Any]]:
    rows = _common_stage_rows(output_dict)
    raw_stages = list(output_dict.get("adjusting_stage_results", []))
    if output_dict.get("testing_stage_result"):
        raw_stages.append(output_dict["testing_stage_result"])
    for row, br in zip(rows, raw_stages):
        nh_output = br.get("nighthawk_service_output", {})
        options = nh_output.get("options", {})
        counts = counters(global_result(nh_output))
        for name in EGRESS_EXTRA_COUNTERS:
            row[name] = counts.get(name, 0)
        cx_total = (
            counts.get("upstream_cx_total")
            or counts.get("upstream_cx_http1_total")
            or counts.get("ssl.handshake", 0)
        )
        http_2xx = row.get("http_2xx", 0)
        row["requests_per_connection"] = (
            round(http_2xx / cx_total, 2) if cx_total > 0 else 0.0
        )
        conns_per_worker = int(options.get("connections", 0) or 0)
        configured_pool = row.get("concurrency", 1) * conns_per_worker
        row["cx_churn_ratio"] = (
            round(cx_total / configured_pool, 4) if configured_pool > 0 else 0.0
        )
        row["error_rate"] = round(1.0 - row[builtin_key("success-rate")], 6)
    return rows


def parse_gateway_prometheus(text: str) -> dict[str, int]:
    """Parse atenet-egress:15090/stats/prometheus into the canonical counter keys."""
    totals = {k: 0 for k in GATEWAY_COUNTER_KEYS}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        m = _PROM_LINE_RE.match(line)
        if not m:
            continue
        name, label_str, val_str = m.groups()
        labels = frozenset(_LABEL_RE.findall(label_str or ""))
        key = _GATEWAY_PROMETHEUS_MATCHERS.get((name, labels))
        if key:
            totals[key] += int(float(val_str))
    return totals


def gateway_counter_deltas(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {k: after.get(k, 0) - before.get(k, 0) for k in GATEWAY_COUNTER_KEYS}


def _check_mode_exclusivity(mode: str | None, gateway_counters: dict[str, int]) -> bool | None:
    if mode == "http":
        return (
            gateway_counters.get("passthrough_cx_total", 0) == 0
            and gateway_counters.get("mitm_ssl_handshake", 0) == 0
            and gateway_counters.get("mitm_ext_proc_streams", 0) == 0
        )
    if mode == "mitm":
        return (
            gateway_counters.get("passthrough_cx_total", 0) == 0
            and gateway_counters.get("cleartext_ext_proc_streams", 0) == 0
        )
    if mode == "passthrough":
        return (
            gateway_counters.get("mitm_ssl_handshake", 0) == 0
            and gateway_counters.get("cleartext_ext_proc_streams", 0) == 0
            and gateway_counters.get("mitm_ext_proc_streams", 0) == 0
        )
    return None


def capacity_summary(
    output_dict: dict[str, Any],
    *,
    mode: str | None = None,
    envoy_cpu: int,
    client_concurrency: int | None = None,
    connections: int | None = None,
    tail_latency_slo_ms: float | None = None,
    gateway_counters: dict[str, int] | None = None,
) -> dict[str, Any]:
    summary = nighthawk_common.output.capacity_summary(
        output_dict,
        envoy_cpu=envoy_cpu,
        actors=None,
        client_concurrency=client_concurrency,
        tail_latency_slo_ms=tail_latency_slo_ms,
    )

    # Egress-specific fields
    rows = stage_rows(output_dict)
    testing = next((r for r in rows if r["stage"] == "testing"), None)
    slo_max_stage = summary.get("slo_max_stage")
    ref = None
    if slo_max_stage:
        ref = next((r for r in rows if r["stage"] == slo_max_stage), None)
    if not ref:
        ref = testing if testing else (rows[-1] if rows else None)

    summary["mode"] = mode
    summary["connections"] = connections
    summary["gateway_counters"] = dict(gateway_counters or {})
    summary["testing_stage_completed"] = testing is not None
    summary["testing_passed"] = testing is not None and not testing.get("failed_thresholds")
    summary["tunnels_per_stage"] = (
        round((gateway_counters or {}).get("egress_connect_2xx", 0) / len(rows), 2)
        if rows
        else 0.0
    )

    if testing:
        summary["error_rate"] = testing.get("error_rate")

    if ref is not None:
        summary["requests_per_connection"] = ref.get("requests_per_connection", 0.0)
        summary["cx_churn_ratio"] = ref.get("cx_churn_ratio", 0.0)
        for key in (
            "latency_pstdev_ms",
            "queue_to_connect_count",
            "queue_to_connect_mean_ms",
            "queue_to_connect_pstdev_ms",
            "queue_to_connect_p50_ms",
            "queue_to_connect_p99_ms",
            "queue_to_connect_max_ms",
            "sequencer_blocking_p99_ms",
        ):
            if key in ref:
                summary[key] = ref[key]

        # client_pool_bound is true if the client ran out of concurrent TCP connections
        # (upstream_rq_pending_overflow) or waited >1ms at p50 to acquire one.
        # This usually implies --connections must be increased.
        summary["client_pool_bound"] = (ref.get("upstream_rq_pending_overflow", 0) > 0) or ((ref.get("queue_to_connect_p50_ms") or 0.0) > 1.0)
        summary["telemetry_stage"] = ref["stage"]
    else:
        summary["client_pool_bound"] = None
        summary["telemetry_stage"] = None

    if gateway_counters:
        summary["ext_proc_invariant_ok"] = gateway_counters.get("ext_proc_upstream_rq_total", 0) == (
            gateway_counters.get("egress_connect_2xx", 0) +
            gateway_counters.get("cleartext_ext_proc_streams", 0) +
            gateway_counters.get("mitm_ext_proc_streams", 0)
        )
        summary["mode_exclusivity_ok"] = _check_mode_exclusivity(mode, gateway_counters)
        summary["forward_proxy_overflow_ok"] = (
            gateway_counters.get("forward_proxy_cx_overflow", 0) == 0
            and gateway_counters.get("forward_proxy_rq_pending_overflow", 0) == 0
        )
        summary["gateway_counter_reset"] = any(v < 0 for v in gateway_counters.values())
    else:
        summary["ext_proc_invariant_ok"] = None
        summary["mode_exclusivity_ok"] = None
        summary["forward_proxy_overflow_ok"] = None
        summary["gateway_counter_reset"] = False

    return summary
