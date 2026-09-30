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

"""AdaptiveLoadSessionSpec builder.

Specs are plain dicts, validated and rendered to textproto through the
FileDescriptorSet built by protogen/generate_descriptor.sh — field names
are checked against the real Nighthawk protos at spec-build time.
"""

import json

SPEC_MESSAGE = "nighthawk.adaptive_load.AdaptiveLoadSessionSpec"
OUTPUT_MESSAGE = "nighthawk.adaptive_load.AdaptiveLoadSessionOutput"
CLIENT_OUTPUT_MESSAGE = "nighthawk.client.Output"

# Registered plugin names: adaptive-load plugins use underscores upstream,
# request-source plugins use hyphens.
STEP_CONTROLLER_PLUGIN = "nighthawk.exponential_search"
STEP_CONTROLLER_TYPE_URL = (
    "type.googleapis.com/nighthawk.adaptive_load.ExponentialSearchStepControllerConfig"
)
BINARY_SCORING_PLUGIN = "nighthawk.binary_scoring"
BINARY_SCORING_TYPE_URL = (
    "type.googleapis.com/nighthawk.adaptive_load.BinaryScoringFunctionConfig"
)
BUILTIN_METRICS_PLUGIN = "nighthawk.builtin"
REQUEST_SOURCE_PLUGIN = "nighthawk.in-line-options-list-request-source-plugin"
REQUEST_SOURCE_TYPE_URL = (
    "type.googleapis.com/nighthawk.request_source.InLineOptionsListRequestSourceConfig"
)
FILE_REQUEST_SOURCE_PLUGIN = "nighthawk.file-based-request-source-plugin"
FILE_REQUEST_SOURCE_TYPE_URL = (
    "type.googleapis.com/nighthawk.request_source.FileBasedOptionsListRequestSourceConfig"
)

# The default failure predicates (all 0) abort an execution on the first
# 4xx/5xx; measuring periods must instead run to completion so overload
# scores as degraded success-rate. All five defaults overridden.
PERMISSIVE_FAILURE_PREDICATES = {
    "benchmark.http_4xx": "1000000000",
    "benchmark.http_5xx": "1000000000",
    "benchmark.pool_connection_failure": "1000000000",
    "benchmark.stream_resets": "1000000000",
    "requestsource.upstream_rq_5xx": "1000000000",
}

INFORMATIONAL_METRICS = (
    "attempted-rps",
    "achieved-rps",
    "send-rate",
    "latency-ns-mean",
    "latency-ns-mean-plus-2stdev",
)


def _metric_spec(name: str) -> dict:
    return {"metric_name": name, "metrics_plugin_name": BUILTIN_METRICS_PLUGIN}


def _binary_threshold(
    metric_name: str,
    lower_threshold: float | None = None,
    upper_threshold: float | None = None,
) -> dict:
    config: dict = {"@type": BINARY_SCORING_TYPE_URL}
    if lower_threshold is not None:
        config["lower_threshold"] = lower_threshold
    if upper_threshold is not None:
        config["upper_threshold"] = upper_threshold
    return {
        "metric_spec": _metric_spec(metric_name),
        "threshold_spec": {
            "scoring_function": {
                "name": BINARY_SCORING_PLUGIN,
                "typed_config": config,
            },
        },
    }


# TODO: replace the per-actor options list with a dynamic request source
# plugin that generates the ate-target-actor header itself, along the
# lines of https://gist.github.com/bowei/96aab95f5ccaa95dedc80a3983c7b2cb.
# Both the in-line and the file-based configs enumerate every actor, which
# does not scale to large number of actors; a generator also allows
# customizing the traffic distribution across actors.
def request_variants(actor_names: list[str], atespace: str) -> list[dict]:
    """One POST RequestOptions per actor.

    request_options shares a oneof with the request-source plugin config:
    method and headers must come from the per-variant RequestOptions.
    """
    return [
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


def request_source_plugin_config(actor_names: list[str], atespace: str) -> dict:
    """The request variants in-line, cycled indefinitely."""
    return {
        "name": REQUEST_SOURCE_PLUGIN,
        "typed_config": {
            "@type": REQUEST_SOURCE_TYPE_URL,
            "options_list": {"options": request_variants(actor_names, atespace)},
            # 0 = cycle through options_list indefinitely.
            "num_requests": 0,
        },
    }


def request_options_list_json(actor_names: list[str], atespace: str) -> str:
    """A nighthawk.client.RequestOptionsList for the file-based plugin."""
    return json.dumps({"options": request_variants(actor_names, atespace)})


def file_request_source_plugin_config(options_path: str, options_size: int) -> dict:
    """The request variants read from options_path, cycled indefinitely.

    Used on the nighthawk_client command line, where the in-line config is
    a single argument that grows with the actor count and Linux caps a
    single argument at 128 KiB (about 750 actors).
    """
    return {
        "name": FILE_REQUEST_SOURCE_PLUGIN,
        "typed_config": {
            "@type": FILE_REQUEST_SOURCE_TYPE_URL,
            "file_path": options_path,
            "num_requests": 0,
            # The plugin rejects files over this size (default 1 MB).
            "max_file_size": options_size,
        },
    }


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
    """Assemble the AdaptiveLoadSessionSpec as a JSON-shaped dict.

    The controller varies the *per-worker* requests_per_second, so
    initial_value = initial_total_rps / client_concurrency. The template
    must not set duration (the controller rejects it); open_loop is forced
    true by the controller.
    """
    # Threshold roles: tail latency (mean+2stdev, ~p95 proxy — no true
    # percentiles in the builtin adaptive metrics) is the SLO bound;
    # success-rate catches fast-error saturation (503/504) that latency and
    # send-rate miss; send-rate is the open-loop backstop — skipped
    # requests are neither failures nor latency samples, so without it the
    # search ramps unboundedly.
    thresholds = [
        _binary_threshold("success-rate", lower_threshold=success_rate_threshold)
    ]
    if send_rate_threshold is not None:
        thresholds.append(
            _binary_threshold("send-rate", lower_threshold=send_rate_threshold)
        )
    if tail_latency_slo_ms is not None:
        thresholds.append(
            _binary_threshold(
                "latency-ns-mean-plus-2stdev",
                upper_threshold=tail_latency_slo_ms * 1_000_000.0,
            )
        )
    return {
        "nighthawk_traffic_template": {
            "uri": uri,
            "concurrency": str(client_concurrency),
            "connections": connections,
            "max_pending_requests": max_pending_requests,
            "open_loop": True,
            "failure_predicates": dict(PERMISSIVE_FAILURE_PREDICATES),
            "request_source_plugin_config": request_source_plugin_config(
                actor_names, atespace
            ),
        },
        "metric_thresholds": thresholds,
        "informational_metric_specs": [
            _metric_spec(m) for m in INFORMATIONAL_METRICS
        ],
        "step_controller_config": {
            "name": STEP_CONTROLLER_PLUGIN,
            "typed_config": {
                "@type": STEP_CONTROLLER_TYPE_URL,
                "initial_value": max(1.0, initial_total_rps / client_concurrency),
                "exponential_factor": exponential_factor,
            },
        },
        "measuring_period": f"{measuring_period_s}s",
        "convergence_deadline": f"{convergence_deadline_s}s",
        "testing_stage_duration": f"{testing_stage_duration_s}s",
        "benchmark_cooldown_duration": f"{benchmark_cooldown_s}s",
    }


def parse_rps_list(s: str) -> list[int]:
    """'4000,8000' -> [4000, 8000]; '' -> [] (adaptive mode)."""
    rates = [int(r) for r in s.split(",") if r.strip()]
    if any(r < 1 for r in rates):
        raise ValueError(f"rates must be positive: {s}")
    return rates


def per_worker_rps(total_rps: int, client_concurrency: int) -> int:
    """nighthawk_client's --rps is per event loop; the attempted total is
    per_worker_rps * client_concurrency."""
    return max(1, round(total_rps / client_concurrency))


def fixed_rps_client_args(
    *,
    uri: str,
    options_path: str,
    options_size: int,
    client_concurrency: int,
    connections: int,
    max_pending_requests: int,
    total_rps: int,
    duration_s: int,
) -> list[str]:
    """nighthawk_client argv for one open-loop stage at a fixed rate.

    options_path holds request_options_list_json(): the same traffic shape
    as the adaptive template, so fixed-rate and adaptive stages are
    comparable.
    """
    args = [
        "--concurrency",
        str(client_concurrency),
        "--connections",
        str(connections),
        "--max-pending-requests",
        str(max_pending_requests),
        "--rps",
        str(per_worker_rps(total_rps, client_concurrency)),
        "--duration",
        str(duration_s),
        "--open-loop",
        "--output-format",
        "json",
        "--request-source-plugin-config",
        json.dumps(file_request_source_plugin_config(options_path, options_size)),
    ]
    for name, value in PERMISSIVE_FAILURE_PREDICATES.items():
        args += ["--failure-predicate", f"{name}:{value}"]
    return args + [uri]


# protobuf imports are deferred: the dict builder above must work without
# protobuf installed.


def load_pool(desc_path: str):
    from google.protobuf import descriptor_pb2
    from google.protobuf import descriptor_pool

    pool = descriptor_pool.DescriptorPool()
    fds = descriptor_pb2.FileDescriptorSet()
    with open(desc_path, "rb") as f:
        fds.ParseFromString(f.read())
    for file_proto in fds.file:
        pool.Add(file_proto)
    return pool


def message_class(pool, full_name: str):
    from google.protobuf import message_factory

    return message_factory.GetMessageClass(pool.FindMessageTypeByName(full_name))


def spec_dict_to_textproto(spec_dict: dict, desc_path: str) -> str:
    """Validate the dict against the real protos and render textproto.

    No descriptor_pool for MessageToString: expanded-form Any is not
    parseable by nighthawk_adaptive_load_client for plugin types it does
    not link (the request-source config); raw type_url/value form is.
    """
    from google.protobuf import json_format
    from google.protobuf import text_format

    pool = load_pool(desc_path)
    msg = message_class(pool, SPEC_MESSAGE)()
    json_format.ParseDict(spec_dict, msg, descriptor_pool=pool)
    return text_format.MessageToString(msg)
