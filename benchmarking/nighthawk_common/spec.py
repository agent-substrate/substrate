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

"""Shared AdaptiveLoadSessionSpec builder and descriptor-pool textproto serializer."""


SPEC_MESSAGE = "nighthawk.adaptive_load.AdaptiveLoadSessionSpec"
OUTPUT_MESSAGE = "nighthawk.adaptive_load.AdaptiveLoadSessionOutput"

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


def build_session_spec(
    traffic_template: dict,
    *,
    client_concurrency: int,
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
    """Wrap a traffic_template dict in an AdaptiveLoadSessionSpec dict.

    The controller varies the per-worker requests_per_second, so
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
        "nighthawk_traffic_template": traffic_template,
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
