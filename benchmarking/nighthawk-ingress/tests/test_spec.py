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

"""Unit tests for the AdaptiveLoadSessionSpec dict builder.

The dict-shape tests run anywhere (stdlib only, `python3 tests/test_spec.py`
or pytest). The textproto round-trip test needs the FileDescriptorSet built
by the Docker protogen stage and self-skips when NIGHTHAWK_DESC is absent
(i.e. outside the runner image):
  docker run --rm --entrypoint python3 <image> -m pytest /app/tests
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import spec as spec_mod


def build(**overrides):
    kwargs = dict(
        uri="http://atenet-router.ate-system.svc.cluster.local:80/ping",
        actor_names=[f"sb-{i}" for i in range(3)],
        atespace="benchmark",
        client_concurrency=4,
        connections=1000,
        max_pending_requests=10000,
        initial_total_rps=500,
        exponential_factor=2.0,
        measuring_period_s=10,
        convergence_deadline_s=600,
        testing_stage_duration_s=60,
        success_rate_threshold=0.999,
    )
    kwargs.update(overrides)
    return spec_mod.build_spec_dict(**kwargs)


def test_traffic_template_shape():
    spec = build()
    template = spec["nighthawk_traffic_template"]
    # The adaptive controller rejects templates that set duration.
    assert "duration" not in template
    assert template["open_loop"] is True
    assert template["concurrency"] == "4"
    # All five default failure predicates must be overridden (spec.py).
    assert set(template["failure_predicates"]) == {
        "benchmark.http_4xx",
        "benchmark.http_5xx",
        "benchmark.pool_connection_failure",
        "benchmark.stream_resets",
        "requestsource.upstream_rq_5xx",
    }


def test_actor_reference_rotation_covers_all_actors():
    actor_names = [f"sb-{i}" for i in range(5)]
    spec = build(actor_names=actor_names)
    plugin = spec["nighthawk_traffic_template"]["request_source_plugin_config"]
    assert plugin["name"] == spec_mod.REQUEST_SOURCE_PLUGIN
    options = plugin["typed_config"]["options_list"]["options"]
    got = [
        {header["header"]["key"]: header["header"]["value"] for header in o["request_headers"]}
        for o in options
    ]
    assert got == [
        {
            "ate-target-actor": f"benchmark/{actor_name}",
        }
        for actor_name in actor_names
    ]
    assert all(o["request_method"] == "POST" for o in options)
    # 0 = loop the list indefinitely.
    assert plugin["typed_config"]["num_requests"] == 0


def test_initial_rps_is_per_worker():
    spec = build(initial_total_rps=1000, client_concurrency=4)
    controller = spec["step_controller_config"]
    assert controller["name"] == spec_mod.STEP_CONTROLLER_PLUGIN
    assert controller["typed_config"]["initial_value"] == 250.0


def test_thresholds_and_durations():
    # send-rate must be a default threshold: with no SLO set, nothing else
    # bounds the open-loop search.
    spec = build()
    names = [
        t["metric_spec"]["metric_name"] for t in spec["metric_thresholds"]
    ]
    assert names == ["success-rate", "send-rate"]
    scoring = spec["metric_thresholds"][0]["threshold_spec"]["scoring_function"]
    assert scoring["name"] == spec_mod.BINARY_SCORING_PLUGIN
    assert scoring["typed_config"]["lower_threshold"] == 0.999
    assert spec["measuring_period"] == "10s"
    assert spec["convergence_deadline"] == "600s"
    assert spec["testing_stage_duration"] == "60s"


def test_tail_latency_slo_threshold():
    spec = build(tail_latency_slo_ms=25)
    thresholds = spec["metric_thresholds"]
    names = [t["metric_spec"]["metric_name"] for t in thresholds]
    assert names == ["success-rate", "send-rate", "latency-ns-mean-plus-2stdev"]
    latency = thresholds[-1]["threshold_spec"]["scoring_function"]["typed_config"]
    # SLO is an UPPER bound, in nanoseconds (25 ms).
    assert latency["upper_threshold"] == 25_000_000.0
    assert "lower_threshold" not in latency
    # Disabled (None) => no latency threshold at all.
    assert len(build(tail_latency_slo_ms=None)["metric_thresholds"]) == 2


def test_file_plugin_config_parses_with_real_protos():
    """The file-based plugin config and the options file parse as their
    real Nighthawk messages; self-skips outside the runner image."""
    desc = os.environ.get("NIGHTHAWK_DESC", "/app/nighthawk.desc")
    if not os.path.exists(desc):
        print("SKIP: FileDescriptorSet only present inside the runner image")
        return
    from google.protobuf import any_pb2, json_format

    pool = spec_mod.load_pool(desc)
    options_cls = spec_mod.message_class(pool, "nighthawk.client.RequestOptionsList")
    options = json_format.Parse(
        spec_mod.request_options_list_json(["sb-0", "sb-1"], "benchmark"),
        options_cls(),
        descriptor_pool=pool,
    )
    assert len(options.options) == 2
    typed = spec_mod.file_request_source_plugin_config("/tmp/o.json", 4096)[
        "typed_config"
    ]
    any_msg = json_format.ParseDict(typed, any_pb2.Any(), descriptor_pool=pool)
    assert any_msg.type_url == spec_mod.FILE_REQUEST_SOURCE_TYPE_URL


def test_spec_round_trips_through_real_protos():
    """Round-trip through the real Nighthawk protos; self-skips outside the
    runner image (no FileDescriptorSet)."""
    desc = os.environ.get("NIGHTHAWK_DESC", "/app/nighthawk.desc")
    if not os.path.exists(desc):
        print("SKIP: FileDescriptorSet only present inside the runner image")
        return
    from google.protobuf import text_format

    text = spec_mod.spec_dict_to_textproto(build(tail_latency_slo_ms=25), desc)
    pool = spec_mod.load_pool(desc)
    msg = spec_mod.message_class(pool, spec_mod.SPEC_MESSAGE)()
    text_format.Parse(text, msg, descriptor_pool=pool)
    assert msg.nighthawk_traffic_template.concurrency.value == "4"
    assert msg.measuring_period.seconds == 10
    # success-rate + send-rate defaults, plus the tail-latency SLO.
    assert len(msg.metric_thresholds) == 3


def fixed_args(**overrides):
    kwargs = dict(
        uri="http://atenet-router.ate-system.svc.cluster.local:80/ping",
        options_path="/tmp/run/request_options.json",
        options_size=4096,
        client_concurrency=16,
        connections=1000,
        max_pending_requests=10000,
        total_rps=8000,
        duration_s=90,
    )
    kwargs.update(overrides)
    return spec_mod.fixed_rps_client_args(**kwargs)


def flag_values(args, flag):
    return [args[i + 1] for i, a in enumerate(args) if a == flag]


def test_fixed_rps_args_rate_is_per_worker():
    args = fixed_args(total_rps=8000, client_concurrency=16)
    assert flag_values(args, "--rps") == ["500"]
    assert flag_values(args, "--concurrency") == ["16"]
    assert flag_values(args, "--duration") == ["90"]
    assert "--open-loop" in args
    assert flag_values(args, "--output-format") == ["json"]
    # The target URI is the one positional argument, last.
    assert args[-1].endswith("/ping")


def test_fixed_rps_args_read_options_from_file():
    args = fixed_args()
    plugin = json.loads(flag_values(args, "--request-source-plugin-config")[0])
    assert plugin == spec_mod.file_request_source_plugin_config(
        "/tmp/run/request_options.json", 4096
    )
    assert plugin["typed_config"]["num_requests"] == 0
    assert plugin["typed_config"]["max_file_size"] == 4096
    predicates = dict(
        p.split(":", 1) for p in flag_values(args, "--failure-predicate")
    )
    assert predicates == spec_mod.PERMISSIVE_FAILURE_PREDICATES


def test_options_file_matches_adaptive_traffic():
    names = ["sb-0", "sb-1"]
    options = json.loads(spec_mod.request_options_list_json(names, "benchmark"))
    in_line = build(actor_names=names)["nighthawk_traffic_template"][
        "request_source_plugin_config"
    ]["typed_config"]["options_list"]
    assert options == in_line


def test_fixed_rps_args_fit_the_single_argument_limit():
    # Linux rejects any one exec argument over MAX_ARG_STRLEN (128 KiB);
    # the argv must not grow with the actor count.
    args = fixed_args(options_size=10_000_000)
    assert max(len(a.encode()) for a in args) < 128 * 1024


def test_per_worker_rps_never_zero():
    assert spec_mod.per_worker_rps(5, 16) == 1
    assert spec_mod.per_worker_rps(4000, 16) == 250


def test_parse_rps_list():
    assert spec_mod.parse_rps_list("4000,8000, 12000") == [4000, 8000, 12000]
    assert spec_mod.parse_rps_list("") == []
    for bad in ("0", "-5", "abc"):
        try:
            spec_mod.parse_rps_list(bad)
        except ValueError:
            continue
        raise AssertionError(f"accepted {bad!r}")


if __name__ == "__main__":
    for fn_name, fn in sorted(dict(globals()).items()):
        if fn_name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {fn_name}")
