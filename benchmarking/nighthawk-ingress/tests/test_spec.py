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

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import spec as spec_mod


def build(**overrides):
    kwargs = dict(
        uri="http://atenet-router.ate-system.svc.cluster.local:80/ping",
        hosts=[
            spec_mod.actor_host(f"sb-{i}", "benchmark") for i in range(3)
        ],
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


def test_host_rotation_covers_all_actors():
    hosts = [spec_mod.actor_host(f"sb-{i}", "benchmark") for i in range(5)]
    spec = build(hosts=hosts)
    plugin = spec["nighthawk_traffic_template"]["request_source_plugin_config"]
    assert plugin["name"] == spec_mod.REQUEST_SOURCE_PLUGIN
    options = plugin["typed_config"]["options_list"]["options"]
    got = [o["request_headers"][0]["header"]["value"] for o in options]
    assert got == hosts
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


def test_actor_host_format():
    assert (
        spec_mod.actor_host("sb-1", "benchmark")
        == "sb-1.benchmark.actors.resources.substrate.ate.dev"
    )


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


if __name__ == "__main__":
    for fn_name, fn in sorted(dict(globals()).items()):
        if fn_name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {fn_name}")
