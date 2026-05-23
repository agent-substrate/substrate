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

from locust import events
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased, Sampler
from opentelemetry.propagate import set_global_textmap
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
import logging

logger = logging.getLogger(__name__)

class UpdatableSampler(Sampler):
    def __init__(self, initial_probability=0.0):
        self.sampler = TraceIdRatioBased(initial_probability)
        self.probability = initial_probability

    def update_probability(self, probability):
        self.sampler = TraceIdRatioBased(probability)
        self.probability = probability

    def should_sample(self, parent_context, trace_id, name, kind, attributes, links):
        return self.sampler.should_sample(parent_context, trace_id, name, kind, attributes, links)

    def get_description(self):
        return f"UpdatableSampler(probability={self.probability})"

_initialized = False
_sampler = UpdatableSampler(0.0)

def init_tracing(service_name):
    global _initialized, _sampler
    if _initialized:
        logger.info("Tracing already initialized, skipping.")
        return

    @events.init_command_line_parser.add_listener
    def _(parser):
        parser.add_argument(
            "--trace-probability",
            type=float,
            default=0.0,
            help="Probability of tracing requests (0.0 to 1.0)"
        )

    @events.init.add_listener
    def on_locust_init(environment, **kwargs):
        options = environment.parsed_options
        probability = getattr(options, 'trace_probability', 0.0)

        _sampler.update_probability(probability)

        resource = Resource(attributes={
            SERVICE_NAME: service_name
        })
        provider = TracerProvider(sampler=_sampler, resource=resource)

        # Always add the exporter so it's ready if probability is increased later
        processor = BatchSpanProcessor(OTLPSpanExporter())
        provider.add_span_processor(processor)

        trace.set_tracer_provider(provider)
        set_global_textmap(TraceContextTextMapPropagator())
        logger.info(f"Tracing initialized for {service_name} with initial probability {probability}")

    @events.test_start.add_listener
    def on_test_start(environment, **kwargs):
        options = environment.parsed_options
        probability = getattr(options, 'trace_probability', 0.0)
        _sampler.update_probability(probability)
        logger.info(f"Test started, updated trace probability to {probability}")

    _initialized = True


def get_tracer(name):
    return trace.get_tracer(name)

