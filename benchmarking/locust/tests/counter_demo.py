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

from locust import HttpUser, task, events
import uuid
import time
import logging
import grpc
from common import ateapi_pb2
from common import ateapi_pb2_grpc

logger = logging.getLogger(__name__)
from common.metrics import init_metrics, update_user_count


from common.trace import init_tracing, get_tracer
from common.wait_time import init_wait_time, dynamic_wait_time
from opentelemetry.propagate import inject

init_tracing("locust-counter-demo")

# Initialize metrics
init_metrics()

# Initialize wait time
init_wait_time()

tracer = get_tracer(__name__)


class CounterUser(HttpUser):
    wait_time = dynamic_wait_time

    host = "http://atenet-router.ate-system.svc.cluster.local:80"
    api_host = "api.ate-system.svc.cluster.local:443"

    def on_start(self):
        update_user_count(1, self.__class__.__name__)

        # Setup gRPC
        target = self.api_host.replace("http://", "").replace("https://", "")
        with open("/run/servicedns-ca/ca.crt", "rb") as f:
            ca_cert = f.read()
        options = [('grpc.ssl_target_name_override', 'api.ate-system.svc')]
        self.channel = grpc.secure_channel(target, grpc.ssl_channel_credentials(root_certificates=ca_cert), options=options)
        self.stub = ateapi_pb2_grpc.ControlStub(self.channel)

        # Call CreateActor
        self.actor_id = f"sb-{uuid.uuid4()}"
        try:
            self.stub.CreateActor(
                ateapi_pb2.CreateActorRequest(
                    actor_id=self.actor_id,
                    actor_template_namespace="ate-demo-counter",
                    actor_template_name="counter"
                )
            )
        except Exception as e:
            logger.error(f"Failed to create actor {self.actor_id}: {e}")

    def on_stop(self):
        update_user_count(-1, self.__class__.__name__)
        try:
            self.stub.SuspendActor(
                ateapi_pb2.SuspendActorRequest(actor_id=self.actor_id)
            )
        except Exception as e:
            logger.error(f"Failed to suspend actor {self.actor_id}: {e}")
        self.channel.close()

    @task
    def run_and_suspend(self):
        # 1. ResumeActor (gRPC)
        start_time = time.time()
        with tracer.start_as_current_span("ResumeActor") as span:
            headers = {}
            inject(headers)
            metadata = list(headers.items())
            try:
                response = self.stub.ResumeActor(
                    ateapi_pb2.ResumeActorRequest(actor_id=self.actor_id),
                    metadata=metadata
                )
                duration = (time.time() - start_time) * 1000
                events.request.fire(
                    request_type="grpc",
                    name="ResumeActor",
                    response_time=duration,
                    response_length=0,
                    exception=None,
                    user_class=self.__class__.__name__
                )
                if span.get_span_context().trace_flags.sampled:
                    logger.info(f"Traced ResumeActor: trace_id={span.get_span_context().trace_id:032x}, duration={duration:.2f}ms")
            except Exception as e:
                duration = (time.time() - start_time) * 1000
                events.request.fire(
                    request_type="grpc",
                    name="ResumeActor",
                    response_time=duration,
                    response_length=0,
                    exception=e,
                    user_class=self.__class__.__name__
                )
                if span.get_span_context().trace_flags.sampled:
                    logger.info(f"Traced ResumeActor (failed): trace_id={span.get_span_context().trace_id:032x}, duration={duration:.2f}ms")

        # 2. Run/Increment (HTTP via atenet-router)
        start_time = time.time()
        with tracer.start_as_current_span("RunCounter") as span:
            headers = {
                "Host": f"{self.actor_id}.actors.resources.substrate.ate.dev"
            }
            inject(headers)
            try:
                response = self.client.post("/", name="RunCounter", headers=headers, context={"user_class": self.__class__.__name__})
                response.raise_for_status()
                duration = (time.time() - start_time) * 1000
                if span.get_span_context().trace_flags.sampled:
                    logger.info(f"Traced RunCounter: trace_id={span.get_span_context().trace_id:032x}, duration={duration:.2f}ms")
            except Exception as e:
                duration = (time.time() - start_time) * 1000
                logger.error(f"RunCounter failed: {e}")
                if span.get_span_context().trace_flags.sampled:
                    logger.info(f"Traced RunCounter (failed): trace_id={span.get_span_context().trace_id:032x}, duration={duration:.2f}ms")

        # 3. SuspendActor (gRPC)
        start_time = time.time()
        with tracer.start_as_current_span("SuspendActor") as span:
            headers = {}
            inject(headers)
            metadata = list(headers.items())
            try:
                response = self.stub.SuspendActor(
                    ateapi_pb2.SuspendActorRequest(actor_id=self.actor_id),
                    metadata=metadata
                )
                duration = (time.time() - start_time) * 1000
                events.request.fire(
                    request_type="grpc",
                    name="SuspendActor",
                    response_time=duration,
                    response_length=0,
                    exception=None,
                    user_class=self.__class__.__name__
                )
                if span.get_span_context().trace_flags.sampled:
                    logger.info(f"Traced SuspendActor: trace_id={span.get_span_context().trace_id:032x}, duration={duration:.2f}ms")
            except Exception as e:
                duration = (time.time() - start_time) * 1000
                events.request.fire(
                    request_type="grpc",
                    name="SuspendActor",
                    response_time=duration,
                    response_length=0,
                    exception=e,
                    user_class=self.__class__.__name__
                )
                if span.get_span_context().trace_flags.sampled:
                    logger.info(f"Traced SuspendActor (failed): trace_id={span.get_span_context().trace_id:032x}, duration={duration:.2f}ms")
