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

from locust import User, task, events
from locust.exception import StopUser
import time
import uuid
import grpc
from common import ateapi_pb2
from common import ateapi_pb2_grpc
from common import glutton_pb2
from common import glutton_pb2_grpc
from common.metrics import init_metrics, update_user_count
from common.trace import init_tracing, get_tracer
from common.wait_time import init_wait_time, dynamic_wait_time
from opentelemetry.propagate import inject
import logging

logger = logging.getLogger(__name__)

init_tracing("locust-workloads")
init_metrics()
init_wait_time()

tracer = get_tracer(__name__)


# The actor sandbox only has TCP/80 forwarded in from the worker pod
# (see cmd/ateom-gvisor/main.go), so the glutton ActorTemplate is
# configured with --grpc-listen-addr=:80 and we connect there.
GLUTTON_PORT = 80


class GluttonUser(User):
    """Spins up a single glutton actor on start, pings it repeatedly in
    @task, and tears the actor down on stop."""

    wait_time = dynamic_wait_time
    # `host` is what locust shows in the web UI / --host flag; it can be
    # overridden by the user at test start. Keep the api target in a
    # separate attribute so it's not clobbered when host points elsewhere
    # (e.g. when running with other user classes via --class-picker).
    host = "api.ate-system.svc.cluster.local:443"
    api_host = "api.ate-system.svc.cluster.local:443"
    template_name = "glutton"

    def on_start(self) -> None:
        update_user_count(1, self.__class__.__name__)

        # Replace protocol prefix because gRPC does not use it.
        target = self.api_host.replace("http://", "").replace("https://", "")
        with open("/run/servicedns-ca/ca.crt", "rb") as f:
            ca_cert = f.read()
        options = [('grpc.ssl_target_name_override', 'api.ate-system.svc')]
        self.api_channel = grpc.secure_channel(
            target,
            grpc.ssl_channel_credentials(root_certificates=ca_cert),
            options=options,
        )
        self.api_stub = ateapi_pb2_grpc.ControlStub(self.api_channel)

        self.actor_id = f"sb-{uuid.uuid4()}"
        self.glutton_channel = None
        self.glutton_stub = None

        try:
            self.api_stub.CreateActor(
                ateapi_pb2.CreateActorRequest(
                    actor_id=self.actor_id,
                    actor_template_namespace="benchmark-workloads",
                    actor_template_name=self.template_name,
                )
            )
        except Exception as e:
            logger.error(f"Failed to create glutton actor {self.actor_id}: {e}")
            self.api_channel.close()
            raise StopUser()

        # CreateActor leaves the actor SUSPENDED; resume it explicitly with
        # boot=True since there's no golden snapshot for a fresh actor.
        # ResumeActor is synchronous: by the time it returns, the actor is
        # RUNNING on a worker pod and the returned Actor carries that pod's
        # IP (see cmd/ateapi/internal/controlapi/workflow.go).
        try:
            resp = self.api_stub.ResumeActor(
                ateapi_pb2.ResumeActorRequest(actor_id=self.actor_id, boot=True)
            )
        except Exception as e:
            logger.error(f"Failed to resume glutton actor {self.actor_id}: {e}")
            self._teardown_actor()
            self.api_channel.close()
            raise StopUser()

        actor_ip = resp.actor.ateom_pod_ip
        if not actor_ip:
            logger.error(
                f"Glutton actor {self.actor_id} resumed without an ateom_pod_ip; "
                f"stopping user"
            )
            self._teardown_actor()
            self.api_channel.close()
            raise StopUser()

        self.glutton_channel = grpc.insecure_channel(f"{actor_ip}:{GLUTTON_PORT}")
        self.glutton_stub = glutton_pb2_grpc.GluttonStub(self.glutton_channel)

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        if self.glutton_channel is not None:
            try:
                self.glutton_channel.close()
            except Exception as e:
                logger.warning(f"Failed to close glutton channel: {e}")
        self._teardown_actor()
        self.api_channel.close()

    def _teardown_actor(self) -> None:
        try:
            self.api_stub.SuspendActor(
                ateapi_pb2.SuspendActorRequest(actor_id=self.actor_id)
            )
        except Exception as e:
            logger.warning(
                f"Failed to suspend glutton actor {self.actor_id} during teardown: {e}"
            )
        try:
            self.api_stub.DeleteActor(
                ateapi_pb2.DeleteActorRequest(actor_id=self.actor_id)
            )
        except Exception as e:
            logger.warning(
                f"Failed to delete glutton actor {self.actor_id} during teardown: {e}"
            )

    @task
    def ping(self) -> None:
        if self.glutton_stub is None:
            return

        msg = uuid.uuid4().hex
        start_time = time.time()
        with tracer.start_as_current_span("GluttonPing") as span:
            headers = {}
            inject(headers)
            metadata = list(headers.items())
            try:
                resp = self.glutton_stub.Ping(
                    glutton_pb2.PingRequest(message=msg),
                    metadata=metadata,
                )
                duration_ms = (time.time() - start_time) * 1000
                if resp.message != msg:
                    raise RuntimeError(
                        f"Ping echo mismatch: sent={msg!r}, recv={resp.message!r}"
                    )
                events.request.fire(
                    request_type="grpc",
                    name="GluttonPing",
                    response_time=duration_ms,
                    response_length=len(resp.message),
                    exception=None,
                    user_class=self.__class__.__name__,
                )
                if span.get_span_context().trace_flags.sampled:
                    logger.info(
                        f"Traced GluttonPing: trace_id={span.get_span_context().trace_id:032x}, "
                        f"duration={duration_ms:.2f}ms"
                    )
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                events.request.fire(
                    request_type="grpc",
                    name="GluttonPing",
                    response_time=duration_ms,
                    response_length=0,
                    exception=e,
                    user_class=self.__class__.__name__,
                )
                if span.get_span_context().trace_flags.sampled:
                    logger.info(
                        f"Traced GluttonPing (failed): trace_id={span.get_span_context().trace_id:032x}, "
                        f"duration={duration_ms:.2f}ms"
                    )
