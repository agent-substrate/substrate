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
from common.grpc_setup import init_grpc_gevent

# Patch gRPC to cooperate with locust's gevent loop before any channel exists.
init_grpc_gevent()

import time
import uuid
import gevent
import grpc
import requests
from common import ateapi_pb2
from common import ateapi_pb2_grpc
from common import glutton_pb2
from common.grpc_tracing import traced_grpc
from common.metrics import init_metrics, update_user_count
from common.trace import init_tracing, get_tracer
from common.wait_time import init_wait_time, dynamic_wait_time
from opentelemetry.propagate import inject
import logging

logger = logging.getLogger(__name__)

init_tracing()
init_metrics()
init_wait_time()

tracer = get_tracer(__name__)


# Atenet router fronts all actor traffic. Actors are addressed by setting
# the HTTP Host header to <actor_id>.actors.resources.substrate.ate.dev;
# the router resolves that to the actor's current worker pod (and so we
# never need to know the per-resume pod IP ourselves). Glutton is launched
# with --mode=http so /ping speaks HTTP/1.1.
ROUTER_URL = "http://atenet-router.ate-system.svc.cluster.local"
ACTOR_DOMAIN = "actors.resources.substrate.ate.dev"


class GluttonUser(User):
    """Creates a glutton actor on start. Each @task iteration resumes the
    actor, pings it, then suspends it again; on stop the actor is deleted."""

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
        # First ResumeActor needs boot=True since there's no snapshot yet;
        # subsequent resumes restore from the snapshot the prior suspend wrote.
        self._first_resume = True
        # Tracks whether the actor is currently RUNNING so teardown only
        # suspends when something interrupted the resume/suspend pairing.
        self._actor_running = False

        try:
            with traced_grpc("CreateActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.api_stub.CreateActor.with_call(
                    ateapi_pb2.CreateActorRequest(
                        actor_id=self.actor_id,
                        actor_template_namespace="benchmark-workloads",
                        actor_template_name=self.template_name,
                    ),
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Failed to create glutton actor {self.actor_id}: {e}")
            self.api_channel.close()
            raise StopUser()

        # One HTTP session per user, talking to the router. The Host header
        # pins each request to this actor regardless of which worker pod
        # hosts it after a resume.
        self.http_session = requests.Session()
        self.ping_url = f"{ROUTER_URL}/ping"
        self.host_header = f"{self.actor_id}.{ACTOR_DOMAIN}"

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        self._teardown_actor()
        self.api_channel.close()

    def _teardown_actor(self) -> None:
        """ResumeActor; the router handles addressing so no channel work."""
        boot = self._first_resume
        # First resume pays for golden-snapshot creation; bucket it separately
        # so the warm-resume stats aren't skewed by the cold path.
        metric = "ResumeActorColdStart" if boot else "ResumeActor"
        try:
            with traced_grpc(metric, self.__class__.__name__) as metadata:
                _, metadata.call = self.api_stub.ResumeActor.with_call(
                    ateapi_pb2.ResumeActorRequest(
                        actor_id=self.actor_id, boot=boot
                    ),
                    metadata=metadata,
                )
        except Exception as e:
            logger.warning(f"Failed to resume glutton actor {self.actor_id}: {e}")
            return False
        self._first_resume = False
        self._actor_running = True
        return True

    def _suspend_actor(self):
        """SuspendActor (channel stays open across iterations)."""
        try:
            with traced_grpc("SuspendActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.api_stub.SuspendActor.with_call(
                    ateapi_pb2.SuspendActorRequest(actor_id=self.actor_id),
                    metadata=metadata,
                )
        except Exception as e:
            logger.warning(f"Failed to suspend glutton actor {self.actor_id}: {e}")
        self._actor_running = False

    def _teardown_actor(self):
        # If we crashed mid-iteration before _suspend_actor ran, suspend now.
        if self._actor_running:
            self._suspend_actor()
        try:
            with traced_grpc("DeleteActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.api_stub.DeleteActor.with_call(
                    ateapi_pb2.DeleteActorRequest(actor_id=self.actor_id),
                    metadata=metadata,
                )
        except Exception as e:
            logger.warning(
                f"Failed to delete glutton actor {self.actor_id} during teardown: {e}"
            )
        try:
            self.http_session.close()
        except Exception as e:
            logger.warning(f"Failed to close http session: {e}")

    @task
    def ping(self) -> None:
        if not self._resume_actor():
            return
        try:
            self._do_ping()
        finally:
            self._suspend_actor()

    def _do_ping(self):
        msg = uuid.uuid4().hex
        body = glutton_pb2.PingRequest(message=msg).SerializeToString()
        start_time = time.time()
        with tracer.start_as_current_span("GluttonPing") as span:
            headers = {
                "Host": self.host_header,
                "Content-Type": "application/x-protobuf",
            }
            inject(headers)
            resp = None
            try:
                resp = self.http_session.post(
                    self.ping_url, data=body, headers=headers
                )
                resp.raise_for_status()
<<<<<<< HEAD
                duration_ms = (time.time() - start_time) * 1000
=======
>>>>>>> 81b1450 (Use server-side request timing)
                pong = glutton_pb2.PingResponse()
                pong.ParseFromString(resp.content)
                if pong.message != msg:
                    raise RuntimeError(
                        f"Ping echo mismatch: sent={msg!r}, recv={pong.message!r}"
                    )
                duration, source = self._ping_duration_ms(resp, start_time)
                if source == "router":
                    span.set_attribute("router.elapsed_ms", duration)
                events.request.fire(
                    request_type="http",
                    name="GluttonPing",
                    response_time=duration_ms,
                    response_length=len(resp.content),
                    exception=None,
                    user_class=self.__class__.__name__,
                )
                if span.get_span_context().trace_flags.sampled:
                    gevent.spawn(
                        logger.info,
                        f"Traced GluttonPing: trace_id={span.get_span_context().trace_id:032x}, "
<<<<<<< HEAD
                        f"duration={duration_ms:.2f}ms"
                    )
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
=======
                        f"duration={duration:.2f}ms ({source})",
                    )
            except Exception as e:
                duration, source = self._ping_duration_ms(resp, start_time)
>>>>>>> 81b1450 (Use server-side request timing)
                events.request.fire(
                    request_type="http",
                    name="GluttonPing",
                    response_time=duration_ms,
                    response_length=0,
                    exception=e,
                    user_class=self.__class__.__name__,
                )
                if span.get_span_context().trace_flags.sampled:
                    gevent.spawn(
                        logger.info,
                        f"Traced GluttonPing (failed): trace_id={span.get_span_context().trace_id:032x}, "
<<<<<<< HEAD
                        f"duration={duration_ms:.2f}ms"
=======
                        f"duration={duration:.2f}ms ({source})",
>>>>>>> 81b1450 (Use server-side request timing)
                    )

    @staticmethod
    def _ping_duration_ms(resp, start_time):
        """Prefer the router-emitted elapsed (excludes client scheduling), fall
        back to the client-side wall clock when the header is absent (e.g.
        connection failures, or atenet older than the matching server change)."""
        if resp is not None:
            raw = resp.headers.get("x-atenet-elapsed-us")
            if raw is not None:
                try:
                    return int(raw) / 1000.0, "router"
                except ValueError:
                    pass
        return (time.time() - start_time) * 1000, "client"
