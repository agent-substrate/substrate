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

from locust import User, task
from locust.exception import StopUser
import time
import uuid
import grpc
from common import ateapi_pb2
from common import ateapi_pb2_grpc
from common.atespace import ATESPACE, ensure_atespace
from common.grpc_tracing import traced_grpc
from common.metrics import init_metrics, update_user_count
from common.trace import init_tracing
from common.wait_time import init_wait_time, dynamic_wait_time
import logging

logger = logging.getLogger(__name__)

init_tracing()
init_metrics()
init_wait_time()


class SleepUser(User):
    wait_time = dynamic_wait_time
    host = "api.ate-system.svc.cluster.local:443"
    template_name = "sleep"

    def on_start(self) -> None:
        update_user_count(1, self.__class__.__name__)

        # Setup gRPC
        target = self.host.replace("http://", "").replace("https://", "")
        with open("/run/servicedns-ca/ca.crt", "rb") as f:
            ca_cert = f.read()
        options = [('grpc.ssl_target_name_override', 'api.ate-system.svc')]
        self.channel = grpc.secure_channel(target, grpc.ssl_channel_credentials(root_certificates=ca_cert), options=options)
        self.stub = ateapi_pb2_grpc.ControlStub(self.channel)

        try:
            ensure_atespace(self.stub, self.__class__.__name__)
        except Exception as e:
            logger.error(f"Failed to ensure atespace {ATESPACE}: {e}")
            self.channel.close()
            raise StopUser()

        # Call CreateActor
        self.actor_id = f"sb-{uuid.uuid4()}"
        self.actor_ref = ateapi_pb2.ActorRef(atespace=ATESPACE, name=self.actor_id)
        try:
            self.stub.CreateActor(
                ateapi_pb2.CreateActorRequest(
                    actor_ref=self.actor_ref,
                    actor_template_namespace="benchmark-workloads",
                    actor_template_name=self.template_name
                )
            )
        except Exception as e:
            logger.error(f"Failed to create actor {self.actor_id}: {e}")
            self.channel.close()
            raise StopUser()

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        # Suspend first
        try:
            self.stub.SuspendActor(
                ateapi_pb2.SuspendActorRequest(actor_ref=self.actor_ref)
            )
        except Exception as e:
            logger.warning(f"Failed to suspend actor {self.actor_id} during teardown: {e}")

        # Delete actor
        try:
            self.stub.DeleteActor(
                ateapi_pb2.DeleteActorRequest(actor_ref=self.actor_ref)
            )
        except Exception as e:
            logger.warning(f"Failed to delete actor {self.actor_id}: {e}")

        self.channel.close()

    @task
    def workload_cycle(self) -> None:
        # Start with a half-second sleep
        time.sleep(0.5)

        try:
            with traced_grpc("SuspendActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.SuspendActor.with_call(
                    ateapi_pb2.SuspendActorRequest(actor_ref=self.actor_ref),
                    metadata=metadata,
                )
        except Exception:
            pass

        # Add a sleep between the two operations of half a second
        time.sleep(0.5)

        try:
            with traced_grpc("ResumeActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.ResumeActor.with_call(
                    ateapi_pb2.ResumeActorRequest(actor_ref=self.actor_ref),
                    metadata=metadata,
                )
        except Exception:
            pass
