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

"""Cold-start (time-to-first-execution) load shape.

AteAPIUser measures the WARM path: it resumes one long-lived actor
repeatedly, so every ResumeActor after the first hits an already-RUNNING
actor. That never exercises actor cold-start latency.

ColdStartUser measures the COLD path. Each task iteration provisions a
FRESH actor, times a single first ResumeActor on it, then tears it down,
so every sample is a genuine cold start. The ResumeActor handler blocks
until the workload's readyz endpoint reports 200 (see the
actortemplate_controller golden-snapshot warmup path), so the resume's
elapsed time IS the actor's time-to-first-execution.

Two cold-start axes, one per task, each on its own fresh actor:

  * ColdStartSnapshotResume -- ResumeActor(boot=False): cold start VIA the
    golden snapshot restore path (substrate's fast-start differentiator).
  * ColdStartBootResume     -- ResumeActor(boot=True): cold start via a
    full workload boot from scratch, skipping the golden snapshot
    (worst-case / baseline).

Reported latency is the server-handler elapsed time when the
x-server-elapsed-us trailer is present (which spans the block-until-readyz
wait), falling back to client wall-clock -- same convention as the warm
path, via traced_grpc + with_call.

Run cold-start at LOW concurrency: TTFE is a per-actor boot latency, not a
saturation metric. Driving high user counts here measures scheduler
queueing, not cold-start. Pair with the SLO-knee sweep (which uses the
warm path) for the throughput axis.
"""

import logging
import uuid

import grpc
from locust import User, task
from common import ateapi_pb2
from common import ateapi_pb2_grpc
from common.ateapi_channel import ateapi_channel
from common.atespace import ATESPACE, ensure_atespace
from common.grpc_tracing import traced_grpc
from common.metrics import init_metrics, update_user_count
from common.trace import init_tracing
from common.wait_time import init_wait_time, dynamic_wait_time

logger = logging.getLogger(__name__)

init_tracing()
init_metrics()
init_wait_time()


class ColdStartUser(User):
    wait_time = dynamic_wait_time

    host = "api.ate-system.svc.cluster.local:443"

    def on_start(self) -> None:
        update_user_count(1, self.__class__.__name__)

        self.channel = ateapi_channel(self.host)
        self.stub = ateapi_pb2_grpc.ControlStub(self.channel)

        try:
            ensure_atespace(self.stub, self.__class__.__name__)
        except Exception as e:
            print(f"Failed to ensure atespace {ATESPACE}: {e}")

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        self.channel.close()

    def _create_fresh_actor(self) -> ateapi_pb2.ObjectRef:
        """Provision a brand-new actor and return its ref.

        A unique name per call guarantees the subsequent ResumeActor is a
        genuine cold start rather than a warm re-resume.
        """
        actor_name = str(uuid.uuid4())
        actor_ref = ateapi_pb2.ObjectRef(atespace=ATESPACE, name=actor_name)
        self.stub.CreateActor(
            ateapi_pb2.CreateActorRequest(
                actor=ateapi_pb2.Actor(
                    metadata=ateapi_pb2.ResourceMetadata(
                        atespace=ATESPACE, name=actor_name
                    ),
                    actor_template_namespace="ate-demo-counter",
                    actor_template_name="counter",
                )
            )
        )
        return actor_ref

    def _teardown_actor(self, actor_ref: ateapi_pb2.ObjectRef) -> None:
        """Best-effort suspend + delete so cold actors don't accumulate.

        Runs outside any traced block -- teardown cost must not pollute the
        cold-start latency sample.
        """
        try:
            self.stub.SuspendActor(
                ateapi_pb2.SuspendActorRequest(actor=actor_ref)
            )
        except Exception:
            pass
        try:
            self.stub.DeleteActor(
                ateapi_pb2.DeleteActorRequest(actor=actor_ref)
            )
        except Exception:
            pass

    def _measure_cold_start(self, metric_name: str, boot: bool) -> None:
        actor_ref = None
        try:
            actor_ref = self._create_fresh_actor()
        except Exception as e:
            print(f"Failed to create actor for {metric_name}: {e}")
            return

        try:
            with traced_grpc(metric_name, self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.ResumeActor.with_call(
                    ateapi_pb2.ResumeActorRequest(actor=actor_ref, boot=boot),
                    metadata=metadata,
                )
        except Exception:
            # traced_grpc already fired the failure event; keep looping.
            pass
        finally:
            self._teardown_actor(actor_ref)

    @task
    def cold_start_snapshot(self) -> None:
        """Cold start via golden-snapshot restore (boot=False)."""
        self._measure_cold_start("ColdStartSnapshotResume", boot=False)

    @task
    def cold_start_boot(self) -> None:
        """Cold start via full boot from scratch (boot=True)."""
        self._measure_cold_start("ColdStartBootResume", boot=True)
