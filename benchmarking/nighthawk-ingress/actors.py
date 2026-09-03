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

"""Actor lifecycle for the capacity benchmark.

Creates N glutton actors via ateapi and warms each by POSTing /ping through
the router until it answers 200 — the same data path the load test
exercises. Actors stay resumed for the whole session; cleanup is
best-effort suspend+delete.
"""

import concurrent.futures
import time

import grpc
import requests

from common import ateapi_pb2
from common import ateapi_pb2_grpc
from common.ateapi_channel import CA_FILE, SERVER_NAME, ateapi_channel

import spec as spec_mod

ATEAPI_HOST = "api.ate-system.svc.cluster.local:443"
TOKEN_FILE = "/run/ateapi-token/token"

# Default --atespace (see the knob docs in the README). Actors are named
# nh-<idx>.
ATESPACE = "ingress-benchmark"

# The glutton template's (atespace, name) identity; see
# benchmarking/workloads/manifests/glutton-template.yaml.tmpl.
TEMPLATE_ATESPACE = "benchmark-workloads"
TEMPLATE_NAME = "glutton"


def _token_channel(host: str) -> grpc.Channel:
    """Bearer-token variant of common.ateapi_channel (mTLS-only): server
    validated against the projected CA, identity from the projected SA
    token (ATE_ATEAPI_CLIENT_AUTH=token)."""
    with open(CA_FILE, "rb") as f:
        ca_cert = f.read()
    with open(TOKEN_FILE) as f:
        token = f.read().strip()
    creds = grpc.composite_channel_credentials(
        grpc.ssl_channel_credentials(root_certificates=ca_cert),
        grpc.access_token_call_credentials(token),
    )
    return grpc.secure_channel(
        host, creds, options=[("grpc.ssl_target_name_override", SERVER_NAME)]
    )


def ateapi_stub(auth_mode: str = "cert"):
    if auth_mode == "token":
        channel = _token_channel(ATEAPI_HOST)
    else:
        channel = ateapi_channel(ATEAPI_HOST)
    return ateapi_pb2_grpc.ControlStub(channel)


def ensure_atespace(stub, name: str = ATESPACE) -> None:
    """CreateAtespace, treating ALREADY_EXISTS as success (idempotent)."""
    try:
        stub.CreateAtespace(
            ateapi_pb2.CreateAtespaceRequest(
                atespace=ateapi_pb2.Atespace(
                    metadata=ateapi_pb2.ResourceMetadata(name=name)
                )
            )
        )
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.ALREADY_EXISTS:
            raise


def create_actor(stub, name: str, atespace: str = ATESPACE) -> None:
    try:
        stub.CreateActor(
            ateapi_pb2.CreateActorRequest(
                actor=ateapi_pb2.Actor(
                    metadata=ateapi_pb2.ResourceMetadata(
                        atespace=atespace, name=name
                    ),
                    actor_template=ateapi_pb2.ObjectRef(
                        atespace=TEMPLATE_ATESPACE,
                        name=TEMPLATE_NAME,
                    ),
                )
            )
        )
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.ALREADY_EXISTS:
            raise


def _warm_actor(
    stub, router_url: str, name: str, atespace: str, deadline: float, log
) -> None:
    """Bring one created actor to serving, so the load ladder never
    measures a cold start: resume the actor, then poll POST /ping through
    the router (addressed by the actor's Host header) until it answers
    200 or the deadline expires. Resume errors are retried: ateapi
    returns FailedPrecondition/Unavailable until a worker frees up."""
    ref = ateapi_pb2.ObjectRef(atespace=atespace, name=name)
    host = spec_mod.actor_host(name, atespace)
    session = requests.Session()
    last_err: str = "not attempted"
    while time.time() < deadline:
        try:
            stub.ResumeActor(ateapi_pb2.ResumeActorRequest(actor=ref))
        except grpc.RpcError as e:
            last_err = f"ResumeActor: {e.code().name}"
        try:
            resp = session.post(
                f"{router_url.rstrip('/')}/ping",
                headers={"Host": host},
                data=b"",
                timeout=10,
            )
            if resp.status_code == 200:
                return
            last_err = f"POST /ping -> {resp.status_code}"
        except requests.RequestException as e:
            last_err = f"POST /ping: {e}"
        time.sleep(2)
    raise RuntimeError(f"actor {name} not warm by deadline ({last_err})")


def create_and_warm(
    stub,
    router_url: str,
    count: int,
    created: list[str],
    atespace: str = ATESPACE,
    warm_deadline_s: int = 600,
    parallelism: int = 16,
    log=print,
) -> list[str]:
    """Create `count` actors and warm them all; returns their names.

    Appends each name to `created` at creation time so the caller can
    cleanup() a partial fleet when this raises.
    """
    ensure_atespace(stub, atespace)
    names = [f"nh-{i:03d}" for i in range(count)]
    for name in names:
        create_actor(stub, name, atespace)
        created.append(name)
    log(f"Created {count} actors from {TEMPLATE_ATESPACE}/{TEMPLATE_NAME}")

    deadline = time.time() + warm_deadline_s
    failures: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as ex:
        futures = {
            ex.submit(
                _warm_actor, stub, router_url, name, atespace, deadline, log
            ): name
            for name in names
        }
        warmed = 0
        for fut in concurrent.futures.as_completed(futures):
            name = futures[fut]
            try:
                fut.result()
                warmed += 1
                if warmed % 10 == 0 or warmed == count:
                    log(f"Warmed {warmed}/{count} actors")
            except Exception as e:
                failures.append(name)
                log(f"Warm failed: {e}")
    if failures:
        raise RuntimeError(
            f"{len(failures)}/{count} actors failed to warm "
            f"within {warm_deadline_s}s: {failures[:5]}..."
        )
    return names


def cleanup(stub, names: list[str], atespace: str = ATESPACE, log=print) -> None:
    """Best-effort delete; never raises."""
    for name in names:
        ref = ateapi_pb2.ObjectRef(atespace=atespace, name=name)
        try:
            stub.DeleteActor(ateapi_pb2.DeleteActorRequest(actor=ref))
        except Exception as e:
            log(f"DeleteActor({name}) failed: {e}")
