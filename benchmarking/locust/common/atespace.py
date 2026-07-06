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

"""Idempotent atespace bootstrap shared by every locust test.

CreateAtespace returns ALREADY_EXISTS once the atespace is in place; that
is the success signal for the second caller, so concurrent users can race
without coordinating. The call is wrapped in traced_grpc so it shows up
in locust stats / traces alongside every other API call.
"""

import grpc

from common import ateapi_pb2
from common.grpc_tracing import traced_grpc

# Single atespace for all benchmark runs. Actor names within an atespace
# must be unique, and every user picks an `sb-<uuid>` actor id, so a
# shared atespace doesn't introduce collisions.
ATESPACE = "benchmark"


def ensure_atespace(stub, user_class: str, name: str = ATESPACE) -> None:
    with traced_grpc("CreateAtespace", user_class) as metadata:
        try:
            _, metadata.call = stub.CreateAtespace.with_call(
                ateapi_pb2.CreateAtespaceRequest(name=name),
                metadata=metadata,
            )
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.ALREADY_EXISTS:
                raise
            # with_call raises RpcError on failure but the exception is
            # itself a Call object, so we can still pull server-elapsed
            # trailers off the (expected) ALREADY_EXISTS response.
            if isinstance(e, grpc.Call):
                metadata.call = e
