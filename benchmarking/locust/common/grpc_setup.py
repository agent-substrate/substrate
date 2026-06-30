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

_initialized = False


def init_grpc_gevent() -> None:
    """Patch gRPC to cooperate with the gevent hub.

    Locust monkey-patches the stdlib for gevent on import, but gRPC's C
    extension uses native threads that don't yield to the hub, so a single
    in-flight gRPC call stalls every other user's HTTP request. This patch
    routes gRPC's poller through the gevent loop.

    Must be called after gevent monkey-patching (locust does that on import)
    and before any gRPC channel is created. Upstream init_gevent() is NOT
    idempotent — a second call orphans the gevent pool and double-registers
    an atexit hook — so this helper guards against repeat calls in case
    multiple test modules are loaded into one locust process.
    """
    global _initialized
    if _initialized:
        return
    from grpc.experimental import gevent as grpc_gevent
    grpc_gevent.init_gevent()
    _initialized = True
