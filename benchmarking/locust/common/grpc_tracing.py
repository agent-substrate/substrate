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

import logging
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager

import gevent
import grpc
from locust import events
from opentelemetry.propagate import inject

from common.trace import get_tracer

logger = logging.getLogger(__name__)
_tracer = get_tracer(__name__)

# Server-side handler duration, in microseconds, emitted by
# internal/ateinterceptors. Lets us report a latency that excludes the
# gevent scheduling delay between the C-layer response and the Python
# greenlet resuming.
SERVER_ELAPSED_TRAILER = "x-server-elapsed-us"


class TracedMetadata(list):
    """gRPC metadata list with a slot for the post-call Call object.

    Callers that want server-reported latency should invoke the stub via
    ``stub.X.with_call(req, metadata=metadata)`` and assign the returned
    Call to ``metadata.call``; traced_grpc then reads the server's elapsed
    trailer and reports that instead of the client-measured duration.
    """

    def __init__(self, items: Iterable[tuple[str, str]]) -> None:
        super().__init__(items)
        self.call: grpc.Call | None = None


def _read_server_elapsed_ms(call: grpc.Call | None) -> float | None:
    """Return server-reported handler duration in ms, or None if absent."""
    if call is None:
        return None
    try:
        trailers = call.trailing_metadata()
    except Exception:
        return None
    for key, value in trailers or ():
        if key == SERVER_ELAPSED_TRAILER:
            try:
                return int(value) / 1000.0
            except (TypeError, ValueError):
                return None
    return None


@contextmanager
def traced_grpc(name: str, user_class: str) -> Iterator[TracedMetadata]:
    """Wrap a gRPC unary call with tracing + locust reporting.

    Yields a TracedMetadata (a list subclass) with W3C trace context already
    injected; pass it as the call's ``metadata=`` argument. To report
    server-side handler time (unaffected by client-side gevent scheduling),
    use ``stub.X.with_call(...)`` and assign the returned Call to
    ``metadata.call`` before the with block exits. On exit, fires the
    locust request event (success or failure) and logs the trace id when
    sampled. Exceptions re-raise so callers apply their own policy
    (warn / abort / StopUser).

    Usage:
        with traced_grpc("ResumeActor", self.__class__.__name__) as metadata:
            _, metadata.call = stub.ResumeActor.with_call(request, metadata=metadata)
    """
    start_time = time.time()
    with _tracer.start_as_current_span(name) as span:
        headers = {}
        inject(headers)
        metadata = TracedMetadata(headers.items())
        exception = None
        try:
            yield metadata
        except Exception as e:
            exception = e
            raise
        finally:
            client_ms = (time.time() - start_time) * 1000
            server_ms = _read_server_elapsed_ms(metadata.call)
            reported_ms = server_ms if server_ms is not None else client_ms
            if server_ms is not None:
                span.set_attribute("server.elapsed_ms", server_ms)
            events.request.fire(
                request_type="grpc",
                name=name,
                response_time=reported_ms,
                response_length=0,
                exception=exception,
                user_class=user_class,
            )
            ctx = span.get_span_context()
            if ctx.trace_flags.sampled:
                suffix = " (failed)" if exception else ""
                source = "server" if server_ms is not None else "client"
                # Log out-of-band so the request greenlet returns to its task
                # immediately. Letting logger.info run inline pads the parent
                # span (and tail latency under load) because stderr writes
                # yield to the gevent hub.
                gevent.spawn(
                    logger.info,
                    f"Traced {name}{suffix}: trace_id={ctx.trace_id:032x}, "
                    f"duration_ms={reported_ms:.2f} ({source})",
                )
