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
from contextlib import contextmanager

from locust import events
from opentelemetry.propagate import inject

from common.trace import get_tracer

logger = logging.getLogger(__name__)
_tracer = get_tracer(__name__)


@contextmanager
def traced_grpc(name, user_class):
    """Wrap a gRPC unary call with tracing + locust reporting.

    Yields a metadata list with W3C trace context already injected (pass it
    as the call's `metadata=` argument). On exit, fires the locust request
    event (success or failure) and logs the trace id when the span is
    sampled. Exceptions re-raise so callers apply their own policy
    (warn / abort / StopUser).

    Usage:
        with traced_grpc("ResumeActor", self.__class__.__name__) as metadata:
            stub.ResumeActor(request, metadata=metadata)
    """
    start_time = time.time()
    with _tracer.start_as_current_span(name) as span:
        headers = {}
        inject(headers)
        exception = None
        try:
            yield list(headers.items())
        except Exception as e:
            exception = e
            raise
        finally:
            duration_ms = (time.time() - start_time) * 1000
            events.request.fire(
                request_type="grpc",
                name=name,
                response_time=duration_ms,
                response_length=0,
                exception=exception,
                user_class=user_class,
            )
            ctx = span.get_span_context()
            if ctx.trace_flags.sampled:
                suffix = " (failed)" if exception else ""
                logger.info(
                    f"Traced {name}{suffix}: trace_id={ctx.trace_id:032x}, "
                    f"duration={duration_ms:.2f}ms"
                )
