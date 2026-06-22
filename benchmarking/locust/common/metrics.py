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

from prometheus_client import start_http_server
from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from locust import events
import logging

logger = logging.getLogger(__name__)

_initialized = False
request_counter = None
request_latency = None
locust_users = None

def init_metrics() -> None:
    global _initialized, request_counter, request_latency, locust_users
    if _initialized:
        logger.info("Metrics already initialized, skipping.")
        return

    logger.info("Initializing Prometheus metrics...")
    # Start prometheus client metrics server on port 8000
    try:
        start_http_server(8000)
    except Exception as e:
        logger.warning(f"Metrics server already started or failed: {e}")

    _reader = PrometheusMetricReader()
    _provider = MeterProvider(metric_readers=[_reader])
    metrics.set_meter_provider(_provider)

    meter = metrics.get_meter("locust_common")

    request_counter = meter.create_counter(
        name="locust_requests_total",
        description="Total number of requests"
    )

    request_latency = meter.create_histogram(
        name="locust_request_duration_milliseconds",
        description="Request latency in milliseconds"
    )

    locust_users = meter.create_up_down_counter(
        name="locust_users",
        description="Number of active locust users"
    )

    _initialized = True
    logger.info("Metrics initialized.")

@events.request.add_listener
def on_request(
    request_type: str,
    name: str,
    response_time: float,
    response_length: int,
    exception: Exception | None,
    context: dict | None = None,
    **kwargs,
) -> None:
    if not _initialized:
        return

    if context is None:
        context = kwargs.get('context', {})

    user_class = context.get('user_class', 'unknown')

    if 'user_class' in kwargs:
        user_class = kwargs['user_class']

    attributes = {
        "method": request_type,
        "name": name,
        "status": "success" if exception is None else "failure",
        "user_class": user_class
    }
    request_counter.add(1, attributes)
    request_latency.record((response_time or 0), attributes)


def update_user_count(delta: int, user_class: str) -> None:
    if not _initialized:
        logger.warning("Metrics not initialized, cannot update user count.")
        return
    locust_users.add(delta, {"user_class": user_class})


