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

"""SWE-perf benchmark runtime flags."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_sweperf_arguments(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--sweperf-template",
        type=str,
        default="sweperf-astropy-7336",
        env_var="LOCUST_SWEPERF_TEMPLATE",
        help="ActorTemplate name for the sweperf workload (default: sweperf-astropy-7336)",
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--sweperf-num-cycles",
        type=int,
        default=4,
        env_var="LOCUST_SWEPERF_NUM_CYCLES",
        help="Number of suspend/resume cycles to partition the workload steps into (default: 4)",
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--sweperf-poll-interval-ms",
        type=int,
        default=100,
        env_var="LOCUST_SWEPERF_POLL_INTERVAL_MS",
        help="Interval in ms between /status polls while a cycle's job runs (default: 100)",
        include_in_web_ui=True,
    )
