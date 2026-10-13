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

"""CPU-load benchmark runtime flags."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_cpuload_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("CPU Benchmark")
    group.add_argument(
        "--cpu-cores",
        type=int,
        default=0,
        help="Goroutines each GluttonUser starts spinning in its actor via "
        "the glutton UseCPU API before the first suspend, so suspend/resume "
        "cycles run against an actor with a steady CPU draw (default: 0 = "
        "disabled). Capped at the actor's GOMAXPROCS.",
    )
    group.add_argument(
        "--cpu-duty-cycle",
        type=float,
        default=0.0,
        help="Fraction of one core, in [0, 1], each --cpu-cores goroutine "
        "consumes (e.g. 0.1 with --cpu-cores=1 draws 0.1 vCPU). "
        "Requires --cpu-cores.",
    )
