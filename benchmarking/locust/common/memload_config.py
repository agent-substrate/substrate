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

"""Memory-load benchmark runtime flags."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_memload_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("Memory Benchmark")
    group.add_argument(
        "--mem-target",
        type=str,
        default="",
        help="Resident working set each GluttonUser gives its actor via the "
        "glutton WriteRAM API before the first suspend, with an optional "
        "unit suffix (e.g. '1Gi'), so suspend/resume cycles run against "
        "realistically-sized memory (default: empty = disabled). Size the "
        "actorMemory limit above it for headroom.",
    )
