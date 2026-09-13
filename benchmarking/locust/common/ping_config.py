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

"""Per-wake ping-loop tuning for the GluttonUser boomer worker."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_ping_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("Ping Loop")
    group.add_argument(
        "--max-pings-per-wake",
        type=int,
        default=1,
        help="Cap on pings the GluttonUser sends during one resume/suspend "
        "cycle. Pings after the first are spaced 0.2-1.0s apart, and the "
        "loop stops early if the live window (--min/--max-live-time) "
        "closes first, so values above 1 need a live window that fits the "
        "gaps. Must be >= 1 (default: 1).",
    )
