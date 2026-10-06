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

"""Egress benchmark runtime flags."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_egress_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("Egress Benchmark")
    group.add_argument(
        "--egress-url",
        type=str,
        default="",
        help="http URL each GluttonUser actor GETs through its egress path "
        "while it is awake (default: empty = disabled). The host must be a "
        "lowercase DNS name; the driver creates each actor's egress policy "
        "for it. Needs a live window (--min-live-time / --max-live-time).",
    )
    group.add_argument(
        "--egress-interval",
        type=float,
        default=1.0,
        help="Seconds between two egress calls, which is also each call's "
        "timeout (default: 1.0).",
    )
    group.add_argument(
        "--egress-connection",
        type=str,
        default="reuse",
        choices=["reuse", "new"],
        help="'reuse' keeps the connection alive between calls; 'new' opens "
        "a new connection, and so a new egress tunnel, for every call "
        "(default: reuse).",
    )
