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

"""Locust custom arguments for the actor resume mode."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_resume_mode_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("Resume Mode")
    group.add_argument(
        "--resume-mode",
        type=str,
        default="explicit",
        choices=["explicit", "implicit"],
        help="Resume mode: 'explicit' issues ResumeActor RPC before sending traffic; "
        "'implicit' sends traffic directly and lets the router wake the actor (default: explicit)",
    )
