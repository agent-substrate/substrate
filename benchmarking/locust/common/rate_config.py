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

"""Request-rate runtime flag, for boomer user classes that pace (resumecold)."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_rate_arguments(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--target-rps",
        type=float,
        default=0.0,
        env_var="LOCUST_TARGET_RPS",
        help=(
            "Requests per second the whole boomer worker paces to, for user "
            "classes that pace (resumecold). 0 removes the cap. "
            "A --ladder step with @target_rps changes it while the run "
            "continues"
        ),
        include_in_web_ui=True,
    )
