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

"""Coding-agent-session benchmark runtime flags."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_agentsession_arguments(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--agentsession-script",
        type=str,
        default="coding-session",
        env_var="LOCUST_AGENTSESSION_SCRIPT",
        help=(
            "Built-in agent-session script variant to run; one of the YAML "
            "files under internal/benchmarking/boomer/agentsession/scripts/ "
            "(default: coding-session)"
        ),
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--agentsession-think-scale",
        type=float,
        default=1.0,
        env_var="LOCUST_AGENTSESSION_THINK_SCALE",
        help=(
            "Multiplier on the script's per-step LLM think times: the gap the "
            "actor spends suspended between steps. 0.5 halves every gap, 2.0 "
            "doubles them (default: 1.0)"
        ),
        include_in_web_ui=True,
    )
