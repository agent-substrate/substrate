# Copyright 2026 The Agent Substrate Authors
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

"""ActorTemplate selection for the GluttonUser boomer worker."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_glutton_arguments(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--glutton-template",
        type=str,
        default="glutton",
        help="ActorTemplate the GluttonUser creates actors from, such as "
        "glutton-node, which serves the ping and memory routes but not "
        "--cpu-cores (default: glutton).",
    )
