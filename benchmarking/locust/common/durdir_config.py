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

"""DurDir benchmark runtime flags."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_durdir_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("DurDir Benchmark")
    group.add_argument(
        "--durdir-file-size-bytes",
        type=int,
        default=8388608,
        help="Size of the test file written and read during the DurDir benchmark (default: 8388608 = 8 MiB)",
    )
    group.add_argument(
        "--durdir-read-mode",
        type=str,
        default="data",
        choices=["data", "digest"],
        help="Read mode for DurDir serves: 'data' (default) returns and client-verifies the full payload; "
        "'digest' returns only size+sha256 for reduced network overhead",
    )
    group.add_argument(
        "--durdir-template",
        type=str,
        default="glutton-durdir-data",
        help="ActorTemplate name to benchmark (default: glutton-durdir-data)",
    )
