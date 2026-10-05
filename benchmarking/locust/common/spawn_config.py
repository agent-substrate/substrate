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

"""Spawn benchmark runtime flags."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_spawn_arguments(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--total-actors",
        type=int,
        default=0,
        env_var="LOCUST_TOTAL_ACTORS",
        help="Total actors in the batch; 0 uses boomer-worker's --total-actors (default 100)",
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--spawn-concurrency",
        type=int,
        default=0,
        env_var="LOCUST_SPAWN_CONCURRENCY",
        help="Actors created concurrently; 0 uses boomer-worker's --spawn-concurrency (default 1)",
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--actor-deadline",
        type=float,
        default=0.0,
        env_var="LOCUST_ACTOR_DEADLINE",
        help="Per-actor timeout in seconds; 0 uses boomer-worker's --actor-deadline (default 120s)",
        include_in_web_ui=True,
    )
