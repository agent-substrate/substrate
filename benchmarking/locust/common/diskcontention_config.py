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

"""Disk-contention benchmark runtime flags (DiskContentionUser)."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_diskcontention_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("Disk Contention Benchmark")
    group.add_argument(
        "--dc-probe-users",
        type=int,
        default=None,
        help="Number of VUs (the first ones spawned) that run probe Glutton "
        "actors through suspend/resume cycles, reported as Probe* rows. "
        "Every later VU is a noise VU.",
    )
    group.add_argument(
        "--dc-probe-min-wait-time",
        type=float,
        default=None,
        help="Probe think time lower bound in seconds; unset or 0 falls back to "
        "--min-wait-time. Lets noise VUs keep the global --min/--max-wait-time.",
    )
    group.add_argument(
        "--dc-probe-max-wait-time",
        type=float,
        default=None,
        help="Probe think time upper bound in seconds; unset or 0 falls back to "
        "--max-wait-time.",
    )
    group.add_argument(
        "--dc-noise-mode",
        type=str,
        default=None,
        choices=["none", "disk", "sweperf"],
        help="What noise VUs run: 'disk' resumes one glutton actor and writes "
        "fsynced data to its host-backed data dir; 'sweperf' runs the SWE-bench "
        "replay; 'none' leaves them idle.",
    )
    group.add_argument(
        "--dc-noise-template",
        type=str,
        default=None,
        help="ActorTemplate for disk noise actors (default: glutton-durdir-data, "
        "whose data dir is a durable dir on the host disk).",
    )
    group.add_argument(
        "--dc-noise-write-size-bytes",
        type=int,
        default=None,
        help="Bytes per noise WriteDisk call (default: 67108864 = 64 MiB).",
    )
    group.add_argument(
        "--dc-noise-files",
        type=int,
        default=None,
        help="Distinct files each noise actor rotates through (default: 4).",
    )
    group.add_argument(
        "--dc-noise-rate-mbps",
        type=float,
        default=None,
        help="Write rate cap per noise actor in MB/s (default: 0 = uncapped).",
    )
    group.add_argument(
        "--dc-noise-no-fsync",
        type=int,
        default=None,
        help="1 to skip fsync after each noise write (page-cache-only writes).",
    )
