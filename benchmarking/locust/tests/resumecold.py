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

"""Stub ResumeColdUser declaration.

The real load implementation lives in the boomer-Go worker at
internal/benchmarking/boomer/resumecold/ (registered by the boomer-worker
binary in cmd/benchmarking/boomer-worker/); this Python class is declared
only so the master recognizes the name and attributes boomer's stats rows
to it. A Python worker container sets LOCUST_NO_RESUMECOLD_USER=1 to skip
loading this file, leaving boomer as the sole owner of ResumeColdUser load.

Each user takes a paused actor from a shared fleet (boomer's
--total-actors, forwarded by runner.py), resumes it, and pauses it again,
with no wait. Pace the cold resumes with --target-rps, or with @target_rps
steps of a --ladder.
"""

import os

if os.environ.get("LOCUST_NO_RESUMECOLD_USER") != "1":
    from locust import User, task
    from common.boomer_config import init_boomer_config

    # Master serves /boomer-config so the boomer-worker workers can fetch
    # runtime flag values (target rate, trace probability). No-op on
    # workers without a web UI.
    init_boomer_config()

    class ResumeColdUser(User):
        host = "api.ate-system.svc.cluster.local:443"

        @task
        def noop(self) -> None:
            # Unreached under normal operation: the Python worker container
            # does not load this file (LOCUST_NO_RESUMECOLD_USER=1). Body is
            # required because locust validates that every User has at least
            # one @task method.
            pass
