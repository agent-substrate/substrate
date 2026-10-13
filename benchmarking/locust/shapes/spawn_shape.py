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

"""Load shape for the Spawn benchmark.

Holds (1, 1.0) until TimeToAllReady has num_requests >= 1 or run_time expires.
"""

import logging

from locust import LoadTestShape

logger = logging.getLogger(__name__)


class SpawnShape(LoadTestShape):
    def tick(self) -> tuple[int, float] | None:
        if self.runner is not None:
            # Use .get() rather than entries[...]: indexing directly on entries
            # creates an empty StatsEntry row if the key is not present, which
            # would pollute the stats table.
            entry = self.runner.environment.stats.entries.get(
                ("TimeToAllReady", "summary")
            )
            if entry is not None and entry.num_requests >= 1:
                return None
            run_time = getattr(self.runner.environment.parsed_options, "run_time", None)
            if run_time and self.get_run_time() >= run_time:
                logger.info("Spawn test reached run_time deadline (%.0fs); stopping", run_time)
                return None
        return (1, 1.0)
