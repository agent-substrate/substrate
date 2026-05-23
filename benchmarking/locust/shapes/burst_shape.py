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

from locust import LoadTestShape

class BurstShape(LoadTestShape):
    # Overall duration for one burst cycle
    cycle_length = 30
    spawn_duration = 10

    # 3 clients peak per burst, spawned at 1/sec
    peak_users = 3
    spawn_rate = 1

    def tick(self):
        run_time = self.get_run_time()
        t = run_time % self.cycle_length

        if t < self.spawn_duration:
            # Active Spawning Phase
            # Target increases steadily towards the peak
            target = min(int((t + 1) * self.spawn_rate), self.peak_users)
            return (target, self.spawn_rate)
        else:
            # Idle Phase
            # To honor the user's natural expiration, we disable forcefully stopping
            # active clients by returning the currently active user count as our target
            active_users = self.runner.user_count
            return (active_users, self.spawn_rate)
