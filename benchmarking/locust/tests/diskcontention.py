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

"""Stub DiskContentionUser declaration.

The real load implementation lives in the boomer-Go worker
(internal/benchmarking/boomer/glutton/diskcontention.go); this Python class
is declared only so the master's --class-picker enumerates the name. The
Python worker container sets LOCUST_NO_DISKCONTENTION_USER=1 to skip loading
this file, leaving boomer as the sole owner of DiskContentionUser load.
"""

import os

if os.environ.get("LOCUST_NO_DISKCONTENTION_USER") != "1":
    from locust import User, task
    from common.boomer_config import init_boomer_config

    init_boomer_config()

    class DiskContentionUser(User):
        host = "api.ate-system.svc.cluster.local:443"

        @task
        def noop(self) -> None:
            # Unreached under normal operation; see the module docstring.
            pass
