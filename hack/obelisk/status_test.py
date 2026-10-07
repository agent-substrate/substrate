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

import importlib.util
import json
import pathlib
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("status", pathlib.Path(__file__).with_name("status.py"))
status = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(status)


class WorkloadStatusTest(unittest.TestCase):
    @mock.patch.object(status.subprocess, "run")
    def test_requires_every_workload_replica(self, run):
        responses = [
            {"spec": {"replicas": 1}, "status": {"readyReplicas": 1}},
            {"spec": {"replicas": 1}, "status": {}},
            {"spec": {"replicas": 1}, "status": {"readyReplicas": 1}},
        ]
        run.side_effect = [mock.Mock(stdout=json.dumps(response)) for response in responses]

        healthy, workloads = status.workload_status()

        self.assertFalse(healthy)
        self.assertEqual("0/1", workloads["ate-api-server"]["replicas"])
        self.assertFalse(workloads["ate-api-server"]["ready"])


if __name__ == "__main__":
    unittest.main()
