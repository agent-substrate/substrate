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

"""Unit tests for util.py: python3 benchmarking/automation/test_util.py"""

import unittest
from unittest import mock

import util


class ParseDurationSecondsTest(unittest.TestCase):
    def test_units(self):
        self.assertEqual(util.parse_duration_seconds("30"), 30)
        self.assertEqual(util.parse_duration_seconds("30s"), 30)
        self.assertEqual(util.parse_duration_seconds("5m"), 300)
        self.assertEqual(util.parse_duration_seconds("2h"), 7200)

    def test_whitespace(self):
        self.assertEqual(util.parse_duration_seconds(" 10 m "), 600)

    def test_invalid(self):
        for bad in ("10 parsecs", "", "m", "1d", "-5s"):
            with self.assertRaises(ValueError):
                util.parse_duration_seconds(bad)


class BuildAndPushTest(unittest.TestCase):
    @mock.patch("util.run")
    def test_builds_amd64_and_pushes(self, run_mock):
        image = util.build_and_push("gcr.io/p/repo/img:tag", "path/Dockerfile")

        self.assertEqual(image, "gcr.io/p/repo/img:tag")
        build_cmd, push_cmd = (c.args[0] for c in run_mock.call_args_list)
        self.assertEqual(build_cmd[:2], ["docker", "build"])
        self.assertIn("--platform", build_cmd)
        self.assertEqual(build_cmd[build_cmd.index("--platform") + 1], "linux/amd64")
        self.assertEqual(build_cmd[build_cmd.index("-t") + 1], "gcr.io/p/repo/img:tag")
        self.assertEqual(build_cmd[build_cmd.index("-f") + 1], "path/Dockerfile")
        self.assertEqual(push_cmd, ["docker", "push", "gcr.io/p/repo/img:tag"])


class WorkerPoolsTest(unittest.TestCase):
    def test_parse_and_boomer_worker_pools(self):
        import orchestrator

        spec = (
            "n4d:50:cloud.google.com/machine-family=n4d,"
            "c4:30:cloud.google.com/machine-family=c4,"
            "default:20"
        )
        self.assertEqual(
            orchestrator.parse_worker_pools(spec),
            [
                ("n4d", 50, "cloud.google.com/machine-family=n4d"),
                ("c4", 30, "cloud.google.com/machine-family=c4"),
                ("default", 20, ""),
            ],
        )
        self.assertEqual(
            orchestrator.boomer_worker_pools(spec),
            "n4d:50,c4:30,default:20",
        )

    def test_parse_worker_pools_invalid(self):
        import orchestrator

        for bad in (
            "",
            "   ,  ",
            "n4d",
            ":10",
            "PoolA:10",
            "pool_a:10",
            "-n4:10",
            "n4-:10",
            "a" * 48 + ":10",
            "n4:10,n4:20",
            "n4d:0",
            "n4d:-5",
            "n4d:abc",
            "n4d:10:badselector",
            "n4d:10:=val",
            "n4d:10:key=",
        ):
            with self.assertRaises(ValueError, msg=f"expected ValueError for {bad!r}"):
                orchestrator.parse_worker_pools(bad)

    def test_validate_rejects_worker_pools_on_non_locust(self):
        import orchestrator

        with self.assertRaisesRegex(
            ValueError, "workerPools is only supported for 'locust' tests"
        ):
            orchestrator.validate_and_normalize_tests(
                [
                    {
                        "name": "nh",
                        "type": "nighthawk-ingress",
                        "targetCluster": "c1",
                        "duration": "1m",
                        "workerPools": "n4d:10,c4:10",
                    }
                ]
            )

    @mock.patch("orchestrator.run")
    @mock.patch("orchestrator.run_no_check")
    def test_deploy_and_teardown_workloads_worker_pools(self, teardown_mock, run_mock):
        import orchestrator

        spec = "n4d:50:cloud.google.com/machine-family=n4d,c4:50:cloud.google.com/machine-family=c4"
        orchestrator.deploy_workloads(
            worker_count=1,
            sandbox_class="gvisor",
            worker_pools=spec,
        )
        self.assertEqual(
            run_mock.call_args.args[0],
            [
                "benchmarking/workloads/deploy.sh",
                "--deploy",
                "--worker-count",
                "1",
                "--sandbox-class",
                "gvisor",
                "--worker-pools",
                spec,
            ],
        )

        orchestrator.teardown_workloads()
        self.assertEqual(
            teardown_mock.call_args.args[0],
            ["benchmarking/workloads/deploy.sh", "--delete"],
        )


if __name__ == "__main__":
    unittest.main()
