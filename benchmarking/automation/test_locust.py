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

"""Unit tests for testtypes/locust.py:
python3 benchmarking/automation/test_locust.py"""

import json
import os
import unittest
from unittest import mock

import yaml

import controlplane
import orchestrator
from testtypes import locust


def entry(**fields):
    return {
        "name": "t",
        "file": "/app/tests/resumepaused.py,/app/shapes/ladder_shape.py",
        "duration": "21m",
        "users": 200,
        **fields,
    }


class ValidateTest(unittest.TestCase):
    def test_without_ateapi(self):
        locust.validate(entry())
        self.assertIsNone(locust.ateapi_config(entry()))

    def test_with_ateapi(self):
        test = entry(runnerCpu="8", runnerMemory="2Gi", ateapi={"cpu": 2})
        locust.validate(test)
        self.assertEqual(locust.ateapi_config(test), controlplane.config({"cpu": 2}))

    # controlplane checks the block; its error names the test.
    def test_invalid_ateapi_names_the_test(self):
        with self.assertRaisesRegex(ValueError, "locust test 't': needs ateapi.cpu"):
            locust.validate(entry(ateapi={}))
        with self.assertRaisesRegex(ValueError, "cordon"):
            locust.validate(entry(ateapi={"cpu": 2}, ateArgs=["--cordon-control-plane"]))

    def test_missing_required(self):
        test = entry()
        del test["users"]
        with self.assertRaises(ValueError):
            locust.validate(test)


class PreTestTest(unittest.TestCase):
    def shaped(self, test):
        calls = []
        patcher = mock.patch.object(controlplane, "shape", lambda cfg, cpu: calls.append((cfg, cpu)))
        patcher.start()
        self.addCleanup(patcher.stop)
        locust.pre_test(test)
        return calls

    def test_without_ateapi_leaves_the_cluster(self):
        self.assertEqual(self.shaped(entry()), [])

    def test_shapes_for_the_runner(self):
        self.assertEqual(
            self.shaped(entry(ateapi={"cpu": 2})),
            [(controlplane.config({"cpu": 2}), locust.DEFAULT_RUNNER_CPU)],
        )
        self.assertEqual(self.shaped(entry(runnerCpu="8", ateapi={"cpu": 2}))[0][1], "8")


class JobTest(unittest.TestCase):
    def render(self, test):
        subs = {
            "JOB_NAME": "j", "NAME": "t", "TAG": "c", "IMAGE": "i", "DEST": "d",
            **locust.job_subs(test),
        }
        tmpl = locust.job_tmpl(os.path.join(os.path.dirname(__file__), "manifests"))
        text = orchestrator.render_template(tmpl, subs, test.get("flags", []))
        self.assertNotIn("${", text)
        jobs = [d for d in yaml.safe_load_all(text) if d and d["kind"] == "Job"]
        self.assertEqual(len(jobs), 1)
        return jobs[0]["spec"]["template"]["spec"]

    def test_defaults_render(self):
        test = entry()
        pod = self.render(test)
        self.assertEqual(pod["affinity"], {})
        self.assertEqual(
            pod["containers"][0]["resources"],
            {"requests": {"cpu": "500m", "memory": "512Mi"}},
        )
        # monitoring.yaml's boomer-worker job scrapes this port.
        self.assertIn(
            {"name": "boomer-metrics", "containerPort": 8001},
            pod["containers"][0]["ports"],
        )

    def test_pinned_runner_renders(self):
        test = entry(runnerCpu="8", runnerMemory="2Gi", ateapi={"cpu": 2},
                     flags=["--actors", "200"])
        pod = self.render(test)
        self.assertEqual(pod["affinity"], controlplane.role_affinity("runner"))
        self.assertEqual(
            pod["containers"][0]["resources"]["requests"],
            {"cpu": "8", "memory": "2Gi"},
        )
        self.assertEqual(pod["containers"][0]["args"][-2:], ["--actors", "200"])

    def test_unpinned_runner_has_no_affinity(self):
        subs = locust.job_subs(entry(ateapi={"cpu": 2, "pinNodes": False}))
        self.assertEqual(json.loads(subs["RUNNER_AFFINITY"]), {})


class TestsYamlTest(unittest.TestCase):
    def test_entries_validate(self):
        path = os.path.join(os.path.dirname(__file__), "tests.yaml")
        with open(path) as f:
            tests = yaml.safe_load(f)["tests"]
        orchestrator.validate_and_normalize_tests(tests)


if __name__ == "__main__":
    unittest.main()
