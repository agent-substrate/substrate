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

"""Unit tests for shapes/ladder_shape.py.

Run via: python3 benchmarking/locust/unit_tests/test_ladder_shape.py
Needs locust: pip install -r benchmarking/locust/requirements.txt
"""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shapes.ladder_shape import LadderShape, parse_ladder


class ParseLadderTest(unittest.TestCase):
    def test_users_and_duration(self):
        self.assertEqual(
            parse_ladder("5:3m,15:90s,30:1h"),
            [(5, 180, None, None), (15, 90, None, None), (30, 3600, None, None)],
        )

    def test_trace_probability(self):
        self.assertEqual(parse_ladder("15:3m:0.1"), [(15, 180, 0.1, None)])

    def test_target_rps(self):
        self.assertEqual(
            parse_ladder("200:3m@1000, 200:3m@2500.5"),
            [(200, 180, None, 1000.0), (200, 180, None, 2500.5)],
        )

    def test_probability_and_target_rps(self):
        self.assertEqual(parse_ladder("200:3m:0@0"), [(200, 180, 0.0, 0.0)])

    def test_invalid(self):
        for bad in ("", "200", "200:3m@", "200:3m@x", "200:0m@100",
                    "200:3m@1.2.3", "200:3m:2@100", "200:3m@100:0.1"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                parse_ladder(bad)


class TickTest(unittest.TestCase):
    def shape(self, ladder, run_time):
        options = SimpleNamespace(
            ladder=ladder, ladder_spawn_rate=5.0, target_rps=0.0
        )
        shape = LadderShape()
        shape.runner = SimpleNamespace(
            environment=SimpleNamespace(parsed_options=options)
        )
        shape.get_run_time = lambda: run_time[0]
        return shape, options

    def test_step_sets_target_rps_and_keeps_it(self):
        run_time = [0.0]
        shape, options = self.shape("200:60s@1000,100:60s,200:60s@0", run_time)

        self.assertEqual(shape.tick(), (200, 5.0))
        self.assertEqual(options.target_rps, 1000.0)

        run_time[0] = 61.0
        self.assertEqual(shape.tick(), (100, 5.0))
        self.assertEqual(options.target_rps, 1000.0)

        run_time[0] = 121.0
        self.assertEqual(shape.tick(), (200, 5.0))
        self.assertEqual(options.target_rps, 0.0)

        run_time[0] = 181.0
        self.assertIsNone(shape.tick())

    def test_no_ladder_is_inert(self):
        shape, _ = self.shape("", [0.0])
        self.assertIsNone(shape.tick())


if __name__ == "__main__":
    unittest.main()
