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

"""Unit tests for testtypes/nighthawk_ingress.py:
python3 benchmarking/automation/test_nighthawk_ingress.py"""

import unittest

from testtypes import nighthawk_ingress


def entry(**knobs):
    return {
        "name": "t",
        "duration": "30m",
        "workerCount": 200,
        "nighthawk-ingress": {"envoyCpu": 4, **knobs},
    }


class FixedRpsTest(unittest.TestCase):
    def test_default_is_adaptive(self):
        test = entry()
        nighthawk_ingress.validate(test)
        subs = nighthawk_ingress.job_subs(test)
        self.assertEqual(subs["FIXED_RPS"], "")
        self.assertEqual(subs["FIXED_STAGE_DURATION"], "90s")

    def test_rates_render_comma_separated(self):
        test = entry(fixedRps=[4000, 8000], fixedStageDuration="120s")
        nighthawk_ingress.validate(test)
        subs = nighthawk_ingress.job_subs(test)
        self.assertEqual(subs["FIXED_RPS"], "4000,8000")
        self.assertEqual(subs["FIXED_STAGE_DURATION"], "120s")

    def test_invalid_rates(self):
        for bad in ("4000", [0], [True], ["4000"], [1.5]):
            with self.assertRaises(ValueError, msg=repr(bad)):
                nighthawk_ingress.validate(entry(fixedRps=bad))

    def test_invalid_stage_duration(self):
        with self.assertRaises(ValueError):
            nighthawk_ingress.validate(entry(fixedStageDuration="1d"))


if __name__ == "__main__":
    unittest.main()
