# Copyright 2026 The Agent Substrate Authors
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

"""Run evaluations: judge a finished benchmark run against conditions declared in its test entry.

A test entry's `evaluations:` (config.py) name sources, which read the run's
files into values (sources/), and CEL conditions over those values
(cel_env.py). evaluator.py judges them into a Report. Depends on
cel-expr-python and pydantic (requirements.txt). See README.md.
"""
