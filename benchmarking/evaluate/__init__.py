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

"""Run evaluations: judge a finished benchmark run against conditions declared in its test entry.

A test entry's `evaluations:` (config.py) name sources, which read the run's
files into values (sources/), and threshold conditions over those values
(checks.py). evaluator.py judges them; outputs.py writes evaluation.json and
stats.jsonl rows. runner.py calls prepare when a run starts and write once it
has finished; rundir.py finds the runs a run is paired with under the
runner's --dest. `python -m evaluate` (__main__.py) judges a stored run with
the same code. Standard library only, plus cel-expr-python for the cel
source (requirements.txt). See README.md.
"""

import json
from pathlib import Path
from typing import Any, Optional

from . import config, outputs
from .evaluator import Evaluator, error_report  # noqa: F401
from .outputs import summary_lines  # noqa: F401
from .sources import BenchmarkRunContext  # noqa: F401


def check_config(evaluations_json: str) -> Optional[str]:
    """Why evaluations_json is not a valid evaluations block, as "config: ..."; None when it is. Never raises."""
    try:
        config.load(json.loads(evaluations_json))
    except ValueError as e:  # ConfigError and json.JSONDecodeError are ValueErrors
        return f"config: {e}"
    except Exception as e:  # pylint: disable=broad-except  # a shape no check expected
        return f"config: {type(e).__name__}: {e}"
    return None


def prepare(evaluations_json: str, run: BenchmarkRunContext) -> Evaluator:
    """The evaluator of run, whose sources have run their init hook. Raises when evaluations_json is invalid."""
    return Evaluator(config.load(json.loads(evaluations_json)), run)


def write(report: dict[str, Any], out_json: Path, jsonl_path: Path, timestamp: str, tag: str,
          test_name: str) -> None:
    """Writes report to out_json and appends its stats rows to jsonl_path."""
    outputs.write_evaluation_json(report, out_json)
    outputs.append_jsonl(outputs.stats_rows(report, timestamp, tag, test_name), jsonl_path)
