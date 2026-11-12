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
(cel_env.py). evaluator.py judges them into a Report, which outputs.py writes
as evaluation.json and stats.jsonl rows. runner.py calls prepare when a run
starts and write once it has finished. rundir.py finds the runs a run is
paired with under the runner's --dest. Depends on cel-expr-python
and pydantic (requirements.txt). See README.md.
"""

from pathlib import Path
from typing import Any, Optional

from . import config, outputs
from .evaluator import Evaluator, Report, error_report  # noqa: F401
from .outputs import summary_lines  # noqa: F401
from .sources.base import BenchmarkRunContext, LocalArtifacts, RunIdentity  # noqa: F401


def check_config(evaluations: Any) -> Optional[str]:
    """Why evaluations (a test entry's block, as parsed) is not valid, as "config: ..."; None when it is. Never raises."""
    try:
        config.load(evaluations)
    except ValueError as e:  # a ConfigError
        return f"config: {e}"
    except Exception as e:  # pylint: disable=broad-except  # a shape no check expected
        return f"config: {type(e).__name__}: {e}"
    return None


def prepare(evaluations: Any, run: BenchmarkRunContext) -> Evaluator:
    """The evaluator of run, whose sources have run their init hook. Raises when evaluations is invalid."""
    return Evaluator(config.load(evaluations), run)


def write(report: Report, out_json: Path, jsonl_path: Path, timestamp: str, tag: str, test_name: str) -> None:
    """Writes report to out_json and appends its stats rows to jsonl_path."""
    outputs.write_evaluation_json(report, out_json)
    outputs.append_jsonl(outputs.stats_rows(report, timestamp, tag, test_name), jsonl_path)
