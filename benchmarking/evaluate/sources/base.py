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

"""The source hooks: how the evaluator reads a run through a source.

A source is a class, picked by a variable's `source` key, with three hooks:

  validate(cfg)  A classmethod. Raises ValueError for a bad source_cfg. Called
                 when the evaluations are loaded, before anything is read.
  init(run)      Once per run, when the run starts, so its files may not exist
                 yet: what the source will read. The default keeps run.
  query(cfg)     Once per distinct source_cfg, after the run: named values, or
                 SourceInvalid when the source can't judge the run.

A source can also say, with the value_names classmethod, which values query
gives for a valid cfg, so a condition that reads another fails at load.

Sources read only what the BenchmarkRunContext gives them and never write files, so the
same code serves the runner, which evaluates the run it just finished, and a
re-evaluation of a stored run.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Union

Scalar = Union[int, float, bool, str, None]

# Names of evaluations, variables, conditions and values: lower snake case,
# without "__", which separates a variable from its value in the stats rows.
NAME = re.compile(r"^[a-z][a-z0-9_]*$")
# A value's name may also have capitals, so a measurement keeps the name its
# stats.jsonl row gives it (locust_ResumeActor).
VALUE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
NAME_MAX = 64
# A numeric string: run_results reads a measurement like this as a number, and
# a check reads its limit the same way.
NUMBER = re.compile(r"^\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$")


def valid_name(name: Any) -> bool:
    return isinstance(name, str) and bool(NAME.match(name)) and "__" not in name and len(name) <= NAME_MAX


def valid_value_name(name: Any) -> bool:
    return isinstance(name, str) and bool(VALUE_NAME.match(name)) and "__" not in name and len(name) <= NAME_MAX


@dataclass(frozen=True)
class BenchmarkRunContext:
    """One benchmark run (one run of a test entry), as a source sees it at init.

    files is the run's files by name ("logs.txt", "stats.jsonl",
    "server_summary.json", "stats_history.csv"), as local paths that exist once
    the run has finished.
    """
    files: Mapping[str, Path]

    def file(self, name: str) -> Optional[Path]:
        """The local path of artifact name; None when the run did not produce it."""
        path = self.files.get(name)
        return path if path is not None and Path(path).is_file() else None


class SourceInvalid(Exception):
    """The source can't judge the run: why, and the values it measured anyway."""

    def __init__(self, reason: str, values: Optional[Mapping[str, Scalar]] = None):
        super().__init__(reason)
        self.reason = reason
        self.values = dict(values or {})


class Source:
    """A source; see the module docstring for when each hook runs."""

    run: BenchmarkRunContext

    @classmethod
    def validate(cls, cfg: Mapping[str, Any]) -> None:
        raise NotImplementedError

    @classmethod
    def value_names(cls, cfg: Mapping[str, Any]) -> Optional[set[str]]:
        """The names of the values query gives for a valid cfg; None when the source can't tell before the run."""
        return None

    def init(self, run: BenchmarkRunContext) -> None:
        self.run = run

    def query(self, cfg: Mapping[str, Any]) -> Mapping[str, Scalar]:
        raise NotImplementedError
