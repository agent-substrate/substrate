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

"""The source hooks: how the evaluator reads a run through a source.

A source is a class, picked by a variable's `source` key, with three hooks:

  validate(cfg)  A classmethod. Raises ValueError for a bad source_cfg. Called
                 when the evaluations are loaded, before anything is read.
  init(run)      Once per run, when the run starts, so its files may not exist
                 yet: what the source will read. The default stores run as
                 self.run for query to read.
  query(cfg)     Once per distinct source_cfg, after the run: named values, or
                 SourceInvalid when the source can't judge the run. Value names
                 come from source_cfg or are fixed by the source, never from the
                 run's data.

Sources read only what the BenchmarkRunContext gives them and never write files, so the
same code serves the runner, which evaluates the run it just finished, and a
re-evaluation of a stored run.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Mapping, Optional, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationError

Scalar = Union[int, float, bool, str, None]

# A name the config defines: an evaluation, variable or condition.
Name = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=64)]


class Model(BaseModel):
    """A config shape: unknown keys are errors and loaded values don't change."""
    model_config = ConfigDict(extra="forbid", frozen=True)


ModelT = TypeVar("ModelT", bound=BaseModel)


def parse(model: type[ModelT], obj: Any) -> ModelT:
    """obj as a model, or ValueError with describe's message."""
    try:
        return model.model_validate(obj)
    except ValidationError as e:
        raise ValueError(describe(e)) from None


def describe(e: ValidationError) -> str:
    """e's errors on one line, each after where it is in the input."""
    def text(err) -> str:
        return str(err["ctx"]["error"]) if err["type"] == "value_error" else err["msg"]
    return "; ".join(f"{'.'.join(map(str, err['loc']))}: {text(err)}" if err["loc"] else text(err)
                     for err in e.errors())


@dataclass(frozen=True)
class BenchmarkRunContext:
    """The run being evaluated: what every source gets at init, whatever it reads.

    files is the run's files by name, as local paths that exist once the run has
    finished; the locust runner produces "logs.txt", "stats.jsonl",
    "server_summary.json" and "stats_history.csv", for example.
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

    def init(self, run: BenchmarkRunContext) -> None:
        self.run = run

    def query(self, cfg: Mapping[str, Any]) -> Mapping[str, Scalar]:
        raise NotImplementedError
