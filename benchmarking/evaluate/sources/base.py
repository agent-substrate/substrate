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

Sources read the run only through its BenchmarkRunContext and never write
files, so the same code serves the runner, which evaluates the run it just
finished, and a re-evaluation of a stored run.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Callable, Mapping, Optional, Protocol, TypeVar, Union

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


def no_paired_runs(test: str) -> "BenchmarkRunContext":
    """BenchmarkRunContext.paired_run of a run that is paired with nothing."""
    raise LookupError(f"no run of {test} to pair with")


@dataclass(frozen=True)
class RunIdentity:
    """Which run it is. uri is where the run is stored; the other fields are
    None when they aren't known, as for a local directory outside the runner's
    layout."""
    uri: str
    test_name: Optional[str] = None
    tag: Optional[str] = None
    run_date: Optional[str] = None  # YYYY-MM-DD
    run_ts: Optional[int] = None  # unix seconds

    def record(self) -> dict[str, str]:
        """The known fields as strings, as a Report records them."""
        known = {"test_name": self.test_name, "tag": self.tag, "run_date": self.run_date,
                 "run_ts": None if self.run_ts is None else str(self.run_ts), "uri": self.uri}
        return {k: v for k, v in known.items() if v is not None}


class ArtifactStore(Protocol):
    """Where a run's artifacts are stored."""

    def fetch(self, name: str) -> Optional[Path]:
        """The local path of the stored artifact name; None when the run didn't store it."""


@dataclass(frozen=True)
class LocalArtifacts:
    """A run's artifacts in a local directory, under the names they are stored under."""
    dir: Path

    def fetch(self, name: str) -> Optional[Path]:
        path = self.dir / name
        return path if path.is_file() else None


@dataclass(frozen=True)
class BenchmarkRunContext:
    """The run being evaluated: which run it is (identity), where it is stored
    (identity.uri, artifacts) and its stored artifacts. Each source chooses
    what it reads: one of the run's artifacts, by name, or another store,
    keyed by the run's identity.

    paired_run(test) returns the run of another test from the same invocation
    (e.g. the suite's 1-user run), with its own identity, or raises
    LookupError saying why there is none; whoever creates the context says
    what "the same invocation" means. Call it from query: pairing may read this
    run's artifacts.
    """
    identity: RunIdentity
    artifacts: ArtifactStore
    paired_run: Callable[[str], "BenchmarkRunContext"] = no_paired_runs

    def artifact(self, name: str) -> Optional[Path]:
        """The local path of the run's stored artifact name, fetched on first
        use; None when the run didn't store it (or hasn't yet). Raises
        ValueError for a name outside the run."""
        if name.startswith("/") or ".." in name.split("/"):
            raise ValueError(f"artifact name {name!r} is outside the run")
        return self.artifacts.fetch(name)


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
