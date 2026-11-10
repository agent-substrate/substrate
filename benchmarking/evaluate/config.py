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

"""The `evaluations:` block of a test entry: loading and validation.

  evaluations:
  - name: envelope_health              # unique in the test
    description: "..."                 # optional
    variables:                         # each is one source query, with several named values
    - {name: trial, source: run_results, source_cfg: {...}}
    conditions:                        # AND-ed
    - {name: failure_ratio, cel: "trial.aggregate_failure_ratio <= 0.001"}

The models below give the shape. Loading also checks what a shape can't: that
names are unique, each variable's source_cfg (by its source's validate hook),
and that each condition compiles, so a bad config fails before a run is read.
There is no version field while the schema is experimental.
"""

from typing import Annotated, Any

from cel_expr_python import cel
from pydantic import Field, JsonValue, PrivateAttr, TypeAdapter, ValidationError, field_validator, model_validator

from . import cel_env
from .sources import SOURCES
from .sources.base import Model, Name, describe

CelExpression = cel.Expression


class ConfigError(ValueError):
    """The evaluations block is invalid; the message names where."""


# A variable name can't contain "__" or end in "_": stats.jsonl rows key each value as
# <variable>__<value name>, so trial__x with value y and trial with value x__y would collide.
VariableName = Annotated[str, Field(pattern=r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$", max_length=64)]


class Variable(Model):
    name: VariableName
    source: str
    # JSON, as the results echo it: a YAML date or a non-string key is an error here.
    # Validation builds new containers, so later edits to the input don't reach it.
    source_cfg: dict[str, JsonValue] = {}

    @field_validator("source")
    @classmethod
    def _known(cls, source: str) -> str:
        if source not in SOURCES:
            raise ValueError(f"source {source!r} is not one of {sorted(SOURCES)}")
        return source

    @model_validator(mode="after")
    def _source_cfg(self) -> "Variable":
        try:
            SOURCES[self.source].validate(self.source_cfg)
        except ValueError as e:
            raise ValueError(f"source_cfg: {e}") from None
        return self


class Condition(Model):
    name: Name
    cel: str
    _expr: CelExpression = PrivateAttr()  # compiled by the Evaluation, which knows the variables

    @property
    def expr(self) -> CelExpression:
        return self._expr


class Evaluation(Model):
    name: Name
    description: str = ""
    # Lists, not tuples: pydantic counts a tuple's length after dropping the items that failed,
    # so one bad item would also report the tuple as empty.
    variables: list[Variable] = Field(min_length=1)
    conditions: list[Condition] = Field(min_length=1)

    @model_validator(mode="after")
    def _compile(self) -> "Evaluation":
        _unique([v.name for v in self.variables], "variable")
        _unique([c.name for c in self.conditions], "condition")
        env = cel_env.Env({v.name: cel.Type.Map(cel.Type.STRING, cel.Type.DYN) for v in self.variables},
                          functions=(cel_env.ROUND,))
        for c in self.conditions:
            # DYN too, as when a condition is just a value (`trial.ok`); a non-bool
            # result then makes the condition invalid when it runs.
            c._expr = env.compile(c.cel, f"condition {c.name!r}: cel",  # pylint: disable=protected-access
                                  gives=(cel.Type.BOOL, cel.Type.DYN))
        return self


_EVALUATIONS = TypeAdapter(Annotated[list[Evaluation], Field(min_length=1)])


def load(evaluations: Any) -> list[Evaluation]:
    """The evaluations of a test entry, validated; raises ConfigError."""
    try:
        out = _EVALUATIONS.validate_python(evaluations)
        _unique([e.name for e in out], "evaluation")
    except ValidationError as e:
        raise ConfigError(f"evaluations: {describe(e)}") from None
    except ValueError as e:
        raise ConfigError(f"evaluations: {e}") from None
    return out


def to_json(evaluations: list[Evaluation]) -> list[dict[str, Any]]:
    """The evaluations as loaded, defaults filled: what the results echo, and load() reads back."""
    return [e.model_dump(mode="json") for e in evaluations]


def _unique(names: list[str], what: str) -> None:
    if dupes := sorted({n for n in names if names.count(n) > 1}):
        raise ValueError(f"{what} names repeated: {dupes}")
