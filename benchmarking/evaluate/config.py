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

Everything is checked up front, source_cfg and CEL conditions included, so a
bad config fails before a run is read. Unknown or non-string keys are errors.
There is no version field while the schema is experimental.
"""

import json
from dataclasses import dataclass
from typing import Any, Mapping

from cel_expr_python import cel

from . import cel_env
from .sources import SOURCES
from .sources.base import NAME, NAME_MAX, check_mapping, valid_name

class ConfigError(ValueError):
    """The evaluations block is invalid; the message names where."""


@dataclass(frozen=True)
class Variable:
    name: str
    source: str
    source_cfg: Mapping[str, Any]


@dataclass(frozen=True)
class Condition:
    name: str
    cel: str
    expr: cel.Expression
    reads: tuple[str, ...]


@dataclass(frozen=True)
class Evaluation:
    name: str
    description: str
    variables: tuple[Variable, ...]
    conditions: tuple[Condition, ...]


def load(evaluations: Any) -> list[Evaluation]:
    """The evaluations of a test entry, validated; raises ConfigError."""
    if not isinstance(evaluations, list) or not evaluations:
        raise ConfigError("evaluations: needs a non-empty list (no evaluations would pass vacuously)")
    out = [_evaluation(e, f"evaluations[{i}]") for i, e in enumerate(evaluations)]
    _unique([e.name for e in out], "evaluation", "evaluations")
    return out


def to_json(evaluations: list[Evaluation]) -> list[dict[str, Any]]:
    """The evaluations as loaded, defaults filled: what the results echo, and load() reads back."""
    return [{"name": e.name, "description": e.description,
             "variables": [{"name": v.name, "source": v.source, "source_cfg": dict(v.source_cfg)}
                           for v in e.variables],
             "conditions": [{"name": c.name, "cel": c.cel} for c in e.conditions]}
            for e in evaluations]


def _map(obj: Any, *, required: tuple[str, ...] = (), optional: tuple[str, ...] = (), where: str) -> Mapping[str, Any]:
    try:
        return check_mapping(obj, required=required, optional=optional, where=where)
    except ValueError as e:
        raise ConfigError(str(e)) from None


def _name(obj: Mapping[str, Any], where: str) -> str:
    name = obj.get("name")
    if not valid_name(name):
        raise ConfigError(f"{where}: name {name!r} must match {NAME.pattern}, at most {NAME_MAX} characters")
    return name


def _unique(names: list[str], what: str, where: str) -> None:
    if dupes := sorted({n for n in names if names.count(n) > 1}):
        raise ConfigError(f"{where}: {what} names repeated: {dupes}")


def _canonical(obj: Mapping[str, Any]) -> Mapping[str, Any]:
    """A JSON copy: what the results echo, safe from later edits to the input."""
    return json.loads(json.dumps(obj, sort_keys=True))


def _evaluation(e: Any, where: str) -> Evaluation:
    e = _map(e, required=("name",), optional=("description", "variables", "conditions"), where=where)
    name = _name(e, where)
    where = f"evaluation {name!r}"
    description = e.get("description", "")
    if not isinstance(description, str):
        raise ConfigError(f"{where}: description must be a string")
    raw_vars, raw_conds = e.get("variables"), e.get("conditions")
    if not isinstance(raw_vars, list) or not raw_vars:
        raise ConfigError(f"{where}: needs variables, a non-empty list")
    if not isinstance(raw_conds, list) or not raw_conds:
        raise ConfigError(f"{where}: needs conditions, a non-empty list")
    variables = tuple(_variable(v, f"{where} variables[{i}]") for i, v in enumerate(raw_vars))
    _unique([v.name for v in variables], "variable", where)
    try:
        env = cel_env.Env(
            {v.name: cel.Type.Map(cel.Type.STRING, cel.Type.DYN) for v in variables},
            functions=(cel_env.ROUND,),
            ints_as_doubles=True,
        )
    except ValueError as e:
        raise ConfigError(f"{where}: {e}") from None
    conditions = tuple(_condition(c, f"{where} conditions[{i}]", env) for i, c in enumerate(raw_conds))
    _unique([c.name for c in conditions], "condition", where)
    read = {var for c in conditions for var in c.reads}
    if unread := [v.name for v in variables if v.name not in read]:
        raise ConfigError(f"{where}: every variable must be read by a condition; unread {unread}")
    return Evaluation(name, description, variables, conditions)


def _variable(v: Any, where: str) -> Variable:
    v = _map(v, required=("name",), optional=("source", "source_cfg"), where=where)
    name = _name(v, where)
    where = f"{where.rsplit(' ', 1)[0]} variable {name!r}"
    if name in cel_env.RESERVED:
        raise ConfigError(f"{where}: {name!r} is reserved in CEL")
    source = v.get("source")
    if not isinstance(source, str) or source not in SOURCES:
        raise ConfigError(f"{where}: source {source!r} is not one of {sorted(SOURCES)}")
    cfg = v.get("source_cfg")
    cfg = {} if cfg is None else cfg
    if not isinstance(cfg, Mapping):
        raise ConfigError(f"{where}: source_cfg must be a mapping")
    if bad := [k for k in cfg if not isinstance(k, str)]:
        raise ConfigError(f"{where}: source_cfg: non-string keys {sorted(bad, key=repr)}")
    try:
        SOURCES[source].validate(cfg)
    except ValueError as e:
        raise ConfigError(f"{where}: source_cfg: {e}") from None
    return Variable(name, source, _canonical(cfg))


def _condition(c: Any, where: str, env: cel_env.Env) -> Condition:
    c = _map(c, required=("name",), optional=("cel",), where=where)
    name = _name(c, where)
    where = f"{where.rsplit(' ', 1)[0]} condition {name!r}"
    if "cel" not in c:
        raise ConfigError(f"{where}: needs 'cel'")
    try:
        expr, reads = env.compile(c["cel"], f"{where}: cel")
    except ValueError as e:
        raise ConfigError(str(e)) from None
    if not reads:
        raise ConfigError(f"{where}: cel must read at least one variable")
    return Condition(name, c["cel"], expr, reads)
