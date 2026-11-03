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

"""The `evaluations:` block of a test entry: loading and validation.

  evaluations:
  - name: envelope_health              # unique in the test; "summary" is reserved
    description: "..."                 # optional
    variables:                         # each is one source query, with several named values
    - {name: trial, source: run_results, source_cfg: {...}}
    conditions:                        # AND-ed
    - name: failure_ratio
      type: threshold                  # the only check type (see checks.py)
      check: {value: trial.aggregate_failure_ratio, op: "<=", limit: 0.001}

Everything is checked up front, source_cfg included (by its source's validate
hook), so a typo fails before a run is read rather than passing it. Unknown
keys are errors. There is no version field while the schema is experimental.
"""

import json
from dataclasses import dataclass
from typing import Any, Mapping

from . import checks
from .sources import SOURCES
from .sources.base import NAME, NAME_MAX, valid_name

TYPES = ("threshold",)
EVALUATION_KEYS = {"name", "description", "variables", "conditions"}
VARIABLE_KEYS = {"name", "source", "source_cfg"}
CONDITION_KEYS = {"name", "type", "check"}
# Evaluation names the outputs use for themselves.
RESERVED_NAMES = {"summary"}


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
    type: str
    check: Mapping[str, Any]


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
    """The evaluations as loaded, defaults filled: what evaluation.json echoes, and load() reads back."""
    return [{"name": e.name, "description": e.description,
             "variables": [{"name": v.name, "source": v.source, "source_cfg": dict(v.source_cfg)}
                           for v in e.variables],
             "conditions": [{"name": c.name, "type": c.type, "check": dict(c.check)} for c in e.conditions]}
            for e in evaluations]


def _name(obj: Mapping[str, Any], where: str) -> str:
    name = obj.get("name")
    if not valid_name(name):
        raise ConfigError(f"{where}: name {name!r} must match {NAME.pattern}, without '__', "
                          f"at most {NAME_MAX} characters")
    return name


def _keys(obj: Any, allowed: set[str], where: str) -> None:
    if not isinstance(obj, Mapping):
        raise ConfigError(f"{where}: not a mapping")
    if unknown := set(obj) - allowed:
        raise ConfigError(f"{where}: unknown keys {sorted(unknown)}")


def _unique(names: list[str], what: str, where: str) -> None:
    if dupes := sorted({n for n in names if names.count(n) > 1}):
        raise ConfigError(f"{where}: {what} names repeated: {dupes}")


def _canonical(obj: Mapping[str, Any]) -> Mapping[str, Any]:
    """A JSON copy: what the outputs echo, safe from later edits to the input."""
    return json.loads(json.dumps(obj, sort_keys=True))


def _evaluation(e: Any, where: str) -> Evaluation:
    _keys(e, EVALUATION_KEYS, where)
    name = _name(e, where)
    where = f"evaluation {name!r}"
    if name in RESERVED_NAMES:
        raise ConfigError(f"{where}: the name is reserved for the outputs")
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
    known = {v.name for v in variables}
    conditions = tuple(_condition(c, f"{where} conditions[{i}]", known) for i, c in enumerate(raw_conds))
    _unique([c.name for c in conditions], "condition", where)
    return Evaluation(name, description, variables, conditions)


def _variable(v: Any, where: str) -> Variable:
    _keys(v, VARIABLE_KEYS, where)
    name = _name(v, where)
    where = f"{where.rsplit(' ', 1)[0]} variable {name!r}"
    source = v.get("source")
    if not isinstance(source, str) or source not in SOURCES:
        raise ConfigError(f"{where}: source {source!r} is not one of {sorted(SOURCES)}")
    cfg = v.get("source_cfg")
    cfg = {} if cfg is None else cfg
    if not isinstance(cfg, Mapping):
        raise ConfigError(f"{where}: source_cfg must be a mapping")
    try:
        SOURCES[source].validate(cfg)
    except ValueError as e:
        raise ConfigError(f"{where}: source_cfg: {e}") from None
    return Variable(name, source, _canonical(cfg))


def _condition(c: Any, where: str, variables: set[str]) -> Condition:
    _keys(c, CONDITION_KEYS, where)
    name = _name(c, where)
    where = f"{where.rsplit(' ', 1)[0]} condition {name!r}"
    ctype = c.get("type")
    if not isinstance(ctype, str) or ctype not in TYPES:
        raise ConfigError(f"{where}: type {ctype!r} is not one of {list(TYPES)}")
    if "check" not in c:
        raise ConfigError(f"{where}: needs check")

    def ref_ok(ref: str) -> str | None:
        parts = ref.split(".")
        if len(parts) != 2 or not all(parts):
            return f"{ref!r} is not <variable>.<value>"
        if parts[0] not in variables:
            return f"{ref!r} names no variable of this evaluation ({', '.join(sorted(variables))})"
        if not valid_name(parts[1]):
            return f"{ref!r}: {parts[1]!r} is not a value name"
        return None

    try:
        checks.validate(c["check"], ref_ok)
    except ValueError as e:
        raise ConfigError(f"{where}: {e}") from None
    return Condition(name, ctype, _canonical(c["check"]))
