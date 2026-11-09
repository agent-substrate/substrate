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

"""Judges one run against its evaluations, into a Report.

Creating an Evaluator runs the sources' init hook: one instance of each source
the evaluations name, given the run, before the run's data exists. evaluate()
runs the query hook once the data is there, once per distinct (source,
source_cfg), and judges the CEL conditions. A condition passes, fails, or is
invalid when a value it reads is missing or its variable couldn't judge the
run. An evaluation fails if any condition failed, else is invalid if any was
invalid, else passes. Every variable is read by a condition (config.py), so an
evaluation with an invalid variable never passes. The run summary combines the
evaluations the same way.
"""

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final, Literal, Mapping, NamedTuple, Optional, Sequence

from . import cel_env
from .config import Condition, Evaluation, to_json
from .sources import SOURCES
from .sources.base import BenchmarkRunContext, Scalar, Source, SourceInvalid

Status = Literal["pass", "fail", "invalid"]
PASS: Final = "pass"
FAIL: Final = "fail"
INVALID: Final = "invalid"


@dataclass(frozen=True)
class ConditionResult:
    name: str
    cel: str
    status: Status
    passed: bool
    reason: str  # why it failed or is invalid, "" when it passed


@dataclass(frozen=True)
class VariableResult:
    name: str
    source: str
    status: Literal["ok", "invalid"]
    invalid_reason: Optional[str]  # None when the source resolved the values
    values: Mapping[str, Scalar]


@dataclass(frozen=True)
class EvaluationResult:
    name: str
    description: str
    status: Status
    passed: bool
    reason: str  # the conditions that failed, else those that are invalid; "" when it passed
    conditions: tuple[ConditionResult, ...]
    variables: tuple[VariableResult, ...]
    config: Mapping[str, Any]  # the evaluation as loaded (config.to_json)


@dataclass(frozen=True)
class Summary:
    status: Status
    passed: bool
    failed: tuple[str, ...]  # the evaluations that failed
    invalid: tuple[str, ...]  # the evaluations that are invalid


@dataclass(frozen=True)
class Report:
    """A run judged against its evaluations."""
    run: Mapping[str, str]  # the run's identity (test_name, tag, run_ts, uri...), which only labels it
    evaluated_at: str
    summary: Summary
    evaluations: tuple[EvaluationResult, ...]


class ResolvedValues(NamedTuple):
    """A variable's values, and why its source couldn't resolve them (None when it could)."""
    values: dict[str, Scalar]
    invalid_reason: Optional[str]


class Evaluator:
    def __init__(self, evaluations: list[Evaluation], run: BenchmarkRunContext):
        self.evaluations = evaluations
        # Each source the evaluations name, or why its init failed.
        self.sources: dict[str, Source | str] = {}
        for name in dict.fromkeys(v.source for e in evaluations for v in e.variables):
            try:
                source = SOURCES[name]()
                source.init(run)
                self.sources[name] = source
            except Exception as e:  # pylint: disable=broad-except  # a source bug must not cost the others
                self.sources[name] = f"init failed: {type(e).__name__}: {e}"

    def evaluate(self, identity: Mapping[str, str], now: Optional[datetime] = None) -> Report:
        """The run's Report; identity (test_name, tag, run_ts, uri...) only labels it."""
        now = now or datetime.now(timezone.utc)
        # Each distinct source query runs once per run, and every variable that makes it shares the values.
        resolved_queries: dict[tuple[str, str], ResolvedValues] = {}
        evaluation_results = []
        for ev, echoed in zip(self.evaluations, to_json(self.evaluations)):
            resolved_variables = {}  # by variable name
            variables = []
            for v in ev.variables:
                key = (v.source, json.dumps(v.source_cfg, sort_keys=True))
                if key not in resolved_queries:
                    resolved_queries[key] = self._resolve_variable(v.source, v.source_cfg)
                r = resolved_variables[v.name] = resolved_queries[key]
                variables.append(VariableResult(name=v.name, source=v.source,
                                                status="ok" if r.invalid_reason is None else INVALID,
                                                invalid_reason=r.invalid_reason, values=r.values))
            conditions = tuple(_condition(c, resolved_variables) for c in ev.conditions)
            status, failed, invalid = _combine(conditions)
            reason = f"failed: {', '.join(failed)}" if failed else f"invalid: {', '.join(invalid)}" if invalid else ""
            evaluation_results.append(EvaluationResult(
                name=ev.name, description=ev.description, status=status, passed=status == PASS, reason=reason,
                conditions=conditions, variables=tuple(variables), config=echoed))
        status, failed, invalid = _combine(evaluation_results)
        return Report(run=dict(identity), evaluated_at=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                      summary=Summary(status=status, passed=status == PASS, failed=failed, invalid=invalid),
                      evaluations=tuple(evaluation_results))

    def _resolve_variable(self, source_name: str, cfg: Mapping[str, Any]) -> ResolvedValues:
        """The values of one source query: scalars under non-empty string
        names. A float that isn't finite becomes None. Anything else makes the
        variable invalid."""
        source = self.sources[source_name]
        if isinstance(source, str):
            return ResolvedValues({}, source)
        try:
            raw, invalid_reason = dict(source.query(cfg)), None
        except SourceInvalid as e:
            raw, invalid_reason = dict(e.values), e.reason
        except Exception as e:  # pylint: disable=broad-except
            return ResolvedValues({}, f"error: {type(e).__name__}: {e}")
        for k, v in raw.items():
            if not isinstance(k, str) or not k:
                return ResolvedValues({}, f"value name {k!r} must be a non-empty string")
            if not isinstance(v, Scalar):
                return ResolvedValues({}, f"value {k!r} is a {type(v).__name__}: a value must be a number, "
                                          "bool, string or null")
        values = {k: None if isinstance(v, float) and not math.isfinite(v) else v for k, v in raw.items()}
        return ResolvedValues(values, invalid_reason)


def _combine(results: Sequence[ConditionResult | EvaluationResult]) -> tuple[Status, tuple[str, ...], tuple[str, ...]]:
    """fail if any result failed, else invalid if any is invalid, else pass; and the names of each."""
    failed = tuple(r.name for r in results if r.status == FAIL)
    invalid = tuple(r.name for r in results if r.status == INVALID)
    return (FAIL if failed else INVALID if invalid else PASS), failed, invalid


def _condition(c: Condition, resolved_variables: Mapping[str, ResolvedValues]) -> ConditionResult:
    data = {}
    for var in c.reads:
        resolved = resolved_variables[var]
        if resolved.invalid_reason is not None:
            return ConditionResult(name=c.name, cel=c.cel, status=INVALID, passed=False,
                                   reason=f"{var} is invalid: {resolved.invalid_reason}")
        data[var] = cel_env.to_cel(resolved.values)
    try:
        passed = cel_env.holds(c.expr, data)
    except ValueError as e:
        return ConditionResult(name=c.name, cel=c.cel, status=INVALID, passed=False, reason=f"{c.name}: {e}")
    return ConditionResult(name=c.name, cel=c.cel, status=PASS if passed else FAIL, passed=passed,
                           reason="" if passed else f"{c.name}: {c.cel}")
