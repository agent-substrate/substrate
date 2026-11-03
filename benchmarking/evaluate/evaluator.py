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

"""Judges one run against its evaluations: the content of evaluation.json.

Creating an Evaluator runs the sources' init hook: one instance of each source
the evaluations name, given the run, before the run's data exists. evaluate()
runs the query hook once the data is there, once per distinct (source,
source_cfg), and judges the conditions. A condition passes, fails, or is
invalid when a value it reads is missing or its variable couldn't judge the
run. An evaluation fails if any condition failed, else is invalid if any was
invalid, else passes. The run summary combines the evaluations the same way.
"""

import json
from datetime import datetime, timezone
from typing import Any, Mapping, NamedTuple, Optional

from . import checks
from .config import Condition, Evaluation, to_json
from .sources import SOURCES
from .sources.base import BenchmarkRunContext, Scalar, Source, SourceInvalid, valid_name

# The stats rows report an invalid variable as <variable>__invalid_reason.
RESERVED_VALUES = {"invalid_reason"}


class Result(NamedTuple):
    """One query: the values, and why the source couldn't judge the run (None when it could)."""
    values: dict[str, Scalar]
    invalid: Optional[str]


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

    def evaluate(self, identity: Mapping[str, str], now: Optional[datetime] = None) -> dict[str, Any]:
        """evaluation.json for the run; identity (test_name, tag, run_ts, uri...) only labels it."""
        now = now or datetime.now(timezone.utc)
        results: dict[tuple[str, str], Result] = {}
        reports = []
        for ev, echoed in zip(self.evaluations, to_json(self.evaluations)):
            got = {}
            for v in ev.variables:
                key = (v.source, json.dumps(v.source_cfg, sort_keys=True))
                if key not in results:
                    results[key] = self._query(v.source, v.source_cfg)
                got[v.name] = results[key]
            variables = [{"name": v.name, "source": v.source, "status": "invalid" if got[v.name].invalid else "ok",
                          "invalid_reason": got[v.name].invalid, "values": got[v.name].values}
                         for v in ev.variables]
            conditions = [_condition(c, got) for c in ev.conditions]
            status, failed, invalid = _combine(conditions)
            reason = f"failed: {', '.join(failed)}" if failed else f"invalid: {', '.join(invalid)}" if invalid else ""
            reports.append({"name": ev.name, "description": ev.description, "status": status,
                            "passed": status == checks.PASS, "reason": reason, "conditions": conditions,
                            "variables": variables, "config": echoed})
        status, failed, invalid = _combine(reports)
        return {"run": dict(identity), "evaluated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "error": None,
                "summary": {"status": status, "passed": status == checks.PASS, "failed": failed, "invalid": invalid},
                "evaluations": reports}

    def _query(self, name: str, cfg: Mapping[str, Any]) -> Result:
        source = self.sources[name]
        if isinstance(source, str):
            return Result({}, source)
        try:
            values, invalid = dict(source.query(cfg)), None
        except SourceInvalid as e:
            values, invalid = e.values, e.reason
        except Exception as e:  # pylint: disable=broad-except
            return Result({}, f"error: {type(e).__name__}: {e}")
        if bad := _bad_values(values):
            return Result({}, f"error: source {name} returned {bad}")
        return Result(values, invalid)


def error_report(identity: Mapping[str, str], error: str) -> dict[str, Any]:
    """evaluation.json for evaluations that could not be loaded: no verdict, and why."""
    return {"run": dict(identity), "evaluated_at": None, "error": error,
            "summary": {"status": checks.INVALID, "passed": False, "failed": [], "invalid": []}, "evaluations": []}


def _combine(items: list[dict[str, Any]]) -> tuple[str, list[str], list[str]]:
    """fail if any item failed, else invalid if any was invalid, else pass; and the names of each."""
    failed = [i["name"] for i in items if i["status"] == checks.FAIL]
    invalid = [i["name"] for i in items if i["status"] == checks.INVALID]
    return (checks.FAIL if failed else checks.INVALID if invalid else checks.PASS), failed, invalid


def _bad_values(values: dict[str, Any]) -> str:
    """What breaks the values contract (names, reserved names, scalar values); "" when nothing does."""
    if bad := sorted(str(k) for k in values if not valid_name(k)):
        return f"value names that aren't names: {bad}"
    if taken := sorted(RESERVED_VALUES & set(values)):
        return f"reserved value names {taken}"
    if bad := sorted(k for k, v in values.items() if v is not None and not isinstance(v, (bool, int, float, str))):
        return f"values that aren't scalars: {bad}"
    return ""


def _condition(c: Condition, results: dict[str, Result]) -> dict[str, Any]:
    def lookup(ref: str) -> Any:
        var, _, name = ref.partition(".")
        result = results[var]
        if result.invalid:
            raise checks.Missing(f"{var} is invalid: {result.invalid}")
        value = result.values.get(name)
        if value is None:
            raise checks.Missing(f"{ref} not measured")
        return value

    verdict = checks.threshold(c.check, lookup)
    limit = c.check["limit"]
    return {"name": c.name, "type": c.type, "status": verdict.status, "passed": verdict.status == checks.PASS,
            "value_ref": c.check["value"], "value": verdict.value, "op": c.check["op"], "limit": verdict.limit,
            "limit_ref": limit["ref"] if isinstance(limit, dict) else None, "reason": verdict.reason}
