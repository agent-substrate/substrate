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

"""The cel source: judges each record of a run with CEL rules, and counts the good ones.

  source_cfg:
    kind: sweperf_tasks         # the kind of record (KINDS), with its own keys
    rules:                      # required, may be []
    - {name: slow_resume, cel: "task.resume_max_ms <= 1500.0"}
    values:                     # optional: more values, from the counts and the kind's facts
      good_ratio_pct: "round(100.0 * good / records, 2)"

A record is good when the kind gives it no failure of its own (a SWE-Perf
task that failed) and it breaks no rule. A rule's name is the reason a record
that breaks it is bad. A rule that can't judge makes the run invalid, with no
values: it reads a variable the run lacks (a baseline value) or a record lacks
(a task field), or it fails to evaluate.

The source gives records, good, bad, bad_<reason> for every rule and reason,
the kind's own values, and those of `values:` (value_names). A value is null
when it reads a fact the run lacks, or isn't finite (x / 0).
"""

from collections import Counter
from typing import Any, Mapping, Optional

from ..base import BenchmarkRunContext, Scalar, Source, SourceInvalid
from . import rules
from .json_lines import JsonLines
from .kind import COUNTS, Kind, Record
from .sweperf import SweperfTasks

KINDS: dict[str, type[Kind]] = {"json_lines": JsonLines, "sweperf_tasks": SweperfTasks}
CFG_KEYS = {"kind", "rules", "values"}


def judge(record: Record, compiled: list[rules.Rule], run_data: Mapping[str, Any]) -> list[str]:
    """Why record is bad: its own failure, else the rules it breaks. [] when it is good."""
    if record.failure:
        return [record.failure]
    return rules.broken(compiled, {**record.data, **run_data}, record.where)


def compile_cfg(cfg: Mapping[str, Any], kind: type[Kind]) -> tuple[list[rules.Rule], list[rules.Value]]:
    """The rules and values of cfg, compiled. Raises ValueError for a bad one."""
    compiled = rules.compile_rules(cfg["rules"], kind.ENV, kind.FAILURE_REASONS)
    taken = {*COUNTS, *kind.value_names(compiled)}
    return compiled, rules.compile_values(cfg.get("values", {}), kind.VALUES_ENV, taken)


class Cel(Source):
    @classmethod
    def validate(cls, cfg: Mapping[str, Any]) -> None:
        kind = KINDS.get(cfg.get("kind")) if isinstance(cfg.get("kind"), str) else None
        if kind is None:
            raise ValueError(f"kind: {cfg.get('kind')!r} is not one of {sorted(KINDS)}")
        if unknown := set(cfg) - CFG_KEYS - kind.CFG_KEYS:
            raise ValueError(f"unknown keys {sorted(unknown, key=repr)}")
        if "rules" not in cfg:
            raise ValueError("needs rules (a list, may be empty)")
        compile_cfg(cfg, kind)
        kind.validate(cfg)

    @classmethod
    def value_names(cls, cfg: Mapping[str, Any]) -> Optional[set[str]]:
        kind = KINDS[cfg["kind"]]
        compiled, defined = compile_cfg(cfg, kind)
        bad = (*kind.FAILURE_REASONS, *(r.name for r in compiled))
        return {*COUNTS, *(f"bad_{name}" for name in bad), *kind.value_names(compiled), *(v.name for v in defined)}

    def init(self, run: BenchmarkRunContext) -> None:
        super().init(run)
        self._kinds: dict[str, Kind] = {}

    def query(self, cfg: Mapping[str, Any]) -> Mapping[str, Scalar]:
        kind = self._kind(cfg["kind"])
        compiled, defined = compile_cfg(cfg, type(kind))
        found = kind.records(cfg)
        for r in compiled:
            if lacked := next((v for v in r.reads if v in kind.RUN_VARIABLES and v not in found.run_data), None):
                why = f" ({found.run_note})" if found.run_note else ""
                raise SourceInvalid(f"rule {r.name!r} reads {lacked}, which the run lacks{why}")
        reasons = [judge(r, compiled, found.run_data) for r in found.records]
        good = sum(1 for r in reasons if not r)

        # The keys don't change from run to run: every rule and reason of the
        # kind has a bad_<name> count, 0 when no record had it.
        values: dict[str, Scalar] = {"records": len(reasons), "good": good, "bad": len(reasons) - good}
        values.update({f"bad_{name}": 0 for name in (*kind.FAILURE_REASONS, *(r.name for r in compiled))})
        values.update({f"bad_{reason}": n for reason, n in sorted(Counter(x for r in reasons for x in r).items())})
        summary = kind.summary(cfg, found, compiled, reasons)
        values.update(summary.values)
        data = {k: float(values[k]) for k in COUNTS}
        data.update({k: float(v) for k, v in summary.facts.items() if v is not None})
        values.update({v.name: rules.evaluate(v, data) for v in defined})
        if summary.invalid:
            raise SourceInvalid(summary.invalid, values)
        return values

    def _kind(self, name: str) -> Kind:
        if name not in self._kinds:
            self._kinds[name] = KINDS[name](self.run)
        return self._kinds[name]
