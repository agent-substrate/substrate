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
    records: sweperf_tasks      # the kind of record (KINDS), with its own keys
    rules:                      # required, may be []
    - {name: slow_resume, cel: "task.resume_max_ms <= 1500.0"}
    values:                     # optional: more values, from the counts and the kind's facts
      good_ratio_pct: "round(100.0 * good / records, 2)"

A record is good when the kind gives it no failure of its own (a SWE-Perf
task that failed) and it breaks no rule. A rule's name is the reason a record
that breaks it is bad. A rule that reads a per-run variable the run lacks (a
baseline value) is skipped, and one that reads a field a record lacks doesn't
judge that record. A rule that fails to evaluate makes the variable invalid.

The source gives records, good, bad, bad_<reason> for every rule and reason,
rules_applied, rules_skipped, the kind's own values, and those of `values:`.
A value is null when it reads a fact the run lacks, or isn't finite (x / 0).
"""

from collections import Counter
from typing import Any, Mapping

from ..base import BenchmarkRunContext, Scalar, Source, SourceInvalid
from . import rules
from .json_lines import JsonLines
from .kind import COUNTS, Kind, Record
from .sweperf import SweperfTasks

KINDS: dict[str, type[Kind]] = {"json_lines": JsonLines, "sweperf_tasks": SweperfTasks}
CFG_KEYS = {"records", "rules", "values"}


def judge(record: Record, compiled: list[rules.Rule], run_data: Mapping[str, Any]) -> list[str]:
    """Why record is bad: its own failure, else the rules it breaks. [] when it is good."""
    if record.failure:
        return [record.failure]
    return rules.broken(compiled, {**record.data, **run_data}, record.where)


def skipped_rules(compiled: list[rules.Rule], kind: type[Kind], run_data: Mapping[str, Any]) -> list[str]:
    """The names of the rules that read a per-run variable the run lacks."""
    return [r.name for r in compiled if any(v in kind.RUN_VARIABLES and v not in run_data for v in r.reads)]


def compile_cfg(cfg: Mapping[str, Any], kind: type[Kind]) -> tuple[list[rules.Rule], list[rules.Value]]:
    """The rules and values of cfg, compiled. Raises ValueError for a bad one."""
    compiled = rules.compile_rules(cfg["rules"], kind.ENV, kind.FAILURE_REASONS)
    taken = {*COUNTS, "rules_applied", "rules_skipped", *kind.value_names(compiled)}
    return compiled, rules.compile_values(cfg.get("values", {}), kind.VALUES_ENV, taken)


class Cel(Source):
    @classmethod
    def validate(cls, cfg: Mapping[str, Any]) -> None:
        kind = KINDS.get(cfg.get("records")) if isinstance(cfg.get("records"), str) else None
        if kind is None:
            raise ValueError(f"records: {cfg.get('records')!r} is not one of {sorted(KINDS)}")
        if unknown := set(cfg) - CFG_KEYS - kind.CFG_KEYS:
            raise ValueError(f"unknown keys {sorted(unknown)}")
        if "rules" not in cfg:
            raise ValueError("needs rules (a list, may be empty)")
        compile_cfg(cfg, kind)
        kind.validate(cfg)

    def init(self, run: BenchmarkRunContext) -> None:
        super().init(run)
        self._kinds: dict[str, Kind] = {}

    def query(self, cfg: Mapping[str, Any]) -> Mapping[str, Scalar]:
        kind = self._kind(cfg["records"])
        compiled, defined = compile_cfg(cfg, type(kind))
        found = kind.records(cfg)
        skipped = skipped_rules(compiled, type(kind), found.run_data)
        reasons = [judge(r, compiled, found.run_data) for r in found.records]
        good = sum(1 for r in reasons if not r)

        # The keys don't change from run to run: every rule and reason of the
        # kind has a bad_<name> count, 0 when no record had it, null for a
        # skipped rule.
        values: dict[str, Scalar] = {"records": len(reasons), "good": good, "bad": len(reasons) - good}
        values.update({f"bad_{reason}": 0 for reason in kind.FAILURE_REASONS})
        values.update({f"bad_{r.name}": None if r.name in skipped else 0 for r in compiled})
        values.update({f"bad_{reason}": n for reason, n in sorted(Counter(x for r in reasons for x in r).items())})
        values["rules_applied"] = ",".join(r.name for r in compiled if r.name not in skipped)
        values["rules_skipped"] = ",".join(skipped)
        summary = kind.summary(cfg, found, compiled, reasons, skipped)
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
