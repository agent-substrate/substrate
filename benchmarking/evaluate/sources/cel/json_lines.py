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

"""json_lines: the lines of one of the run's JSON-lines files, as records of the cel source.

  kind: json_lines
  file: stats.jsonl                          # one of the run's files, by name
  select: 'line.metric.startsWith("grpc_")'  # optional: the lines to judge; all by default
  rules:
  - {name: failed_requests, cel: "line.measurements.failure_count == 0.0"}

A rule reads `line`, the line's JSON object. Its values are coerced as in
run_results: numbers and numeric strings become doubles, "true"/"false" become
bools. A blank line is skipped. A line that isn't a JSON object, or a select or
rule that fails to evaluate on a line (for example on a key the line lacks;
test optional keys with has()), makes the variable invalid.
"""

import json
from typing import Any, Mapping

from cel_expr_python import cel

from ..base import SourceInvalid
from ..run_results import coerce
from . import rules
from .kind import Kind, Record, Records, Summary

ENV = rules.Env({"line": cel.Type.Map(cel.Type.STRING, cel.Type.DYN)})


def coerce_json(v: Any) -> Any:
    """A JSON value with coerce applied to each leaf."""
    if isinstance(v, dict):
        return {k: coerce_json(x) for k, x in v.items()}
    if isinstance(v, list):
        return [coerce_json(x) for x in v]
    return coerce(v)


class JsonLines(Kind):
    ENV = ENV
    CFG_KEYS = frozenset({"file", "select"})

    @classmethod
    def validate(cls, cfg: Mapping[str, Any]) -> None:
        if not isinstance(cfg.get("file"), str) or not cfg["file"]:
            raise ValueError("needs file, the name of one of the run's files")
        if "select" in cfg:
            ENV.compile(cfg["select"], "select")

    def records(self, cfg: Mapping[str, Any]) -> Records:
        name = cfg["file"]
        path = self.run.file(name)
        if path is None:
            raise SourceInvalid(f"{name} absent")
        select = ENV.compile(cfg["select"], "select")[0] if "select" in cfg else None
        found = []
        with open(path, encoding="utf-8") as f:
            for n, text in enumerate(f, 1):
                if not text.strip():
                    continue
                where = f"{name}:{n}"
                try:
                    line = json.loads(text)
                except ValueError as e:
                    raise SourceInvalid(f"{where}: not JSON: {e}") from None
                if not isinstance(line, dict):
                    raise SourceInvalid(f"{where}: not a JSON object")
                data = {"line": coerce_json(line)}
                if select is None or rules.holds(select, data, f"select on {where}"):
                    found.append(Record(where=where, data=data))
        return Records(found)

    def summary(self, cfg: Mapping[str, Any], found: Records, compiled: list[rules.Rule],
                reasons: list[list[str]]) -> Summary:
        return Summary(invalid="" if found.records else f"no line of {cfg['file']} selected")
