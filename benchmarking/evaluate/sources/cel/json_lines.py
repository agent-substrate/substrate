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

"""json_lines: filtered lines of one of the run's JSON-lines or prefixed-JSON log files.

  stats: {reader: json_lines, file: stats.jsonl}
  records:
    reader: json_lines
    file: logs.txt
    prefix: "[boomer] "
    until: "has(line.msg) && line.msg == 'shutdown'"
    select: "has(line.msg) && line.msg == 'record'"
    unique_by: actor

  <input>.lines           selected JSON objects, in file order
  <input>.unparsed_lines  [{line_no, text, error}] for prefixed (or non-blank) lines that aren't JSON objects
  <input>.uri             where the run is stored

Options besides `file`:
- `prefix`: skip lines without this prefix, and strip it before parsing JSON.
  Without `prefix`, blank lines are skipped.
- `until`: CEL boolean expression over `line`; stop reading when a parsed
  object matches (that line is not emitted).
- `select`: CEL boolean expression over `line`; keep only matching objects.
- `unique_by`: field name; keep only the first selected object per distinct
  value of that field.

Values are coerced as in run_results: numbers and numeric strings become
doubles, "true"/"false" become bools, and a null or non-finite number is
left out, so has() tests for it.
"""

import json
from typing import Annotated, Any, Mapping, Optional

import pydantic
from cel_expr_python import cel
from cel_expr_python.ext import ext_math

from ... import cel_env
from ..base import BenchmarkRunContext, Model, parse
from ..run_results import coerce
from .reader import Field, Reader, ReadError, to_data


def coerce_json(v: Any) -> Any:
    """A JSON value with coerce applied to each leaf."""
    if isinstance(v, dict):
        return {k: coerce_json(x) for k, x in v.items()}
    if isinstance(v, list):
        return [coerce_json(x) for x in v]
    return coerce(v)


def _line_env() -> cel_env.Env:
    return cel_env.Env(
        {"line": cel.Type.Map(cel.Type.STRING, cel.Type.DYN)},
        extensions=(ext_math.ExtMath(),),
    )


def _compile_line_expr(raw: str, where: str) -> cel.Expression:
    return _line_env().compile(raw, where, (cel.Type.BOOL, cel.Type.DYN))


def _eval_line_bool(expr: cel.Expression, line: Mapping[str, Any], line_no: int, where: str) -> bool:
    try:
        result = expr.eval(data={"line": line})
    except RuntimeError as e:
        raise ReadError(f"line {line_no}: {where} failed: {e}") from None
    if result.type() != cel.Type.BOOL:
        detail = result.value() if result.type() == cel.Type.ERROR else f"gave {result.type().name()}"
        raise ReadError(f"line {line_no}: {where} failed: {detail}")
    return result.value()


NonEmpty = Annotated[str, pydantic.Field(min_length=1)]


class Options(Model):
    file: NonEmpty  # the name of one of the run's files
    prefix: str = ""
    select: Optional[str] = None
    until: Optional[str] = None
    unique_by: Optional[NonEmpty] = None  # a field name


class JsonLines(Reader):
    fields = (Field("lines", "list"), Field("unparsed_lines", "list"), Field("uri", "string"))

    @classmethod
    def validate(cls, options: Mapping[str, Any]) -> None:
        o = parse(Options, options)
        for key, expression in (("select", o.select), ("until", o.until)):
            if expression is not None:
                _compile_line_expr(expression, key)

    def read(self, run: BenchmarkRunContext, options: Mapping[str, Any]) -> Mapping[str, Any]:
        o = parse(Options, options)
        name, prefix, unique_by = o.file, o.prefix, o.unique_by
        path = run.artifact(name)
        if path is None:
            raise ReadError(f"{name} absent")
        until_expr = None if o.until is None else _compile_line_expr(o.until, "until")
        select_expr = None if o.select is None else _compile_line_expr(o.select, "select")

        lines, unparsed = [], []
        seen: set[tuple[type, Any]] = set()
        with open(path, encoding="utf-8", errors="replace") as f:
            for n, text in enumerate(f, 1):
                raw = text.rstrip("\n")
                if prefix:
                    if not raw.startswith(prefix):
                        continue
                    payload = raw[len(prefix):]
                else:
                    if not raw.strip():
                        continue
                    payload = raw
                try:
                    parsed = json.loads(payload)
                    error = "" if isinstance(parsed, dict) else "not a JSON object"
                except ValueError as e:
                    error = f"not JSON: {e}"
                if error:
                    unparsed.append({"line_no": n, "text": raw, "error": error})
                    continue
                coerced = to_data(coerce_json(parsed))
                if until_expr is not None and _eval_line_bool(until_expr, coerced, n, "until"):
                    break
                if select_expr is not None and not _eval_line_bool(select_expr, coerced, n, "select"):
                    continue
                if unique_by is not None:
                    if unique_by not in coerced:
                        raise ReadError(f"line {n}: unique_by {unique_by!r}: field missing")
                    val = coerced[unique_by]
                    if isinstance(val, (dict, list)):
                        raise ReadError(f"line {n}: unique_by {unique_by!r}: unhashable {type(val).__name__}")
                    dedup = (type(val), val)
                    if dedup in seen:
                        continue
                    seen.add(dedup)
                lines.append(coerced)
        return to_data({"lines": lines, "unparsed_lines": unparsed, "uri": run.identity.uri})
