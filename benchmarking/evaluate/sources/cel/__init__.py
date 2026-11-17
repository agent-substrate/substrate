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

"""The cel source: binds data read from runs to CEL, and evaluates named expressions into values.

  source_cfg:
    inputs:                        # the data: each is a CEL variable
      - {name: stats, reader: json_lines, reader_cfg: {file: stats.jsonl}}
      - {name: other, from_test: some_other_test, reader: json_lines, reader_cfg: {file: stats.jsonl}}
    locals:                        # optional: ordered list of named in-between results
      - {name: rows, cel: "stats.lines.filter(l, l.metric == 'trial_summary')"}
    checks:                        # optional: what must hold for the values to mean anything
      - {name: has_rows, cel: "size(rows) > 0"}
    values:                        # the outputs: each a double, bool, string or null
      - {name: cores, cel: "size(rows) > 0 ? dyn(rows[size(rows) - 1].measurements.allocatable_cores) : null"}

An input names a reader (READERS) and its reader_cfg. By default it reads this
run; with `from_test:` it reads that test's run from the same invocation
(BenchmarkRunContext.run_of). Locals are evaluated top to bottom, each
reading inputs and earlier locals by name. The source knows nothing about what
the data means: what is good, which run to compare against and every formula
live in the config.

At run time, an input that can't be read, a local, check or value that fails
to evaluate, and a check that is false make the variable invalid, and the
reason names which. A value that isn't finite becomes null.
"""

import json
import math
from dataclasses import dataclass
from typing import Annotated, Any, Mapping, Optional

from cel_expr_python import cel
from cel_expr_python.ext import ext_math
from pydantic import Field, JsonValue, field_validator, model_validator

from ... import cel_env
from ..base import BenchmarkRunContext, Model, Name, Scalar, Source, SourceInvalid, parse
from .json_file import Json
from .json_lines import JsonLines
from .reader import Reader, ReadError, check_data

READERS: dict[str, type[Reader]] = {"json": Json, "json_lines": JsonLines}
VALUE_TYPES = (cel.Type.DOUBLE, cel.Type.BOOL, cel.Type.STRING, cel.Type.NULL)
FUNCTIONS = (cel_env.ROUND, cel_env.PERCENTILE)
EXTENSIONS = (ext_math.ExtMath(),)


class Input(Model):
    """An input: its name, the test whose run it reads (None: this run), a reader and the reader's config."""
    name: Name
    from_test: Optional[Annotated[str, Field(min_length=1)]] = None
    reader: str
    reader_cfg: dict[str, JsonValue] = {}

    @field_validator("reader")
    @classmethod
    def _known(cls, reader: str) -> str:
        if reader not in READERS:
            raise ValueError(f"reader {reader!r} is not one of {sorted(READERS)}")
        return reader

    @model_validator(mode="after")
    def _reader_cfg(self) -> "Input":
        try:
            parse(READERS[self.reader].Config, self.reader_cfg)
        except ValueError as e:
            raise ValueError(f"reader_cfg: {e}") from None
        return self


class NamedCel(Model):
    """A local, check or value: a name and a CEL expression."""
    name: Name
    cel: str


class Config(Model):
    # Lists, not tuples, as in config.Evaluation.
    inputs: list[Input] = Field(min_length=1)
    locals: list[NamedCel] = []
    checks: list[NamedCel] = []
    values: list[NamedCel] = Field(min_length=1)


@dataclass(frozen=True)
class Expression:
    kind: str  # "local", "check" or "value", for error messages
    name: str
    expr: cel.Expression


@dataclass(frozen=True)
class Program:
    """A source_cfg, compiled: the inputs, the locals in list order, the checks and the values."""
    inputs: tuple[Input, ...]
    locals: tuple[Expression, ...]
    checks: tuple[Expression, ...]
    values: tuple[Expression, ...]


def _unique(names: list[str], where: str) -> None:
    if repeated := sorted({n for n in names if names.count(n) > 1}):
        raise ValueError(f"{where}: names repeated: {repeated}")


def compile_cfg(cfg: Mapping[str, Any]) -> Program:
    """The program of cfg. Raises ValueError for a bad cfg."""
    c = parse(Config, cfg)
    input_names = [i.name for i in c.inputs]
    local_names = [local.name for local in c.locals]
    _unique(input_names, "inputs")
    _unique(local_names, "locals")
    _unique([check.name for check in c.checks], "checks")
    _unique([value.name for value in c.values], "values")
    if clash := sorted(set(input_names) & set(local_names)):
        raise ValueError(f"locals: {clash} are also inputs")

    # Compile locals top-to-bottom so each local can read inputs and earlier
    # locals with their inferred CEL return type (including TIMESTAMP/DURATION).
    var_types: dict[str, cel.Type] = {name: cel.Type.Map(cel.Type.STRING, cel.Type.DYN) for name in input_names}
    local_exprs: list[Expression] = []
    for local in c.locals:
        expr = cel_env.Env(var_types, FUNCTIONS, EXTENSIONS).compile(local.cel, f"local {local.name!r}", None)
        var_types[local.name] = expr.return_type()
        local_exprs.append(Expression(kind="local", name=local.name, expr=expr))

    env = cel_env.Env(var_types, FUNCTIONS, EXTENSIONS)

    def compiled(kind: str, section: list[NamedCel], gives: tuple[cel.Type, ...]) -> tuple[Expression, ...]:
        return tuple(Expression(kind=kind, name=n.name, expr=env.compile(n.cel, f"{kind} {n.name!r}", gives))
                     for n in section)

    return Program(
        inputs=tuple(c.inputs),
        locals=tuple(local_exprs),
        checks=compiled("check", c.checks, (cel.Type.BOOL, cel.Type.DYN)),
        values=compiled("value", c.values, (*VALUE_TYPES, cel.Type.DYN)),
    )


def run_program(program: Program, data: dict[str, Any]) -> dict[str, Scalar]:
    """The values of program over data (the inputs' data by input name).
    Raises ValueError, naming the expression, when a local, check or value
    fails to evaluate, or a check is false."""
    data = dict(data)
    for local in program.locals:
        data[local.name] = _eval(local, data)
    for check in program.checks:
        ok = _eval(check, data)
        if not isinstance(ok, bool):
            raise ValueError(f"check {check.name!r} gives a {type(ok).__name__}: it must give a bool")
        if not ok:
            raise ValueError(f"check {check.name!r} is false")
    values: dict[str, Scalar] = {}
    for value in program.values:
        x = _eval(value, data)
        if x is not None and type(x) not in (float, bool, str):  # pylint: disable=unidiomatic-typecheck  # int is not a double
            raise ValueError(
                f"value {value.name!r} gives a {type(x).__name__}: it must give a double, bool, string or null")
        values[value.name] = None if isinstance(x, float) and not math.isfinite(x) else x
    return values


def _eval(e: Expression, data: Mapping[str, Any]) -> Any:
    try:
        result = e.expr.eval(data=data)
    except RuntimeError as err:  # a value CEL can't take in, such as a string with a lone surrogate
        raise ValueError(f"{e.kind} {e.name!r} failed to evaluate: {err}") from None
    if result.type() == cel.Type.ERROR:
        raise ValueError(f"{e.kind} {e.name!r} failed to evaluate: {result.value()}")
    return result.plain_value()  # lists and maps as Python ones, which a later expression can read


class Cel(Source):
    @classmethod
    def validate(cls, cfg: Mapping[str, Any]) -> None:
        compile_cfg(cfg)

    def init(self, run: BenchmarkRunContext) -> None:
        super().init(run)
        self._runs: dict[str, BenchmarkRunContext | ReadError] = {}
        self._reads: dict[tuple[Optional[str], str, str], Mapping[str, Any] | ReadError] = {}

    def query(self, cfg: Mapping[str, Any]) -> Mapping[str, Scalar]:
        program = compile_cfg(cfg)
        data = {i.name: self._read(i) for i in program.inputs}
        try:
            return run_program(program, data)
        except ValueError as e:
            raise SourceInvalid(str(e)) from None

    def _read(self, i: Input) -> Mapping[str, Any]:
        """The data of input i, read once per run and reader config; SourceInvalid naming it when it can't be read."""
        key = (i.from_test, i.reader, json.dumps(i.reader_cfg, sort_keys=True))
        if key not in self._reads:
            try:
                reader = READERS[i.reader]
                data = reader().read(self._run(i.from_test), parse(reader.Config, i.reader_cfg))
                check_data(reader, data)
                self._reads[key] = data
            except ReadError as e:
                self._reads[key] = e
        found = self._reads[key]
        if isinstance(found, ReadError):
            raise SourceInvalid(f"input {i.name!r}: {found}")
        return found

    def _run(self, test: Optional[str]) -> BenchmarkRunContext:
        """The run of test (None: this run); ReadError when there is none."""
        if test is None:
            return self.run
        if test not in self._runs:
            try:
                self._runs[test] = self.run.run_of(test)
            except LookupError as e:
                self._runs[test] = ReadError(str(e))
        found = self._runs[test]
        if isinstance(found, ReadError):
            raise found
        return found
