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
      stats: {reader: json_lines, file: stats.jsonl}
      other: {reader: json_lines, file: stats.jsonl, test: some_other_test}
    locals:                        # optional: ordered list of named in-between results
      - {name: rows, cel: "stats.lines.filter(l, l.metric == 'trial_summary')"}
    checks:                        # optional: what must hold for the values to mean anything
      has_rows: "size(rows) > 0"
    values:                        # the outputs: each a double, bool, string or null
      cores: "size(rows) > 0 ? dyn(rows[size(rows) - 1].measurements.allocatable_cores) : null"

An input names a reader (READERS) and its options. By default it reads this
run; with `test:` it reads that test's run from the same invocation
(BenchmarkRunContext.paired_run). Locals are evaluated top to bottom, each
reading inputs and earlier locals by name; a local that fails to evaluate is
left out, so only what reads it fails, with the local's error. The source knows nothing about what
the data means: what is good, which run to compare against and every formula
live in the config.

At run time, a reader that can't read its run, a check that is false or fails
to evaluate, and a local or value that fails to evaluate make the variable
invalid, and the reason names which. A value that isn't finite becomes null.
"""

import json
import math
from dataclasses import dataclass
from typing import Annotated, Any, Mapping, Optional

from cel_expr_python import cel
from cel_expr_python.ext import ext_math
from pydantic import ConfigDict, Field, field_validator, model_validator

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
    """An input: a reader, the test whose run it reads (None: this run), and the reader's options."""
    model_config = ConfigDict(extra="allow")  # the extra keys are the reader's options

    reader: str
    test: Optional[Annotated[str, Field(min_length=1)]] = None

    @field_validator("reader")
    @classmethod
    def _known(cls, reader: str) -> str:
        if reader not in READERS:
            raise ValueError(f"reader {reader!r} is not one of {sorted(READERS)}")
        return reader

    @model_validator(mode="after")
    def _options(self) -> "Input":
        READERS[self.reader].validate(self.options)
        return self

    @property
    def options(self) -> dict[str, Any]:
        return dict(self.model_extra or {})


class Local(Model):
    name: Name
    cel: str


class Config(Model):
    inputs: dict[Name, Input] = Field(min_length=1)
    locals: list[Local] = []
    checks: dict[Name, str] = {}
    values: dict[Name, str] = Field(min_length=1)


@dataclass(frozen=True)
class Expression:
    kind: str  # "local", "check" or "value", for error messages
    name: str
    expr: cel.Expression


@dataclass(frozen=True)
class Program:
    """A source_cfg, compiled: the inputs by name, the locals in list order,
    the checks and the values."""
    inputs: Mapping[str, Input]
    locals: tuple[Expression, ...]
    checks: tuple[Expression, ...]
    values: tuple[Expression, ...]


def compile_cfg(cfg: Mapping[str, Any]) -> Program:
    """The program of cfg. Raises ValueError for a bad cfg."""
    c = parse(Config, cfg)
    local_names = [local.name for local in c.locals]
    if repeated := sorted({n for n in local_names if local_names.count(n) > 1}):
        raise ValueError(f"locals: names repeated: {repeated}")
    if clash := sorted(set(c.inputs) & set(local_names)):
        raise ValueError(f"locals: {clash} are also inputs")

    # Compile locals top-to-bottom so each local can read inputs and earlier
    # locals with their inferred CEL return type (including TIMESTAMP/DURATION).
    var_types: dict[str, cel.Type] = {name: cel.Type.Map(cel.Type.STRING, cel.Type.DYN) for name in c.inputs}
    local_exprs: list[Expression] = []
    for local in c.locals:
        expr = cel_env.Env(var_types, FUNCTIONS, EXTENSIONS).compile(local.cel, f"local {local.name!r}", None)
        var_types[local.name] = expr.return_type()
        local_exprs.append(Expression("local", local.name, expr))

    env = cel_env.Env(var_types, FUNCTIONS, EXTENSIONS)

    def compiled(kind: str, section: Mapping[str, str], gives: tuple[cel.Type, ...]) -> tuple[Expression, ...]:
        return tuple(Expression(kind, name, env.compile(text, f"{kind} {name!r}", gives))
                     for name, text in section.items())

    return Program(c.inputs, tuple(local_exprs), compiled("check", c.checks, (cel.Type.BOOL, cel.Type.DYN)),
                   compiled("value", c.values, (*VALUE_TYPES, cel.Type.DYN)))


def run_program(program: Program, data: dict[str, Any], *, enforce_checks: bool = True) -> dict[str, Scalar]:
    """The values of program over data (the inputs' data by input name).
    Raises ValueError, naming the expression, when a value fails to evaluate,
    or a check fails to evaluate or (with enforce_checks) is false. A local that
    fails to evaluate is left out of the data, and its error is added to the
    error of whatever then fails."""
    data = dict(data)
    failed_locals = []
    for local in program.locals:
        try:
            data[local.name] = _eval(local, data)
        except ValueError as e:
            failed_locals.append(str(e))

    def evaluated(e: Expression) -> Any:
        try:
            return _eval(e, data)
        except ValueError as err:
            raise ValueError("; ".join([str(err), *failed_locals])) from None

    for check in program.checks:
        ok = evaluated(check)
        if not isinstance(ok, bool):
            raise ValueError(f"check {check.name!r} gives a {type(ok).__name__}: it must give a bool")
        if not ok and enforce_checks:
            raise ValueError(f"check {check.name!r} is false")
    values: dict[str, Scalar] = {}
    for value in program.values:
        x = evaluated(value)
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
        self._runs: dict[str, BenchmarkRunContext | LookupError] = {}
        self._reads: dict[tuple[Optional[str], str, str], Mapping[str, Any] | ReadError] = {}

    def query(self, cfg: Mapping[str, Any]) -> Mapping[str, Scalar]:
        program = compile_cfg(cfg)
        data = {name: self._read(name, i) for name, i in program.inputs.items()}
        try:
            return run_program(program, data)
        except ValueError as e:
            raise SourceInvalid(str(e)) from None

    def _read(self, name: str, i: Input) -> Mapping[str, Any]:
        """The data of input i, read once per run and reader options; SourceInvalid naming it when it can't be read."""
        key = (i.test, i.reader, json.dumps(i.options, sort_keys=True))
        if key not in self._reads:
            try:
                reader = READERS[i.reader]
                data = reader().read(self._run(i.test), i.options)
                check_data(reader, data)
                self._reads[key] = data
            except (LookupError, ReadError) as e:
                self._reads[key] = ReadError(str(e))
        found = self._reads[key]
        if isinstance(found, ReadError):
            raise SourceInvalid(f"input {name!r}: {found}")
        return found

    def _run(self, test: Optional[str]) -> BenchmarkRunContext:
        if test is None:
            return self.run
        if test not in self._runs:
            try:
                self._runs[test] = self.run.paired_run(test)
            except LookupError as e:
                self._runs[test] = e
        found = self._runs[test]
        if isinstance(found, LookupError):
            raise found
        return found
