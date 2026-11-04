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

"""CEL rules and values: a rule judges one record, a value is a number from a query's counts and facts."""

import math
import re
from dataclasses import dataclass
from typing import Any, Mapping

from cel_expr_python import cel

from ..base import NAME, Scalar, SourceInvalid
from ..base import valid_name as valid_value_name

KEYS = {"name", "cel"}
NAME_MAX = 60  # bad_<name> stays within the 64 characters of a value name
# String and bytes literal bodies, raw and with escapes.
_RAW = "|".join((r'"""[\s\S]*?"""', r"'''[\s\S]*?'''", r'"[^"\n]*"', r"'[^'\n]*'"))
_ESCAPED = "|".join((r'"""(?:\\[\s\S]|[\s\S])*?"""', r"'''(?:\\[\s\S]|[\s\S])*?'''",
                     r'"(?:\\.|[^"\\\n])*"', r"'(?:\\.|[^'\\\n])*'"))
# The tokens of a CEL expression that hold digits: literals, comments, quoted
# field names and identifiers. A token of digits only is an integer literal.
TOKEN = re.compile("|".join((
    rf"(?:[rR][bB]?|[bB][rR])(?:{_RAW})", rf"[bB]?(?:{_ESCAPED})", r"//[^\n]*", r"`[^`\n]*`",
    r"0[xX][0-9a-fA-F]+[uU]?", r"\d*\.\d+(?:[eE][+-]?\d+)?", r"\d+[eE][+-]?\d+", r"\d+[uU]?", r"[A-Za-z_]\w*",
)), re.A)


def _round(x: float, digits: float) -> float:
    if not float(digits).is_integer():
        raise ValueError(f"round: digits {digits} isn't a whole number")
    return round(x, int(digits))


# round(x, digits) as Python rounds, so a value can keep a fixed number of decimals.
ROUND = cel.FunctionDecl("round", [
    cel.Overload(f"round_double_{t.name().lower()}", return_type=cel.Type.DOUBLE, parameters=[cel.Type.DOUBLE, t],
                 impl=_round) for t in (cel.Type.DOUBLE, cel.Type.INT)])


def ints_as_doubles(expression: str) -> str:
    """expression with each integer literal written as a double (1500 as 1500.0)."""
    parts, last = [], 0
    for m in TOKEN.finditer(expression):
        if m.group().isdigit():
            parts.append(expression[last:m.end()] + ".0")
            last = m.end()
    return "".join(parts) + expression[last:]


class Env:
    """CEL variables: the env with all of them, and for each variable the env
    without it, where an expression that reads it doesn't compile.

    When every variable is a double, so is every integer literal: 1500 reads
    as 1500.0, since CEL doesn't mix ints and doubles.
    """

    def __init__(self, variables: Mapping[str, cel.Type], functions: tuple[cel.FunctionDecl, ...] = ()):
        self.variables = dict(variables)
        self.ints_as_doubles = all(t == cel.Type.DOUBLE for t in self.variables.values())
        self.full = cel.NewEnv(variables=self.variables, functions=list(functions))
        self.without = {v: cel.NewEnv(variables={w: t for w, t in self.variables.items() if w != v},
                                      functions=list(functions))
                        for v in self.variables}

    def compile(self, expression: Any, where: str,
                gives: tuple[cel.Type, ...] = (cel.Type.BOOL,)) -> tuple[cel.Expression, tuple[str, ...]]:
        """expression compiled, and the variables it reads. Raises ValueError
        unless it is a CEL expression that type-checks and gives one of gives."""
        if not isinstance(expression, str):
            raise ValueError(f"{where}: needs a CEL expression")
        source = ints_as_doubles(expression) if self.ints_as_doubles else expression
        try:
            expr = self.full.compile(source)
        except RuntimeError as e:  # a syntax or type error, on the expression as compiled
            raise ValueError(f"{where}: {e}") from None
        if expr.return_type() not in gives:
            wanted = " or a ".join(t.name().lower() for t in gives)
            raise ValueError(f"{where}: must give a {wanted}, gives {expr.return_type().name()}")
        return expr, tuple(v for v, env in self.without.items() if not _compiles(env, source))


def _compiles(env: cel.Env, expression: str) -> bool:
    try:
        env.compile(expression)
    except RuntimeError:
        return False
    return True


@dataclass(frozen=True)
class Rule:
    """A compiled rule: a record breaks it when expr is false."""
    name: str
    expr: cel.Expression
    reads: tuple[str, ...]  # the variables expr reads


def valid_name(name: Any) -> bool:
    """A rule name: a value name short enough for bad_<name> to be one too."""
    return valid_value_name(name) and len(name) <= NAME_MAX


def compile_rules(rules: Any, env: Env, taken: tuple[str, ...] = ()) -> list[Rule]:
    """The rules, compiled. Raises ValueError unless rules is a valid list of them.

    A rule is {name, cel}: a CEL expression over one record that must be true.
    Unknown keys and variables, type errors and results other than a bool are
    errors, so a typo fails here instead of passing every record.
    Each rule has its own name, the reason a record that breaks it is bad, and
    none is one of taken (the kind's own reasons), so bad-reason counts stay
    per rule.
    """
    if not isinstance(rules, list):
        raise ValueError("rules: needs a list (an empty list applies no rule)")
    compiled = []
    for i, rule in enumerate(rules):
        at = f"rules[{i}]"
        if not isinstance(rule, dict):
            raise ValueError(f"{at}: not a mapping")
        if unknown := set(rule) - KEYS:
            raise ValueError(f"{at}: unknown keys {sorted(unknown)}")
        if not valid_name(rule.get("name")):
            raise ValueError(f"{at}: name {rule.get('name')!r} must match {NAME.pattern}, without '__', "
                             f"at most {NAME_MAX} characters")
        expr, reads = env.compile(rule.get("cel"), f"rule {rule['name']!r}: cel")
        compiled.append(Rule(rule["name"], expr, reads))
    names = [r.name for r in compiled]
    if dupes := sorted({n for n in names if names.count(n) > 1}):
        raise ValueError(f"rules: each rule needs its own name; repeated {dupes}")
    if clash := sorted(set(names) & set(taken)):
        raise ValueError(f"rules: names {clash} are reasons the records give")
    return compiled


def holds(expr: cel.Expression, data: Mapping[str, Any], what: str) -> bool:
    """Whether expr is true for data. Raises SourceInvalid, naming what, when it fails to evaluate."""
    result = expr.eval(data=data)
    if result.type() != cel.Type.BOOL:  # an error, such as a missing key or int() out of range
        raise SourceInvalid(f"{what}: {result.value()}")
    return result.value()


def broken(rules: list[Rule], data: Mapping[str, Any], where: str) -> list[str]:
    """The names of the rules data breaks, in order. Raises SourceInvalid
    when data lacks a variable a rule reads."""
    for r in rules:
        if lacked := next((v for v in r.reads if v not in data), None):
            raise SourceInvalid(f"rule {r.name!r} reads {lacked}, which {where} lacks")
    return [r.name for r in rules if not holds(r.expr, data, f"rule {r.name!r} on {where}")]


@dataclass(frozen=True)
class Value:
    """A compiled value: a number (or a bool) from a query's counts and facts."""
    name: str
    expr: cel.Expression
    reads: tuple[str, ...]  # the variables expr reads


def compile_values(values: Any, env: Env, taken: set[str]) -> list[Value]:
    """The values, compiled. Raises ValueError unless values maps value names
    to CEL expressions that type-check and give a double or a bool. A name
    can't be one in taken (the values the source gives) or start with bad_."""
    if not isinstance(values, dict):
        raise ValueError("values: needs a mapping of value names to CEL expressions")
    compiled = []
    for name, expression in values.items():
        if not valid_value_name(name):
            raise ValueError(f"values: {name!r} must match {NAME.pattern}, without '__'")
        if name in taken or name.startswith("bad_"):
            raise ValueError(f"values: {name!r} is a value the source gives")
        expr, reads = env.compile(expression, f"value {name!r}", (cel.Type.DOUBLE, cel.Type.BOOL))
        compiled.append(Value(name, expr, reads))
    return compiled


def evaluate(value: Value, data: Mapping[str, Any]) -> Scalar:
    """value for data: None when it reads a variable data lacks, or gives a
    number that isn't finite (x / 0). Raises SourceInvalid when it fails to
    evaluate otherwise."""
    if not all(v in data for v in value.reads):
        return None
    result = value.expr.eval(data=data)
    if result.type() not in (cel.Type.DOUBLE, cel.Type.BOOL):  # an error
        raise SourceInvalid(f"value {value.name!r}: {result.value()}")
    x = result.value()
    return None if isinstance(x, float) and not math.isfinite(x) else x
