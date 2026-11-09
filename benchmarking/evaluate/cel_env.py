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

"""Shared CEL environment, whole-number literal rewriting, round(), and evaluation."""

import re
from typing import Any, Mapping

from cel_expr_python import cel

# CEL reserved words and built-in type names, which cannot name a CEL variable.
RESERVED = frozenset({
    "as", "bool", "break", "bytes", "const", "continue", "double", "dyn",
    "else", "false", "for", "function", "if", "import", "in", "int",
    "let", "list", "loop", "map", "namespace", "null", "null_type",
    "package", "return", "string", "true", "type", "uint", "var",
    "void", "while",
})

# String and bytes literal bodies, raw and with escapes. A backslash only
# starts an escape, so an unterminated literal fails in linear time.
_RAW = "|".join((r'"""[\s\S]*?"""', r"'''[\s\S]*?'''", r'"[^"\n]*"', r"'[^'\n]*'"))
_ESCAPED = "|".join((r'"""(?:\\[\s\S]|[^\\])*?"""', r"'''(?:\\[\s\S]|[^\\])*?'''",
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


# round(x, digits) as Python rounds, so an expression can keep a fixed number of decimals.
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


def to_cel(values: Mapping[str, Any]) -> dict[str, Any]:
    """values as CEL reads them: ints as doubles, like the literals of an Env
    with ints_as_doubles, and no key for None, so has() tests presence."""
    return {k: float(v) if isinstance(v, int) and not isinstance(v, bool) else v
            for k, v in values.items() if v is not None}


def holds(expr: cel.Expression, data: Mapping[str, Any]) -> bool:
    """Whether the boolean expr is true for data. Raises ValueError with CEL's
    error when it fails to evaluate, for example on a key data lacks or a value
    CEL can't hold."""
    try:
        result = expr.eval(data=data)
    except RuntimeError as e:  # a value CEL can't take in, such as a string with a lone surrogate
        raise ValueError(str(e)) from None
    if result.type() != cel.Type.BOOL:
        raise ValueError(str(result.value()))
    return result.value()


class Env:
    """CEL variables: the env with all of them, and for each variable the env
    without it, where an expression that reads it fails to compile.

    With ints_as_doubles=True, each integer literal compiles as a double
    (1500 becomes 1500.0), since CEL keeps int and double arithmetic separate.
    """

    def __init__(
        self,
        variables: Mapping[str, cel.Type],
        functions: tuple[cel.FunctionDecl, ...] = (),
        *,
        ints_as_doubles: bool = False,
    ):
        self.variables = dict(variables)
        self.ints_as_doubles = ints_as_doubles
        for v in self.variables:
            if v in RESERVED:
                raise ValueError(f"variable {v!r} is reserved in CEL")
        try:
            self.full = cel.NewEnv(variables=self.variables, functions=list(functions))
            self.without = {
                v: cel.NewEnv(
                    variables={w: t for w, t in self.variables.items() if w != v},
                    functions=list(functions),
                )
                for v in self.variables
            }
        except RuntimeError as e:
            raise ValueError(str(e)) from None

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
