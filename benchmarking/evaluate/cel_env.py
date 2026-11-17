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

"""Shared CEL environment, explicit helpers, and evaluation.

Beyond standard CEL and its math extension (ext_math), only two explicit
numeric helpers are exposed:
- round(x, digits): Python-style rounding for doubles (shared with evaluation conditions).
- percentile(xs, p): linear-interpolation percentile over list(double), raising
  ValueError on an empty list like math.least/math.greatest.
"""

import math
from typing import Any, Mapping, Optional

from cel_expr_python import cel


def _round(x: float, digits: float) -> float:
    if not float(digits).is_integer():
        raise ValueError(f"round: digits {digits} isn't a whole number")
    return round(x, int(digits))


# round(x, digits) as Python rounds, so an expression can keep a fixed number of decimals.
ROUND = cel.FunctionDecl("round", [
    cel.Overload(f"round_double_{t.name().lower()}", return_type=cel.Type.DOUBLE, parameters=[cel.Type.DOUBLE, t],
                 impl=_round) for t in (cel.Type.DOUBLE, cel.Type.INT)])


def _percentile(xs: list, p: float) -> float:
    """The p-th percentile of xs (p from 0 to 100), interpolated linearly
    between the closest ranks; raises ValueError when xs is empty."""
    if not 0.0 <= p <= 100.0:
        raise ValueError(f"percentile: p {p} isn't between 0 and 100")
    s = sorted(float(x) for x in xs)
    if not s:
        raise ValueError("percentile: empty list")
    if not all(math.isfinite(x) for x in s):
        raise ValueError("percentile: non-finite value")
    k = (len(s) - 1) * p / 100.0
    lo = math.floor(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


PERCENTILE = cel.FunctionDecl(
    "percentile",
    [cel.Overload("percentile_list_double", return_type=cel.Type.DOUBLE,
                  parameters=[cel.Type.List(cel.Type.DOUBLE), cel.Type.DOUBLE], impl=_percentile)],
)


def to_cel(values: Mapping[str, Any]) -> dict[str, Any]:
    """values as CEL reads them: ints as doubles, and no key for None, so has()
    tests presence. Source values come from JSON, which has one number type, and
    as ints `a / b` would be integer division."""
    return {k: float(v) if isinstance(v, int) and not isinstance(v, bool) else v
            for k, v in values.items() if v is not None}


def evaluate_condition(expr: cel.Expression, data: Mapping[str, Any]) -> bool:
    """Whether the boolean expr is true for data. Raises ValueError with CEL's
    error when it fails to evaluate, for example on a key data lacks or a value
    CEL can't hold."""
    try:
        result = expr.eval(data=data)
    except RuntimeError as e:  # a value CEL can't take in, such as a string with a lone surrogate
        raise ValueError(str(e)) from None
    if result.type() == cel.Type.ERROR:
        raise ValueError(str(result.value()))
    if result.type() != cel.Type.BOOL:
        raise ValueError(f"gives a {result.type().name().lower()}, not a bool")
    return result.value()


class Env:
    """A CEL environment with variables and functions."""

    def __init__(
        self,
        variables: Mapping[str, cel.Type],
        functions: tuple[cel.FunctionDecl, ...] = (),
        extensions: tuple[cel.CelExtensionBase, ...] = (),
    ):
        self.variables = dict(variables)
        try:
            self.env = cel.NewEnv(variables=self.variables, functions=list(functions), extensions=list(extensions))
        except RuntimeError as e:
            raise ValueError(str(e)) from None

    def compile(self, expression: str, where: str,
                gives: Optional[tuple[cel.Type, ...]] = (cel.Type.BOOL,)) -> cel.Expression:
        """expression compiled. Raises ValueError unless it type-checks and gives
        one of gives (any type when gives is None)."""
        try:
            expr = self.env.compile(expression)
        except RuntimeError as e:  # a syntax or type error, or a variable CEL already declares
            raise ValueError(f"{where}: {e}") from None
        if gives is not None and expr.return_type() not in gives:
            wanted = " or a ".join(t.name().lower() for t in gives if t != cel.Type.DYN)
            raise ValueError(f"{where}: must give a {wanted}, gives {expr.return_type().name()}")
        return expr
