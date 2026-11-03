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

"""The threshold check: `value op limit`, the one check grammar of run evaluations.

  check: {value: trial.aggregate_failure_ratio, op: "<=", limit: 0.001}
  check: {value: server.working_set_node_max_gb, op: "<", limit: {ref: trial.allocatable_ram_gb, times: 0.85}}

A limit is a literal (number, bool or string) or relative to another value:
times (default 1) times the value of ref, clamped to at_least and at_most when
given. < <= > >= compare numbers only (a bool is not a number); == and != also
compare a bool with a bool and a string with a string. Anything else is
invalid, never a pass. The caller resolves refs.
"""

import math
import operator
from typing import Any, Callable, Mapping, NamedTuple, Optional

NUMERIC_OPS = {"<=": operator.le, "<": operator.lt, ">=": operator.ge, ">": operator.gt}
EQUALITY_OPS = {"==": operator.eq, "!=": operator.ne}
OPS = {**NUMERIC_OPS, **EQUALITY_OPS}
CHECK_KEYS = ("value", "op", "limit")
LIMIT_KEYS = {"ref", "times", "at_least", "at_most"}

PASS, FAIL, INVALID = "pass", "fail", "invalid"


class Missing(LookupError):
    """A ref has no value to compare: not measured, or its source couldn't judge the run."""


class Verdict(NamedTuple):
    status: str  # pass | fail | invalid
    value: Any
    limit: Any
    reason: str


def is_number(v: Any) -> bool:
    """A number that compares: bools, NaN and infinities are not."""
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def validate(check: Any, ref_ok: Callable[[str], Optional[str]]) -> None:
    """Raises ValueError unless check is a valid threshold check.

    ref_ok(ref) returns None for a ref the caller can resolve, else why not.
    """
    if not isinstance(check, Mapping):
        raise ValueError("check: not a mapping")
    if unknown := set(check) - set(CHECK_KEYS):
        raise ValueError(f"check: unknown keys {sorted(unknown)}")
    if missing := [k for k in CHECK_KEYS if k not in check]:
        raise ValueError(f"check: needs {missing[0]!r}")
    _validate_ref(check["value"], "check.value", ref_ok)
    op = check["op"]
    if not isinstance(op, str) or op not in OPS:
        raise ValueError(f"check.op: {op!r} is not one of {list(OPS)}")
    limit = check["limit"]
    if isinstance(limit, Mapping):
        if unknown := set(limit) - LIMIT_KEYS:
            raise ValueError(f"check.limit: unknown keys {sorted(unknown)}")
        if "ref" not in limit:
            raise ValueError("check.limit: a relative limit needs 'ref'")
        _validate_ref(limit["ref"], "check.limit.ref", ref_ok)
        for key in ("times", "at_least", "at_most"):
            if key in limit and not is_number(limit[key]):
                raise ValueError(f"check.limit.{key}: must be a number")
        if op not in NUMERIC_OPS:
            raise ValueError(f"check.op: a relative limit needs one of {list(NUMERIC_OPS)}")
        if "at_least" in limit and "at_most" in limit and limit["at_least"] > limit["at_most"]:
            raise ValueError("check.limit: at_least is above at_most")
    elif op in NUMERIC_OPS:
        if not is_number(limit):
            raise ValueError(f"check.limit: {op} needs a number, got {limit!r}")
    elif not (is_number(limit) or isinstance(limit, (bool, str))):
        raise ValueError(f"check.limit: needs a number, bool or string, got {limit!r}")


def _validate_ref(ref: Any, where: str, ref_ok: Callable[[str], Optional[str]]) -> None:
    if not isinstance(ref, str) or not ref:
        raise ValueError(f"{where}: needs a ref string")
    if why := ref_ok(ref):
        raise ValueError(f"{where}: {why}")


def effective_limit(check: Mapping[str, Any], lookup: Callable[[str], Any]) -> Any:
    """The limit to compare against; raises Missing when a relative limit's ref has no number."""
    limit = check["limit"]
    if not isinstance(limit, Mapping):
        return limit
    base = lookup(limit["ref"])
    if not is_number(base):
        raise Missing(f"{limit['ref']} = {base!r} is not a number")
    v = float(limit.get("times", 1)) * base
    if "at_least" in limit:
        v = max(v, float(limit["at_least"]))
    if "at_most" in limit:
        v = min(v, float(limit["at_most"]))
    return v


def threshold(check: Mapping[str, Any], lookup: Callable[[str], Any]) -> Verdict:
    """pass, fail or invalid for a valid check. lookup(ref) returns the ref's
    value, or raises Missing when there is none (null means not measured, never 0)."""
    ref, op = check["value"], check["op"]
    try:
        value = lookup(ref)
        limit = effective_limit(check, lookup)
    except Missing as e:
        return Verdict(INVALID, None, None, str(e))
    shown = _limit_text(check, limit)
    if op in NUMERIC_OPS:
        if not is_number(value):
            return Verdict(INVALID, value, limit, f"{ref} = {value!r} is not a number")
        ok = NUMERIC_OPS[op](float(value), float(limit))
    elif is_number(value) and is_number(limit):
        ok = EQUALITY_OPS[op](float(value), float(limit))
    elif type(value) is type(limit) and isinstance(value, (bool, str)):
        ok = EQUALITY_OPS[op](value, limit)
    else:  # a bool against a string, a number against a bool...
        return Verdict(INVALID, value, limit, f"{ref} = {_fmt(value)} does not compare with {_fmt(limit)}")
    return Verdict(PASS if ok else FAIL, value, limit, f"{ref} = {_fmt(value)}, needs {op} {shown}")


def _limit_text(check: Mapping[str, Any], limit: Any) -> str:
    rel = check["limit"]
    if not isinstance(rel, Mapping):
        return _fmt(limit)
    parts = f"{_fmt(rel.get('times', 1))} x {rel['ref']}"
    clamps = [f"{k} {_fmt(rel[k])}" for k in ("at_least", "at_most") if k in rel]
    return f"{_fmt(limit)} ({parts}{', ' + ', '.join(clamps) if clamps else ''})"


def _fmt(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return f"{v:.6g}"
    return str(v)
