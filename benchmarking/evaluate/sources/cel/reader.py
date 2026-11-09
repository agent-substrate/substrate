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

"""What the cel source needs from a reader: a class that turns one run's files into CEL data.

A reader exposes data and passes no judgment on it. Its data is a mapping
with the top-level fields it declares; lists hold maps or doubles, and a
field a record lacks is absent rather than null, so has() tests for it.
"""

import dataclasses
import math
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, Mapping

from ..base import BenchmarkRunContext

FieldType = Literal["double", "string", "bool", "list", "map"]
PY_TYPES: dict[str, type | tuple[type, ...]] = {"double": float, "string": str, "bool": bool, "list": list,
                                                "map": dict}


class ReadError(Exception):
    """The reader can't read the run: why (e.g. the file is absent)."""


@dataclass(frozen=True)
class Field:
    """A top-level field of a reader's data, with its CEL type."""
    name: str
    type: FieldType
    optional: bool = False


class Reader:
    """A reader; the cel source makes one per input that names it."""

    fields: ClassVar[tuple[Field, ...]]

    @classmethod
    def validate(cls, options: Mapping[str, Any]) -> None:
        """Raises ValueError for a bad option (the input's keys besides reader and test)."""
        raise NotImplementedError

    def read(self, run: BenchmarkRunContext, options: Mapping[str, Any]) -> Mapping[str, Any]:
        """The run's data. Raises ReadError when the run can't be read."""
        raise NotImplementedError


def check_data(reader: type[Reader], data: Mapping[str, Any]) -> None:
    """Raises TypeError unless data has the reader's fields, of their types, and no others."""
    declared = {f.name: f for f in reader.fields}
    if unknown := set(data) - set(declared):
        raise TypeError(f"{reader.__name__} gave undeclared fields {sorted(unknown)}")
    for f in reader.fields:
        if f.name not in data:
            if not f.optional:
                raise TypeError(f"{reader.__name__} gave no {f.name}")
        elif isinstance(data[f.name], bool) != (f.type == "bool") or not isinstance(data[f.name], PY_TYPES[f.type]):
            raise TypeError(f"{reader.__name__}: {f.name} is a {type(data[f.name]).__name__}, not a {f.type}")


def to_data(obj: Any) -> Any:
    """A dataclass (or list or dict of them) as CEL data: dicts and lists,
    numbers as doubles, a None or non-finite field left out."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        obj = {f.name: getattr(obj, f.name) for f in dataclasses.fields(obj)}
    if isinstance(obj, dict):
        return {k: to_data(v) for k, v in obj.items() if _present(v)}
    if isinstance(obj, (list, tuple)):
        return [to_data(v) for v in obj if _present(v)]
    if isinstance(obj, int) and not isinstance(obj, bool):
        return float(obj)
    return obj


def _present(v: Any) -> bool:
    return v is not None and not (isinstance(v, float) and not math.isfinite(v))
