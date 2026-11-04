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

"""The run_results source: measurements of one stats.jsonl row, as values.

  source_cfg:
    metric: trial_summary                    # the row's metric; if repeated, the last row wins
    measurements: [aggregate_failure_ratio]  # returned as values under the same names

stats.jsonl keeps measurements as strings: numeric strings become numbers,
"true" and "false" bools, and a measurement the row lacks is None (not
measured), as is a number that isn't finite.
"""

import json
import math
from pathlib import Path
from typing import Any, Mapping, Optional

from .base import NUMBER, Scalar, Source, SourceInvalid, valid_value_name

CFG_KEYS = {"metric", "measurements"}


def read_stats(path: Optional[Path]) -> dict[str, dict[str, Any]]:
    """stats.jsonl rows by metric name; the last row of a metric wins."""
    rows: dict[str, dict[str, Any]] = {}
    if path is None or not path.exists():
        return rows
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if isinstance(row, dict) and isinstance(row.get("metric"), str) and row["metric"]:
                rows[row["metric"]] = row
    return rows


def coerce(v: Any) -> Scalar:
    """A measurement as a value: numbers (and numeric strings) as float, "true"/"false" as bool.
    A number that isn't finite (NaN, an infinity, "1e999") is None: JSON has no such number."""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) or (isinstance(v, str) and NUMBER.match(v)):
        x = float(v)
        return x if math.isfinite(x) else None
    if isinstance(v, str):
        if v.strip().lower() in ("true", "false"):
            return v.strip().lower() == "true"
        return v
    return json.dumps(v, sort_keys=True)  # a nested value, kept readable


class RunResults(Source):
    @classmethod
    def validate(cls, cfg: Mapping[str, Any]) -> None:
        if unknown := set(cfg) - CFG_KEYS:
            raise ValueError(f"unknown keys {sorted(unknown, key=repr)}")
        if not isinstance(cfg.get("metric"), str) or not cfg["metric"]:
            raise ValueError("needs metric, the stats.jsonl row to read")
        names = cfg.get("measurements")
        if not isinstance(names, list) or not names:
            raise ValueError("needs measurements, a non-empty list of measurement names")
        for name in names:
            if not valid_value_name(name):
                raise ValueError(f"measurements: {name!r} is not a value name (letters, digits and _, without '__')")
        if len(set(names)) != len(names):
            raise ValueError("measurements: listed twice")

    @classmethod
    def value_names(cls, cfg: Mapping[str, Any]) -> Optional[set[str]]:
        return set(cfg["measurements"])

    def init(self, run) -> None:
        super().init(run)
        self._rows: Optional[dict[str, dict[str, Any]]] = None

    def query(self, cfg: Mapping[str, Any]) -> Mapping[str, Scalar]:
        if self._rows is None:
            path = self.run.file("stats.jsonl")
            if path is None:
                raise SourceInvalid("stats.jsonl absent")
            self._rows = read_stats(path)
        row = self._rows.get(cfg["metric"])
        if row is None:
            raise SourceInvalid(f"no stats.jsonl row {cfg['metric']}")
        measurements = row.get("measurements") if isinstance(row.get("measurements"), dict) else {}
        return {name: coerce(measurements.get(name)) for name in cfg["measurements"]}
