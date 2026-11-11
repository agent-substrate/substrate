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

"""The run_results source: measurements of one stats.jsonl row, as values.

  source_cfg:
    metric: trial_summary                    # the row's metric; if repeated, the last row wins
    measurements: [aggregate_failure_ratio]  # returned as values under the same names

stats.jsonl keeps measurements as strings: numeric strings without '_' become
floats, "true" and "false" become bools, and a measurement the row lacks or a
non-finite number becomes None.
"""

import json
import math
from pathlib import Path
from typing import Annotated, Any, Mapping, Optional

from pydantic import Field, field_validator

from .base import Model, Scalar, Source, SourceInvalid, parse


def read_stats(path: Optional[Path]) -> dict[str, dict[str, Any]]:
    """stats.jsonl rows by metric name; the last row of a metric wins.

    The runner writes one JSON object with a "metric" per line, so blank lines
    are skipped and any other line raises ValueError naming its line number.
    """
    rows: dict[str, dict[str, Any]] = {}
    if path is None or not path.exists():
        return rows
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError as e:
                raise ValueError(f"stats.jsonl line {n} isn't JSON: {e}") from None
            if not isinstance(row, dict) or not isinstance(row.get("metric"), str) or not row["metric"]:
                raise ValueError(f"stats.jsonl line {n} isn't an object with a metric")
            rows[row["metric"]] = row
    return rows


def coerce(v: Any) -> Scalar:
    """A measurement as a value: finite numbers (and numeric strings without '_') as float,
    "true"/"false" as bool, and non-finite numbers as None."""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return float(v) if math.isfinite(v) else None
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "false"):
            return s == "true"
        if "_" not in v:
            try:
                x = float(v)
            except ValueError:
                return v
            return x if math.isfinite(x) else None
        return v
    return json.dumps(v, sort_keys=True)  # a nested value, kept readable


class Config(Model):
    metric: Annotated[str, Field(min_length=1)]
    measurements: list[Annotated[str, Field(min_length=1)]] = Field(min_length=1)

    @field_validator("measurements")
    @classmethod
    def _distinct(cls, names: list[str]) -> list[str]:
        if len(set(names)) != len(names):
            raise ValueError("listed twice")
        return names


class RunResults(Source):
    @classmethod
    def validate(cls, cfg: Mapping[str, Any]) -> None:
        parse(Config, cfg)

    def init(self, run) -> None:
        super().init(run)
        self._rows: Optional[dict[str, dict[str, Any]]] = None

    def query(self, cfg: Mapping[str, Any]) -> Mapping[str, Scalar]:
        if self._rows is None:
            path = self.run.artifact("stats.jsonl")
            if path is None:
                raise SourceInvalid("stats.jsonl absent")
            try:
                self._rows = read_stats(path)
            except ValueError as e:
                raise SourceInvalid(str(e)) from None
        row = self._rows.get(cfg["metric"])
        if row is None:
            raise SourceInvalid(f"no stats.jsonl row {cfg['metric']}")
        measurements = row.get("measurements") if isinstance(row.get("measurements"), dict) else {}
        return {name: coerce(measurements.get(name)) for name in cfg["measurements"]}
