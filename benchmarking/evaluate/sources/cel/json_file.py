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

"""json: one JSON object file from the run.

  summary: {reader: json, file: server_summary.json}

  summary.data   the parsed JSON object, with leaf coercion as in run_results
  summary.uri    where the run is stored
"""

import json
from typing import Annotated, Any, Mapping

import pydantic

from ..base import BenchmarkRunContext, Model, parse
from .json_lines import coerce_json
from .reader import Field, Reader, ReadError, to_data


class Options(Model):
    file: Annotated[str, pydantic.Field(min_length=1)]  # the name of one of the run's files


class Json(Reader):
    fields = (Field("data", "map"), Field("uri", "string"))

    @classmethod
    def validate(cls, options: Mapping[str, Any]) -> None:
        parse(Options, options)

    def read(self, run: BenchmarkRunContext, options: Mapping[str, Any]) -> Mapping[str, Any]:
        name = options["file"]
        path = run.artifact(name)
        if path is None:
            raise ReadError(f"{name} absent")
        with open(path, encoding="utf-8", errors="replace") as f:
            try:
                parsed = json.load(f)
            except ValueError as e:
                raise ReadError(f"{name}: not JSON: {e}") from None
        if not isinstance(parsed, dict):
            raise ReadError(f"{name}: not a JSON object")
        return to_data({"data": coerce_json(parsed), "uri": run.identity.uri})
