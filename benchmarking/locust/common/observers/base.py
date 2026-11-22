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

"""Runner execution protocol and registry for Substrate benchmark observers."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Protocol


@dataclass(frozen=True)
class ObserverResult:
    status: Literal["ok", "error"] = "ok"
    error: str | None = None
    counts: dict[str, int] = field(default_factory=dict)


class Observer(Protocol):
    OBSERVER_TYPE: str

    def start(self, name: str, config: dict[str, Any], out_dir: Path) -> None:
        """Initialize the observer and capture any pre-load baseline.

        All artifacts and logs must be written under out_dir (<work_dir>/<type>/).
        """
        ...

    def final_observation(self) -> ObserverResult:
        """Stop background work, cancel in-flight requests, and capture final state."""
        ...


OBSERVERS: dict[str, Callable[[], Observer]] = {}
