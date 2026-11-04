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

"""What the cel source needs from a kind of record."""

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

from cel_expr_python import cel

from ..base import BenchmarkRunContext, Scalar
from . import rules

# The counts every query gives, which `values:` expressions read as doubles.
COUNTS = ("records", "good", "bad")


@dataclass
class Record:
    """One record to judge: its variables, or a reason it is bad that the rules don't change."""
    where: str  # how an error names the record, e.g. an actor or file:line
    data: dict[str, Any] = field(default_factory=dict)
    failure: str = ""
    item: Any = None  # the kind's own view of the record, for its summary


@dataclass
class Records:
    """The records of one query, and the per-run variables this run has."""
    records: list[Record]
    run_data: dict[str, Any] = field(default_factory=dict)
    run_note: str = ""  # why run_data lacks a variable, when it does


@dataclass
class Summary:
    """What a kind adds to the counts of a query."""
    values: dict[str, Scalar] = field(default_factory=dict)  # its own values
    facts: dict[str, Optional[float]] = field(default_factory=dict)  # for `values:`; None when not measured
    invalid: str = ""  # why the run is invalid, "" when it isn't


def values_env(facts: tuple[str, ...]) -> rules.Env:
    """The variables `values:` expressions read: the counts and a kind's facts, as doubles."""
    return rules.Env({name: cel.Type.DOUBLE for name in COUNTS + facts}, (rules.ROUND,))


class Kind:
    """A kind of record: what the records are, which variables rules read, and the kind's own values.

    One instance per run, created on the first query that names the kind.
    """
    ENV: rules.Env  # the variables a rule can read
    VALUES_ENV: rules.Env = values_env(())  # the variables a `values:` expression can read
    # Variables with one value per run: a rule that reads one the run lacks makes the run invalid.
    RUN_VARIABLES: frozenset[str] = frozenset()
    FAILURE_REASONS: tuple[str, ...] = ()  # reasons a record is bad whatever the rules say
    CFG_KEYS: frozenset[str] = frozenset()  # the kind's own source_cfg keys

    @classmethod
    def validate(cls, cfg: Mapping[str, Any]) -> None:
        """Raises ValueError for a bad value of the kind's own keys."""
        raise NotImplementedError

    def __init__(self, run: BenchmarkRunContext) -> None:
        self.run = run

    def records(self, cfg: Mapping[str, Any]) -> Records:
        """The records to judge. Raises SourceInvalid when the run has none to give."""
        raise NotImplementedError

    @classmethod
    def value_names(cls, compiled: list[rules.Rule]) -> set[str]:
        """The names of the values the kind gives with these rules."""
        return set()

    def summary(self, cfg: Mapping[str, Any], found: Records, compiled: list[rules.Rule],
                reasons: list[list[str]]) -> Summary:
        """What the kind adds, given the rules and each record's bad reasons."""
        raise NotImplementedError
