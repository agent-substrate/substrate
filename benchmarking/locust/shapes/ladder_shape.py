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

"""A scenario ladder as one locust run.

The observability scenarios in benchmarking/observability.md are steps of a
ladder: hold a number of users and a sample rate for a period, then go to the
next pair. Each step was a separate run, thus each step also carried a deploy,
a set of cold caches, and a new baseline for the reader to align.

`--ladder` makes the steps one run. Give the steps as a comma-separated list
of `users:duration[:trace_probability][@target_rps]`:

    --ladder 5:3m,15:3m,30:3m          the user sweep, S1
    --ladder 15:3m:0,15:3m:0.1,15:3m:1 the sample-rate sweep, S2
    --ladder 0:5m                      the idle floor, S0
    --ladder 200:3m@1000,200:3m@2000   a rate sweep at 200 users

Name this file in the `file` of the test, after the test itself:

    file: /app/tests/glutton.py,/app/shapes/ladder_shape.py

The shape controls the duration of the run. locust ignores `-t` when a shape
is present, thus the run ends with the last step.

A step that names a trace probability changes the sample rate as it starts,
and the new rate stays until another step changes it. `--trace-probability`
gives the rate before the first step that names one. Thus a ladder with no
probability in any step keeps the rate of that flag from start to end.

The value goes to the parsed options of the master, thus:

  * A Python user class reads it through the sampler of common.trace.
  * A boomer worker reads it from /boomer-config at the spawn message that
    the step change makes. boomer fetches that endpoint on each spawn
    message, and a step change is a spawn message.

A step that names a target rate works the same way: the rate applies as the
step starts and stays until another step changes it, and `--target-rps` gives
the rate before the first step that names one. Only a user class that paces
reads it (resumecold), and it caps the requests per second of the whole
worker. The users must be enough to reach it, as each waits for its whole
iteration to return: for resumecold, a resume plus a pause.
A step that holds the number of users makes no spawn message, thus boomer
reads the new rate at its next poll of /boomer-config, within 10 s; trim the
start of each step. `0` removes the cap.

Use a boomer user class for a ladder. Python and gRPC hold each other at a
high number of users, and the latency of that condition is the latency of the
load generator and not of substrate.
"""

import logging
import re

from locust import LoadTestShape, events
from locust.argument_parser import LocustArgumentParser

logger = logging.getLogger(__name__)

# users:duration[:trace_probability][@target_rps]
_STEP_RE = re.compile(
    r"^(?P<users>\d+):(?P<duration>\d+)(?P<unit>[smh]?)"
    r"(?::(?P<probability>[0-9.]+))?"
    r"(?:@(?P<rps>[0-9.]+))?$"
)

_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600}

# A step of the ladder holds one number of users, for one period, at one
# sample rate and one target request rate. `probability` and `rps` are None
# when the step does not change them.
Step = tuple[int, int, float | None, float | None]


def parse_ladder(spec: str) -> list[Step]:
    """Parse a --ladder value into its steps. Raises ValueError on a step
    that does not parse, because a ladder that runs the wrong scenario gives
    a table that no reader can tell from a correct one."""
    steps: list[Step] = []
    for raw in spec.split(","):
        item = raw.strip()
        if not item:
            continue
        m = _STEP_RE.match(item)
        if not m:
            raise ValueError(
                f"unrecognized ladder step {item!r}; "
                f"the form is users:duration[:trace_probability][@target_rps], "
                f"as in 15:3m, 15:3m:0.1, or 200:3m@1000"
            )
        seconds = int(m.group("duration")) * _UNIT_SECONDS[m.group("unit") or "s"]
        if seconds <= 0:
            raise ValueError(f"ladder step {item!r} has no duration")
        probability = _parse_float(item, m.group("probability"), "trace probability")
        if probability is not None and not 0.0 <= probability <= 1.0:
            raise ValueError(
                f"ladder step {item!r} has a trace probability outside 0.0 to 1.0"
            )
        rps = _parse_float(item, m.group("rps"), "target rate")
        steps.append((int(m.group("users")), seconds, probability, rps))
    if not steps:
        raise ValueError("--ladder has no steps")
    return steps


def _parse_float(item: str, value: str | None, what: str) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"ladder step {item!r} has a {what} that is not a number")


@events.init_command_line_parser.add_listener
def _(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--ladder",
        type=str,
        default="",
        env_var="LOCUST_LADDER",
        help=(
            "Run a ladder of steps in one test, as "
            "users:duration[:trace_probability][@target_rps],... For "
            "example 5:3m,15:3m,30:3m"
        ),
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--ladder-spawn-rate",
        type=float,
        default=5.0,
        env_var="LOCUST_LADDER_SPAWN_RATE",
        help="Users per second at each step change of a --ladder run",
        include_in_web_ui=True,
    )


def _set_trace_probability(environment, probability: float) -> None:
    """Apply `probability` to the master, for both kinds of worker.

    The parsed options are what /boomer-config serves, thus a boomer worker
    reads the new value at the next spawn message. common.trace holds the
    sampler of a Python user class in this process, which reads no endpoint.
    """
    environment.parsed_options.trace_probability = probability
    from common.trace import set_trace_probability

    set_trace_probability(probability)


def _set_target_rps(environment, rps: float) -> None:
    """Apply `rps` to the parsed options, which /boomer-config serves."""
    environment.parsed_options.target_rps = rps


class LadderShape(LoadTestShape):
    """Holds each step of `--ladder` in turn, then stops the run.

    locust loads a shape from the locustfile, and this file has no user
    class, thus runner.py adds it to `-f` next to the test file only when
    --ladder is present. Without the flag the shape is inert: tick() returns
    None at once and locust ends the run, which is why the file must not be
    loaded by itself.
    """

    # Recomputed from the parsed options at the first tick. The command line
    # is not available when locust constructs the shape.
    _steps: list[Step] | None = None
    _boundaries: list[float] = []
    _current: int = -1

    def _load(self) -> bool:
        if self._steps is not None:
            return bool(self._steps)
        spec = getattr(self.runner.environment.parsed_options, "ladder", "")
        self._steps = parse_ladder(spec) if spec else []
        elapsed = 0.0
        self._boundaries = []
        for _, seconds, _, _ in self._steps:
            elapsed += seconds
            self._boundaries.append(elapsed)
        if self._steps:
            logger.info(
                "Ladder: %d steps, %.0fs in total",
                len(self._steps),
                self._boundaries[-1],
            )
        return bool(self._steps)

    def tick(self) -> tuple[int, float] | None:
        if not self._load():
            return None
        assert self._steps is not None

        run_time = self.get_run_time()
        index = next(
            (i for i, end in enumerate(self._boundaries) if run_time < end),
            None,
        )
        if index is None:
            logger.info("Ladder complete after %.0fs", run_time)
            return None

        users, _, probability, rps = self._steps[index]
        spawn_rate = self.runner.environment.parsed_options.ladder_spawn_rate

        if index != self._current:
            self._current = index
            if probability is not None:
                _set_trace_probability(self.runner.environment, probability)
            if rps is not None:
                _set_target_rps(self.runner.environment, rps)
            logger.info(
                "Ladder step %d/%d: users=%d trace_probability=%s target_rps=%s",
                index + 1,
                len(self._steps),
                users,
                "unchanged" if probability is None else probability,
                "unchanged" if rps is None else rps,
            )

        return (users, spawn_rate)
