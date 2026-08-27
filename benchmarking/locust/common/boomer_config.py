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

"""Boomer-tunable runtime flags.

Flag registration lives in the modules that own each flag:
  * --trace-probability             → common.trace.init_tracing
  * --min-wait-time / --max-wait-time → common.wait_time.init_wait_time
  * --resume-mode                   → common.resume_mode.add_resume_mode_arguments
  * --durdir-*                      → common.durdir_config.add_durdir_arguments

This module ties them together so boomer-Go workers can pick up the values
the operator set in the web UI form:
  * init_boomer_config(): ensures the owning init_*() hooks have run, then
    serves the current parsed values at /boomer-config on the master.
  * build_config_json(): parses an argv list and returns the JSON payload
    that runner.py hands to boomer-glutton via --config-json in headless
    mode (no web UI to fetch from).

Keep _FLAGS aligned with internal/benchmarking/boomer/dynconfig.payload.
"""

import argparse
import json
import logging
from collections.abc import Iterable

logger = logging.getLogger(__name__)

# Boomer-tunable flags and their types. CLI form ("--foo-bar") is converted
# to the attribute / JSON-key form ("foo_bar") by _attr().
_FLAGS = {
    "--trace-probability": float,
    "--min-wait-time": float,
    "--max-wait-time": float,
    "--durdir-file-size-bytes": int,
    "--resume-mode": str,
    "--durdir-read-mode": str,
    "--durdir-template": str,
}


def _attr(flag: str) -> str:
    return flag.lstrip("-").replace("-", "_")


def build_config_json(argv: Iterable[str]) -> str:
    """Parse `argv` and return the JSON config payload for boomer-glutton's
    --config-json flag. Unknown args are ignored; unset flags are omitted so
    boomer falls back to its own defaults."""
    p = argparse.ArgumentParser(add_help=False)
    for flag, type_func in _FLAGS.items():
        p.add_argument(flag, type=type_func)
    parsed, _ = p.parse_known_args(argv)
    cfg = {
        _attr(f): getattr(parsed, _attr(f))
        for f in _FLAGS
        if getattr(parsed, _attr(f)) is not None
    }
    return json.dumps(cfg) if cfg else ""


def init_boomer_config() -> None:
    """Ensure the owning modules have registered the boomer-tunable flags,
    then expose their current values at /boomer-config so boomer-Go workers
    can fetch them at runtime."""
    from locust import events
    from locust.env import Environment

    from common.durdir_config import add_durdir_arguments
    from common.resume_mode import add_resume_mode_arguments
    from common.trace import init_tracing
    from common.wait_time import init_wait_time

    init_tracing()
    init_wait_time()

    @events.init.add_listener
    def on_init(environment: Environment, **kwargs) -> None:
        if environment.web_ui is None:
            # Headless / worker process: no Flask app to register against.
            # runner.py forwards the same flags to boomer via --config-json.
            return

        @environment.web_ui.app.route("/boomer-config")
        def boomer_config() -> dict[str, float | int | str | None]:
            opts = environment.parsed_options
            return {_attr(f): getattr(opts, _attr(f), None) for f in _FLAGS}

        logger.info("Registered /boomer-config endpoint for boomer workers")
