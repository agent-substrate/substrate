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

"""Single source of truth for boomer-tunable runtime flags.

Defines the flags once and exposes them three ways:
  * init_boomer_config(): registers them on locust's CLI parser AND serves
    them at /boomer-config so the boomer-Go workers can fetch values the
    operator set in the web UI form.
  * build_config_json(): parses an argv list and returns the JSON payload
    that runner.py hands to boomer-glutton via --config-json in headless
    mode (no web UI to fetch from).

Keep _FLAGS aligned with internal/boomer/dynconfig.payload.
"""

import argparse
import json
import logging

from locust import events

logger = logging.getLogger(__name__)

# Boomer-tunable flags. CLI form ("--foo-bar") is converted to the attribute
# / JSON-key form ("foo_bar") by _attr(). Defaults apply only to locust's
# parser (so the web UI form has sensible values); build_config_json omits
# unset flags so boomer can fall back to its own defaults.
_FLAGS = ("--trace-probability", "--min-wait-time", "--max-wait-time")
_LOCUST_DEFAULTS = {"--min-wait-time": 0.0, "--max-wait-time": 0.5}


def _attr(flag):
    return flag.lstrip("-").replace("-", "_")


def _add_args(parser, locust_aware):
    """Add the boomer flags to `parser`. Locust's configargparse accepts
    env_var/include_in_web_ui; stdlib argparse doesn't — gate via
    locust_aware."""
    for flag in _FLAGS:
        kwargs = {"type": float}
        if locust_aware:
            kwargs["env_var"] = "LOCUST_" + _attr(flag).upper()
            kwargs["include_in_web_ui"] = True
            if flag in _LOCUST_DEFAULTS:
                kwargs["default"] = _LOCUST_DEFAULTS[flag]
        parser.add_argument(flag, **kwargs)


def build_config_json(argv):
    """Parse `argv` and return the JSON config payload for boomer-glutton's
    --config-json flag. Unknown args are ignored; unset flags are omitted so
    boomer falls back to its own defaults."""
    p = argparse.ArgumentParser(add_help=False)
    _add_args(p, locust_aware=False)
    parsed, _ = p.parse_known_args(argv)
    cfg = {
        _attr(f): getattr(parsed, _attr(f))
        for f in _FLAGS
        if getattr(parsed, _attr(f)) is not None
    }
    return json.dumps(cfg) if cfg else ""


def init_boomer_config():
    """Register boomer-tunable CLI flags with locust's parser and expose
    them at /boomer-config so boomer-Go workers can fetch them at runtime.
    Idempotent within a single locust process via locust's event system."""

    @events.init_command_line_parser.add_listener
    def on_init_parser(parser):
        _add_args(parser, locust_aware=True)

    @events.init.add_listener
    def on_init(environment, **kwargs):
        if environment.web_ui is None:
            # Headless / worker process: no Flask app to register against.
            # runner.py forwards the same flags to boomer via --config-json.
            return

        @environment.web_ui.app.route("/boomer-config")
        def boomer_config():
            opts = environment.parsed_options
            return {_attr(f): getattr(opts, _attr(f), None) for f in _FLAGS}

        logger.info("Registered /boomer-config endpoint for boomer workers")
