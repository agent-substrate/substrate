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
  * --mem-target / --mem-churn      → common.memload_config.add_memload_arguments

This module ties them together so boomer-Go workers can pick up the values
the operator set in the web UI form:
  * init_boomer_config(): ensures the owning init_*() hooks have run, then
    serves the current parsed values at /boomer-config on the master.
  * build_config_json(): parses an argv list and returns the JSON payload
    that runner.py hands to boomer-glutton via --config-json in headless
    mode (no web UI to fetch from).
  * serve_config_headless(): the same /boomer-config payload from a plain
    HTTP server, for a headless run whose values change while it runs.

Keep _FLAGS aligned with internal/benchmarking/boomer/dynconfig.payload.
"""

import argparse
import json
import logging
import threading
from collections.abc import Iterable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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
    "--mem-target": str,
    "--mem-churn": str,
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


def _payload(parsed_options) -> dict[str, float | int | str | None]:
    return {_attr(f): getattr(parsed_options, _attr(f), None) for f in _FLAGS}


def serve_config_headless(environment, port: int) -> None:
    """Serve /boomer-config on 127.0.0.1:`port` for a headless run.

    A headless master has no Flask app, thus init_boomer_config registers no
    route and runner.py gives boomer the values one time, through
    --config-json. That is sufficient for a run that holds one set of values
    from start to end, and not for a ladder: a step changes the sample rate
    in the middle of the run (see shapes/ladder_shape.py), and boomer reads
    the new value only from this endpoint.

    The handler reads the parsed options at each request, thus it gives the
    value of the step that runs, and not the value of the command line.

    boomer and locust are in one container in a headless run, thus the
    address of the loopback interface is sufficient and the endpoint is not
    on the network.
    """

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - name comes from the library
            if self.path.split("?", 1)[0] != "/boomer-config":
                self.send_error(404)
                return
            body = json.dumps(_payload(environment.parsed_options)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args) -> None:
            # One line for each fetch would be one line for each spawn
            # message, in the middle of the output of the run.
            pass

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info(f"Serving /boomer-config on 127.0.0.1:{port} for boomer workers")


def init_boomer_config() -> None:
    """Ensure the owning modules have registered the boomer-tunable flags,
    then expose their current values at /boomer-config so boomer-Go workers
    can fetch them at runtime."""
    from locust import events
    from locust.argument_parser import LocustArgumentParser
    from locust.env import Environment

    from common.durdir_config import add_durdir_arguments
    from common.memload_config import add_memload_arguments
    from common.resume_mode import add_resume_mode_arguments
    from common.trace import init_tracing
    from common.wait_time import init_wait_time

    init_tracing()
    init_wait_time()

    @events.init_command_line_parser.add_listener
    def on_init_parser(parser: LocustArgumentParser) -> None:
        parser.add_argument(
            "--boomer-config-port",
            type=int,
            default=0,
            env_var="LOCUST_BOOMER_CONFIG_PORT",
            help=(
                "Serve /boomer-config on this port of the loopback interface "
                "in a headless run. runner.py sets it, and gives the same "
                "port to boomer as --master-web-port, when the values of the "
                "run change while it runs."
            ),
        )

    @events.init.add_listener
    def on_init(environment: Environment, **kwargs) -> None:
        if environment.web_ui is None:
            # Headless / worker process: no Flask app to register against.
            # runner.py forwards the same flags to boomer via --config-json,
            # and starts serve_config_headless when the values of the run
            # change while it runs.
            port = getattr(environment.parsed_options, "boomer_config_port", 0)
            if port:
                serve_config_headless(environment, port)
            return

        @environment.web_ui.app.route("/boomer-config")
        def boomer_config() -> dict[str, float | int | str | None]:
            return _payload(environment.parsed_options)

        logger.info("Registered /boomer-config endpoint for boomer workers")
