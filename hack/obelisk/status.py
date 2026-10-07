#!/usr/bin/env python3

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

import json
import os
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


WORKLOADS = (
    ("statefulset", "postgres"),
    ("deployment", "ate-api-server"),
    ("deployment", "ate-controller"),
)


def workload_status():
    result = {}
    healthy = True
    for kind, name in WORKLOADS:
        command = [
            "/usr/local/bin/k3s",
            "kubectl",
            "-n",
            "ate-system",
            "get",
            kind,
            name,
            "-o",
            "json",
        ]
        try:
            resource = json.loads(subprocess.run(
                command, check=True, capture_output=True, text=True, timeout=5
            ).stdout)
            desired = resource.get("spec", {}).get("replicas", 1)
            ready = resource.get("status", {}).get("readyReplicas", 0)
            is_ready = desired == ready
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
            desired, ready, is_ready = 1, 0, False
        result[name] = {"ready": is_ready, "replicas": f"{ready}/{desired}"}
        healthy = healthy and is_ready
    return healthy, result


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        healthy, workloads = workload_status()
        body = json.dumps({
            "status": "ok" if healthy else "degraded",
            "experimental": True,
            "services": workloads,
            "tailnet": {
                "configured": False,
                "ateapiEndpoint": "<tailscale-ip>:30443",
                "tlsServerName": "api.ate-system.svc",
            },
        }).encode()
        self.send_response(200 if healthy else 503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(os.environ["PORT"])), Handler).serve_forever()
