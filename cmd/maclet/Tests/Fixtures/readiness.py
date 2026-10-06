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

"""Guest-only HTTP smoke fixture, not the Substrate guest agent."""

from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import TCPServer


class ReadinessServer(HTTPServer):
    def server_bind(self):
        # HTTPServer resolves the guest hostname here. DNS may not be available
        # during cold boot; this fixture only needs an IP listener.
        TCPServer.server_bind(self)
        self.server_name = "maclet-smoke"
        self.server_port = self.server_address[1]


class Readiness(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if self.path == "/ready" else 404)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"service":"maclet-smoke-fixture"}\n')


ReadinessServer(("0.0.0.0", 8123), Readiness).serve_forever()
