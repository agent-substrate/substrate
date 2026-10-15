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
import socket
import sys
import time
import urllib.parse
import urllib.request
import http.client

import dns.resolver


def read_ndots():
    try:
        with open("/etc/resolv.conf", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("options "):
                    for part in line[len("options "):].split():
                        if part.startswith("ndots:"):
                            return int(part[len("ndots:"):])
    except Exception:
        pass
    return 1


def run_attempt(runtime, op, host, url, res):
    start = time.monotonic()
    try:
        if op == "http_get":
            parsed = urllib.parse.urlparse(url)
            target_host = parsed.hostname or ""
            target_port = parsed.port or 80
            target_path = parsed.path or "/"
            if parsed.query:
                target_path += "?" + parsed.query

            if runtime == "python-dnspython":
                answer = res.resolve(target_host, "A")
                ip = answer[0].to_text()
                conn = http.client.HTTPConnection(ip, target_port, timeout=8)
                conn.request("GET", target_path, headers={"Host": target_host})
                resp = conn.getresponse()
                resp.read()
                status_code = resp.status
                conn.close()
            else:
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=8) as resp:
                    status_code = resp.status

            dur = int((time.monotonic() - start) * 1000)
            return {
                "success": 200 <= status_code < 400,
                "statusCode": status_code,
                "durationMs": dur,
            }

        if runtime == "python-dnspython":
            # Use search=True when the hostname has fewer dots than resolv.conf ndots
            # so resolv.conf search domains are exercised.
            ndots = read_ndots()
            dots = host.rstrip(".").count(".")
            use_search = (not host.endswith(".")) and (dots < ndots)
            answer = res.resolve(host, "A", search=use_search)
            addrs = [r.to_text() for r in answer]
            dur = int((time.monotonic() - start) * 1000)
            return {
                "success": True,
                "addresses": addrs,
                "durationMs": dur,
            }
        else:
            infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
            addrs = []
            for info in infos:
                ip = info[4][0]
                if ip not in addrs:
                    addrs.append(ip)
            dur = int((time.monotonic() - start) * 1000)
            return {
                "success": True,
                "addresses": addrs,
                "durationMs": dur,
            }
    except socket.gaierror as e:
        dur = int((time.monotonic() - start) * 1000)
        return {
            "success": False,
            "errorCode": f"gaierror:{e.errno}",
            "error": f"socket.gaierror: {e}",
            "durationMs": dur,
        }
    except dns.resolver.NoNameservers as e:
        dur = int((time.monotonic() - start) * 1000)
        return {
            "success": False,
            "errorCode": "NoNameservers",
            "error": f"dns.resolver.NoNameservers: {e}",
            "durationMs": dur,
        }
    except Exception as e:
        dur = int((time.monotonic() - start) * 1000)
        cls_name = f"{type(e).__module__}.{type(e).__qualname__}"
        return {
            "success": False,
            "errorCode": type(e).__name__,
            "error": f"{cls_name}: {e}",
            "durationMs": dur,
        }


def main():
    req = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
    runtime = req.get("runtime", "python-socket")
    op = req.get("op", "resolve")
    host = req.get("host", "")
    url = req.get("url", "")
    attempts = req.get("attempts", 1) or 1
    interval_ms = req.get("intervalMs", 0) or 0
    enable_cache = bool(req.get("enableCache", False))

    res = dns.resolver.Resolver(configure=True)
    res.timeout = 1.5
    res.lifetime = 3.0
    if enable_cache:
        res.cache = dns.resolver.Cache()

    results = []
    for i in range(attempts):
        if i > 0 and interval_ms > 0:
            time.sleep(interval_ms / 1000.0)
        results.append(run_attempt(runtime, op, host, url, res))

    print(json.dumps({"attempts": results}))


if __name__ == "__main__":
    main()
