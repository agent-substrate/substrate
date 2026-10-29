#!/usr/bin/env python3

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""End-to-end test for pluggable actor egress.

Checks two things:
  * POSITIVE: a real, running Actor's plain-HTTP egress is tunneled
    (nftables -> atunnel -> mTLS + CONNECT) through the egress gateway to an
    in-cluster target, and the gateway authenticates the actor certificate
    against the ate API (HTTP 200).
  * NEGATIVE: a pod with a valid pod identity, but no actor certificate,
    cannot open a tunnel. The gateway trusts only the actor-identity CA, so it
    refuses the mTLS handshake.

Prerequisites: a substrate cluster with the egress demo deployed, plus
kubectl, kubectl-ate and python3 on PATH. The install's
--ingress-allowed-spiffe-ids must include
spiffe://cluster.local/ns/ate-system/sa/atenet-client. See
demos/egress/README.md.

Usage:
  demos/egress/test-egress.py            # run the tests
  demos/egress/test-egress.py --cleanup  # remove everything this script created

Set KEEP=1 to leave the test resources in place after a run.
"""

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

CTX = os.environ.get("KUBECTL_CONTEXT", "kind-kind")
# The actor lives in the demo's atespace: --template resolves the template by
# name within the actor's own atespace.
ATESPACE = os.environ.get("ATESPACE", "ate-demo-egress")
ACTOR = os.environ.get("ACTOR", "egress-demo")
TEMPLATE = os.environ.get("TEMPLATE", "egress")
TARGET_NS = os.environ.get("TARGET_NS", "egress-target")
TARGET_PORT = os.environ.get("TARGET_PORT", "80")
PROBE_POD = "egress-identity-probe"
PROXY_PORT = 18099
GATEWAY_URL = "https://atenet-egress.ate-system.svc:443"

# curl exit codes when the gateway rejects the probe's client certificate:
#   35 CURLE_SSL_CONNECT_ERROR: the handshake failed with an alert.
#   55 CURLE_SEND_ERROR, 56 CURLE_RECV_ERROR: in TLS 1.3 the client finishes
#     its side of the handshake before the server checks the certificate, so
#     the alert or close can arrive while curl sends or reads the CONNECT.
# Anything else, such as 28 (timeout), means the probe never got the gateway
# to judge its certificate.
TLS_REJECTED_EXIT_CODES = {35, 55, 56}

EGRESS_POLICY = """\
rules:
- http: {hostnames: ["*"], ports: {all: {}}}
- https: {hostnames: ["*"]}
"""

PROBE_POD_MANIFEST = f"""\
apiVersion: v1
kind: Pod
metadata:
  name: {PROBE_POD}
  namespace: ate-system
  labels:
    # NetworkPolicy/atenet-egress admits ingress on https only from pods with
    # this label. WorkerPoolSyncer ignores a pod whose pool does not exist.
    ate.dev/worker-pool: {PROBE_POD}
spec:
  containers:
  - name: curl
    image: curlimages/curl:latest
    command: ["sleep", "600"]
    volumeMounts:
    - {{ name: podidentity, mountPath: /run/podidentity.podcert.ate.dev, readOnly: true }}
    - {{ name: servicedns,  mountPath: /run/servicedns.podcert.ate.dev,  readOnly: true }}
  volumes:
  - name: podidentity
    projected:
      sources:
      - podCertificate:
          signerName: podidentity.podcert.ate.dev/identity
          keyType: ECDSAP256
          credentialBundlePath: credential-bundle.pem
  - name: servicedns
    projected:
      sources:
      - clusterTrustBundle:
          signerName: servicedns.podcert.ate.dev/identity
          labelSelector: {{ matchLabels: {{ podcert.ate.dev/canarying: live }} }}
          path: trust-bundle.pem
"""

failed = False


def log(msg):
    print(f"\n\033[1;36m== {msg}\033[0m", flush=True)


def info(msg):
    print(f"   {msg}", flush=True)


def passed(msg):
    print(f"\033[1;32mPASS\033[0m {msg}", flush=True)


def fail(msg):
    global failed
    failed = True
    print(f"\033[1;31mFAIL\033[0m {msg}", flush=True)


def run(*args, check=True, stdin=None, capture=True):
    """Runs a command. Output is captured unless capture is False."""
    return subprocess.run(
        args,
        check=check,
        input=stdin,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )


def kubectl(*args, **kwargs):
    return run("kubectl", "--context", CTX, *args, **kwargs)


def kate(*args, **kwargs):
    return run("kubectl-ate", "--context", CTX, *args, **kwargs)


def cleanup():
    log("cleanup")
    kubectl("-n", "ate-system", "delete", "pod", PROBE_POD, "--ignore-not-found", "--wait=false", check=False)
    kate("suspend", "actor", ACTOR, "-a", ATESPACE, check=False)
    kate("delete", "actor", ACTOR, "-a", ATESPACE, check=False)
    kubectl("delete", "namespace", TARGET_NS, "--ignore-not-found", "--wait=false", check=False)
    info("done")


class EgressLog:
    """Reads the egress gateway's access log lines."""

    def __init__(self, dataplane):
        self.dataplane = dataplane
        if dataplane == "envoy":
            self.pattern = re.compile(r"\[egress\]")
        else:
            self.pattern = re.compile(r"substrate\.connect\.authority")

    def lines(self):
        out = kubectl(
            "-n", "ate-system", "logs", "deployment/atenet-egress",
            "-c", self.dataplane, "--tail=-1", check=False,
        ).stdout or ""
        return [line for line in out.splitlines() if self.pattern.search(line)]

    def since(self, before):
        return self.lines()[before:]

    def wait_for(self, before, pattern, attempts=10):
        """Returns the last line after before that matches pattern, or None.

        Retries because the log can lag the request.
        """
        regex = re.compile(pattern)
        for _ in range(attempts):
            matches = [line for line in self.since(before) if regex.search(line)]
            if matches:
                return matches[-1]
            time.sleep(1)
        return None


def wait_for_port(port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def actor_running():
    out = kate("get", "actors", "-a", ATESPACE, check=False).stdout or ""
    return any(ACTOR in line and "ACTOR_STATE_RUNNING" in line for line in out.splitlines())


def preflight():
    log("preflight: egress gateway is running")
    kubectl("-n", "ate-system", "rollout", "status", "deployment/atenet-egress", "--timeout=120s", capture=False)
    dataplane = kubectl(
        "-n", "ate-system", "get", "deployment/atenet-egress",
        "-o", "jsonpath={.spec.template.spec.containers[0].name}",
    ).stdout.strip()
    if dataplane not in ("envoy", "agentgateway"):
        sys.exit(f"unsupported egress dataplane container: {dataplane}")
    info(f"dataplane = {dataplane}")
    return dataplane


def deploy_target():
    log("deploy an in-cluster HTTP target (whoami)")
    kubectl("create", "namespace", TARGET_NS, check=False)
    kubectl("-n", TARGET_NS, "create", "deployment", "whoami", "--image=traefik/whoami", check=False)
    kubectl("-n", TARGET_NS, "expose", "deployment", "whoami", f"--port={TARGET_PORT}", "--target-port=80", check=False)
    kubectl("-n", TARGET_NS, "rollout", "status", "deployment/whoami", "--timeout=120s", capture=False)
    target_ip = kubectl("-n", TARGET_NS, "get", "svc", "whoami", "-o", "jsonpath={.spec.clusterIP}").stdout.strip()
    info(f"target = {target_ip}:{TARGET_PORT}")
    return target_ip


def start_actor():
    log(f"create + resume Actor {ATESPACE}/{ACTOR}, allow all its egress")
    kate("create", "atespace", ATESPACE, check=False)
    kate("create", "actor", ACTOR, "-a", ATESPACE, "--template", TEMPLATE, check=False)
    kate("resume", "actor", ACTOR, "-a", ATESPACE, check=False)
    kate("create", "egress-policy", ACTOR, "-a", ATESPACE, "-f", "-", stdin=EGRESS_POLICY, check=False)
    for _ in range(30):
        if actor_running():
            break
        time.sleep(3)
    if not actor_running():
        sys.exit("actor did not reach RUNNING")
    info(f"actor {ATESPACE}/{ACTOR} is RUNNING")


def actor_round_trip(target_ip):
    """Asks the actor to fetch the target. Returns (HTTP status, body)."""
    with tempfile.NamedTemporaryFile(prefix="egress-proxy-", suffix=".log", delete=False) as proxy_log:
        proxy = subprocess.Popen(
            ["kubectl-ate", "--context", CTX, "proxy", "actor", ACTOR, f"{PROXY_PORT}:80", "-a", ATESPACE],
            stdout=proxy_log,
            stderr=subprocess.STDOUT,
        )
    try:
        if not wait_for_port(PROXY_PORT, timeout=15):
            info(f"kubectl-ate proxy did not listen on {PROXY_PORT}; see {proxy_log.name}")
            return 0, ""
        request = urllib.request.Request(
            f"http://127.0.0.1:{PROXY_PORT}/",
            data=json.dumps({"url": f"http://{target_ip}:{TARGET_PORT}/"}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, response.read().decode(errors="replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode(errors="replace")
        except (urllib.error.URLError, OSError) as e:
            info(f"request to the actor failed: {e}")
            return 0, ""
    finally:
        proxy.terminate()
        try:
            proxy.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proxy.kill()


def positive_check(egress_log, target_ip):
    log("POSITIVE: real Actor egress is tunneled through the gateway (expect 200)")
    before = len(egress_log.lines())
    status, body = actor_round_trip(target_ip)
    info(f"actor round-trip HTTP {status}")
    if status == 200:
        passed("actor fetched the target (HTTP 200)")
    else:
        fail(f"expected HTTP 200, got {status}")

    gateway_ip = kubectl(
        "-n", "ate-system", "get", "pod", "-l", "app=atenet-egress",
        "-o", "jsonpath={.items[0].status.podIP}",
    ).stdout.strip()
    if f"RemoteAddr: {gateway_ip}" in body:
        passed(f"target saw the egress gateway ({gateway_ip}) as its client: traffic went through the gateway")
    else:
        seen = re.search(r"RemoteAddr: [0-9.]*", body)
        info(f"target body {seen.group(0) if seen else 'RemoteAddr: ?'} (gateway IP {gateway_ip})")

    if egress_log.dataplane == "envoy":
        # Envoy's access log identifies the peer by its certificate SAN, not by
        # any header the actor could have written.
        allowed = rf"worker-for-actor/{re.escape(ATESPACE)}/{re.escape(ACTOR)}.*code=200"
    else:
        # agentgateway's structured access log records the CONNECT authority.
        # Actor authentication is also covered by the successful request and
        # the negative check.
        allowed = rf"substrate\.connect\.authority.*{re.escape(f'{target_ip}:{TARGET_PORT}')}"
    line = egress_log.wait_for(before, allowed)
    if line:
        passed(f"gateway logged the CONNECT: {line}")
    else:
        fail(f"{egress_log.dataplane} did not log the allowed CONNECT")


def negative_check(egress_log, target_ip):
    log("NEGATIVE: a pod identity is not an actor identity (expect a refused handshake)")
    kubectl("apply", "-f", "-", stdin=PROBE_POD_MANIFEST)
    kubectl("-n", "ate-system", "wait", "--for=condition=Ready", f"pod/{PROBE_POD}", "--timeout=60s")

    before = len(egress_log.lines())
    # kubectl exec exits with curl's exit code. %{http_connect} is the code the
    # proxy answered CONNECT with, and stays 000 when the tunnel never opens.
    probe = kubectl(
        "-n", "ate-system", "exec", PROBE_POD, "--",
        "curl", "-s", "-o", "/dev/null", "-w", "%{http_connect}",
        "--connect-timeout", "5", "--max-time", "10",
        "--proxy-cacert", "/run/servicedns.podcert.ate.dev/trust-bundle.pem",
        "--proxy-cert", "/run/podidentity.podcert.ate.dev/credential-bundle.pem",
        "--proxy-key", "/run/podidentity.podcert.ate.dev/credential-bundle.pem",
        "--proxytunnel", "-x", GATEWAY_URL,
        f"http://{target_ip}:{TARGET_PORT}/",
        check=False,
    )
    http_connect = probe.stdout.strip() or "000"
    exit_code = probe.returncode
    info(f"pod-identity CONNECT attempt: http_connect={http_connect} exit={exit_code}")

    # A rejected handshake never becomes an HTTP request, so it writes no
    # access-log line. Print whatever the gateway logged so a real failure can
    # be diagnosed, but do not assert on it.
    new_lines = egress_log.since(before)
    if new_lines:
        info(f"gateway egress log since the probe: {new_lines[-1]}")

    if http_connect == "000" and exit_code in TLS_REJECTED_EXIT_CODES:
        passed(f"a pod identity cannot open an egress tunnel (refused at TLS, curl exit={exit_code})")
        if not new_lines:
            info("no new egress access-log lines: the handshake was refused before HTTP, as expected")
    elif http_connect == "200":
        fail("expected the gateway to refuse a non-actor client certificate, but CONNECT returned 200")
    else:
        fail(
            f"expected a TLS rejection (http_connect=000, exit in {sorted(TLS_REJECTED_EXIT_CODES)}), "
            f"got http_connect={http_connect} exit={exit_code}"
        )


def main():
    parser = argparse.ArgumentParser(description="End-to-end test for pluggable actor egress.")
    parser.add_argument("--cleanup", action="store_true", help="remove everything this script created and exit")
    args = parser.parse_args()

    for tool in ("kubectl", "kubectl-ate"):
        if shutil.which(tool) is None:
            sys.exit(f"missing required tool: {tool}")

    if args.cleanup:
        cleanup()
        return

    try:
        egress_log = EgressLog(preflight())
        target_ip = deploy_target()
        start_actor()
        positive_check(egress_log, target_ip)
        negative_check(egress_log, target_ip)
    except subprocess.CalledProcessError as e:
        print(f"command failed (exit {e.returncode}): {' '.join(e.cmd)}", file=sys.stderr)
        if e.stderr:
            print(e.stderr, file=sys.stderr)
        sys.exit(1)
    finally:
        if os.environ.get("KEEP") != "1":
            cleanup()

    print()
    if failed:
        print("\033[1;31mSOME CHECKS FAILED\033[0m")
        sys.exit(1)
    print("\033[1;32mALL CHECKS PASSED\033[0m: pluggable egress + identity authentication working.")


if __name__ == "__main__":
    main()
