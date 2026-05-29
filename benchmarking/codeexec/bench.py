#!/usr/bin/env python3
#  Copyright 2026 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Substrate lifecycle benchmark via kubectl-ate + atenet-router.

Measures create / cold-activation / warm-serving latency for the codeexec
actor template. Keeps <=1 actor active at a time so the 2-worker pool never
starves. See docs/codeexec-benchmark.md for setup and results.

Prereqs: kubectl-ate on PATH, and a port-forward to the router, e.g.
    kubectl port-forward -n ate-system svc/atenet-router 8000:80
Usage:  python3 bench.py [N_actors]
"""
import json, subprocess, time, urllib.request, sys

NS = "ate-demo-codeexec"
TPL = f"{NS}/codeexec-template"
ROUTER = "http://localhost:8000"
DOMAIN = "actors.resources.substrate.ate.dev"

def kate(*args, timeout=120):
    return subprocess.run(["kubectl-ate", *args], capture_output=True, text=True, timeout=timeout)

def status(actor):
    r = kate("get", "actor", actor, "-o", "json")
    try: return json.loads(r.stdout).get("status", "?")
    except Exception: return "?"

def wait_status(actor, target, timeout=90):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if status(actor) == target: return time.monotonic() - t0
        time.sleep(0.25)
    return None

def http(actor, path="/healthz", method="GET", body=None):
    req = urllib.request.Request(f"{ROUTER}{path}", method=method,
        headers={"Host": f"{actor}.{DOMAIN}", "Content-Type": "application/json"},
        data=body.encode() if body else None)
    return urllib.request.urlopen(req, timeout=30)

def ensure_suspended(actor):
    if status(actor) != "STATUS_SUSPENDED":
        kate("suspend", "actor", actor); wait_status(actor, "STATUS_SUSPENDED", 90)

def cold_activation(actor):
    """Measure wall-clock from first request to first HTTP 200 (cold actor)."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < 90:
        try:
            if http(actor).status == 200: return time.monotonic() - t0
        except Exception: pass
        time.sleep(0.05)
    return None

def pctl(xs, p):
    if not xs: return None
    xs = sorted(xs); k = (len(xs)-1)*p/100; f = int(k); c = min(f+1, len(xs)-1)
    return xs[f] + (xs[c]-xs[f])*(k-f)

def ms(s): return "n/a" if s is None else f"{s*1000:.0f} ms"

def summarize(name, xs):
    ok = [x for x in xs if x is not None]
    if not ok: print(f"[{name}] no successful samples"); return
    print(f"[{name}] n={len(ok)}/{len(xs)}  p50={ms(pctl(ok,50))}  p95={ms(pctl(ok,95))}  min={ms(min(ok))}  max={ms(max(ok))}")

N = int(sys.argv[1]) if len(sys.argv) > 1 else 5
actors = [f"codeexec-bench-{i}" for i in range(1, N+1)]
print(f"== Substrate lifecycle benchmark (N={N}, pool=2 workers, e2-standard-2) ==\n", flush=True)

# free the pool: suspend the pre-existing demo actor
ensure_suspended("codeexec-1")

# 1. CREATE latency
create_t = []
for a in actors:
    t0 = time.monotonic(); kate("create", "actor", a, "-t", TPL); create_t.append(time.monotonic()-t0)
summarize("create", create_t)

# 2+5. COLD ACTIVATION + SUSPEND, one actor active at a time
cold, susp = [], []
for a in actors:
    ensure_suspended(a)                      # freshly created -> already suspended (fast)
    c = cold_activation(a); cold.append(c)
    wait_status(a, "STATUS_RUNNING", 90)
    t0 = time.monotonic(); kate("suspend", "actor", a); d = wait_status(a, "STATUS_SUSPENDED", 90)
    susp.append(d)
    print(f"   {a}: cold-activation={ms(c)}  suspend={ms(d)}", flush=True)
summarize("cold-activation", cold)
summarize("suspend", susp)

# 3+4. WARM request + code-exec latency on a single running actor
a = actors[0]
cold_activation(a); wait_status(a, "STATUS_RUNNING", 90)
warm = []
for _ in range(40):
    t0 = time.monotonic()
    try: http(a); warm.append(time.monotonic()-t0)
    except Exception: pass
summarize("warm-healthz", warm)
ex = []
for _ in range(20):
    t0 = time.monotonic()
    try: http(a, "/execute", "POST", '{"code":"print(1+1)"}'); ex.append(time.monotonic()-t0)
    except Exception: pass
summarize("warm-exec(POST /execute)", ex)

print("\n== cleanup ==", flush=True)
for a in actors: kate("delete", "actor", a)
print(f"deleted {len(actors)} bench actors")
