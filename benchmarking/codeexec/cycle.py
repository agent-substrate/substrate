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

"""Measure control-plane resume/suspend latency by timing the (synchronous)
kubectl-ate commands directly. Single actor, sequential cycles -> no pool churn.

This avoids the status-polling artifact in bench.py (kubectl-ate opens a fresh
port-forward per call, so high-frequency `get` polling is flaky). See
docs/codeexec-benchmark.md.

Usage:  python3 cycle.py [N_cycles]
"""
import subprocess, time, sys

ACTOR = "codeexec-1"
CYCLES = int(sys.argv[1]) if len(sys.argv) > 1 else 8

def kate(*args):
    t0 = time.monotonic()
    r = subprocess.run(["kubectl-ate", *args], capture_output=True, text=True, timeout=120)
    return time.monotonic()-t0, r.returncode, (r.stdout+r.stderr)

def ms(s): return f"{s*1000:.0f} ms"
def pctl(xs, p):
    xs=sorted(xs); k=(len(xs)-1)*p/100; f=int(k); c=min(f+1,len(xs)-1)
    return xs[f]+(xs[c]-xs[f])*(k-f)
def summ(name, xs):
    print(f"[{name}] n={len(xs)}  p50={ms(pctl(xs,50))}  p95={ms(pctl(xs,95))}  min={ms(min(xs))}  max={ms(max(xs))}")

# make sure it's suspended to start
kate("suspend", "actor", ACTOR)
print(f"== control-plane resume/suspend, {CYCLES} cycles on {ACTOR} ==\n", flush=True)
res, sus = [], []
for i in range(1, CYCLES+1):
    rt, rc, _ = kate("resume", "actor", ACTOR)
    st, sc, _ = kate("suspend", "actor", ACTOR)
    ok = "ok" if (rc==0 and sc==0) else f"rc={rc},sc={sc}"
    if rc==0: res.append(rt)
    if sc==0: sus.append(st)
    print(f"  cycle {i}: resume={ms(rt)}  suspend={ms(st)}  [{ok}]", flush=True)
print()
if res: summ("resume (control-plane)", res)
if sus: summ("suspend (control-plane)", sus)
