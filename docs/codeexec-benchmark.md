# Agent Substrate — codeexec Actor Lifecycle Benchmark

A focused performance evaluation of Agent Substrate's actor lifecycle (create,
cold activation, resume, suspend, warm serving) on a proof-of-concept GKE
cluster, using a Python code-execution service packaged as a Substrate actor.

- **Date:** 2026-05-29
- **Cluster:** `substrate-poc` (project `project-12a68715-1e4f-4b3c-9cd`)
- **Tooling:** `kubectl-ate` (built from this repo) + HTTP through `atenet-router`

> These numbers are a baseline from a small POC cluster, not a tuned or
> at-scale result. See [Caveats](#caveats--limitations).

---

## 1. Test setup

### 1.1 Infrastructure

| Component | Value |
|---|---|
| GKE cluster | `substrate-poc`, zonal `us-central1-c` |
| Cluster / node version | `1.35.3-gke.1389002` |
| Node pool | `substrate-node-pool`, machine type `e2-standard-2` (2 vCPU / 8 GB), 2 nodes |
| Sandboxing | gVisor `runsc` **inside the worker pods** (downloaded per the ActorTemplate `runsc` block) — *not* GKE Sandbox at the node level |
| Snapshot store | GCS bucket `snapshot-substrate-test-project-12a68715-1e4f-4b3c-9cd` |
| State store | ValKey/Redis cluster (6 pods) in `ate-system` |
| Image registry | Artifact Registry `us-central1-docker.pkg.dev/.../ate-images` |

### 1.2 Substrate system (namespace `ate-system`)

All components healthy at test time: `ate-api-server`, `ate-controller`,
`atelet` (DaemonSet, 2 pods), `atenet-router` (Envoy + ext-proc), `dns`, and the
6-node ValKey cluster.

### 1.3 Workload under test (namespace `ate-demo-codeexec`)

- **WorkerPool** `codeexec-workerpool` — **2 replicas** (this is the key capacity
  constraint for the tests below).
- **ActorTemplate** `codeexec-template` — golden snapshot `Ready`. Runs the
  container image `code-exec:actor-v1`:
  `uvicorn app:app --host 0.0.0.0 --port 80`, a FastAPI service exposing:
  - `GET /healthz` → `{"status":"ok"}`
  - `POST /execute {code,...}` → runs `python3 -I -B` in a subprocess with
    rlimits + wall-clock timeout, returns stdout/stderr/exit_code/duration.

### 1.4 Access paths

- **Data plane (HTTP):** `kubectl port-forward -n ate-system svc/atenet-router
  8000:80`, then requests carry `Host: <actor-id>.actors.resources.substrate.ate.dev`.
  The router extracts the actor ID, looks up its location, and triggers a resume
  if the actor is suspended.
- **Control plane:** `kubectl-ate {create,resume,suspend,delete,get} actor`.
  With no `--endpoint`, `kubectl-ate` auto-port-forwards to the `api` service
  (mTLS gRPC, :443) for each invocation.

### 1.5 How to reproduce

```bash
cd ~/substrate
source .ate-dev-env.sh
export PATH="$HOME/go/bin:$PATH"

# build + install the CLI
go install ./cmd/kubectl-ate

# data-plane access (leave running in the background)
kubectl port-forward -n ate-system svc/atenet-router 8000:80 &

# run the benchmarks (scripts in Appendix A/B)
python3 bench.py 5     # create / cold-activation / warm serving
python3 cycle.py 8     # control-plane resume/suspend cycles
```

---

## 2. Tests performed

| # | Test | What it measures | Method |
|---|---|---|---|
| 1 | **Create actor** | Time to register a new actor from the template | wall-time of `kubectl-ate create actor` |
| 2 | **Cold activation** | Suspended actor → first HTTP 200 (full user-visible cold start) | suspend, then poll `GET /healthz` through the router until 200 |
| 3 | **Resume (control-plane)** | Suspended → RUNNING via explicit API | wall-time of `kubectl-ate resume actor` (synchronous) |
| 4 | **Suspend (control-plane)** | RUNNING → SUSPENDED incl. checkpoint + GCS upload | wall-time of `kubectl-ate suspend actor` (synchronous) |
| 5 | **Warm request** | Steady-state routing + serving latency | 40× `GET /healthz` on a running actor |
| 6 | **Warm `/execute`** | Per-request Python subprocess spawn under gVisor | 20× `POST /execute {"code":"print(1+1)"}` |
| — | **State persistence** | Filesystem survives suspend/resume | write `/tmp/state.txt`, suspend, resume, read back |

Because the pool has only **2 workers**, lifecycle tests were run **sequentially
with ≤1 actor active at a time** to avoid worker starvation. Concurrency /
throughput-under-load was **not** measured (see Caveats).

---

## 3. Results

### 3.1 Summary

| Operation | p50 | p95 | min | max | n |
|---|---|---|---|---|---|
| Create actor | 181 ms | 393 ms | 145 ms | 439 ms | 5 |
| **Cold activation** (suspend → first HTTP 200) | **6.19 s** | 7.42 s | 6.11 s | 7.54 s | 5 |
| Resume (control-plane) | 5.73 s | 6.14 s | 5.54 s | 6.27 s | 8 |
| Suspend (control-plane) | 1.42 s | 1.53 s | 1.35 s | 1.54 s | 8 |
| Warm request (`GET /healthz`) | 26 ms | 32 ms | 23 ms | 37 ms | 40 |
| Warm `/execute` (Python in gVisor) | 172 ms | 219 ms | 143 ms | 227 ms | 20 |

### 3.2 Raw per-sample data

**Cold activation (test 2):**

```
codeexec-bench-1: 6967 ms
codeexec-bench-2: 7535 ms
codeexec-bench-3: 6106 ms
codeexec-bench-4: 6178 ms
codeexec-bench-5: 6189 ms
```

**Resume / suspend cycles (tests 3 & 4), 8 cycles on one actor:**

```
cycle 1: resume=6268 ms  suspend=1448 ms
cycle 2: resume=5901 ms  suspend=1535 ms
cycle 3: resume=5605 ms  suspend=1354 ms
cycle 4: resume=5711 ms  suspend=1524 ms
cycle 5: resume=5853 ms  suspend=1486 ms
cycle 6: resume=5682 ms  suspend=1387 ms
cycle 7: resume=5745 ms  suspend=1365 ms
cycle 8: resume=5541 ms  suspend=1368 ms
```

**State persistence:** PASS — `/tmp/state.txt` written before suspend
(`hello-from-before-suspend`) was returned intact after resume onto a fresh
worker, confirming memory+disk snapshot/restore via GCS.

### 3.3 Interpretation

- **Cold activation ≈ 6–7.5 s.** This is the snapshot-restore path: claim a warm
  worker → download snapshot from GCS → `runsc` restore of memory+disk → serve.
  Cold activation ≈ resume (5.7 s) + the extra HTTP route/retry hop. It is far
  above Substrate's aspirational **100 ms p95** activation north-star — expected
  on small `e2-standard-2` nodes pulling snapshots from GCS. Likely levers:
  worker-local/peer snapshot caching, larger nodes, smaller snapshots.
- **Suspend ≈ 1.4 s**, very low variance — checkpoint + GCS upload.
- **Warm path is healthy:** ~26 ms router (Envoy) overhead; `/execute` ~172 ms is
  dominated by per-request `python3` process spawn under gVisor (consistent with
  the standalone code-exec service's ~122–178 ms).
- **Create** is cheap (~180 ms) — just a control-plane/DB record + golden
  snapshot reference, no compute scheduled.

---

## 4. Caveats / limitations

- **Pool size = 2.** Tests were sequential; **no concurrency or throughput
  numbers** were collected. That requires a larger pool or the `benchmarking/`
  Locust + Prometheus/Grafana stack.
- **Client-observed latency.** Control-plane timings include `kubectl-ate`'s
  per-call port-forward setup to the `api` service, so they slightly overstate
  pure server-side latency.
- **Measurement artifact corrected.** An initial run reported every *suspend* as
  `n/a`. This was **not** a real failure: `kubectl-ate` opens a fresh
  port-forward per call, and polling actor status ~4×/s made the polls flaky.
  Timing the synchronous `suspend`/`resume` commands directly (Appendix B) gave
  the clean, consistent numbers above (8/8 `ok`); a manual suspend completed in
  ~1 s.
- **Small sample sizes** (n=5–40) — directional, not statistically rigorous.
- Single workload (Python/FastAPI). Snapshot size and restore time will vary by
  actor image and resident memory.

---

## Appendix A — `bench.py` (create / cold-activation / warm serving)

```python
#!/usr/bin/env python3
"""Substrate lifecycle benchmark via kubectl-ate + atenet-router.
Keeps <=1 actor active at a time so the 2-worker pool never starves."""
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
```

> Note: the `suspend` column printed by this script can show `n/a` due to the
> status-polling artifact described in Caveats. Use `cycle.py` (Appendix B) for
> reliable suspend/resume latency.

## Appendix B — `cycle.py` (control-plane resume/suspend)

```python
#!/usr/bin/env python3
"""Measure control-plane resume/suspend latency by timing the (synchronous)
kubectl-ate commands directly. Single actor, sequential cycles -> no pool churn."""
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
```
