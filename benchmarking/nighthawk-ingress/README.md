# Nighthawk ingress-capacity benchmark

## Objective

Measure the scale and performance of substrate's networking
infrastructure. This benchmark covers the ingress routing path
(`atenet-router --mode=ingress`); egress will be a separate benchmark.

The concrete measurement, per Envoy CPU configuration:

> **What is the maximum request rate the ingress path sustains while
> tail latency stays under the SLO?\***

The benchmark finds that number automatically using
[Nighthawk](https://github.com/envoyproxy/nighthawk) (Envoy's benchmarking
client) in
[adaptive mode](https://github.com/envoyproxy/nighthawk/blob/main/docs/root/adaptive_load_controller.md):
it ramps open-loop load until a threshold breaks, binary-searches the
boundary, and confirms with a longer run. Run
it across `envoyCpu` configs to get a capacity-per-core curve, or
continuously in CI to catch routing-path performance regressions.

\* *The SLO bounds latency **mean+2σ**, not a true percentile: Nighthawk's
adaptive search can only gate on its
[built-in metrics](https://github.com/envoyproxy/nighthawk/blob/main/docs/root/adaptive_load_controller.md#available-metric-plugins)
(success-rate, send-rate, rps, latency mean/stdev). mean+2σ ≈ p95–p97 for
bell-shaped latency; true p50/p95/p99 are still reported per stage, they
just don't steer the search.*

## What it measures

Every request exercises the **full production routing path** — created and
warmed sandboxed actors, Host-header routing, the ext_proc routing
decision, and the mTLS hop to the worker:

```mermaid
flowchart LR
    subgraph job["nighthawk runner Job"]
        alc["nighthawk_adaptive_load_client<br/>exponential ramp + binary search"]
        svc["nighthawk_service<br/>16 event loops, Host header<br/>rotated across all actors"]
        alc -->|gRPC| svc
    end

    subgraph pod["atenet-router pod — pinned to envoyCpu CPUs"]
        envoy["envoy :8080<br/>--concurrency envoyCpu"]
        extproc["atenet-router sidecar<br/>ext_proc routing decision"]
        envoy -->|request headers| extproc
        extproc -->|x-ate-original-dst| envoy
    end

    ateapi["ateapi<br/>worker assignment"]

    subgraph worker["worker pod — xN (workerCount), one running actor each"]
        atunnel["atunnel :443"] --> glutton["glutton actor<br/>POST /ping :80"]
    end

    svc -->|"HTTP :80, Host:<br/>actor-N.benchmark.actors..."| envoy
    extproc -->|ResumeActor| ateapi
    envoy -->|"mTLS :443"| atunnel
```

Client-measured latency therefore covers: network to Envoy, Envoy
processing, the routing decision in the ext_proc sidecar (including its
ateapi lookup), the mTLS hop, and the actor's handler.

**"Capacity" means all three of the following hold at once** — each threshold
catches a failure mode the others can't see:

| Threshold (tests.yaml knob) | Guards against | Typical symptom at the limit |
|---|---|---|
| `tailLatencySloMs` — latency mean+2σ ≤ X ms (~p95 proxy, see [Objective](#objective)) | the router getting *slow* | queues build inside Envoy/router, tail latency climbs past the SLO |
| `successRateThreshold` (0.999) | the router getting *fast but wrong* | 503s from parking overflow, 504 route timeouts |
| `sendRateThreshold` (0.9) | the *client* being unable to deliver the load | paced requests skipped once latency × RPS exceeds the connection pools |

## How it works

One Kubernetes Job per `type: nighthawk-ingress` tests.yaml entry, driven by
[`../automation/orchestrator.py`](../automation/orchestrator.py):

1. **Pin the router.** The orchestrator patches the `atenet-router`
   Deployment: both containers get cpu `requests=limits=envoyCpu`
   (Guaranteed QoS), Envoy gets `--concurrency envoyCpu`, and the debug
   log flags are dropped. Waits for rollout; every test tears substrate
   down afterwards, so nothing leaks.
2. **Create + warm actors.** The runner creates one glutton actor per
   WorkerPool worker (the entry's `workerCount`) via ateapi and POSTs
   `/ping` through the router with each actor's Host header until it
   answers 200.
3. **Adaptive search.** Open-loop traffic with the Host header rotated
   across all actors; `clientConcurrency` event loops (default 16,
   decoupled from `envoyCpu`) and large per-loop pools so the harness is
   never the bottleneck. Exponential ramp → binary search → a 60s
   **testing stage** at the converged rate.
4. **Upload results** (see below). The Job exits 0 only if the session
   converged and all artifacts uploaded.

## How to run it

### In CI

Add a `type: nighthawk-ingress` entry to the tests file the orchestrator
runs (`--tests`), e.g.:

```yaml
- name: ingress_routercap_envoy_2cpu
  type: nighthawk-ingress
  targetCluster: dev
  duration: 30m          # job-wait budget only
  workerCount: 50        # also the actor fleet size: one warm actor per worker
  nighthawk-ingress:
    envoyCpu: 2
    tailLatencySloMs: 25
```

### On a dev cluster

One-time prerequisites: a cluster from the GKE Quickstart in the repo
README, with nodes big enough for the two large pods (the router pod
requests 2×`envoyCpu` CPUs; the runner requests `clientConcurrency`+1).

```bash
# .ate-dev-env.sh at the repo root, then:
hack/install-ate.sh --deploy-ate-system
benchmarking/workloads/deploy.sh --deploy --worker-count 50 --sandbox-class gvisor
```

Then each benchmark run is one command, ~8–10 min:

```bash
./benchmarking/nighthawk-ingress/run-dev.sh --envoy-cpu 2
```

[`run-dev.sh`](run-dev.sh) runs only the benchmark layer. One invocation:

1. Verifies the prerequisites above, printing the fix for anything missing.
2. Pins the router if its current cpu differs from `--envoy-cpu`, so CPU
   sweeps need no redeploy.
3. Builds the runner image **from your working tree** — uncommitted changes
   included, and such runs are tagged `-dirty`.
4. Submits the Job, streams its logs, and prints the resulting
   `capacity.json`.

Flags: `--envoy-cpu`, `--actors`, `--tail-latency-slo-ms`, `--atespace`,
`--dest` (see
`--help`). It needs a running Docker daemon and gcloud/kubectl credentials.

Notes:

- The script does not redeploy substrate — if substrate code changed,
  rerun `hack/install-ate.sh --deploy-ate-system` first.
- `--atespace` isolates actor *fleets* between experiments sharing a
  cluster; capacity runs themselves still need the router exclusively
  (the cpu pin is global and the run saturates it).
- The full clone + deploy/teardown-per-test cycle is the orchestrator's
  job and runs in CI — see "In CI" above.

### Reading the results

Artifacts land under
`<dest>/runs/<name>/run_date=.../run_ts=.../run_tag=<commit>/`.
Start with `capacity.json`, drill into `stats.jsonl`:

| file | contents |
|---|---|
| `capacity.json` | **the verdict**: `slo_max_rps` — the highest rate that passed every threshold (the number to track), `binding_threshold` — which limit defined it, plus the testing stage's metrics and p50/p95/p99 |
| `stats.jsonl` | one record per stage: Nighthawk's builtin metrics under `metric_nighthawk.builtin_*` keys (note: `achieved_rps` counts requests *sent*, errors included), status counts, latency percentiles, and the stage's `failed_thresholds` |
| `results.json` | full session, 1:1 with Nighthawk's output |
| `spec.textproto`, `output.textproto` | exact Nighthawk input/output, for reproducibility |
| `status.json`, `logs.txt` | run metadata + process logs |

## Tuning

### Configuration knobs (the `nighthawk-ingress:` block)

The fleet size is the entry's top-level `workerCount` (required): the
benchmark warms one actor per worker, so it is also the number of glutton
actors receiving rotated-Host traffic. Everything else lives in the
`nighthawk-ingress:` block:

| Knob | Default | Meaning |
|---|---|---|
| `envoyCpu` | required | cpu `requests=limits` on both router containers, and Envoy's `--concurrency`. The benchmark's independent variable. |
| `atespace` | `ingress-benchmark` | Actor namespace; name it per *experiment*, never per run (atespaces are never auto-deleted). |
| `tailLatencySloMs` | 0 (disabled) | The SLO: upper bound on latency mean+2σ (~p95 proxy), in ms. |
| `successRateThreshold` | 0.999 | Minimum 2xx fraction of sent requests. |
| `sendRateThreshold` | 0.9 | Minimum sent fraction of the paced schedule (open-loop backstop). |
| `clientConcurrency` | 16 | Nighthawk event loops; the runner Job requests `clientConcurrency+1` CPUs. Deliberately decoupled from `envoyCpu`. |
| `connections` | 1000 | Client connections per event loop. |
| `maxPendingRequests` | 10000 | Client-side queue per event loop. |
| `initialRps` | 500 | Total starting RPS of the exponential ramp. |
| `exponentialFactor` | 2.0 | Ramp multiplier per step; smaller = tighter bracket around the knee, more steps. |
| `measuringPeriod` | 10s | Length of each adjusting step. |
| `convergenceDeadline` | 600s | The session errors if the search hasn't converged by then. |
| `testingStageDuration` | 60s | Final confirmation run at the converged rate. |

### Sizing the rig: hit the router's ceiling, not the harness's

Three components sit in series; the reported capacity is whichever
saturates first. The sizing knobs exist to make that the router:

```mermaid
flowchart LR
    client["nighthawk client"] --> router["atenet-router<br/>(under test)"] --> fleet["actor fleet"]
```

**One rule:** client and fleet must each handle ~2× the capacity you
expect to measure — the ramp overshoots before converging, and the rig
must survive the overshoot stages, not just the converged rate. A failed
stage's row in `stats.jsonl` tells you who saturated:

- `send_rate` below threshold — the client; raise `clientConcurrency`.
- 502s — the actor fleet; raise `workerCount`.
- latency climbing smoothly into the SLO — the router itself; that is
  the measurement.


