# Request Parking (atenet router)

## Summary

**Request parking** lets the `atenet` router hold ("park") an inbound request
whose target actor cannot be served *yet* because of transient worker-pool
saturation, retrying the resume until the actor becomes routable or a bounded
wait elapses — instead of immediately returning `503` to the client.

## Motivation

When a request arrives for a suspended actor, the router resumes it before
routing:

```
Envoy --(ext_proc RequestHeaders)--> router.handleRequestHeaders
    --> ActorResumer.ResumeActor --> ateapi ResumeActor (gRPC)
```

`ateapi`'s `AssignWorkerStep` claims a free worker from the actor's `WorkerPool`.
In an oversubscribed system — the core premise of Substrate, where many actors
multiplex onto few workers — a burst of traffic can momentarily exhaust the
pool. `AssignWorkerStep` then returns `ResourceExhausted: "no free workers
available"`.

Previously the router mapped that straight to an HTTP `503` and failed the
request. But such saturation is usually momentary: another actor suspends within
milliseconds and frees its worker. Failing fast turns a sub-second blip into a
user-visible error.

## Behavior

With parking enabled (the default), the router treats `ResourceExhausted`,
`FailedPrecondition` and `Unavailable` from `ResumeActor` as **retryable**
conditions (alongside the existing `Aborted` concurrent-resume conflict) — a
parked request rides out transient pool saturation and control-plane blips
(e.g. an ateapi rolling restart) alike. The request is *parked*: the resumer
keeps retrying with exponential backoff until either

- the resume succeeds (the actor is `RUNNING` and has a worker IP) — the request
  is then routed normally; or
- the **park budget** (`--parked-request-budget`, default `5s`) elapses — the
  underlying capacity error is returned, surfacing as `503 "actor <id>
  unavailable: no free workers available"`.

**The budget bounds retries, not a committed resume.** When the budget elapses
the router stops starting new resume attempts, but an attempt already in
flight is **never canceled**: by then the control plane has committed work to
it, and canceling would discard an in-progress restore. Instead the router
waits for that attempt's real result: a restore that overshoots the budget
(routine under node contention) is **served late** rather than failed, and a
late retryable error still surfaces as the capacity `503`. The attempt is
bounded by the control plane's own server-side RPC deadline, and Envoy's
ext_proc message timeout (`budget + 5s`) remains the ceiling on how long a
client is held either way.

**Worst-case occupancy.** A parked request's lot slot is held until its caller
stops waiting; with the Envoy dataplane that is bounded by the ext_proc
message timeout (`budget + 5s`). The resume attempt itself can outlive every
caller: it carries no client-side deadline and runs until ateapi's
server-side maximum RPC deadline, holding that actor's singleflight entry.
New requests for the same actor during that window do not start another
control-plane call — they join the in-flight attempt, and if it has not
resolved by their own stream deadline they are ended by the dataplane's
timeout rather than a router verdict.

To bound resource use and provide backpressure, the router admits requests to a
**parking lot** of fixed capacity (`--parked-request-max`, default `1024`). Each
in-flight resume occupies one slot. When the lot is full, further requests are
shed immediately with `503 "actor <id> unavailable: router at capacity"` rather
than queueing without bound.

Every parked request holds one ext_proc stream — one active request against
Envoy's ext_proc cluster — for its entire wait, while ordinary requests hold
one only for a millisecond-scale header exchange. The cluster's circuit breaker
is therefore the hard ceiling on concurrent parked requests. By default the
router **derives** it as twice `--parked-request-max` (minimum `1024`), so the
lot always fits and an equal share of **fast-path headroom** remains — a
saturated lot cannot starve requests to already-running actors, at any lot
size. `--extproc-max-requests` overrides the derivation; explicit values are
validated `>= --parked-request-max` at startup, because a breaker below the lot
would silently truncate it — Envoy would reject the overflow itself, with 503s
that never reach the lot and never count in `parking.rejected`.

Concurrent requests for the *same* actor are de-duplicated by the resumer's
`singleflight` group: they share a single in-flight `ResumeActor` call and all
park on its result, so a hot actor consumes N parking slots but only one
control-plane RPC.

**The park budget is per-flight, not per-request.** The budget clock starts
when a flight's first caller begins the resume; every later request for the
same actor joins that flight and shares its remaining budget and outcome. A
request that joins late may therefore see `budget_exhausted` after waiting far
less than a full budget itself — the accepted cost of collapsing a hot actor's
requests into one control-plane call. (`parking.wait.duration` records each
request's *own* parked time, so sub-budget `budget_exhausted` samples are
expected under sustained saturation.)

### What is *not* parked

Only transient conditions — capacity (`FailedPrecondition`), concurrency
(`Aborted`), and control-plane unavailability (`Unavailable`) — are parked.
Errors that will not resolve by waiting are returned immediately (fail fast):

| Resume result                          | Behavior                          |
| -------------------------------------- | --------------------------------- |
| `OK`                                   | Route to worker                   |
| `Aborted` (concurrent resume)          | Retry (always)                    |
| `FailedPrecondition` (no free worker)  | **Park & retry** (when enabled)   |
| `Unavailable` (control-plane blip)     | **Park & retry** (when enabled)   |
| `NotFound`                             | Fail fast → `404`                 |
| `DeadlineExceeded`                     | Fail fast → `504`                 |
| `PermissionDenied` / `Unauthenticated` | Fail fast → `403` / `401`         |

When parking is **disabled** (`--parked-request-max=0`), the router fails fast:
`FailedPrecondition` and `Unavailable` are returned immediately, there is no
admission cap, and only `Aborted` (concurrent-resume) conflicts are retried,
within a `15s` budget.

### Parked requests survive router shutdown

A request parked when the router pod receives SIGTERM is **not** reset: the
shutdown sequence keeps the ext_proc server (and, via a preStop handshake, the
Envoy sidecar) alive until in-flight streams finish, and the ext_proc drain
deadline (`--drain-timeout`) defaults to a value derived from
`--parked-request-budget` and is validated at startup to be `>=` the budget —
so a parked request always gets its full budget and a normal verdict (routed
`200` or capacity `503`) even mid-termination. See the graceful-shutdown knobs
(`--drain-delay`, `--drain-timeout`) in `manifests/ate-install/atenet-router.yaml`.

## Configuration

| Flag                             | Default | Meaning                                                            |
| -------------------------------- | ------- | ------------------------------------------------------------------ |
| `--parked-request-budget`         | `5s`    | Park budget per resume *flight*; requests de-duplicated onto an in-flight resume share its remaining budget (see Behavior). |
| `--parked-request-max`            | `1024`  | Max concurrent parked/in-flight resume requests; excess shed (503). `0` disables parking. |
| `--parked-request-retry-interval` | `100ms` | Delay before a parked request's first resume retry.                |
| `--parked-request-retry-factor`   | `1.1`   | Multiplier applied to the retry delay after each attempt (>= 1).   |
| `--parked-request-retry-jitter`   | `0.1`   | Random fraction in `[0, 1)` added per retry to de-synchronize parked requests. |
| `--extproc-max-requests`          | `0` (auto) | Envoy circuit-breaker `max_requests` for the ext_proc cluster. `0` derives twice `--parked-request-max` (min `1024`); explicit values must be `>= --parked-request-max` (enforced at startup). The excess is fast-path headroom (see Behavior). |

The retry backoff deliberately has no cap and no attempt limit: the budget alone
bounds the wait.

## Observability

**Metrics** (OpenTelemetry, meter `atenet-router`):

- `atenet.router.parking.active` — up/down counter: requests currently parked.
- `atenet.router.parking.wait.duration` — histogram (seconds) of time spent
  parked. Recorded **exactly once per admitted request**, at the moment its
  resume attempt completes; never recorded for shed requests (those only
  increment `parking.rejected`) nor when parking is disabled. The `outcome`
  label says how the park ended:

  | `outcome`          | When it is set                                                              |
  | ------------------ | --------------------------------------------------------------------------- |
  | `served`           | The resume succeeded and the request was routed to its worker.              |
  | `budget_exhausted` | The park budget elapsed while the resume was still blocked on a retryable condition (pool saturated, a concurrent operation holding the actor, or the control plane unavailable) — the signal that capacity, not a fault, is the bottleneck. |
  | `canceled`         | The client disconnected while parked (request context canceled).            |
  | `timeout`          | The request's own deadline expired while parked (distinct from the park budget). |
  | `error`            | The resume failed with a non-retryable error (`NotFound`, `PermissionDenied`, ...). |

- `atenet.router.parking.rejected` — counter: requests shed because the lot was
  full.

**Status page** (`/statusz`): a "Request Parking" card shows whether parking is
enabled, the current vs. maximum parked count, and the max wait.
