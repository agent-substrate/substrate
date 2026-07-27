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
pool. `AssignWorkerStep` then returns `FailedPrecondition: "no free workers
available"`.

Previously the router mapped that straight to an HTTP `503` and failed the
request. But such saturation is usually momentary: another actor suspends within
milliseconds and frees its worker. Failing fast turns a sub-second blip into a
user-visible error.

## Behavior

With parking enabled (the default), the router treats `FailedPrecondition` and
`Unavailable` from `ResumeActor` as **retryable** conditions (alongside the
existing `Aborted` concurrent-resume conflict) — a parked request rides out
transient pool saturation and control-plane blips (e.g. an ateapi rolling
restart) alike. The request is *parked*: the resumer keeps retrying with
exponential backoff until either

- the resume succeeds (the actor is `RUNNING` and has a worker IP) — the request
  is then routed normally; or
- the **park budget** (`--parked-request-budget`, default `5s`) elapses — the
  underlying capacity error is returned, surfacing as `503 "actor <id>
  unavailable: no free workers available"`.

To bound resource use and provide backpressure, the router admits requests to a
**parking lot** of fixed capacity (`--parked-request-max`, default `2048`). Each
in-flight resume occupies one slot. When the lot is full, further requests are
shed immediately with `503 "actor <id> unavailable: router at capacity"` rather
than queueing without bound.

Concurrent requests for the *same* actor are de-duplicated by the resumer's
`singleflight` group: they share a single in-flight `ResumeActor` call and all
park on its result, so a hot actor consumes N parking slots but only one
control-plane RPC.

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

## Configuration

| Flag                             | Default | Meaning                                                            |
| -------------------------------- | ------- | ------------------------------------------------------------------ |
| `--parked-request-budget`         | `5s`    | Max time a single request may stay parked awaiting resume.         |
| `--parked-request-max`            | `2048`  | Max concurrent parked/in-flight resume requests; excess shed (503). `0` disables parking. |
| `--parked-request-retry-interval` | `100ms` | Delay before a parked request's first resume retry.                |
| `--parked-request-retry-factor`   | `1.1`   | Multiplier applied to the retry delay after each attempt (>= 1).   |
| `--parked-request-retry-jitter`   | `0.1`   | Random fraction in `[0, 1)` added per retry to de-synchronize parked requests. |

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
  | `error`            | The resume failed with a non-retryable error (`NotFound`, `Unavailable`, ...). |

- `atenet.router.parking.rejected` — counter: requests shed because the lot was
  full.

**Status page** (`/statusz`): a "Request Parking" card shows whether parking is
enabled, the current vs. maximum parked count, and the max wait.

