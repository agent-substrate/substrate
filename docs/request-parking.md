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

With parking enabled (the default), the router treats `FailedPrecondition` from
`ResumeActor` as a **retryable** condition (alongside the existing `Aborted`
concurrent-resume conflict). The request is *parked*: the resumer keeps retrying
with capped exponential backoff until either

- the resume succeeds (the actor is `RUNNING` and has a worker IP) — the request
  is then routed normally; or
- the **park budget** (`--parking-max-wait`, default `30s`) elapses — the
  underlying capacity error is returned, surfacing as `503 "actor <id>
  unavailable: no free workers available"`.

To bound resource use and provide backpressure, the router admits requests to a
**parking lot** of fixed capacity (`--parking-max-parked`, default `2048`). Each
in-flight resume occupies one slot. When the lot is full, further requests are
shed immediately with `503 "actor <id> unavailable: router at capacity"` rather
than queueing without bound.

Concurrent requests for the *same* actor are de-duplicated by the resumer's
`singleflight` group: they share a single in-flight `ResumeActor` call and all
park on its result, so a hot actor consumes N parking slots but only one
control-plane RPC.

### What is *not* parked

Only transient capacity (`FailedPrecondition`) and concurrency (`Aborted`)
conditions are parked. Errors that will not resolve by waiting are returned
immediately, preserving prior semantics:

| Resume result                         | Behavior                          |
| ------------------------------------- | --------------------------------- |
| `OK`                                  | Route to worker                   |
| `Aborted` (concurrent resume)         | Retry (always)                    |
| `FailedPrecondition` (no free worker) | **Park & retry** (when enabled)   |
| `NotFound`                            | Fail fast → `404`                 |
| `Unavailable`                         | Fail fast → `503`                 |
| `DeadlineExceeded`                    | Fail fast → `504`                 |
| `PermissionDenied` / `Unauthenticated`| Fail fast → `403` / `401`         |

When parking is **disabled** (`--parking-max-parked=0`), the router preserves
its legacy fail-fast behavior: `FailedPrecondition` fails fast, there is no
admission cap, and only `Aborted` (concurrent-resume) conflicts are retried,
within the historical `15s` budget.

## Configuration

| Flag                   | Default | Meaning                                                            |
| ---------------------- | ------- | ------------------------------------------------------------------ |
| `--parking-max-wait`   | `30s`   | Max time a single request may stay parked awaiting resume.         |
| `--parking-max-parked` | `2048`  | Max concurrent parked/in-flight resume requests; excess shed (503). `0` disables parking. |

## Observability

**Metrics** (OpenTelemetry, meter `atenet-router`):

- `atenet.router.parking.active` — up/down counter: requests currently parked.
- `atenet.router.parking.wait.duration` — histogram (seconds) of time spent
  parked, labeled `outcome` ∈ {`served`, `budget_exhausted`, `timeout`,
  `canceled`, `error`}. `budget_exhausted` means the full park budget elapsed
  while the pool stayed saturated — the signal that capacity, not a fault, is
  the bottleneck.
- `atenet.router.parking.rejected` — counter: requests shed because the lot was
  full.

**Status page** (`/statusz`): a "Request Parking" card shows whether parking is
enabled, the current vs. maximum parked count, and the max wait.

