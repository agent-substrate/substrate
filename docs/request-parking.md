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
- the **park budget** (`--parked-request-budget`, default `5s`) elapses. If a
  retryable error was observed, that underlying error is preserved (for
  example, capacity remains a `503`); if the first RPC occupied the whole
  budget, the request ends with `504`.

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
shared flight, so a hot actor consumes N parking slots without starting an
independent sequence of `ResumeActor` RPCs for every request. When an expired
flight is replaced, cancellation of its last RPC may briefly overlap the new
flight; ateapi's per-Actor lock makes this handoff safe and retryable.

**The park budget is per-request.** Each caller receives the full configured
budget from the time it joins. A late caller extends the shared flight's
execution deadline, while every caller still stops waiting on its own timer.
The join also resets accumulated exponential growth and ensures the next retry
is no farther away than one initial retry interval; simultaneous joins coalesce
into one adjustment rather than issuing one RPC each. Thus de-duplication does
not make a newly arrived request inherit either an almost-expired wait budget
or a long backoff accumulated before it arrived.

A shared flight can remain alive while requests for that Actor keep arriving:
each arrival moves its execution deadline to cover that caller. Once arrivals
stop, it expires no later than one configured budget after the last join. A
caller cancellation does not immediately abort the detached flight, so another
request arriving within that bounded window can still share its work.

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
| `--parked-request-budget`         | `5s`    | Park budget for each request; requests for one actor share the control-plane retry loop, not the remaining wait budget. |
| `--parked-request-max`            | `1024`  | Max concurrent parked/in-flight resume requests; excess shed (503). `0` disables parking. |
| `--parked-request-retry-interval` | `100ms` | Delay before a parked request's first resume retry.                |
| `--parked-request-retry-factor`   | `1.1`   | Multiplier applied to the retry delay after each attempt (>= 1).   |
| `--parked-request-retry-jitter`   | `0.1`   | Random fraction in `[0, 1)` added per retry to de-synchronize parked requests. |
| `--extproc-max-requests`          | `0` (auto) | Envoy circuit-breaker `max_requests` for the ext_proc cluster. `0` derives twice `--parked-request-max` (min `1024`); explicit values must be `>= --parked-request-max` (enforced at startup). The excess is fast-path headroom (see Behavior). |

The retry backoff deliberately has no cap and no attempt limit. Each caller's
budget bounds its own wait; later arrivals may extend the lifetime of the shared
flight.

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
  | `budget_exhausted` | The caller's park budget elapsed. A saved retryable cause is preserved (for example, pool saturation remains a 503); if the first RPC consumed the whole budget, it maps to 504 instead. |
  | `canceled`         | The client disconnected while parked (request context canceled).            |
  | `timeout`          | The request's own deadline expired while parked (distinct from the park budget). |
  | `error`            | The resume failed with a non-retryable error (`NotFound`, `PermissionDenied`, ...). |

- `atenet.router.parking.rejected` — counter: requests shed because the lot was
  full.

**Status page** (`/statusz`): a "Request Parking" card shows whether parking is
enabled, the current vs. maximum parked count, and the max wait.
