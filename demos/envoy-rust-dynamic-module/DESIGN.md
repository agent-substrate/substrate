# Co-existence design: Rust dynamic module in front of ext_proc

A minimal, reversible way to put the Rust dynamic module into the ingress path
**without changing the ext_proc contract, the resumer, parking, or anything the
Go router does today.**

Implemented and measured in this directory as arm C.

## The principle

> The module is a **cache**. ext_proc stays the **only** thing that resolves an
> actor, and the only thing that talks to ate-apiserver.

The module never calls the control plane, never holds a client certificate,
never decides that an actor exists. It answers repeat requests from a binding
that **ext_proc itself already produced**, and gets out of the way for
everything else.

That single constraint removes most of the risk. The module cannot route
anywhere ext_proc has not already routed; the worst a stale entry can do is send
a request to a worker that ext_proc chose recently.

## Request flow

```
                    ┌─ hit ──→ publish original_dst metadata
                    │          set x-ate-route-resolved: 1
 request → module ──┤          clear_route_cache()  ───→ route "resolved_by_module"
                    │                                     (ext_proc disabled) ──→ worker
                    │
                    └─ miss ─→ (does nothing) ──→ ext_proc ──→ ResumeActor ──→ worker
                                                                       │
                                          on_response_headers: read the metadata
                                          ext_proc published, store it in the cache
```

**Miss** is byte-for-byte today's path. The module contributes one map lookup
and, on the way back, one map insert.

**Hit** skips the ext_proc filter entirely — proven in this demo: 23 requests
produced 2 `ext_proc.streams_started`.

## How a hit skips ext_proc

ext_proc has no "enabled by metadata" switch — only `ExtProcPerRoute.disabled`,
which is per **route**. So the module selects a different route:

1. The module sets `x-ate-route-resolved: 1`.
2. It calls `clear_route_cache()`, forcing Envoy to re-run route matching.
3. The route config gains one route, matched on that header, carrying
   `typed_per_filter_config: {envoy.filters.http.ext_proc: {disabled: true}}`.
4. When the ext_proc filter runs, it reads that per-route override and skips
   itself.

The header is set by the module, never trusted from the client — a client that
sends `x-ate-route-resolved: 1` itself would select the ext_proc-disabled route
with **no** metadata published, and the ORIGINAL_DST cluster would fail the
request rather than route it anywhere. Still, the module should overwrite the
header unconditionally on every request (set to `1` on a hit, remove it on a
miss). See "Open items".

## The change to substrate

Three additive edits, all in
[`xds.go`](../../cmd/atenet/internal/router/xds.go):

**1. `buildRoutes()` — add one route above the catch-all** (guarded by the flag):

```go
// Requests the dynamic module already resolved skip the ext_proc call.
{
    Name: "resolved_by_module",
    Match: &routev3.RouteMatch{
        PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"},
        Headers: []*routev3.HeaderMatcher{{
            Name: ingress.RouteResolvedHeader,
            HeaderMatchSpecifier: &routev3.HeaderMatcher_StringMatch{
                StringMatch: &matcherv3.StringMatcher{
                    MatchPattern: &matcherv3.StringMatcher_Exact{Exact: "1"},
                },
            },
        }},
    },
    Action: /* same RouteAction as the catch-all */,
    TypedPerFilterConfig: map[string]*anypb.Any{
        "envoy.filters.http.ext_proc": newAny(&extprocv3filter.ExtProcPerRoute{
            Override: &extprocv3filter.ExtProcPerRoute_Disabled{Disabled: true},
        }),
    },
},
```

**2. `buildHcm()` — insert the module filter immediately before ext_proc**, only
when the flag is on. It goes *after* `authorityFilterStateFilter()` so the
module can read `dev.ate.authority`.

**3. `cmd.go` — one flag**, defaulting off:

```
--ingress-route-cache-ttl duration
    How long the dataplane's in-process route cache may serve an actor->worker
    binding that ext_proc previously resolved, skipping the ext_proc call for
    repeat requests. 0 (the default) disables the cache and the dataplane
    filter entirely.
```

No change to `ingress/`, `extproc/`, `resumer.go`, or `parking.go`.

## Why this is safe to land

* **Default off.** `--ingress-route-cache-ttl=0` emits neither the filter nor
  the route. The generated xDS is identical to today's, byte for byte.
* **Rollback is a flag flip**, applied over xDS with no restart and no image
  change.
* **Fail-open by construction.** Every path the module does not understand — an
  authority it cannot parse, a missing filter state, an empty cache, an expired
  entry — returns `Continue` and lets ext_proc handle it, including producing
  the exact 404/503 bodies clients see today.
* **The error contract is untouched**, because the module never generates
  errors. All of `ingress/errors.go` still runs on every miss.
* **Parking, singleflight and the resume metrics keep working**, because every
  cold actor is a miss and every miss is ext_proc.
* **Blast radius is bounded by TTL.** A few seconds of stale routing for actors
  that moved, versus a permanent per-request RPC.

## Measured effect

Arm C vs arm A, same host, same load, 50 hot actors, concurrency 8, 20s
(reproduced twice, within 2%):

| | A: ext_proc today | C: module + ext_proc | change |
|---|---:|---:|---:|
| throughput | 3,735 rps | 42,766 rps | **11.5×** |
| p50 | 2.11 ms | 0.17 ms | **−92%** |
| p95 | 2.53 ms | 0.30 ms | **−88%** |
| p99 | 3.01 ms | 0.44 ms | **−85%** |
| ResumeActor RPCs | 87,231 | 262 | **−99.7%** |
| atenet-router CPU | ~47% | ~1% | **−98%** |
| CPU per request | ~0.23 ms | ~0.037 ms | **~6×** |

Arm C reaches **96% of the throughput of arm B2**, the full replacement that
drops ext_proc and calls the control plane itself. Nearly the entire win comes
from not making the call — not from removing the Go router. That is the whole
argument for this design: take the win, keep the router.

## Rollout

1. **Land the module and the flag, default off.** Nothing changes in production.
2. **Enable with a 1s TTL in one cell.** Compare `ate_router.cache_hit` /
   `cache_miss` against the existing route-duration histogram, and watch the
   404/503 rate for any divergence.
3. **Raise the TTL** toward — but well under — the idle-suspend timeout, watching
   for requests routed to workers that no longer host the actor.
4. **Then, and only then**, consider whether the module should resolve misses
   itself (arm B2). It buys ~4% more throughput and costs the dataplane a client
   certificate, an HTTP resolve endpoint on ate-apiserver, and its own
   coalescing. On these numbers that trade is not obviously worth making.

## The correctness point that matters most

**A cache hit skips `ResumeActor`, and `ResumeActor` is what wakes a suspended
actor.** Today, a request for an actor that has been suspended *causes* it to
resume. With a cache in front, a stale hit routes to the old worker and fails
instead.

So eviction must **re-run the slow path inside the same request**, never
evict-and-fail. Concretely: on the stale signal the module must clear the entry,
remove `x-ate-route-resolved`, `clear_route_cache()`, and let the request retry
through ext_proc — not return the error to the client. Without that, the TTL
becomes a user-visible error window on exactly the request that should have
triggered a cold resume.

## The stale signal already exists, and nothing reads it

`internal/atunnel/ingress.go:46` defines `StaleAssignmentHeader =
"X-Ate-Assignment-Stale"`, and `reject()` (`ingress.go:524-527`) sets it with a
**421** whenever a worker is asked for an actor it no longer hosts. There are no
consumers anywhere in the repo: the router cannot even see it, because
`xds.go:1016` sets `ResponseHeaderMode: SKIP`.

That is precisely the negative-feedback signal this design needs, and it is
**free to a dynamic module** — `on_response_headers` is a local function call —
where it would cost Go ext_proc a second gRPC round trip per response to obtain.

Three eviction triggers are required, not one:

1. `421` + `x-ate-assignment-stale: true` → evict, then re-run the slow path.
2. **Upstream connect failure / reset.** If the worker pod is gone there is no
   421 at all — the ORIGINAL_DST cluster produces a local 503. Evict on stream
   reset too, or a vanished worker burns every request until the TTL expires.
3. Hard TTL expiry.

**Prerequisite, one line of Go:** the header is currently forgeable. atunnel's
`ReverseProxy` (`ingress.go:130-150`) has no `ModifyResponse`, so an actor can
emit `421 + X-Ate-Assignment-Stale: true` itself and force a `ResumeActor` per
response — amplification onto exactly the load the cache removes. Strip the
header from proxied responses before the module trusts it.

## How stale can a binding actually get

Exhaustive grep for `Status.WorkerAssignment` writes gives **six** invalidation
events: suspend, pause, crash, worker-pod delete, actor delete, and re-resume.
Of those, only worker-pod deletion is spontaneous — and **there is no
idle-suspend controller in the tree at all** (`docs/roadmap.md:114` still lists
TTL-based GC as a future idea), so suspension is externally driven. Worker pods
also carry a 3600s termination grace period
(`workerpool_apply.go:39`) and keep hosting their actor while draining.

Bindings are considerably more stable than the request rate, which is what makes
even a multi-second TTL reasonable. Start at 1s anyway, and raise it only once
the eviction loop is proven.

## Known gap this widens

`atunnel.authorize()` compares actor **ref** (atespace+name), not **uid**. A
delete-and-recreate of the same name landing on the same worker will not produce
a 421. No cross-tenant exposure — same atespace, same name — but the cache
widens that cross-*incarnation* window from milliseconds to the TTL. The worker
already pins `ExpectedActorUID` (`internal/atunnel/credential.go:44`), so the
fix is threading it into the comparison; no proto change.

## Open items before this ships

* **Strip a client-supplied `x-ate-route-resolved`.** Have the module remove the
  header on every miss, or use dynamic metadata plus a metadata-matched route
  instead of a header, so client input can never select the route.
* **Coalesce misses**, so a cold actor with many simultaneous requests does not
  produce a burst of ext_proc streams. Today they all fall through, which is the
  same as current behaviour — ext_proc's own singleflight collapses the
  `ResumeActor` calls behind them — so this is a refinement, not a blocker.
* **Cache bound.** The `DashMap` grows with the actor working set and is never
  evicted except on expiry lookup. It needs a size cap with LRU, or a sweeper on
  a `new_scheduler` timer.
* **Crash blast radius.** A panic in the module takes Envoy down, where a Go
  router crash fails only ext_proc streams. The SDK catches unwinds at the ABI
  boundary, but this deserves a soak test before it fronts real traffic.
