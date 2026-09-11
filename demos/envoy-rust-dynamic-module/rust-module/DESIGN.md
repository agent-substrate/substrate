# Rust module design

Internal design of `ate_router_module`. The system-level case for it — why a
cache in front of ext_proc, what changes in `xds.go`, how to roll it out — is in
[../DESIGN.md](../DESIGN.md). This document is about the module itself.

## What it is

Envoy's ext_proc client has **no cache and no way to grow one**: the filter's
only cache-shaped fields (`disable_clear_route_cache`, `route_cache_action`)
concern Envoy's route cache, not memoizing a `ProcessingResponse`. Adding one
means patching Envoy C++.

This module is that cache, assembled from two supported extension points:

* `envoy.filters.http.dynamic_modules` — an HTTP filter running in-process,
  ahead of ext_proc in the chain;
* `ExtProcPerRoute.disabled` — the per-route override the module steers into to
  suppress the ext_proc call on a hit.

Nothing else in the chain changes.

## Compatibility

The SDK is a git dependency on the Envoy repo, pinned to **the commit of the
Envoy binary it loads into** — `b579d07d3ad7ee11d32b105e91a5a39ad24718d7`
(v1.39.1), matching the `envoyproxy/envoy:v1.39-latest` substrate pins. Envoy
guarantees a module built for X.Y works on X.Y and X.(Y+1) only, so **the pin is
a hard coupling**: bumping the Envoy image is a module rebuild, and that
constraint belongs in whatever bumps the image.

The `.so` must also be built against a glibc **no newer** than the Envoy image's
(2.35 on Ubuntu 22.04). The build container is `rust:1-slim-bullseye`
(glibc 2.31) for exactly this reason.

## Layout

```
declare_init_functions!(init, new_http_filter_config_fn)   lib.rs:40
  │
  ├─ init()                        once per process, at dlopen
  └─ new_http_filter_config_fn()   once per filter-chain config
       │  parses JSON config, defines Envoy-native counters
       └─ FilterConfig ────────────────── lib.rs:160
            │  new_http_filter() per HTTP request
            ├─ Filter        (mode: replace)   lib.rs:207
            └─ CacheFilter   (mode: co-exist)  lib.rs:414
                                   ▲
                    both read/write one process-global cache
                                   │
                    cache(): &'static DashMap  lib.rs:153
```

`filter_name` in the Envoy config selects the mode: `actor_router` replaces
ext_proc outright, `actor_router_cache` sits in front of it. **`actor_router_cache`
is the mode intended for production**; `actor_router` exists to measure the
ceiling.

## State and the thread model

Envoy `dlopen`s the `.so` once and runs the filter chain on N worker threads.
Filter instances are per-request and single-threaded; anything shared is not.

The cache is therefore process-global, behind a `OnceLock`:

```rust
fn cache() -> &'static DashMap<String, Binding> {
    static CACHE: OnceLock<DashMap<String, Binding>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}
```

`DashMap` shards its locks, so worker threads don't serialize on a single mutex
the way they would behind `RwLock<HashMap>`. Process-global also means the cache
**survives an ECDS config redelivery** that re-runs `new_http_filter_config_fn` —
a config push does not cold-start the cache.

Key: `format!("{}/{}", atespace, name)`, matching `resources.ActorRef`. It must
be atespace-qualified; an actor name is only unique within its atespace.

Value:

```rust
struct Binding {
    worker_ip: String,
    expires_at: Instant,
}
```

Only the worker IP is cached. The port is configuration, never a parsed value,
so a malformed control-plane answer cannot redirect traffic to another port.

## Request path (`actor_router_cache`)

**`on_request_headers`**

1. Read the authority: `get_filter_state_bytes(b"dev.ate.authority")`, falling
   back to `:authority`. Filter state is what the Go handler reads too, and it
   is the only correct source — a reinjected CONNECT tunnel's own `:authority`
   has nothing to do with the actor.
2. Parse `<actor>.<atespace>.<suffix>[:port]`.
3. Cache lookup.
   * **Hit, live** → publish `envoy.filters.listener.original_dst` dynamic
     metadata (`local` = `ip:443`, `port` = actor port), set the target-port
     header, set `x-ate-route-resolved: 1`, `clear_route_cache()`, `Continue`.
     Route re-selection now lands on the route carrying
     `ExtProcPerRoute.disabled`, and ext_proc never runs.
   * **Hit, expired** → remove, fall through.
   * **Miss** → record the key in `learn_key`, `Continue`. ext_proc runs exactly
     as today.

**`on_response_headers`** — if this request missed, read back the
`original_dst`/`local` metadata **ext_proc published**, split off the port, and
insert the binding. Reaching response headers means the request was actually
routed, so the module only ever caches a decision that demonstrably worked.

The module never calls the control plane in this mode. It cannot route anywhere
ext_proc has not already routed.

## Failure behaviour: everything unknown is a `Continue`

A helper filter must never turn a request away. Missing filter state, an
unparseable authority, an empty cache, an expired entry, a malformed metadata
value — all return `Continue` and let ext_proc handle it, including producing the
exact 404/503 bodies clients see today. `ingress/errors.go` still runs on every
miss. The module has **no error responses of its own** in co-existence mode.

That is what makes removal a no-op: delete the filter, and every path it touches
was already the ext_proc path.

## Config

JSON, via `filter_config` as a `google.protobuf.StringValue`:

```json
{
  "ateapi_cluster": "ateapi",
  "cache_ttl_seconds": 5,
  "callout_timeout_ms": 5000,
  "actor_dns_suffix": "actors.resources.substrate.ate.dev",
  "atunnel_port": 443,
  "target_port_header": "x-ate-target-port",
  "cache_enabled": true
}
```

Every field except `ateapi_cluster` has a serde default. `ateapi_cluster` is
unused in co-existence mode. A config that fails to parse returns `None` from
the factory, which makes Envoy **reject the config** rather than start with a
filter that silently does nothing.

## Observability

Counters are Envoy-native, defined once per filter config via `define_counter`
and incremented by id — `ate_router.cache_hit`, `ate_router.cache_miss`. They
appear in Envoy's own `/stats` (`dynamicmodulescustom.ate_router.*`) next to
every other dataplane counter, with no per-request attribute allocation, unlike
the OTel histogram with four string attributes the Go path records per request.

Hit rate is the operational signal: it should approach `1 - M/(N·T)`. A hit rate
that falls without a matching change in the actor working set means bindings are
churning, and the TTL is too long.

## Replacement mode (`actor_router`), and why it is not the recommendation

`Filter` resolves misses itself: `send_http_callout` to a configured cluster,
`StopIteration`, then `on_http_callout_done` parses the JSON, validates the IP,
caches, routes, and `continue_decoding()`. Measured 4% faster than co-existence.

It is not the recommendation because it costs: an ate-apiserver client identity
in the dataplane (callouts are HTTP to an Envoy cluster, not authenticated gRPC),
a JSON resolve endpoint that does not exist today, and Rust reimplementations of
parking, singleflight and the error contract. It exists in the tree to show what
the remaining 4% is worth, which is: not much.

## Not yet implemented

The module as written is TTL-only. Before it fronts traffic it needs, in
`on_response_headers` and on stream reset:

1. **Evict on `421` + `x-ate-assignment-stale`**, then **re-run the slow path in
   the same request** — clear the header, `clear_route_cache()`, retry through
   ext_proc. A cache hit skips `ResumeActor`, which is what wakes a suspended
   actor, so evict-and-fail would turn the TTL into a user-visible error window.
   This is the one place the design can regress behaviour.
2. **Evict on upstream reset / local 503**, since a vanished worker produces no
   421 at all.
3. **A bounded cache.** The `DashMap` grows with the actor working set and is
   only pruned on lookup. It needs a size cap with LRU, or a sweeper on a
   `new_scheduler` timer.
4. **Miss coalescing**, so a cold actor with simultaneous requests does not
   produce a burst of ext_proc streams. Not a blocker — ext_proc's own
   singleflight collapses the `ResumeActor` calls behind them — but it gives up
   an easy win.

## Risk that does not go away

The module is not sandboxed. A panic or a bad pointer takes Envoy down with it,
where a Go router crash fails only ext_proc streams and Envoy keeps serving. The
SDK catches unwinds at the ABI boundary, and this module holds no unsafe code and
no raw pointers, but "in-process" is a different operational class from "separate
container" and it should be soaked before it fronts real traffic.
