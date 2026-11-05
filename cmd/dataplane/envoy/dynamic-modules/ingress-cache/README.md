# Envoy Substrate Ingress Cache - Rust Dynamic Module

An Envoy dynamic module, written in Rust, that runs as an HTTP filter ahead of
`ext_proc` on the ingress router and caches `<atespace>/<actor>` to worker IP
bindings. With upstream CONNECT (the default), the same library also provides
the `tunnel_watcher` network filter on the router's `actor_connect`
internal listener, which marks cache entries from what happens to the tunnels
they route to.

The cache is per Envoy worker thread. Each thread learns bindings on its own,
which costs more misses than a shared cache but keeps the network filter's
marks on the thread, and in the cache, that the request was routed from.

The cache is a preview feature and is **disabled by default**. Read the
caveats below before enabling it.

## Enabling

The cache is controlled by the `IngressCache` preview gate on `atenet-router`
and is only implemented by the Envoy dataplane (`--atenet-dataplane=envoy`, the
default). Enable it at install time with:

```bash
ATE_PREVIEW=IngressCache go run ./cmd/ate-setup deploy ate-system
```

or pass `--preview=IngressCache` to the `atenet-router` container directly.

With the gate off, the router installs neither these filters nor the cached
routes that skip `ext_proc`, so every request is resolved through `ext_proc`.

`atenet-router` sets the filter config from `--ingress-cache-ttl` (default 5s)
and `--ingress-cache-max-cached-actors` (default 10,000 per Envoy worker
thread).

## Caveats

- **Stale routing for up to the TTL.** A cache hit skips `ext_proc` and so does
  not observe suspends, migrations, or reassignments made in the last
  `cache_ttl_seconds` (default 5s). A request to the old worker evicts the
  entry and, when that is safe, is replayed through `ext_proc`, as described
  below.
- **Only bodiless requests use the cache.** A request with a body, including
  every gRPC call, always goes through `ext_proc`: if it hit a stale entry, the
  router couldn't replay it. Its response still refreshes the cache.
- **Clients must retry some failures.** Only bodiless requests that failed
  before reaching the actor are replayed by the router. One interrupted
  mid-flight returns the error to the client after evicting the entry; the
  client's retry is resolved through `ext_proc`.
- **No replays while an entry is spoiled.** If an actor is suspended while a
  request is in flight on an established tunnel, before it responded, its entry
  is marked spoiled and no request on it is replayed, including one refused at
  the same moment. Suspends follow an idle signal, so this is rare, and those
  clients get a `503` rather than risk running a request twice.
- **Partial response headers.** If an actor writes only part of its response
  headers and then dies or is suspended, the network filter treats the request
  as answered and doesn't spoil the entry. The router returns `503 UC`, which
  can then be replayed although the actor ran the request, if another request's
  refusal marked the entry stale. A 1xx followed by a drop isn't replayed: the
  router reports it as `upstream_reset_after_response_started`.
- **Each thread caches at most `max_cached_actors` actors** (10,000 by
  default). A full thread cache drops expired bindings before it caches another
  actor. If none had expired, the new actor isn't cached and its requests keep
  going through `ext_proc`.

## How it works

- **Cache hit (`on_request_headers`)**: When an unexpired binding exists for the
  target actor, the filter writes the `ORIGINAL_DST` dynamic metadata
  (`envoy.filters.listener.original_dst`), sets the `x-ate-target-port` and
  `ate-target-actor` request headers that `ext_proc` would otherwise set, sets
  the `ate.ingress_cache` `hit` dynamic metadata to select the route with
  `ext_proc` disabled, and clears the route cache. Clients can't set dynamic
  metadata, so they can't select that route. The two headers are sent to
  `atunnel` on the worker, so their names are `atunnel`'s
  (`internal/atunnel/ingress`, `internal/atenet`).
- **Cache miss (`on_request_headers` / `on_response_headers`)**: The request
  proceeds through `ext_proc`, which resolves or resumes the actor and populates
  the `ORIGINAL_DST` dynamic metadata. On response headers, the filter records
  the worker IP in the cache for `cache_ttl_seconds`.
- **Request with a body (`on_request_headers`)**: The filter skips the cache
  lookup and the request proceeds through `ext_proc`, then is learned from like a
  miss. A request counts as having a body unless it ends with its headers or has
  `content-length: 0`. Envoy delivers a bodiless HTTP/1 request's headers that
  way, so this sends chunked or HTTP/2 GETs and upgrades such as WebSocket
  through `ext_proc`.
- **gRPC `UNAVAILABLE` (`on_response_headers` / `on_response_trailers`)**: A
  response with gRPC status `14` is not learned from. If the status arrives in
  the trailers, after the headers were learned from, the actor's binding is
  removed.
- **Stale binding eviction (`on_response_headers`)**: If a fast-path request
  fails with `421 Misdirected Request` (with `x-ate-assignment-stale: true`,
  the response header `atunnel` adds to its own refusals), or `503 Service
  Unavailable` with upstream transport failure response flags (`UF`, `UH`,
  `UC`, `UR`), the filter immediately evicts the cached binding so subsequent
  requests resolve through `ext_proc`.
- **Tunnel marks (`tunnel_watcher`)**: On each `actor_connect`
  connection, the network filter reads the actor and worker from the tunnel
  filter state (`dev.ate.tunnel.actor`, `dev.ate.tunnel.worker`) and notes
  whether request bytes went upstream without response bytes coming back; it
  never parses them. When the connection closes:
  - with `UF`, the tunnel was never established (refused CONNECT, failed dial,
    or connect timeout), so it marks every live entry for that actor and worker
    **stale** and removes the current binding;
  - after being established with a request outstanding, it marks them
    **spoiled**, since that request may have reached the actor.
  The marks are in place before the router reports the `503`: the close
  callback runs inside `tcp_proxy`'s close, and the router sees the close on the
  next event-loop iteration. A request routed from the cache holds its entry
  until it ends, so marks reach it even after a newer binding replaces it.
- **Stream replay (`recreate_stream`)**: To prevent duplicate execution of requests
  with side effects, automatic replay is only performed when failure is
  guaranteed to have occurred before the request reached the upstream actor
  (only bodiless requests are served from the cache):
  - `421 Misdirected Request` with `x-ate-assignment-stale: true` (rejected by
    worker `atunnel` prior to forwarding).
  - `503 Service Unavailable` with zero-byte upstream failure response flags:
    `UF` (upstream connection failure / connection refused before connection
    establishment) or `UH` (no healthy upstream host in cluster).
  - `503 Service Unavailable` with `UC` (upstream connection termination) when
    the entry the request was routed from is stale and not spoiled, and the
    response code details start with `upstream_reset_before_response_started`.
    On the CONNECT path a refused tunnel reaches the router as `503 UC`, and
    stale-but-not-spoiled means no request on the entry was cut off after
    reaching the actor. The details rule out a 1xx, which the router doesn't
    forward and the network filter counts as an answer.
  Other `503 UC` and `503 UR` responses evict the entry but are not replayed,
  leaving retries to the client. Eviction removes the binding only if it is
  still the one the request used, so a newer binding learned by another request
  stays.

## Filter configuration

The filter accepts an optional JSON configuration object in `filter_config`
(an empty config defaults to `{}`):

| Field | Type | Default | Description |
|---|---|---|---|
| `cache_ttl_seconds` | `u64` | `5` | Time-to-live in seconds for cached actor -> worker bindings |
| `atunnel_port` | `u16` | `443` | Port `atunnel` listens on at the worker |
| `target_port_header` | `String` | `"x-ate-target-port"` | Request header carrying the actor's target port to `atunnel` |
| `max_cached_actors` | `usize` | `10000` | Most actors cached per Envoy worker thread |

## Metrics

| Counter name | Description |
|---|---|
| `ate_ingress_cache.hit` | Incremented when an unexpired actor -> worker binding is served from cache |
| `ate_ingress_cache.miss` | Incremented when a bodiless request finds no unexpired binding in the cache |
| `ate_ingress_cache.bypass` | Incremented when a request with a body skips the cache lookup and is routed through `ext_proc` |
| `ate_ingress_cache.eviction` | Incremented when an expired or stale cache entry is evicted |
| `ate_ingress_cache.replay` | Incremented when a stale fast-path request is replayed via `recreate_stream` |
| `ate_ingress_cache.marked_stale` | Incremented when a tunnel closes with `UF` and marks at least one live entry stale (`tunnel_watcher`) |
| `ate_ingress_cache.marked_spoiled` | Incremented when an established tunnel closes with a request outstanding and marks at least one live entry spoiled (`tunnel_watcher`) |

`hit`, `miss`, and `bypass` partition the requests the filter resolves an actor
for, so the hit rate among cacheable requests is `hit / (hit + miss)`. Envoy's
Prometheus endpoint exports the counters with `.` replaced by `_`, e.g.
`ate_ingress_cache_bypass`.

## Building and testing

```bash
hack/test-dynamic-modules.sh
```

