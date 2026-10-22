# Envoy Substrate Ingress Cache - Rust Dynamic Module

An Envoy dynamic module, written in Rust, that runs as an HTTP filter ahead of
`ext_proc` on the ingress router and caches `<atespace>/<actor>` to worker IP
bindings in a process-wide `DashMap`.

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

With the gate off, the router installs neither this filter nor the cached
routes that skip `ext_proc`, so every request is resolved through `ext_proc`.

## Caveats

- **Stale routing for up to the TTL.** A cache hit skips `ext_proc` and so does
  not observe suspends, migrations, or reassignments made in the last
  `cache_ttl_seconds` (default 5s). The first request to the old worker fails
  and evicts the entry, as described below.
- **Clients must retry some failures.** Only bodiless requests that failed
  before reaching the actor are replayed by the router. A request with a body,
  or one interrupted mid-flight (`503 UC` or `UR`), returns the error to the
  client after evicting the entry; the client's retry is resolved through
  `ext_proc`.
- **gRPC clients need a retry policy for `UNAVAILABLE`.** A gRPC call that
  fails with status `14` evicts the entry and is returned to the client. Standard
  gRPC clients do not retry it automatically unless a retry policy is
  configured.
- **The cached route header is reserved.** A request that arrives with
  `ate-endpoint-cached` set and no cache entry is rejected with `400 Bad Request`.

## How it works

- **Cache hit (`on_request_headers`)**: When an unexpired binding exists for the
  target actor, the filter writes the `ORIGINAL_DST` dynamic metadata
  (`envoy.filters.listener.original_dst`), sets the `x-ate-target-port` and
  `ate-target-actor` request headers, sets `ate-endpoint-cached: 1` to select
  the route with `ext_proc` disabled, and clears the route cache.
- **Cache miss (`on_request_headers` / `on_response_headers`)**: The request
  proceeds through `ext_proc`, which resolves or resumes the actor and populates
  the `ORIGINAL_DST` dynamic metadata. On response headers, the filter records
  the worker IP in the cache for `cache_ttl_seconds`.
- **Stale binding eviction (`on_response_headers`)**: If a fast-path request
  fails with `421 Misdirected Request` (with `x-ate-assignment-stale: true`),
  `503 Service Unavailable` with upstream transport failure response flags (`UF`,
  `UH`, `UC`, `UR`), or gRPC status `14` (`UNAVAILABLE`), the filter immediately
  evicts the cached binding so subsequent requests resolve through `ext_proc`.
- **Stream replay (`recreate_stream`)**: To prevent duplicate execution of requests
  with side effects, automatic replay is only performed for bodiless requests
  (such as `GET` or `HEAD`) when failure is guaranteed to have occurred before any
  request reached or executed on the upstream actor:
  - `421 Misdirected Request` with `x-ate-assignment-stale: true` (rejected by
    worker `atunnel` prior to forwarding).
  - `503 Service Unavailable` with zero-byte upstream failure response flags:
    `UF` (upstream connection failure / connection refused before connection
    establishment) or `UH` (no healthy upstream host in cluster).
  Other failure conditions such as `503 UC` (upstream connection termination) or
  `503 UR` (upstream remote reset) are evicted immediately from the cache but are
  not silently replayed by the router, leaving retries to the client.

## Filter configuration

The filter accepts an optional JSON configuration object in `filter_config`
(an empty config defaults to `{}`):

| Field | Type | Default | Description |
|---|---|---|---|
| `cache_ttl_seconds` | `u64` | `5` | Time-to-live in seconds for cached actor -> worker bindings |
| `atunnel_port` | `u16` | `443` | Port `atunnel` listens on at the worker |
| `target_port_header` | `String` | `"x-ate-target-port"` | Request header carrying the actor's target port to `atunnel` |

## Metrics

| Counter name | Description |
|---|---|
| `ate_ingress_cache.hit` | Incremented when an unexpired actor -> worker binding is served from cache |
| `ate_ingress_cache.miss` | Incremented when no unexpired binding exists in the cache |
| `ate_ingress_cache.eviction` | Incremented when an expired or stale cache entry is evicted |
| `ate_ingress_cache.recreate_stream` | Incremented when a stale fast-path request is replayed via `recreate_stream` |

## Building and testing

```bash
hack/test-dynamic-modules.sh
```

