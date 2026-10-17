# Envoy Substrate Ingress Cache - Rust Dynamic Module

An Envoy dynamic module, written in Rust, that runs as an HTTP filter ahead of
`ext_proc` on the ingress router and caches `<atespace>/<actor>` to worker IP
bindings in a process-wide `DashMap`.

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
- **Stale binding recovery (`on_response_headers`)**: If a fast-path request
  receives `421 Misdirected Request` (or `503` across an internal `CONNECT`
  tunnel), the filter evicts the cached entry, removes `ate-endpoint-cached`,
  and calls `recreate_stream` so the request replays through `ext_proc`.

## Filter configuration

The filter accepts an optional JSON configuration object in `filter_config`
(an empty config defaults to `{}`):

| Field | Type | Default | Description |
|---|---|---|---|
| `cache_ttl_seconds` | `u64` | `5` | Time-to-live in seconds for cached actor -> worker bindings |
| `atunnel_port` | `u16` | `443` | Port `atunnel` listens on at the worker |
| `target_port_header` | `String` | `"x-ate-target-port"` | Request header carrying the actor's target port to `atunnel` |
| `cache_enabled` | `bool` | `true` | Enables or disables reading from and writing to the cache |

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

