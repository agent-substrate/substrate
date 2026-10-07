# Envoy Substrate Egress Policy Cache - Rust Dynamic Module

An Envoy dynamic module, written in Rust, that runs as an HTTP filter on the
egress gateway's outer `CONNECT` listener and caches egress policy SNI rules
per actor certificate and destination port in a thread-local LRU cache so
repeat `CONNECT` requests from the same actor to the same destination can
use locally cached policy and avoid external callout to get the policy.

It is safe because external callout evaluates if there are policies that allow
given actor to egress to specific destination port and then provides SNI and host
matching rules to the dataplane. Since local cache is populated only when CONNECT
was allowed and keyed by the actor ID and destination port it safe to use cached
SNI and host matching rules. The freshness configuration `cache_ttl` determines
responsiveness of the dataplane to policy changes.

## How it works

Each `EgressPolicyCacheFilterConfig` owns a `ThreadLocal` store of
`LruCache<String, CachedPolicy>` instances (`max_cache_items` capacity per
Envoy worker thread). Each `CachedPolicy` entry stores the serialized egress
policy JSON string alongside the `Instant` (`stored_at`) when it was cached,
keyed by `<peer_cert_digest>;<destination_port>`, where `<peer_cert_digest>` is
the downstream peer certificate's SHA-256 digest
(`connection.sha256_peer_certificate_digest`) and `<destination_port>` is the
destination port extracted from the CONNECT `:authority` request header.

### Request path (`on_request_headers`)

When an actor opens a `CONNECT` tunnel on the outer listener:

1. The filter builds the cache key `<peer_cert_digest>;<destination_port>` from
   `connection.sha256_peer_certificate_digest` and the port from the CONNECT `:authority`
   header, and looks up cached policy in worker thread local LRU cache:
   - **If unexpired entry was found.**:
     - Writes the cached policy JSON to filter state under key
       `dev.ate.policy.egress.cached`.
     - Sets filter state key `dev.ate.policy.egress.skip_callout` to `"true"` to skip the ext_proc
       callout.
     - Sets `has_cached_policy = true` on the per-stream
       `EgressPolicyCacheFilter`.
     - Sets `dev.ate.egress:dialed_port` in the dynamic metadata, so that inner
       request is sent to the port in the CONNECT authority.
     - Increments the `ate_egress.connect_cache_hit` counter.
   - **If expired entry (`stored_at.elapsed() > cache_ttl`) was found**:
     - Removes the expired entry from the LRU cache without setting filter
       state.
     - Increments the `ate_egress.connect_cache_miss` counter.
   - **Cache miss (or missing certificate digest / destination port)**:
     - Increments the `ate_egress.connect_cache_miss` counter.
2. Subsequent filters in the outer HTTP filter chain act on the result:
   - `envoy.filters.http.composite` checks whether
     `dev.ate.policy.egress.skip_callout` equals `"true"` in filter state and
     invokes `envoy.filters.http.ext_proc` only when it does not.
   - `envoy.filters.http.set_filter_state` copies either
     `%DYNAMIC_METADATA(dev.ate.policy.egress)%` (on a cache miss, populated by
     `ext_proc`) or `%FILTER_STATE(dev.ate.policy.egress.cached:PLAIN)%` (on a
     cache hit) into the `dev.ate.policy.egress` filter state with
     `shared_with_upstream: ONCE`, making it available to the inner listener's
     `egress-policy` listener filter.

### Response path (`on_response_headers`)

When the `CONNECT` response headers arrive:

1. If `cache_enabled` is `false`, the filter returns `Continue` immediately.
2. If `:status` is `200` and `self.has_cached_policy` is `false`:
   - Builds cache value from the `dev.ate.policy.egress` filer state.
   - Inserts `CachedPolicy { policy, stored_at: Instant::now() }` into the
     current worker thread's LRU cache at the the `<peer_cert_digest>;<destination_port>` key.
3. If `self.has_cached_policy` is `true` (the policy for this stream came from
   the cache), caching is skipped so the entry's original `stored_at` timestamp
   is preserved until `cache_ttl` expires.

## Filter configuration

The filter accepts an optional JSON configuration object in `filter_config`
(an empty config defaults to `{}`):

| Field | Type | Default | Description |
|---|---|---|---|
| `cache_ttl` | `u64` (seconds) | `5` | Time-to-live in seconds for cached policy entries before they expire |
| `cache_enabled` | `bool` | `true` | Enables or disables reading from and writing to the policy cache |
| `max_cache_items` (or `max-cache-items`) | `usize` | `1000` | Maximum number of entries in each worker thread's LRU cache |

Example JSON configuration:

```json
{
  "cache_ttl": 5,
  "cache_enabled": true,
  "max_cache_items": 1000
}
```

## Metrics

The filter defines two Envoy counters on config initialization:

| Counter name | Envoy stat name | Description |
|---|---|---|
| `ate_egress.connect_cache_hit` | `dynamic_modules.custom.egress_policy_cache.ate_egress.connect_cache_hit` | Incremented when a non-expired policy is found in the cache on `CONNECT` |
| `ate_egress.connect_cache_miss` | `dynamic_modules.custom.egress_policy_cache.ate_egress.connect_cache_miss` | Incremented when no entry or an expired entry is found in the cache on `CONNECT` |

## Building

Prerequisites: Rust toolchain (Cargo, rustc 1.75+), plus `clang` and
`libclang-dev` for the SDK's bindgen step.

```bash
cargo build --release
```

The compiled shared object will be located at
`target/release/libenvoy_substrate_egress_policy_cache.so`.
`cmd/dataplane/envoy/Dockerfile` builds it and packages it into the Envoy
dataplane image.

## Testing

```bash
cargo test
```

Or from the repository root:

```bash
hack/test-dynamic-modules.sh
```

> **Note:** Because the LRU cache is thread-local per Envoy worker thread, set
> `export E2E_ENVOY_CONCURRENCY=1` before deploying `ate-system` when running
> the networking E2E suite (`./internal/e2e/suites/networking`) to test caching
> behavior (`TestActorEgressPolicyCache` and
> `TestActorEgressPolicyCacheExpiration`). This configures Envoy with a single
> worker thread (`--concurrency 1`) so consecutive `CONNECT` requests are
> routed to the same thread-local cache instance.

See `manifests/ate-install/atenet-egress.yaml` for the complete configuration.
