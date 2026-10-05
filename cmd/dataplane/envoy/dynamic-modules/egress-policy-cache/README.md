# Envoy Substrate Egress Policy Cache - Rust Dynamic Module

An Envoy dynamic module, written in Rust, that runs as an HTTP filter on the
egress gateway's outer `CONNECT` listener and caches egress policy SNI rules
per actor certificate in a thread-local LRU cache so repeat `CONNECT` requests
from the same actor can bypass the `ext_proc` sidecar.

## How it works

Each `EgressPolicyCacheFilterConfig` owns a `ThreadLocal` store of
`LruCache<String, CachedPolicy>` instances (`max_cache_items` capacity per
Envoy worker thread). Each `CachedPolicy` entry stores the serialized egress
policy JSON string alongside the `Instant` (`stored_at`) when it was cached,
keyed by `<peer_cert_digest>;<destination_port>`, where `<peer_cert_digest>` is
the downstream peer certificate's SHA-256 digest
(`connection.sha256_peer_certificate_digest`) and `<destination_port>` is the
destination port extracted from the `:authority` request header.

### Request path (`on_request_headers`)

When an actor opens a `CONNECT` tunnel on the outer listener:

1. The filter builds the cache key `<peer_cert_digest>;<destination_port>` from
   `connection.sha256_peer_certificate_digest` and the `:authority` request
   header, and looks it up in the current worker thread's LRU cache:
   - **Fresh hit (`stored_at.elapsed() <= cache_ttl`)**:
     - Writes the cached policy JSON to filter state under key
       `dev.ate.policy.egress.cached`.
     - Sets `has_cached_policy = true` on the per-stream
       `EgressPolicyCacheFilter`.
     - Increments the `ate_egress.connect_cache_hit` counter.
   - **Expired entry (`stored_at.elapsed() > cache_ttl`)**:
     - Removes the expired entry from the LRU cache without setting filter
       state.
     - Increments the `ate_egress.connect_cache_miss` counter.
   - **Cache miss (or missing certificate digest / destination port)**:
     - Increments the `ate_egress.connect_cache_miss` counter.
2. Subsequent filters in the outer HTTP filter chain act on the result:
   - `envoy.filters.http.composite` checks for the presence of
     `dev.ate.policy.egress.cached` in filter state and invokes
     `envoy.filters.http.ext_proc` only when it is absent.
   - `envoy.filters.http.set_filter_state` copies either
     `%DYNAMIC_METADATA(dev.ate.policy.egress)%` (on a cache miss, populated by
     `ext_proc`) or `%FILTER_STATE(dev.ate.policy.egress.cached:PLAIN)%` (on a
     cache hit) into the `dev.ate.policy.egress` filter state with
     `shared_with_upstream: ONCE`, making it available to the inner listener's
     `egress-policy` listener filter.

### Response path (`on_response_headers`)

When the `CONNECT` response headers arrive:

1. If `cache_enabled` is `false`, the filter returns `Continue` immediately.
2. If `:status` is `200` and `!self.has_cached_policy`:
   - Reads the egress policy JSON from filter state key
     `dev.ate.policy.egress` and builds the cache key
     `<peer_cert_digest>;<destination_port>` from
     `connection.sha256_peer_certificate_digest` and `:authority`.
   - Inserts `CachedPolicy { policy, stored_at: Instant::now() }` into the
     current worker thread's LRU cache.
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

## Envoy configuration

In the outer `CONNECT` listener's `http_filters`, place the dynamic module
filter before the conditional `ext_proc` composite filter and the post-`ext_proc`
`set_filter_state` filter:

```yaml
- name: envoy.filters.http.dynamic_modules
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
    dynamic_module_config:
      name: envoy_substrate_egress_policy_cache
    filter_name: envoy_substrate_egress_policy_cache
- name: envoy.filters.http.composite
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.composite.v3.Composite
    matcher:
      matcher_list:
        matchers:
        - predicate:
            not_matcher:
              single_predicate:
                input:
                  name: filter_state
                  typed_config:
                    "@type": type.googleapis.com/envoy.extensions.matching.common_inputs.network.v3.FilterStateInput
                    key: dev.ate.policy.egress.cached
                value_match:
                  safe_regex:
                    google_re2: {}
                    regex: ".*"
          on_match:
            action:
              name: composite-action
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.http.composite.v3.ExecuteFilterAction
                typed_config:
                  name: envoy.filters.http.ext_proc
                  typed_config:
                    "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
                    # ...
- name: envoy.filters.http.set_filter_state
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.set_filter_state.v3.Config
    on_request_headers:
    - object_key: dev.ate.policy.egress
      factory_key: envoy.string
      skip_if_empty: true
      shared_with_upstream: ONCE
      format_string:
        text_format_source:
          inline_string: "%DYNAMIC_METADATA(dev.ate.policy.egress)%"
        omit_empty_values: true
    - object_key: dev.ate.policy.egress
      factory_key: envoy.string
      skip_if_empty: true
      shared_with_upstream: ONCE
      format_string:
        text_format_source:
          inline_string: "%FILTER_STATE(dev.ate.policy.egress.cached:PLAIN)%"
        omit_empty_values: true
```

See `manifests/ate-install/atenet-egress.yaml` for the complete configuration.
