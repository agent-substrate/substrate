# Envoy Substrate Egress Policy Implementation - Rust Dynamic Module

An Envoy dynamic module, written in Rust, that stores and evaluates per-tunnel
egress policies in the egress gateway (`atenet-egress`).

The module registers two filters from the same shared library
(`libenvoy_substrate_egress_policy.so`):

1. **`envoy_substrate_egress_policy_store` (`EgressPolicyStoreFilter`)** — an
   HTTP filter on the outer `egress` listener (`CONNECT` leg) that parses the
   serialized SNI rules returned by `ext_proc`, deduplicates and caches the
   parsed `Rc<EgressPolicy>` in a worker-thread-local store
   (`GlobalEgressPolicyStore`), and writes the `u64` policy hash key to filter
   state so it can be shared with the inner listener.
2. **`envoy_substrate_egress_policy` (`EgressPolicyFilter`)** — a listener
   filter on the inner `mitm_listener` that reads the policy hash key from
   filter state, looks up the cached `Rc<EgressPolicy>` in the worker-thread-local
   store, evaluates the connection's transport protocol and TLS `ClientHello`
   SNI, and writes the selected filter chain name to
   `dev.ate.egress.filter_chain`.

## Request flow through the Envoy dataplane

```mermaid
sequenceDiagram
  autonumber
  participant Actor as "Actor / atunnel"
  participant Outer as "Outer Listener (egress :443)"
  participant ExtProc as "ext_proc Sidecar"
  participant StoreFilter as "EgressPolicyStoreFilter (HTTP Filter)"
  participant LocalStore as "Thread-Local EgressPolicyStore"
  participant InnerListener as "Inner Listener (mitm_listener + EgressPolicyFilter)"
  participant InnerChain as "Inner Filter Chain (mitm / cleartext / passthrough)"
  participant Origin as "Upstream Origin"

  Actor->>Outer: "mTLS + HTTP/1.1 CONNECT <ip>:<port>"
  Note over Outer: "set_filter_state writes dev.ate.actor.identity & dev.ate.connect.authority (shared_with_upstream: ONCE)"
  Outer->>ExtProc: "Check CONNECT (actor cert, xds.filter_chain_name=egress, authority)"
  ExtProc-->>Outer: "Allow + dynamic metadata: dev.ate.policy.egress:policy_json & dev.ate.egress:dialed_port"

  Outer->>StoreFilter: "on_request_headers()"
  StoreFilter->>StoreFilter: "EgressPolicy::new_from_metadata() parses JSON & computes RapidHasher u64 hash"
  StoreFilter->>LocalStore: "lookup(policy.hash)"
  alt "Policy already in thread-local store"
    LocalStore-->>StoreFilter: "Some(existing Rc<EgressPolicy>)"
    StoreFilter->>StoreFilter: "Increment ate_egress.policy_shared counter"
  else "New policy for this worker thread"
    StoreFilter->>LocalStore: "store(policy.hash, &policy)"
    StoreFilter->>StoreFilter: "Increment ate_egress.policy_store_size gauge"
  end
  StoreFilter-->>Outer: "Hold Rc<EgressPolicy> on filter & set filter state dev.ate.policy.egress.hash.not_shared = <hash>"

  Note over Outer: "set_filter_state copies hash to dev.ate.policy.egress.hash & port to envoy.upstream.dynamic_port (shared_with_upstream: ONCE)"
  Outer->>InnerListener: "Route upgraded CONNECT stream via mitm_internal cluster"
  Outer-->>Actor: "HTTP/1.1 200 OK (tunnel established)"

  Actor->>InnerListener: "First tunneled bytes (TLS ClientHello or cleartext HTTP)"
  Note over InnerListener: "tls_inspector & http_inspector detect transport protocol and SNI"
  InnerListener->>LocalStore: "Read dev.ate.policy.egress.hash from filter state & lookup(hash)"
  LocalStore-->>InnerListener: "Option<Rc<EgressPolicy>>"
  InnerListener->>InnerListener: "Evaluate transport_verdict() / tls_verdict(policy, sni) -> write dev.ate.egress.filter_chain"
  InnerListener->>InnerChain: "filter_chain_matcher selects chain (egress_tls_mitm, egress_passthrough, egress_cleartext, or deny)"

  alt "verdict == mitm (egress_tls_mitm) or cleartext (egress_cleartext)"
    Note over InnerChain: "If mitm: terminate TLS with on-demand leaf cert from sds_mint"
    Actor->>InnerChain: "Inner HTTP/gRPC request"
    InnerChain->>ExtProc: "Authorize inner request (actor identity, connect authority, SNI, Host, headers)"
    ExtProc-->>InnerChain: "Allow + dynamic metadata: dev.ate.egress:dial (name|address) & dev.ate.egress:host"
    InnerChain->>Origin: "Forward request via dynamic_forward_proxy or original_dst cluster"
    Origin-->>Actor: "HTTP/gRPC response"
  else "verdict == passthrough (egress_passthrough)"
    InnerChain->>Origin: "Resolve SNI on envoy.upstream.dynamic_port & relay raw TLS via tcp_proxy"
    Origin-->>Actor: "Raw TLS stream"
  else "verdict == denied"
    InnerListener-->>Actor: "No matching filter chain (close connection)"
  end

  Actor->>Outer: "Close CONNECT tunnel"
  Outer->>StoreFilter: "on_stream_complete()"
  opt "Rc::strong_count(&self.policy) == 2 (last active stream using this policy on thread)"
    StoreFilter->>LocalStore: "remove(policy.hash)"
    StoreFilter->>StoreFilter: "Decrement ate_egress.policy_store_size gauge"
  end
```

## How Egress Policy is implemented

### 1. Outer `CONNECT` leg and thread-local policy caching (`policy_store_filter.rs`, `egress_policy.rs`, `egress_policy_store.rs`)

When an actor opens an outbound connection through `atunnel`, `atunnel` sends an
HTTP/1.1 `CONNECT <ip>:<port>` over mTLS to the `egress` listener:

1. **Identity and authority capture**: Before `ext_proc`, Envoy's
   `set_filter_state` filter records the verified client certificate's URI SAN
   in `dev.ate.actor.identity` and the `CONNECT` target in
   `dev.ate.connect.authority`, both marked `shared_with_upstream: ONCE`.
2. **`CONNECT` `ext_proc` callout**: The `ext_proc` sidecar verifies the actor's
   certificate and checks that the actor has an `EgressPolicy`. It returns the
   dialed port in dynamic metadata (`dev.ate.egress:dialed_port`) and the
   policy's TLS SNI rules for that port in dynamic metadata namespace
   `dev.ate.policy.egress` under key `policy_json`, ordered most-specific first
   (exact hostname before wildcard, port-specific rule before all-ports rule):

   ```json
   {"rules": [{"pattern": "api.example.com", "mode": "mitm"},
              {"pattern": "*.example.com", "mode": "passthrough"}]}
   ```

3. **Parsing and hashing (`EgressPolicy::new_from_metadata`)**:
   `EgressPolicyStoreFilter::on_request_headers` reads
   `dev.ate.policy.egress:policy_json` from dynamic metadata, deserializes it
   into an `EgressPolicy`, and hashes the rules using `RapidHasher` into a
   `u64` (`policy.hash`). If the metadata is missing or invalid JSON, the filter
   increments the `ate_egress.parse_errors` counter.
4. **Thread-local deduplication (`GlobalEgressPolicyStore`)**:
   Because Envoy processes the outer `CONNECT` stream and the inner
   `mitm_internal` connection on the same worker thread, policies are stored in
   a `ThreadLocal<RefCell<EgressPolicyStore>>` mapping `u64 -> Rc<EgressPolicy>`:
   - If `local_store.lookup(policy.hash)` finds an existing entry, the filter
     reuses that `Rc<EgressPolicy>` and increments the
     `ate_egress.policy_shared` counter.
   - Otherwise, it inserts `Rc<EgressPolicy>` into the thread-local store and
     increments the `ate_egress.policy_store_size` gauge.
   - The filter stores the `Rc<EgressPolicy>` in `self.policy` for the lifetime
     of the `CONNECT` stream.
5. **Propagating the store key to the inner listener**:
   `EgressPolicyStoreFilter` writes `self.policy.hash.to_string()` into filter
   state key `dev.ate.policy.egress.hash.not_shared`. The subsequent
   `set_filter_state` HTTP filter copies that value into
   `dev.ate.policy.egress.hash` with `shared_with_upstream: ONCE` so Envoy
   propagates the key across the `mitm_internal` upstream connection to
   `mitm_listener`.
6. **Reference-counted eviction (`on_stream_complete`)**:
   When the `CONNECT` tunnel closes, `EgressPolicyStoreFilter::on_stream_complete`
   checks `Rc::strong_count(&self.policy)`. A count of `2` means only the filter
   instance (`self.policy`) and the thread-local `HashMap` hold references to
   the policy (no other active tunnel on that worker thread shares it). The
   filter removes the entry from `GlobalEgressPolicyStore::get_local_store()`
   and decrements the `ate_egress.policy_store_size` gauge.

### 2. Inner listener filter and SNI evaluation (`listener_filter.rs`)

After `tls_inspector` and `http_inspector` inspect the first bytes sent inside
the tunnel on `mitm_listener`, `EgressPolicyFilter::on_accept` determines the
filter chain:

1. For TLS connections (`transport == "tls"`), the filter reads the `u64` hash
   key from filter state `dev.ate.policy.egress.hash` and looks up the
   `Rc<EgressPolicy>` in `GlobalEgressPolicyStore::get_local_store()`.
2. It normalizes the client's SNI (ASCII-lowercased with at most one trailing
   dot stripped) and matches it against `policy.rules` in order.
3. It writes one of the following verdicts to `dev.ate.egress.filter_chain`,
   which `mitm_listener`'s `filter_chain_matcher` uses to select the chain:

| First bytes | Verdict | Chain |
|---|---|---|
| A ClientHello whose SNI matches an `https` (`mitm`) rule (first match wins; `*` matches every name, `*.suffix` exactly one label, anything else the whole name, ASCII case folded) | `mitm` | `egress_tls_mitm`: terminated with a minted leaf, decided per request |
| A ClientHello whose first matching rule is `tls_passthrough` (`passthrough`) | `passthrough` | `egress_passthrough`: relayed unread to the resolved SNI on the dialed port |
| Any other ClientHello: no SNI, no match, no rules, missing or unknown policy hash | `denied` | none: the connection is closed |
| Not TLS (`None` or `raw_buffer`) | `cleartext` | `egress_cleartext`: decided per request |
| A transport protocol other than `tls` or `raw_buffer` | `denied` | none |

### 3. Inner request processing (`egress_tls_mitm`, `egress_cleartext`, `egress_passthrough`)

- **`egress_tls_mitm`**: Terminates the tunneled TLS connection using an
  on-demand leaf certificate minted for the SNI by `sds_mint`. Each inner
  HTTP/gRPC request is sent to `ext_proc` along with
  `filter_state['dev.ate.actor.identity']`,
  `filter_state['dev.ate.connect.authority']`, and
  `connection.requested_server_name`. `ext_proc` evaluates the full `https`
  rules (including `Host`, methods, paths, headers, and credential injection)
  and sets `dev.ate.egress:dial` (`name` or `address`) and
  `dev.ate.egress:host`. `set_filter_state` copies `dev.ate.egress:host` into
  `envoy.upstream.dynamic_host`, and `dynamic_forward_proxy` resolves and dials
  that host on `envoy.upstream.dynamic_port` (the port the actor dialed).
- **`egress_cleartext`**: Processes cleartext HTTP requests without TLS
  termination. Each request calls out to `ext_proc` with the shared actor
  identity and connect authority, evaluates the policy's `http` rules, and
  routes via `dynamic_forward_proxy` or original destination according to
  `dev.ate.egress:dial`.
- **`egress_passthrough`**: Uses `sni_dynamic_forward_proxy` and `tcp_proxy` to
  resolve the verified SNI on `envoy.upstream.dynamic_port` and relay the raw
  TLS bytes without terminating TLS or calling `ext_proc` again. The IP address
  the actor originally dialed in `CONNECT` is never used.

The Go side of the contract is `cmd/atenet/internal/router/extproc`
(`EgressPolicyMetadataNamespace`, `EgressFilterChainFilterStateKey`) and
`internal/egresspolicy` (`SNIRules`, whose pattern grammar this filter
mirrors). The manifest tests in `cmd/atenet/internal/router` hold the
listener configuration to it.

## Envoy stats

The HTTP filter (`envoy_substrate_egress_policy_store`) defines the following
metrics:

| Metric | Type | Description |
|---|---|---|
| `ate_egress.policy_store_size` | Gauge | Current number of unique `EgressPolicy` entries stored in the worker thread's local store (summed across workers by Envoy). |
| `ate_egress.parse_errors` | Counter | Number of times `EgressPolicy::new_from_metadata` returned `None` due to missing or malformed policy metadata. |
| `ate_egress.policy_shared` | Counter | Number of times a `CONNECT` request reused an `EgressPolicy` already present in the worker thread's local store. |

## Building

Prerequisites: Rust toolchain (Cargo, rustc 1.75+), plus `clang` and
`libclang-dev` for the SDK's bindgen step.

```bash
cargo build --release
```

The compiled shared object will be located at
`target/release/libenvoy_substrate_egress_policy.so`. `cmd/dataplane/envoy/Dockerfile`
builds it the same way and packages it into the Envoy image.

## Testing

```bash
cargo test
```

## Envoy configuration

On the outer `egress` listener's HTTP filter chain (after `ext_proc`):

```yaml
- name: envoy.filters.http.dynamic_modules
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
    dynamic_module_config:
      name: envoy_substrate_egress_policy
    filter_name: envoy_substrate_egress_policy_store
- name: envoy.filters.http.set_filter_state
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.set_filter_state.v3.Config
    on_request_headers:
    - object_key: dev.ate.policy.egress.hash
      factory_key: envoy.hashable_string
      skip_if_empty: true
      shared_with_upstream: ONCE
      format_string:
        text_format_source:
          inline_string: "%FILTER_STATE(dev.ate.policy.egress.hash.not_shared:PLAIN)%"
        omit_empty_values: true
```

On the inner `mitm_listener`'s `listener_filters` (after the inspectors):

```yaml
listener_filters:
- name: envoy.filters.listener.tls_inspector
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.listener.tls_inspector.v3.TlsInspector
- name: envoy.filters.listener.http_inspector
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.listener.http_inspector.v3.HttpInspector
- name: envoy.filters.listener.dynamic_modules
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.listener.dynamic_modules.v3.DynamicModuleListenerFilter
    dynamic_module_config:
      name: envoy_substrate_egress_policy
    filter_name: envoy_substrate_egress_policy
```

Set the environment variable `ENVOY_DYNAMIC_MODULES_SEARCH_PATH` to the
directory containing `libenvoy_substrate_egress_policy.so` (e.g.
`export ENVOY_DYNAMIC_MODULES_SEARCH_PATH=/path/to/target/release`).
`manifests/ate-install/atenet-egress.yaml` is the complete
configuration, matcher and chains included.
