# Envoy Substrate Egress Policy Implementation - Rust Dynamic Module

An Envoy dynamic module, written in Rust, that runs as a listener filter on
the sdsmint egress gateway's inner listener and names the filter chain each
tunneled connection belongs on.

## What it decides

The gateway's ext_proc sidecar answers every allowed CONNECT with the rules
that decide the tunnel's TLS: the policy's `https` rules for the port the
actor dialed, most specific first (a pattern without a wildcard before one
with, then a rule naming its ports before one naming all of them). The outer
chain copies that answer into the `dev.ate.policy.egress` filter state as
JSON, shared with the inner listener:

```json
{"rules": [{"pattern": "api.example.com", "mode": "mitm"},
           {"pattern": "*.example.com", "mode": "mitm"}]}
```

After `tls_inspector` and `http_inspector` have looked at the first bytes,
this filter writes one of these verdicts to the `dev.ate.egress.filter_chain`
filter state, and the listener's `filter_chain_matcher` selects the chain by
it:

| First bytes | Verdict | Chain |
|---|---|---|
| A ClientHello whose SNI matches a rule (first match wins; `*` matches every name, `*.suffix` exactly one label, anything else the whole name, ASCII case folded) | `mitm` | `egress_tls_mitm`: terminated with a minted leaf, decided per request |
| Any other ClientHello: no SNI, no match, no rules, unparseable rules | `denied` | none: the connection is closed |
| Not TLS | `cleartext` | `egress_cleartext`: decided per request |
| A transport protocol other than `tls` or `raw_buffer` | `denied` | none |

`tls_passthrough` rules are not in the answer yet, so their names are closed
rather than forwarded. A connection that sends nothing before the listener
filter timeout never reaches this filter, sets no verdict, and is closed.

The Go side of the contract is `cmd/atenet/internal/router/extproc`
(`EgressPolicyMetadataNamespace`, `EgressFilterChainFilterStateKey`) and
`internal/egresspolicy` (`SNIRules`, whose pattern grammar this filter
mirrors). The manifest tests in `cmd/atenet/internal/router` hold the
listener configuration to it.

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

Add the filter after the inspectors in the listener's `listener_filters`:

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
`manifests/ate-install/atenet-egress-with-sdsmint.yaml` is the complete
configuration, matcher and chains included.
