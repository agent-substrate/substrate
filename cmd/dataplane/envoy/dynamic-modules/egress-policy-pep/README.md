# Envoy Substrate Egress Policy PEP - Rust Dynamic Module

An Envoy HTTP filter dynamic module (`envoy_substrate_egress_policy_pep`), written in Rust, for enforcing egress policy on HTTP request and response headers.

## Building

From `cmd/dataplane/envoy/dynamic-modules`:

```bash
cargo build --release -p substrate-envoy-egress-policy-pep
```

The compiled shared object will be located at `target/release/libenvoy_substrate_egress_policy_pep.so`.

## Testing

```bash
cargo test -p substrate-envoy-egress-policy-pep
```
