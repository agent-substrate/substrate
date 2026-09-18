# Envoy SNI Matcher Policy - Rust Dynamic Module

This directory contains an Envoy Dynamic Module written in Rust implementing a custom listener filter.

## Overview

- **Extension Point:** Listener filter (`envoy.filters.listener.dynamic_modules`)
- **Filter Configuration:** Empty (`EmptyFilterConfig`)
- **Callbacks:** Implements `on_data` returning `Continue`
- **Output:** Shared library `libenvoy_sni_matcher_policy.so` (`cdylib`)

## Building

Prerequisites: Rust toolchain (Cargo, rustc 1.75+).

```bash
cargo build --release
```

The compiled shared object will be located at `target/release/libenvoy_sni_matcher_policy.so`.

## Testing

```bash
cargo test
```

## Envoy Configuration

To load this dynamic module as a listener filter in Envoy, add the dynamic modules filter to the listener's `listener_filters`:

```yaml
listener_filters:
- name: envoy.filters.listener.dynamic_modules
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.listener.dynamic_modules.v3.DynamicModuleListenerFilter
    dynamic_module_config:
      name: envoy_sni_matcher_policy
    filter_name: envoy_sni_matcher_policy
    filter_config: {}
```

Set the environment variable `ENVOY_DYNAMIC_MODULES_SEARCH_PATH` to the directory containing `libenvoy_sni_matcher_policy.so` (e.g. `export ENVOY_DYNAMIC_MODULES_SEARCH_PATH=/path/to/target/release`).
