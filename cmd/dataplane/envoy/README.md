# Envoy Dataplane Image

This directory contains the build definition and custom extensions for the Envoy dataplane container image (`envoy-dataplane`) used by Agent Substrate's `atenet-egress` gateway.

## Directory Contents

```
cmd/dataplane/envoy/
├── Dockerfile                       # Multi-stage build for the Envoy dataplane image
└── dynamic-modules/
    └── egress-policy/               # Rust Envoy Dynamic Module for egress policy enforcement
```

- **`Dockerfile`**: Multi-stage container build that:
  1. Compiles the Rust dynamic module (`envoy-substrate-egress-policy`) into a shared library (`libenvoy_substrate_egress_policy.so`) in a `rust:bookworm` builder stage.
  2. Packages the compiled `.so` into the `envoyproxy/envoy:v1.39-latest` runtime image under `/usr/local/lib/libenvoy_substrate_egress_policy.so` and sets `ENVOY_DYNAMIC_MODULES_SEARCH_PATH=/usr/local/lib`.
- **`dynamic-modules/egress-policy/`**: A Rust crate using the Envoy Dynamic Modules SDK (`envoy-proxy-dynamic-modules-rust-sdk`) that implements a custom Envoy listener filter for Substrate egress policy evaluation. See [`dynamic-modules/egress-policy/README.md`](dynamic-modules/egress-policy/README.md) for module-specific build, test, and Envoy configuration details.

## Building and Deployment

During cluster setup (`ate-setup`), the image is built from this directory via `docker buildx`, pushed to `$KO_DOCKER_REPO/envoy-dataplane`, and its resolved digest replaces the `${ENVOY_DATAPLANE_IMAGE}` placeholder in the egress manifests (`manifests/ate-install/atenet-egress*.yaml`).

To build the image manually from the repository root:

```bash
docker buildx build -t envoy-dataplane cmd/dataplane/envoy
```
