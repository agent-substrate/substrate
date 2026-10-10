# Envoy Dataplane Image

This directory contains the build definition and custom extensions for the Envoy dataplane container image (`envoy-dataplane`) used by Agent Substrate's `atenet-egress` gateway.

## Directory Contents

```
cmd/dataplane/envoy/
├── Dockerfile                       # Multi-stage build for the Envoy dataplane image
└── dynamic-modules/                 # Cargo workspace for Rust Envoy Dynamic Modules
    ├── Cargo.toml
    ├── egress-policy/               # Listener filter for egress policy chain selection
    └── egress-policy-pep/           # HTTP filter for egress policy enforcement (PEP)
```

- **`Dockerfile`**: Multi-stage container build that:
  1. Compiles the Rust dynamic modules workspace into shared libraries (`libenvoy_substrate_egress_policy.so` and `libenvoy_substrate_egress_policy_pep.so`) in a `rust:bookworm` builder stage.
  2. Packages the compiled `.so` files into the `envoyproxy/envoy:v1.39.1` runtime image under `/usr/local/lib/` and sets `ENVOY_DYNAMIC_MODULES_SEARCH_PATH=/usr/local/lib`.
- **`dynamic-modules/egress-policy/`**: A Rust crate using the Envoy Dynamic Modules SDK (`envoy-proxy-dynamic-modules-rust-sdk`) that implements a custom Envoy listener filter for Substrate egress policy evaluation. See [`dynamic-modules/egress-policy/README.md`](dynamic-modules/egress-policy/README.md) for module-specific build, test, and Envoy configuration details.
- **`dynamic-modules/egress-policy-pep/`**: A Rust crate using the Envoy Dynamic Modules SDK (`envoy-proxy-dynamic-modules-rust-sdk`) that implements an Envoy HTTP filter for egress policy enforcement. See [`dynamic-modules/egress-policy-pep/README.md`](dynamic-modules/egress-policy-pep/README.md).

## Building and Deployment

During a build from source, `ate-setup` builds the image from this directory via `docker buildx`, pushes it to `$KO_DOCKER_REPO/envoy-dataplane`, and its resolved digest replaces the `${ENVOY_DATAPLANE_IMAGE}` placeholder in the egress manifest (`manifests/ate-install/atenet-egress.yaml`).

A pre-built install (`ate-setup deploy --image-repo REPO --image-tag TAG`) builds nothing and pins `REPO/envoy-dataplane:TAG` instead, so a release publishes it with the other images:

```bash
VERSION=TAG go run ./cmd/ate-setup publish release-images --ko-docker-repo REPO
```

The image targets the same platforms as the ko images: `KO_DEFAULTPLATFORMS`, else `defaultPlatforms` in `.ko.yaml` (linux/amd64 and linux/arm64), else `linux/amd64`. Building another architecture compiles Rust under QEMU, which must be registered with binfmt (e.g. `docker run --privileged --rm tonistiigi/binfmt --install arm64`).

To build the image locally without pushing it:

```bash
docker buildx build -t envoy-dataplane cmd/dataplane/envoy
```
