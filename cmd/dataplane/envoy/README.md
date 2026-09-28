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

During a build from source, `ate-setup` builds the image from this directory via `docker buildx`, pushes it to `$KO_DOCKER_REPO/envoy-dataplane`, and its resolved digest replaces the `${ENVOY_DATAPLANE_IMAGE}` placeholder in the egress manifests (`manifests/ate-install/atenet-egress*.yaml`).

An install of pre-built images (`ate-setup deploy --image-repo REPO --image-tag TAG`) builds nothing, and pins `REPO/envoy-dataplane:TAG` instead, so a release has to publish the image alongside the `ko` images. From the repository root:

```bash
make build-release-images KO_DOCKER_REPO=REPO VERSION=TAG
```

That builds every image of the release under `TAG`, this one with `make build-envoy-dataplane`, which also works on its own.

This builds for `KO_DEFAULTPLATFORMS`, or `linux/amd64` when that is unset, the same platforms `ate-setup` builds the image for. The `ko` images default to `linux/amd64,linux/arm64` (`.ko.yaml`); to match them, set `DOCKERFILE_PLATFORMS=linux/amd64,linux/arm64`. On an amd64 host the arm64 build compiles Rust under emulation, which needs QEMU registered with binfmt (e.g. `docker run --privileged --rm tonistiigi/binfmt --install arm64`) and a buildx builder that supports multi-platform builds. Extra `docker buildx` flags go in `DOCKER_BUILD_FLAGS`.

To build the image locally without pushing it:

```bash
docker buildx build -t envoy-dataplane cmd/dataplane/envoy
```
