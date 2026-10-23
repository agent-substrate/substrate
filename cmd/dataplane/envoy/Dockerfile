# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Stage 1: Build the release version of the Rust dynamic modules
# This stage runs on the build host and cross-compiles for the target platform,
# so building for another architecture avoids slow emulation.
FROM --platform=$BUILDPLATFORM rust:bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS builder
ARG BUILDARCH
ARG TARGETARCH

WORKDIR /usr/src/dynamic-modules

# Install build dependencies required by bindgen for generating Envoy ABI bindings,
# and when cross-compiling, the target's linker and libc
RUN case "$TARGETARCH" in \
      amd64) echo x86_64 > /target-arch ;; \
      arm64) echo aarch64 > /target-arch ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && arch="$(cat /target-arch)" \
    && if [ "$BUILDARCH" != "$TARGETARCH" ]; then cross="gcc-$(echo "$arch" | tr _ -)-linux-gnu libc6-dev-${TARGETARCH}-cross"; fi \
    && apt-get update && apt-get install -y --no-install-recommends \
    clang \
    libclang-dev \
    $cross \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add "${arch}-unknown-linux-gnu"

# Copy rust workspace source code
COPY dynamic-modules/ .

# Build release version of the dynamic module shared libraries
RUN arch="$(cat /target-arch)" \
    && if [ "$BUILDARCH" != "$TARGETARCH" ]; then \
      export "CARGO_TARGET_$(echo "$arch" | tr a-z A-Z)_UNKNOWN_LINUX_GNU_LINKER=${arch}-linux-gnu-gcc"; \
    fi \
    && cargo build --release --locked --target "${arch}-unknown-linux-gnu" \
    && mkdir -p target/release \
    && cp "target/${arch}-unknown-linux-gnu/release/libenvoy_substrate_egress_policy.so" target/release/

# Stage 2: Create the runtime image containing Envoy and the built dynamic module shared libraries
# When changing Envoy version, the version of SDK in the Cargo.toml files SHOULD match.
# Pin the multi-platform image index, not one platform's manifest, so that the
# --platform the installer passes (the host architecture on kind, linux/amd64
# otherwise) selects a matching Envoy binary.
FROM envoyproxy/envoy:v1.39.5@sha256:8fc594a5d85eb76dca24155cfb9208882406873932b20cf33649a0a611194c43

# Copy the built dynamic module shared library from the builder stage
COPY --from=builder /usr/src/dynamic-modules/target/release/libenvoy_substrate_egress_policy.so /usr/local/lib/libenvoy_substrate_egress_policy.so

# Set default search path for Envoy dynamic modules
ENV ENVOY_DYNAMIC_MODULES_SEARCH_PATH=/usr/local/lib
