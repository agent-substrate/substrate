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

# Stage 1: Build the release version of the Rust dynamic module
FROM rust:bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS builder

WORKDIR /usr/src/dynamic-modules/egress-policy

# Install build dependencies required by bindgen for generating Envoy ABI bindings
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy rust module source code
COPY dynamic-modules/egress-policy/ .

# Build release version of the dynamic module shared library
RUN cargo build --release --locked

# Stage 2: Create the runtime image containing Envoy and the built dynamic module shared library
# When changing Envoy version, the version of SDK in the Cargo.toml files SHOULD match.
# Pin the multi-platform image index, not one platform's manifest, so that the
# --platform the installer passes (the host architecture on kind, linux/amd64
# otherwise) selects a matching Envoy binary.
FROM envoyproxy/envoy:v1.39.1@sha256:57e14a549d7bd43c8d3f6d03e8cfa653e037d4b38e133acd9b54f38c524401b4

# Copy the built dynamic module shared library from the builder stage
COPY --from=builder /usr/src/dynamic-modules/egress-policy/target/release/libenvoy_substrate_egress_policy.so /usr/local/lib/libenvoy_substrate_egress_policy.so

# Set default search path for Envoy dynamic modules
ENV ENVOY_DYNAMIC_MODULES_SEARCH_PATH=/usr/local/lib
