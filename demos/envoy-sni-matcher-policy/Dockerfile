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
FROM rust:bookworm AS builder

WORKDIR /usr/src/rust-module

# Install build dependencies required by bindgen for generating Envoy ABI bindings
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy rust module source code
COPY rust-module/ .

# Build release version of the dynamic module shared library
RUN cargo build --release

# Stage 2: Create the runtime image containing Envoy and the built dynamic module shared library
FROM envoyproxy/envoy:v1.39-latest

# Copy the built dynamic module shared library from the builder stage
COPY --from=builder /usr/src/rust-module/target/release/libenvoy_sni_matcher_policy.so /usr/local/lib/libenvoy_sni_matcher_policy.so

# Set default search path for Envoy dynamic modules
ENV ENVOY_DYNAMIC_MODULES_SEARCH_PATH=/usr/local/lib
