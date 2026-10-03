#!/usr/bin/env bash
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
#
# Builds everything the demo needs: the Go binaries (including substrate's own
# atenet) for the container architecture, and the Rust dynamic module.
set -euo pipefail
cd "$(dirname "$0")/.."
REPO_ROOT="$(cd ../.. && pwd)"

ARCH="$(docker info --format '{{.Architecture}}')"
case "$ARCH" in
  x86_64|amd64) GOARCH=amd64; RUST_IMAGE=rust:1-slim-bullseye ;;
  aarch64|arm64) GOARCH=arm64; RUST_IMAGE=rust:1-slim-bullseye ;;
  *) echo "unsupported docker architecture: $ARCH" >&2; exit 1 ;;
esac
echo "building for linux/$GOARCH"

mkdir -p bin
for b in fakeate actorbackend loadgen; do
  ( cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH="$GOARCH" \
      go build -o "demos/envoy-rust-dynamic-module/bin/$b" "./demos/envoy-rust-dynamic-module/$b/" )
done
# substrate's real router binary: arm A runs it unmodified.
( cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH="$GOARCH" \
    go build -o "demos/envoy-rust-dynamic-module/bin/atenet" ./cmd/atenet/ )

# The module is built inside a container so no Rust toolchain is needed on the
# host. bullseye (glibc 2.31) is deliberately older than the Envoy image's
# glibc 2.35, so the .so loads there; building on a newer glibc would not.
docker run --rm \
  -v ate-cargo:/usr/local/cargo/registry \
  -v ate-cargo-git:/usr/local/cargo/git \
  -v "$(pwd)/rust-module:/src" -w /src "$RUST_IMAGE" sh -c '
    apt-get update -qq >/dev/null 2>&1
    apt-get install -y -qq git ca-certificates libclang-dev clang >/dev/null 2>&1
    cargo build --release'

ls -la bin/ rust-module/target/release/*.so
