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

# Prepares an amd64 GitHub Actions Ubuntu runner to build release images for
# linux/arm64 as well (see .github/workflows/release.yaml). Uses sudo.
#
# ko cross-compiles Go and needs no emulation. The envoy-dataplane Dockerfile
# compiles Rust inside the target platform's image, which on an amd64 host
# runs arm64 binaries under QEMU user-mode emulation.
#
# 1. Installs qemu-user-static, which registers QEMU with the kernel's
#    binfmt_misc so arm64 binaries run transparently.
# 2. Fails unless the arm64 handler has the F (fix-binary) flag. Without it the
#    kernel looks the interpreter up inside each container, where it does not
#    exist, and arm64 build steps fail partway through the build.
# 3. Creates and selects a docker-container buildx builder. The default
#    "docker" driver cannot push a multi-platform image. network=host lets the
#    builder reach a registry on the runner's localhost, which the dry run
#    pushes to.

set -o errexit -o nounset -o pipefail

sudo apt-get update -qq
sudo apt-get install -y -qq --no-install-recommends qemu-user-static

handler=/proc/sys/fs/binfmt_misc/qemu-aarch64
if [[ ! -e "${handler}" ]]; then
  echo "error: ${handler} is missing; QEMU did not register for arm64" >&2
  exit 1
fi
if ! grep -qE '^flags:.*F' "${handler}"; then
  echo "error: ${handler} lacks the F flag; arm64 binaries cannot run in containers:" >&2
  cat "${handler}" >&2
  exit 1
fi

docker buildx create --use --name release --driver docker-container --driver-opt network=host
docker buildx inspect --bootstrap
