#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Build the slim micro-VM guest image, rootfs.img, from hack/microvm-assets/rootfs/:
# debian:trixie-slim holding only the packages the guest runs, plus kata-agent
# rebuilt at KATA_VER without the policy engine and initdata support. See the
# Dockerfile for what goes in and why.
#
# Everything happens in one container build. apt resolves what the guest's tools need,
# mkfs.ext4 -d and sfdisk pack the tree into an image file, and the builder hands the
# files back: nothing runs privileged, loop-mounts or needs root on this host. So a
# rootless builder works as well, e.g. a rootless BuildKit passed as --builder, and
# builds the same bytes.
#
# Writes into $OUT: rootfs.img, kata-agent.slim (the agent it holds) and
# rootfs-packages.txt (the Debian packages it holds, with versions).
#
# Env: ARCH (arm64|amd64, default amd64), KATA_VER (required), OUT (required),
#      OPT_LEVEL (kata-agent's opt-level, default s),
#      CONTAINER_CLI (default docker; another CLI needs a docker-compatible
#      `build --output`).
# Extra arguments go to the build, e.g. --builder or --build-arg PRUNE=false.
#
# Build on a host of the target arch: another arch builds under emulation, and the
# agent's Rust build then takes hours rather than minutes.

set -o errexit -o nounset -o pipefail

ARCH="${ARCH:-amd64}"
# assemble.sh owns the kata pin and passes it in.
KATA_VER="${KATA_VER:?KATA_VER is required (assemble.sh passes its KATA_VER)}"
OUT="${OUT:?OUT (the directory to write rootfs.img into) is required}"
OPT_LEVEL="${OPT_LEVEL:-s}"
CONTAINER_CLI="${CONTAINER_CLI:-docker}"
CONTEXT="$(cd "$(dirname "${BASH_SOURCE[0]}")/rootfs" && pwd)"

case "$ARCH" in
  arm64|amd64) ;;
  *) echo "unsupported ARCH=$ARCH" >&2; exit 1 ;;
esac
if ! command -v "${CONTAINER_CLI}" >/dev/null 2>&1; then
  echo "build-rootfs.sh needs ${CONTAINER_CLI} (set CONTAINER_CLI to use another builder)" >&2
  exit 1
fi

mkdir -p "${OUT}"
# The build exports into a scratch dir beside the outputs, so a failed build leaves
# $OUT as it was and the moves below stay on one filesystem.
STAGE="$(mktemp -d "${OUT}/.build-rootfs.XXXXXX")"
trap 'rm -rf "${STAGE}"' EXIT

echo ">> Building rootfs.img (${ARCH}, kata-agent ${KATA_VER}, opt-level=${OPT_LEVEL})..."
"${CONTAINER_CLI}" build \
  --platform "linux/${ARCH}" \
  --build-arg KATA_VER="${KATA_VER}" \
  --build-arg OPT_LEVEL="${OPT_LEVEL}" \
  --target export \
  --output "type=local,dest=${STAGE}" \
  "$@" \
  "${CONTEXT}"

mv "${STAGE}/rootfs.img" "${OUT}/rootfs.img"
mv "${STAGE}/kata-agent" "${OUT}/kata-agent.slim"
mv "${STAGE}/rootfs-packages.txt" "${OUT}/rootfs-packages.txt"
echo ">> Wrote ${OUT}/rootfs.img ($(( $(wc -c < "${OUT}/rootfs.img") / 1048576 )) MiB," \
  "$(( $(wc -l < "${OUT}/rootfs-packages.txt") )) Debian packages," \
  "kata-agent $(( $(wc -c < "${OUT}/kata-agent.slim") )) bytes)"
