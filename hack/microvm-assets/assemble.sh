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

# Assemble the micro-VM (kata + cloud-hypervisor) runtime asset set that
# ateom-microvm fetches at runtime (fetch-not-bake). Run this on a Linux
# host of the TARGET arch.
#
# Produces, under $OUT, the four assets named as the SandboxConfig expects, plus
# their sha256 sums (paste into demos/counter/counter-microvm.yaml.tmpl):
#   cloud-hypervisor  vmlinux  rootfs.img  configuration-clh.toml
#
# ateom owns the cloud-hypervisor boot and gives the actor a writable virtio-blk
# rootfs, so neither the kata shim nor virtiofsd is part of the asset set.
#
# Env: ARCH (arm64|amd64, default arm64), KATA_VER (3.31.0), CH_VER (v52.0),
#      OUT (default ./bin/microvm-assets/$ARCH, under the gitignored bin/).

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"

ARCH="${ARCH:-arm64}"
KATA_VER="${KATA_VER:-3.31.0}"
CH_VER="${CH_VER:-v52.0}"
OUT="${OUT:-${ROOT}/bin/microvm-assets/$ARCH}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

case "$ARCH" in
  arm64) CH_ASSET="cloud-hypervisor-static-aarch64" ;;
  amd64) CH_ASSET="cloud-hypervisor-static" ;;
  *) echo "unsupported ARCH=$ARCH" >&2; exit 1 ;;
esac

mkdir -p "$OUT"
cd "$WORK"

echo ">> Downloading kata-static ${KATA_VER} (${ARCH})..."
curl -fSL -o kata-static.tar.zst \
  "https://github.com/kata-containers/kata-containers/releases/download/${KATA_VER}/kata-static-${KATA_VER}-${ARCH}.tar.zst"
mkdir -p kata
tar --zstd -xf kata-static.tar.zst -C kata
KROOT="kata/opt/kata"

cp "$(readlink -f "${KROOT}/share/kata-containers/vmlinux.container")" "${OUT}/vmlinux"
cp "$(readlink -f "${KROOT}/share/kata-containers/kata-containers.img")" "${OUT}/rootfs.img"
cp "${KROOT}/share/defaults/kata-containers/configuration-clh.toml" "${OUT}/configuration-clh.toml"

echo ">> Downloading cloud-hypervisor ${CH_VER} (${CH_ASSET})..."
curl -fSL -o "${OUT}/cloud-hypervisor" \
  "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/${CH_VER}/${CH_ASSET}"
chmod +x "${OUT}/cloud-hypervisor"

echo
echo ">> Assets assembled in ${OUT}:"
cd "${OUT}"
for f in cloud-hypervisor vmlinux rootfs.img configuration-clh.toml; do
  [ -f "$f" ] || { echo "MISSING: $f" >&2; exit 1; }
done
echo
echo ">> sha256 (paste into demos/counter/counter-microvm.yaml.tmpl runtime.assets):"
sha256sum cloud-hypervisor vmlinux rootfs.img configuration-clh.toml
