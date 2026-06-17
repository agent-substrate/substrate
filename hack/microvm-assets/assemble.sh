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
# Produces, under $OUT, the six assets named as the ActorTemplate expects, plus
# their sha256 sums (paste into demos/counter/counter-microvm.yaml.tmpl):
#   containerd-shim-kata-v2  cloud-hypervisor  virtiofsd-patched
#   vmlinux  rootfs.img  configuration-clh.toml
#
# virtiofsd is BUILT from source with the vhost-0.16 bump (stock virtiofsd breaks
# CH snapshot/restore; the bumped vhost crates carry the REPLY_ACK fix). Build deps on
# Debian/Ubuntu: apt-get install -y git libcap-ng-dev libseccomp-dev pkg-config
# Rust: use rustup (current stable). Distro cargo (e.g. apt's 1.75) is too old —
# virtiofsd's Cargo.lock is lockfile v4, which needs cargo >= 1.78:
#   curl -fsSL https://sh.rustup.rs | sh -s -- -y && . "$HOME/.cargo/env"
#
# Env: ARCH (arm64|amd64, default arm64), KATA_VER (3.31.0), CH_VER (v52.0),
#      VIRTIOFSD_REF (upstream commit; default b6d2aaa), OUT (default ./microvm-assets-$ARCH).

set -o errexit -o nounset -o pipefail

ARCH="${ARCH:-arm64}"
KATA_VER="${KATA_VER:-3.31.0}"
CH_VER="${CH_VER:-v52.0}"
VIRTIOFSD_REF="${VIRTIOFSD_REF:-b6d2aaa}"
OUT="${OUT:-$PWD/microvm-assets-$ARCH}"
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

cp "${KROOT}/bin/containerd-shim-kata-v2" "${OUT}/containerd-shim-kata-v2"
cp "$(readlink -f "${KROOT}/share/kata-containers/vmlinux.container")" "${OUT}/vmlinux"
cp "$(readlink -f "${KROOT}/share/kata-containers/kata-containers.img")" "${OUT}/rootfs.img"
cp "${KROOT}/share/defaults/kata-containers/configuration-clh.toml" "${OUT}/configuration-clh.toml"

echo ">> Downloading cloud-hypervisor ${CH_VER} (${CH_ASSET})..."
curl -fSL -o "${OUT}/cloud-hypervisor" \
  "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/${CH_VER}/${CH_ASSET}"
chmod +x "${OUT}/cloud-hypervisor"

echo ">> Building patched virtiofsd (vhost 0.16) from ${VIRTIOFSD_REF}..."
if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not found; install rust + build deps (see header)" >&2
  exit 1
fi
git clone https://gitlab.com/virtio-fs/virtiofsd.git
(
  cd virtiofsd
  git checkout "${VIRTIOFSD_REF}"
  # Bump the vhost crates to the versions that contain rust-vmm/vhost@14db3cd
  # (the snapshot/restore REPLY_ACK fix). Robust to the exact pinned versions.
  sed -i -E 's/^vhost-user-backend = "[^"]*"/vhost-user-backend = "0.22"/' Cargo.toml
  sed -i -E 's/^vhost = "[^"]*"/vhost = "0.16"/' Cargo.toml
  grep -E '^(vhost|vhost-user-backend) =' Cargo.toml
  cargo build --release
)
cp "virtiofsd/target/release/virtiofsd" "${OUT}/virtiofsd-patched"
chmod +x "${OUT}/virtiofsd-patched"

echo
echo ">> Assets assembled in ${OUT}:"
cd "${OUT}"
for f in containerd-shim-kata-v2 cloud-hypervisor virtiofsd-patched vmlinux rootfs.img configuration-clh.toml; do
  [ -f "$f" ] || { echo "MISSING: $f" >&2; exit 1; }
done
"${OUT}/virtiofsd-patched" --version 2>/dev/null | head -1 || true
echo
echo ">> sha256 (paste into demos/counter/counter-microvm.yaml.tmpl runtime.assets):"
sha256sum containerd-shim-kata-v2 cloud-hypervisor virtiofsd-patched vmlinux rootfs.img configuration-clh.toml
