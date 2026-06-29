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
# Produces, under $OUT, the five assets named as the SandboxConfig expects:
#   cloud-hypervisor  virtiofsd  vmlinux  rootfs.img  configuration-clh.toml
# The four DOWNLOADED assets are reproducible, so paste their sha256 sums into the
# manifest (demos/counter/counter-microvm.yaml.tmpl). virtiofsd is built from source
# (non-reproducible bytes), so its sha is NOT pinned there — run-microvm-demo.sh
# computes it from the staged binary and injects it at deploy.
#
# ateom drives the kata-agent directly (the kata containerd shim is NOT an asset). The
# actor rootfs is overlay(virtio-fs RO lower + guest-tmpfs upper), so virtiofsd IS an
# asset; it is built from source (pinned commit, see VIRTIOFSD_COMMIT below) because the
# vhost-0.16 snapshot/restore fix (REPLY_ACK) is not in a release tag yet — the
# kata-bundled v1.13.3 virtiofsd hangs CH's restore handshake. Tracking issue:
# https://gitlab.com/virtio-fs/virtiofsd/-/work_items/236 — switch to a release once it
# lands. Building it needs rust (rustup) + libcap-ng-dev libseccomp-dev pkg-config.
#
# Env: ARCH (arm64|amd64, default arm64), KATA_VER (3.32.0), CH_VER (v52.0),
#      OUT (default ./bin/microvm-assets/$ARCH, under the gitignored bin/).

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"

ARCH="${ARCH:-arm64}"
KATA_VER="${KATA_VER:-3.32.0}"
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

# virtiofsd pinned commit. The vhost-0.16 / vhost-user-backend-0.22 snapshot-restore
# fix (REPLY_ACK) is upstream but not in a release tag yet (tracking issue
# https://gitlab.com/virtio-fs/virtiofsd/-/work_items/236) — the kata-bundled v1.13.3
# (old vhost) hangs CH's restore handshake. Pin a known-good commit until a release
# carries the fix.
VIRTIOFSD_COMMIT="acb3d506a9f1b256fff7327023df85570caf1e75"
echo ">> Building virtiofsd @ ${VIRTIOFSD_COMMIT} (vhost 0.16)..."
# Build deps (Debian): apt-get install -y git libcap-ng-dev libseccomp-dev pkg-config; rust via rustup.
if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not found; install rust (rustup) + libcap-ng-dev libseccomp-dev pkg-config" >&2
  exit 1
fi
git clone https://gitlab.com/virtio-fs/virtiofsd.git
(
  cd virtiofsd
  git checkout --quiet "${VIRTIOFSD_COMMIT}"
  grep -E '^(vhost|vhost-user-backend) =' Cargo.toml   # expect vhost 0.16 / backend 0.22
  cargo build --release
)
cp "virtiofsd/target/release/virtiofsd" "${OUT}/virtiofsd"
chmod +x "${OUT}/virtiofsd"

echo
echo ">> Assets assembled in ${OUT}:"
cd "${OUT}"
for f in cloud-hypervisor virtiofsd vmlinux rootfs.img configuration-clh.toml; do
  [ -f "$f" ] || { echo "MISSING: $f" >&2; exit 1; }
done
"${OUT}/virtiofsd" --version 2>/dev/null | head -1 || true
echo
echo ">> sha256 (paste the DOWNLOADED assets into counter-microvm.yaml.tmpl;"
echo ">> virtiofsd's sha is injected at deploy by run-microvm-demo.sh, not pinned):"
sha256sum cloud-hypervisor virtiofsd vmlinux rootfs.img configuration-clh.toml
