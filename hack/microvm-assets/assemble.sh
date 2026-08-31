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
# The DOWNLOADED assets are reproducible, so paste their sha256 sums into the
# manifest (demos/counter/counter-microvm.yaml.tmpl). That now includes virtiofsd on
# amd64 (upstream prebuilt); on arm64 virtiofsd is still built from source
# (non-reproducible bytes), so its sha is NOT pinned there — run-microvm-demo.sh
# computes it from the staged binary and injects it at deploy.
#
# ateom drives the kata-agent directly (the kata containerd shim is NOT an asset). The
# actor rootfs is overlay(virtio-fs RO lower + guest-tmpfs upper), so virtiofsd IS an
# asset; kata-static (4.0.0 included) still bundles virtiofsd v1.13.3, whose old vhost
# hangs CH's restore handshake, so we take virtiofsd v1.14.0 — the first release with
# the vhost-0.16 / vhost-user-backend-0.22 snapshot-restore fix (REPLY_ACK). Upstream
# publishes a prebuilt static binary for x86_64 only; arm64 builds from the release
# tag, which needs rust (rustup) + libcap-ng-dev libseccomp-dev pkg-config.
#
# Env: ARCH (arm64|amd64, default arm64), KATA_VER (4.0.0), CH_VER (v53.0),
#      OUT (default ./bin/microvm-assets/$ARCH, under the gitignored bin/).
#
# Always re-downloads and overwrites — there is no incremental mode. It clears
# $OUT/.asset-versions before the first write and re-stamps it with the versions that
# produced the set only once the run completes, so the stamp is present only on a dir
# assembled end-to-end by those pins. install-microvm-deps.sh uses it to decide whether
# a cached $OUT is still current.
# `--print-stamp` prints that stamp for the current env and exits without downloading.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"

ARCH="${ARCH:-arm64}"
KATA_VER="${KATA_VER:-4.0.0}"
CH_VER="${CH_VER:-v53.0}"
# Not env-overridable: the amd64 prebuilt zip URL and its sha below are pinned to
# this exact release, so bumping it means editing all three together.
VIRTIOFSD_VER="v1.14.0"
OUT="${OUT:-${ROOT}/bin/microvm-assets/$ARCH}"

case "$ARCH" in
  arm64) CH_ASSET="cloud-hypervisor-static-aarch64" ;;
  amd64) CH_ASSET="cloud-hypervisor-static" ;;
  *) echo "unsupported ARCH=$ARCH" >&2; exit 1 ;;
esac

# Identifies the asset set this script produces. Cleared before the first write into
# $OUT and re-written to $OUT/$STAMP_FILE on success; install-microvm-deps.sh compares
# it against what the current checkout would build, because the five filenames stay the
# same when a pin moves and an asset dir from an older checkout is otherwise
# indistinguishable from a current one.
STAMP_FILE=".asset-versions"
asset_stamp() {
  printf 'arch=%s\nkata=%s\ncloud-hypervisor=%s\nvirtiofsd=%s\n' \
    "$ARCH" "$KATA_VER" "$CH_VER" "$VIRTIOFSD_VER"
}

if [ "${1:-}" = "--print-stamp" ]; then
  asset_stamp
  exit 0
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$OUT"
# Drop any stamp before the first overwrite into $OUT. Assets are replaced in place,
# so a run that dies partway leaves a dir mixing old and new bytes; the stamp it
# inherited describes neither. Clearing it up front means an unstamped dir is the only
# thing a failed run can leave, whatever the pins were before.
rm -f "${OUT}/${STAMP_FILE}"
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

# virtiofsd v1.14.0 (pinned at the top): first release with the vhost-0.16 /
# vhost-user-backend-0.22 snapshot-restore fix (REPLY_ACK) — the kata-bundled v1.13.3
# (old vhost) hangs CH's restore handshake, so this stays a separately-sourced asset.
# Upstream attaches a prebuilt static binary to the release for x86_64 only; other
# arches build from the release tag.
# The x86_64-musl binary attached to the v1.14.0 release notes
# (https://gitlab.com/virtio-fs/virtiofsd/-/releases/v1.14.0). 21523468 is the
# gitlab.com project id of virtio-fs/virtiofsd: the /-/project/<id>/uploads/ form
# is the canonical unauthenticated upload URL (the /<path>/-/uploads/ form the
# release notes link to 403s outside a browser session). The zip sha is pinned
# because a bad URL yields a login page, which would otherwise only fail later
# at unzip.
VIRTIOFSD_ZIP_URL="https://gitlab.com/-/project/21523468/uploads/f505704014ae7a816e515f2a05a93d8b/virtiofsd-v1.14.0.zip"
VIRTIOFSD_ZIP_SHA256="2e4fe9571f492b00baa34bc4e708e950039c5da05b830b31a8d179cb6ac8978e"
if [ "$ARCH" = "amd64" ]; then
  echo ">> Downloading prebuilt virtiofsd ${VIRTIOFSD_VER} (x86_64-musl)..."
  curl -fSL -o virtiofsd.zip "${VIRTIOFSD_ZIP_URL}"
  echo "${VIRTIOFSD_ZIP_SHA256}  virtiofsd.zip" | sha256sum -c -
  unzip -q -o virtiofsd.zip
  cp "target/x86_64-unknown-linux-musl/release/virtiofsd" "${OUT}/virtiofsd"
else
  echo ">> Building virtiofsd ${VIRTIOFSD_VER} (no upstream prebuilt for ${ARCH})..."
  # Build deps (Debian): apt-get install -y git libcap-ng-dev libseccomp-dev pkg-config; rust via rustup.
  if ! command -v cargo >/dev/null 2>&1; then
    echo "cargo not found; install rust (rustup) + libcap-ng-dev libseccomp-dev pkg-config" >&2
    exit 1
  fi
  git clone --depth 1 --branch "${VIRTIOFSD_VER}" https://gitlab.com/virtio-fs/virtiofsd.git
  (
    cd virtiofsd
    grep -E '^(vhost|vhost-user-backend) =' Cargo.toml   # expect vhost 0.16 / backend 0.22
    cargo build --release
  )
  cp "virtiofsd/target/release/virtiofsd" "${OUT}/virtiofsd"
fi
chmod +x "${OUT}/virtiofsd"

echo
echo ">> Assets assembled in ${OUT}:"
cd "${OUT}"
for f in cloud-hypervisor virtiofsd vmlinux rootfs.img configuration-clh.toml; do
  [ -f "$f" ] || { echo "MISSING: $f" >&2; exit 1; }
done
# Written only once all five are present, and only after the up-front rm, so the stamp
# exists exactly when this dir was assembled end-to-end by these pins.
asset_stamp > "${OUT}/${STAMP_FILE}"
"${OUT}/virtiofsd" --version 2>/dev/null | head -1 || true
echo
echo ">> sha256 (paste the DOWNLOADED assets into counter-microvm.yaml.tmpl; that"
echo ">> includes virtiofsd on amd64 (prebuilt). The arm64 virtiofsd is built from"
echo ">> source, so its sha is injected at deploy by run-microvm-demo.sh, not pinned):"
sha256sum cloud-hypervisor virtiofsd vmlinux rootfs.img configuration-clh.toml
