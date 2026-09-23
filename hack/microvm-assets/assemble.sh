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
# Produces, under $OUT, the four assets named as the SandboxConfig expects:
#   cloud-hypervisor  virtiofsd  vmlinux  rootfs.img
# Every asset is downloaded rather than built, so all four have reproducible bytes:
# paste their sha256 sums into the manifest
# (manifests/microvm/sandboxconfig-microvm.yaml.tmpl).
#
# ateom drives the kata-agent directly (the kata containerd shim is NOT an asset). The
# actor rootfs is overlay(virtio-fs RO lower + guest-tmpfs upper), so virtiofsd IS an
# asset. CH's restore handshake hangs against virtiofsd v1.13.3, which kata bundled up
# to and including 4.0.0; kata 4.1.0 bundles v1.14.0, the first release carrying the
# vhost-0.16 / vhost-user-backend-0.22 snapshot-restore fix (REPLY_ACK). So virtiofsd
# now comes out of kata-static with the kernel and rootfs instead of being sourced
# separately per arch.
#
# Env: ARCH (arm64|amd64, default arm64), KATA_VER (4.1.0), CH_VER (v53.0),
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
KATA_VER="${KATA_VER:-4.1.0}"
CH_VER="${CH_VER:-v53.0}"
# Not env-overridable: this is whatever KATA_VER bundles. It is declared rather than
# read off the binary because --print-stamp has to answer before anything is
# downloaded, and checked against the extracted binary below so it cannot drift from
# what kata ships.
VIRTIOFSD_VER="1.14.0"
OUT="${OUT:-${ROOT}/bin/microvm-assets/$ARCH}"

case "$ARCH" in
  arm64) CH_ASSET="cloud-hypervisor-static-aarch64" ;;
  amd64) CH_ASSET="cloud-hypervisor-static" ;;
  *) echo "unsupported ARCH=$ARCH" >&2; exit 1 ;;
esac

# Identifies the asset set this script produces. Cleared before the first write into
# $OUT and re-written to $OUT/$STAMP_FILE on success; install-microvm-deps.sh compares
# it against what the current checkout would build, because the filenames stay the
# same when a pin moves and an asset dir from an older checkout is otherwise
# indistinguishable from a current one. virtiofsd is stamped even though KATA_VER
# already determines it: its version is what the CH restore handshake turns on, so the
# dir should say which one it holds.
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
# Statically linked, so it runs as-is outside the kata layout it is packaged for.
cp "${KROOT}/libexec/virtiofsd" "${OUT}/virtiofsd"
chmod +x "${OUT}/virtiofsd"

echo ">> Downloading cloud-hypervisor ${CH_VER} (${CH_ASSET})..."
curl -fSL -o "${OUT}/cloud-hypervisor" \
  "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/${CH_VER}/${CH_ASSET}"
chmod +x "${OUT}/cloud-hypervisor"

echo
echo ">> Assets assembled in ${OUT}:"
cd "${OUT}"
for f in cloud-hypervisor virtiofsd vmlinux rootfs.img; do
  [ -f "$f" ] || { echo "MISSING: $f" >&2; exit 1; }
done
# The stamp names a virtiofsd version, so confirm the tarball carried that one before
# writing it: a kata-side bump would otherwise stamp a version this dir does not hold.
# Only checkable where the binary runs, and assembling for another arch (or on macOS)
# is legitimate, so a binary this host cannot exec is skipped rather than fatal.
if GOT_VIRTIOFSD="$("${OUT}/virtiofsd" --version 2>/dev/null | head -1 | awk '{print $2}')" \
   && [ -n "${GOT_VIRTIOFSD}" ]; then
  if [ "${GOT_VIRTIOFSD}" != "${VIRTIOFSD_VER}" ]; then
    echo "kata ${KATA_VER} bundles virtiofsd ${GOT_VIRTIOFSD}, not ${VIRTIOFSD_VER}: update VIRTIOFSD_VER" >&2
    exit 1
  fi
  echo "virtiofsd ${GOT_VIRTIOFSD}"
fi
# Written only once all four are present and virtiofsd matches, and only after the
# up-front rm, so the stamp exists exactly when this dir was assembled end-to-end by
# these pins.
asset_stamp > "${OUT}/${STAMP_FILE}"
echo
echo ">> sha256 (paste all four into the per-arch block in"
echo ">> manifests/microvm/sandboxconfig-microvm.yaml.tmpl):"
sha256sum cloud-hypervisor virtiofsd vmlinux rootfs.img
