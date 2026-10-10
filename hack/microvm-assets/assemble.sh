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
#      OUT (default ./bin/microvm-assets/$ARCH, under the gitignored bin/),
#      SLIM_ROOTFS (true|false, default false; see below),
#      CONTAINER_CLI (the builder SLIM_ROOTFS=true runs, default docker),
#      OPT_LEVEL (kata-agent's opt-level for SLIM_ROOTFS=true, default 3),
#      ALLOW_CROSS_ARCH_BUILD (true lets SLIM_ROOTFS=true build for an ARCH other
#      than the host's, under emulation, which takes hours).
#
# SLIM_ROOTFS=true builds rootfs.img instead of taking kata's: build-rootfs.sh puts
# only the packages the guest runs on debian:trixie-slim, adds kata-agent recompiled
# at KATA_VER without the policy engine and initdata support (which ateom never uses),
# and packs the result. It is an unprivileged container build on this host, so the
# host must be the TARGET arch, and the run takes minutes rather than seconds, which is
# why it is off by default. The built rootfs.img is then the one asset without a
# committed pin: install-microvm-deps.sh swaps its sha256 in for the kata-image pin at
# apply time. So kata's image still has to be the pinned one, which also catches a
# KATA_VER the manifest does not pin before the build starts; its sha256 is saved to
# $OUT/.upstream-rootfs.sha256 for install-microvm-deps.sh.
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
SLIM_ROOTFS="${SLIM_ROOTFS:-false}"
OPT_LEVEL="${OPT_LEVEL:-3}"
CONTAINER_CLI="${CONTAINER_CLI:-docker}"
BUILD_ROOTFS="${ROOT}/hack/microvm-assets/build-rootfs.sh"
# Written only by a SLIM_ROOTFS=true run, before rootfs.img is built.
UPSTREAM_ROOTFS_SHA_FILE=".upstream-rootfs.sha256"
# Holds the committed per-arch pins a SLIM_ROOTFS=true run checks the download against.
MANIFEST_TEMPLATE="${ROOT}/manifests/microvm/sandboxconfig-microvm.yaml.tmpl"

case "$ARCH" in
  arm64) CH_ASSET="cloud-hypervisor-static-aarch64" ;;
  amd64) CH_ASSET="cloud-hypervisor-static" ;;
  *) echo "unsupported ARCH=$ARCH" >&2; exit 1 ;;
esac
case "$SLIM_ROOTFS" in
  true|false) ;;
  *) echo "SLIM_ROOTFS must be true or false, got '${SLIM_ROOTFS}'" >&2; exit 1 ;;
esac

# Identifies the asset set this script produces. Cleared before the first write into
# $OUT and re-written to $OUT/$STAMP_FILE on success; install-microvm-deps.sh compares
# it against what the current checkout would build, because the filenames stay the
# same when a pin moves and an asset dir from an older checkout is otherwise
# indistinguishable from a current one. virtiofsd is stamped even though KATA_VER
# already determines it: its version is what the CH restore handshake turns on, so the
# dir should say which one it holds.
# Only a slim set carries the extra slim-rootfs line, so a default dir's stamp does not
# depend on the slim build. The line holds a hash of everything that build reads rather
# than just "true", so editing any of it re-assembles a cached slim set instead of
# reusing the image the old inputs built.
STAMP_FILE=".asset-versions"
asset_stamp() {
  printf 'arch=%s\nkata=%s\ncloud-hypervisor=%s\nvirtiofsd=%s\n' \
    "$ARCH" "$KATA_VER" "$CH_VER" "$VIRTIOFSD_VER"
  if [ "$SLIM_ROOTFS" = "true" ]; then
    local slim_hash
    slim_hash="$({
      printf 'opt_level=%s\n' "$OPT_LEVEL"
      # Paths relative to the checkout, so adding, removing or renaming an input
      # changes the stamp but moving the checkout does not.
      cd "${ROOT}"
      find hack/microvm-assets/build-rootfs.sh hack/microvm-assets/rootfs -type f -print0 \
        | LC_ALL=C sort -z \
        | xargs -0 sha256sum
    } | sha256sum | cut -c1-12)"
    printf 'slim-rootfs=%s\n' "${slim_hash}"
  fi
}

if [ "${1:-}" = "--print-stamp" ]; then
  asset_stamp
  exit 0
fi

# Fail before the ~1 GiB download rather than after it.
if [ "$SLIM_ROOTFS" = "true" ]; then
  if ! command -v "${CONTAINER_CLI}" >/dev/null 2>&1; then
    echo "SLIM_ROOTFS=true needs ${CONTAINER_CLI}: build-rootfs.sh builds rootfs.img in containers" >&2
    echo "(set CONTAINER_CLI to use another builder)" >&2
    exit 1
  fi
  case "$(uname -m)" in
    x86_64)        HOST_ARCH=amd64 ;;
    aarch64|arm64) HOST_ARCH=arm64 ;;
    *)             HOST_ARCH="$(uname -m)" ;;
  esac
  if [ "${ARCH}" != "${HOST_ARCH}" ] && [ "${ALLOW_CROSS_ARCH_BUILD:-false}" != "true" ]; then
    echo "SLIM_ROOTFS=true requires host arch (${HOST_ARCH}) to match target ARCH (${ARCH});" >&2
    echo "building kata-agent under emulation takes hours (set ALLOW_CROSS_ARCH_BUILD=true to override)." >&2
    exit 1
  fi
  # The slim guest has no chrony, so only a VMM that advances the guest clock across
  # a restore keeps its time right: v53 and later (clockAdvanceSince in
  # cmd/ateom-microvm/internal/ch/guestclock.go).
  ch_major="${CH_VER#v}"
  ch_major="${ch_major%%.*}"
  if ! [[ "${ch_major}" =~ ^[0-9]+$ ]] || (( ch_major < 53 )); then
    echo "SLIM_ROOTFS=true needs CH_VER v53.0 or later, got '${CH_VER}': the slim guest has" >&2
    echo "no chrony, so an older VMM leaves restored guests with a frozen clock." >&2
    exit 1
  fi
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$OUT"
# Drop any stamp before the first overwrite into $OUT. Assets are replaced in place,
# so a run that dies partway leaves a dir mixing old and new bytes; the stamp it
# inherited describes neither. Clearing it up front means an unstamped dir is the only
# thing a failed run can leave, whatever the pins were before.
rm -f "${OUT}/${STAMP_FILE}"
# Likewise a previous slim run's by-products, which a default run never rewrites.
rm -f "${OUT}/${UPSTREAM_ROOTFS_SHA_FILE}" "${OUT}/kata-agent.slim" \
      "${OUT}/rootfs-packages.txt" "${OUT}/image-packages.txt"
cd "$WORK"

echo ">> Downloading kata-static ${KATA_VER} (${ARCH})..."
curl -fSL -o kata-static.tar.zst \
  "https://github.com/kata-containers/kata-containers/releases/download/${KATA_VER}/kata-static-${KATA_VER}-${ARCH}.tar.zst"
mkdir -p kata
tar --zstd -xf kata-static.tar.zst -C kata
KROOT="kata/opt/kata"
KATA_ROOTFS="$(readlink -f "${KROOT}/share/kata-containers/kata-containers.img")"

cp "$(readlink -f "${KROOT}/share/kata-containers/vmlinux.container")" "${OUT}/vmlinux"
# Statically linked, so it runs as-is outside the kata layout it is packaged for.
cp "${KROOT}/libexec/virtiofsd" "${OUT}/virtiofsd"
chmod +x "${OUT}/virtiofsd"

if [ "$SLIM_ROOTFS" = "true" ]; then
  # kata's image is not shipped, but its pin is the one the built image replaces, so it
  # must be in the manifest. Recorded for install-microvm-deps.sh, which repeats this
  # exact check before swapping in the built image's sha256.
  UPSTREAM_ROOTFS_SHA="$(sha256sum "${KATA_ROOTFS}" | awk '{print $1}')"
  if [ "$(grep -c "sha256: \"${UPSTREAM_ROOTFS_SHA}\"" "${MANIFEST_TEMPLATE}" || true)" != "1" ]; then
    echo "kata ${KATA_VER} (${ARCH}) rootfs.img has sha256 ${UPSTREAM_ROOTFS_SHA}, which is not a" >&2
    echo "kata-image pin in ${MANIFEST_TEMPLATE}. Pin it first (a SLIM_ROOTFS=false run" >&2
    echo "prints the sha256s to paste); the built image stands in for a pinned one." >&2
    exit 1
  fi
  echo "${UPSTREAM_ROOTFS_SHA}" > "${OUT}/${UPSTREAM_ROOTFS_SHA_FILE}"
  ARCH="$ARCH" KATA_VER="$KATA_VER" OPT_LEVEL="$OPT_LEVEL" OUT="$OUT" CONTAINER_CLI="$CONTAINER_CLI" "${BUILD_ROOTFS}"
else
  cp "${KATA_ROOTFS}" "${OUT}/rootfs.img"
fi

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
if [ "$SLIM_ROOTFS" = "true" ]; then
  echo ">> sha256 (rootfs.img was built on this host and stands in for the kata-image pin"
  echo ">> ${UPSTREAM_ROOTFS_SHA}: do NOT paste it; install-microvm-deps.sh swaps it in"
  echo ">> at apply time):"
else
  echo ">> sha256 (paste all four into the per-arch block in"
  echo ">> manifests/microvm/sandboxconfig-microvm.yaml.tmpl):"
fi
sha256sum cloud-hypervisor virtiofsd vmlinux rootfs.img
