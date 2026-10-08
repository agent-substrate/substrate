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

# Rebuild kata-agent without the features we do not use and patch it into the guest
# image, in place.
#
# The agent is by far the largest binary the guest runs, and kata's release pipeline
# builds it with the OPA/regorus policy engine and initdata support enabled
# (kata-deploy-binaries.sh defaults AGENT_POLICY=yes even though src/agent/Makefile
# defaults it to no). ateom never sends a policy and never uses initdata, so both are
# dead weight in every boot and every snapshot. Built without them (and with fat LTO
# at opt-level=s), kata 4.1.0's stripped amd64 agent goes from 30.8 MB to 15.5 MB.
#
# Measured with kata 4.0.0 on GKE (amd64, counter demo, medians of 7-9 cold bakes
# each):
#
#   agent            binary     agent_dial   since_boot   golden snapshot
#   stock (kata's)   30.63 MB     377 ms       498 ms       27.7 MiB
#   this script      18.59 MB     356 ms       468 ms       24.3 MiB
#
# ~30 ms off a ~500 ms cold boot and ~3.5 MiB off the compressed golden, which is
# object-store cost, suspend upload and cross-node resume download.
#
# The image is patched with debugfs rather than a loop mount so this needs no root:
# write the new binary, restore its mode/uid/gid, then let e2fsck settle the bitmaps.
#
# Env: ARCH (arm64|amd64), KATA_VER, IMAGE (the rootfs.img to patch, in place),
#      OPT_LEVEL (default s; z measured the same within noise and is ~0.5 MB smaller,
#      3 is upstream's default and ~2.8 MB larger).

set -o errexit -o nounset -o pipefail

ARCH="${ARCH:-amd64}"
# assemble.sh owns the kata pin and passes it in.
KATA_VER="${KATA_VER:?KATA_VER is required (assemble.sh passes its KATA_VER)}"
IMAGE="${IMAGE:?IMAGE (path to rootfs.img) is required}"
OPT_LEVEL="${OPT_LEVEL:-s}"
# Dynamically detect Partition 1 byte offset from GPT (0xee) or DOS/MBR so this
# script works on both stock upstream Kata images (LBA 6144 = 3145728) and
# already-slimmed MBR images (LBA 2048 = 1048576).
PART_TYPE="$(od -An -tx1 -j 450 -N 1 "${IMAGE}" | tr -d ' ')"
if [[ "${PART_TYPE}" == "ee" ]]; then
  START_SECTOR="$(od -An -tu8 -j 1056 -N 8 "${IMAGE}" | tr -d ' ')"
else
  START_SECTOR="$(od -An -tu4 -j 454 -N 4 "${IMAGE}" | tr -d ' ')"
fi
ROOTFS_OFFSET=$(( ${START_SECTOR:-6144} * 512 ))
# Staged beside the image rather than under TMPDIR: the build and the patch both run in
# a container with this directory bind-mounted, and macOS's default TMPDIR (/var/folders)
# is not a path Docker Desktop shares, so the mount comes up empty and debugfs fails on a
# rootfs.img that is present on the host. The image's own directory is necessarily
# visible to whoever is assembling it.
WORK="$(mktemp -d "$(dirname "${IMAGE}")/.slim-agent.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

case "$ARCH" in
  arm64|amd64) ;;
  *) echo "unsupported ARCH=$ARCH" >&2; exit 1 ;;
esac

command -v docker >/dev/null || { echo "docker is required to build the agent" >&2; exit 1; }

cp "$IMAGE" "${WORK}/rootfs.img"

CARGO_CACHE_DIR="${CARGO_CACHE_DIR:-/tmp/kata-agent-slim-cache}"
mkdir -p "${CARGO_CACHE_DIR}/registry" "${CARGO_CACHE_DIR}/git"
# Pinned to the multi-arch index digest, so --platform still selects the arch. That
# fixes gcc and binutils; the apt packages below come from bookworm's live archive, and
# rustc from kata's rust-toolchain.toml.
BUILD_IMAGE="rust:bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e"

echo ">> Building kata-agent ${KATA_VER} (${ARCH}, no policy/initdata, opt-level=${OPT_LEVEL})..."
docker run --rm --platform "linux/${ARCH}" \
  -e HOST_UID="$(id -u)" \
  -e HOST_GID="$(id -g)" \
  -v "${CARGO_CACHE_DIR}/registry:/usr/local/cargo/registry" \
  -v "${CARGO_CACHE_DIR}/git:/usr/local/cargo/git" \
  -v "${WORK}:/work" -w /work "${BUILD_IMAGE}" bash -eu -c '
  trap '\''chown -R "${HOST_UID}:${HOST_GID}" /work /usr/local/cargo/registry /usr/local/cargo/git 2>/dev/null || true'\'' EXIT
  apt-get update -qq
  apt-get install -y -qq build-essential clang pkg-config protobuf-compiler \
    libseccomp-dev git e2fsprogs >/dev/null
  git clone --depth 1 --branch "'"${KATA_VER}"'" \
    https://github.com/kata-containers/kata-containers /work/kata
  cd /work/kata/src/agent
  # SECCOMP stays on: it is what applies a container seccomp profile inside the
  # guest, and it measured free (identical stripped size with and without).
  LIBC=gnu SECCOMP=yes AGENT_POLICY=no INIT_DATA=no \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
    CARGO_PROFILE_RELEASE_LTO=true \
    CARGO_PROFILE_RELEASE_OPT_LEVEL='"${OPT_LEVEL}"' \
    make
  cp /work/kata/target/*-unknown-linux-gnu/release/kata-agent /work/kata-agent
  strip --strip-all /work/kata-agent
  rm -rf /work/kata

  # Patch it in. debugfs rm unlinks and, once the link count reaches 0, frees the
  # inode and its blocks (kill_file before rm would free them twice). sif
  # restores the mode/uid/gid that write does not carry over.
  cd /work
  debugfs -w -f - "rootfs.img?offset='"${ROOTFS_OFFSET}"'" >/dev/null <<EOF
cd /usr/bin
rm kata-agent
write /work/kata-agent kata-agent
sif kata-agent mode 0100755
sif kata-agent uid 0
sif kata-agent gid 0
EOF
  fsck_rc=0
  E2FSPROGS_FAKE_TIME=1700000000 e2fsck -fy "rootfs.img?offset='"${ROOTFS_OFFSET}"'" >/dev/null 2>&1 || fsck_rc=$?
  if (( fsck_rc > 1 )); then
    echo "error: e2fsck failed after patching kata-agent (exit ${fsck_rc})" >&2
    exit 1
  fi
  debugfs -R "stat /usr/bin/kata-agent" "rootfs.img?offset='"${ROOTFS_OFFSET}"'" 2>/dev/null \
    | grep -E "Mode|Size:"
'

cp "${WORK}/rootfs.img" "$IMAGE"
cp "${WORK}/kata-agent" "$(dirname "$IMAGE")/kata-agent.slim"
echo ">> Patched $(basename "$IMAGE") and wrote kata-agent.slim ($(stat -c %s "${WORK}/kata-agent" 2>/dev/null || stat -f %z "${WORK}/kata-agent") bytes)"
