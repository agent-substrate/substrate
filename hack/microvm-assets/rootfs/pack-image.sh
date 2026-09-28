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

# Pack a guest root tree into the disk image ateom boots, in the image stage of
# Dockerfile.
#
# Usage: pack-image.sh <root-tree> <output-image>
#
# ateom boots root=/dev/vda1 rootfstype=ext4 ro rootflags=data=ordered
# (cmd/ateom-microvm/run.go, buildVMConfig), so the image is an MBR disk whose first
# partition is ext4. It is built entirely in userspace, from files: mkfs.ext4 -d
# populates the filesystem from the tree, sfdisk writes the partition table into the
# image file and dd places the filesystem behind it. No loop device, mount or root on
# the host, so the build runs under a rootless builder. Ownership comes from the tree,
# which BuildKit presents with the rootfs stage's own uids.
#
# With the same e2fsprogs, the same packages always pack into the same bytes, whichever
# builder produced the tree. With SOURCE_DATE_EPOCH set, e2fsprogs 1.47.1 and later
# clamp every timestamp copied from the tree to it and use it for every timestamp they
# write themselves. Earlier timestamps are kept. An mtime comes from the package, but
# an atime depends on how the builder unpacked and read the file, so every atime is
# pinned to SOURCE_DATE_EPOCH on a copy of the tree (the source is mounted read-only).
# The filesystem's starting size shapes its layout (reserved GDT blocks), so it comes
# from file sizes, not from the disk usage the builder's storage reports. mkfs.ext4 -d
# adds directory entries in sorted order, and the UUID, directory hash seed and disk
# identifier are fixed. Nothing below repairs the filesystem, because a repairing
# e2fsck would record the real time of the check.

set -o errexit -o nounset -o pipefail

SRC="${1:?usage: pack-image.sh <root-tree> <output-image>}"
IMG="${2:?usage: pack-image.sh <root-tree> <output-image>}"
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1700000000}"
# The partition starts at 1 MiB, the usual alignment.
PART_START_SECTOR=2048

# Runs a command and shows its output only if it fails.
quiet() {
  local out
  if ! out="$("$@" 2>&1)"; then
    echo "${out}" >&2
    echo "pack-image.sh: failed: $*" >&2
    exit 1
  fi
}

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

TREE="${WORK}/tree"
cp -a "${SRC}" "${TREE}"
find "${TREE}" -exec touch -h -a -d "@${SOURCE_DATE_EPOCH}" {} +

# Oversize the filesystem for mkfs.ext4 -d, then let resize2fs -M shrink it to what
# the tree needs: the blocks each file's contents fill, and one per other entry. Only
# as many inodes as the tree has, plus headroom: the default ratio would spend
# megabytes on empty inode tables.
TREE_KIB="$(find "${TREE}" -printf '%y %s\n' \
  | awk '{ kib += ($1 == "f") ? int(($2 + 4095) / 4096) * 4 : 4 } END { print kib }')"
INODES="$(( $(find "${TREE}" | wc -l) + 256 ))"
PART="${WORK}/part.ext4"
truncate -s "$(( TREE_KIB + TREE_KIB / 4 + 16 * 1024 ))K" "${PART}"
# No journal: the guest mounts it read-only.
mkfs.ext4 -q -F -b 4096 -m 0 -N "${INODES}" -O ^has_journal \
  -U 00000000-0000-4000-8000-000000000001 \
  -E root_owner=0:0,hash_seed=00000000-0000-4000-8000-000000000002 \
  -d "${TREE}" "${PART}"
quiet e2fsck -fn "${PART}"
quiet resize2fs -M "${PART}"
# The kernel rejects a data= mount option that differs from the superblock default on
# a filesystem without a journal, and ateom mounts with rootflags=data=ordered, so
# record ordered as the default or the guest panics at boot.
quiet tune2fs -o journal_data_ordered "${PART}"

PART_BYTES="$(stat -c %s "${PART}")"
truncate -s "$(( PART_START_SECTOR * 512 + PART_BYTES ))" "${IMG}"
sfdisk --quiet "${IMG}" <<EOF
label: dos
label-id: 0x65a39876
unit: sectors
start=${PART_START_SECTOR}, size=$(( PART_BYTES / 512 )), type=83
EOF
dd if="${PART}" of="${IMG}" bs=512 seek="${PART_START_SECTOR}" conv=notrunc status=none

# Check the filesystem once more where the guest will find it.
quiet e2fsck -fn "${IMG}?offset=$(( PART_START_SECTOR * 512 ))"
echo ">> $(basename "${IMG}"): $(( $(stat -c %s "${IMG}") / 1048576 )) MiB" \
  "(tree $(( TREE_KIB / 1024 )) MiB, ${INODES} inodes reserved)"
