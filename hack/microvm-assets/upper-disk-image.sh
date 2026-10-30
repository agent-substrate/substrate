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

# Regenerates upper-disk.ext4.zst, the empty ext4 image every microVM upper
# disk starts from (cmd/ateworker-microvm/rootfsdisk.go). assemble.sh stages it
# as the SandboxConfig's rootfs-upper-disk asset. The output only changes with
# the e2fsprogs version. After regenerating, update its sha256 in
# manifests/microvm/sandboxconfig-microvm.yaml.tmpl.
#
# The image size is each container's rootfs write limit. For another limit,
# build an image with SIZE and OUT set, and stage it with its sha256 in your
# SandboxConfig.
#
# Needs Linux, mke2fs 1.47.1 or later, unshare, python3 and zstd.
#
# Env: SIZE (default 16G), INODE_RATIO (bytes per inode, default 65536),
#      OUT (default hack/microvm-assets/upper-disk.ext4.zst).

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
SIZE="${SIZE:-16G}"
INODE_RATIO="${INODE_RATIO:-65536}"
OUT="${OUT:-${ROOT}/hack/microvm-assets/upper-disk.ext4.zst}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
img="${work}/upper.ext4"
mkdir -p "${work}/seed/fs" "${work}/seed/work"
chmod 0755 "${work}/seed" "${work}/seed/fs"
chmod 0700 "${work}/seed/work"
touch -h -d @0 "${work}/seed/fs" "${work}/seed/work" "${work}/seed"

# mke2fs -d copies the seed's owners. unshare -r makes them root.
unshare -r env SOURCE_DATE_EPOCH=0 MKE2FS_CONFIG=/nonexistent mke2fs -q -F -t ext4 \
  -b 4096 -I 256 \
  -U 6b1c9e3a-0d4f-4a52-9c1e-2f7d8a6b5c40 \
  -O ^has_journal,^orphan_file,^resize_inode,sparse_super2,metadata_csum_seed \
  -E lazy_itable_init=1,root_owner=0:0,packed_meta_blocks=1,num_backup_sb=0,hash_seed=2f0e4c1a-8b3d-4e6f-a1c2-5d7e9f0b3a64 \
  -m 0 -i "${INODE_RATIO}" -d "${work}/seed" "${img}" "${SIZE}"

end="$(python3 - "${img}" <<'PY'
import os, sys
fd = os.open(sys.argv[1], os.O_RDONLY)
size, off, end = os.fstat(fd).st_size, 0, 0
while off < size:
    try:
        data = os.lseek(fd, off, os.SEEK_DATA)
    except OSError:
        break
    off = end = os.lseek(fd, data, os.SEEK_HOLE)
print(end)
PY
)"
truncate -s "${end}" "${img}"
zstd -q -19 -f --no-progress "${img}" -o "${OUT}"
echo "${OUT}: $(stat -c %s "${OUT}") bytes"
