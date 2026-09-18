#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

root=${1:?usage: generate-kata-runtime-manifest.sh KATA_ROOT}
root=$(cd "$root" && pwd)
manifest="$root/bundle-manifest.json"

find_one() {
  local path
  for path in "$@"; do
    if [[ -f "$root/$path" ]]; then
      printf '%s' "$path"
      return 0
    fi
  done
  return 1
}

shim=$(find_one runtime-rs/bin/containerd-shim-kata-v2 bin/containerd-shim-kata-v2 bin/containerd-shim-kata-v2-rs) || { echo "missing Kata shim" >&2; exit 1; }
qemu=$(find_one bin/qemu-system-x86_64 bin/qemu-system-aarch64) || { echo "missing Kata QEMU" >&2; exit 1; }
kernel=$(find_one share/vmlinux share/kata-vmlinux share/kata-containers/vmlinux.container) || { echo "missing Kata kernel" >&2; exit 1; }
initrd=$(find_one share/kata-initrd.img share/kata.initrd share/kata-initrd share/kata-containers/kata-alpine-3.22.initrd) || true
image=$(find_one share/kata-image share/kata-image.img share/kata-containers/kata-containers.img) || true
if [[ -n "$initrd" && -n "$image" ]] || [[ -z "$initrd" && -z "$image" ]]; then
  echo "require exactly one Kata initrd or image" >&2
  exit 1
fi

python3 - "$root" "$manifest" "$shim" "$qemu" "$kernel" "$initrd" "$image" <<'PY'
import hashlib, json, os, stat, sys
root, manifest, shim, qemu, kernel, initrd, image = sys.argv[1:]
files = [("kata-shim", shim), ("kata-qemu", qemu), ("kata-kernel", kernel)]
if initrd:
    files.append(("kata-initrd", initrd))
else:
    files.append(("kata-image", image))
known_paths = set(filespec[1] for filespec in files)
for base, dirs, names in os.walk(root):
    dirs.sort()
    for filename in sorted(names):
        rel = os.path.relpath(os.path.join(base, filename), root)
        if rel == "bundle-manifest.json" or rel in known_paths:
            continue
        digest = hashlib.sha256(rel.encode()).hexdigest()[:16]
        files.append((f"kata-qemu-support-{digest}", rel))
out = []
for name, rel in files:
    path = os.path.join(root, rel)
    if stat.S_ISLNK(os.lstat(path).st_mode):
        raise SystemExit(f"Kata asset {rel!r} must not be a symlink")
    real_path = os.path.realpath(path)
    if os.path.commonpath((root, real_path)) != root:
        raise SystemExit(f"Kata asset {rel!r} resolves outside {root!r}")
    if not stat.S_ISREG(os.stat(real_path).st_mode):
        raise SystemExit(f"Kata asset {rel!r} does not resolve to a regular file")
    # The runtime verifier deliberately rejects symlinks.  Record the actual
    # in-tree file used by Kata packages that expose stable symlink names.
    rel = os.path.relpath(real_path, root)
    h = hashlib.sha256()
    with open(real_path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    mode = os.stat(real_path).st_mode
    out.append({"name": name, "path": rel, "size": os.path.getsize(real_path),
                "sha256": h.hexdigest(),
                "executable": bool(mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))})
with open(manifest, "w", encoding="utf-8") as f:
    json.dump({"format": "ateom-kata-runtime-bundle", "version": 1, "files": out},
              f, indent=2, sort_keys=True)
    f.write("\n")
PY

echo "wrote $manifest"
echo "bundle sha256 (manifest): $(sha256sum "$manifest" | awk '{print $1}')"
