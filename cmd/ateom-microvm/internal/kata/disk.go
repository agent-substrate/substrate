//go:build linux

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kata

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

// rootfsDiskScratchBytes is the free-space headroom added on top of a bundle's
// contents when sizing its writable rootfs disk: room for the actor to write
// during a single activation. It stays sparse (unused space is holes), so it
// costs nothing in the image file or the memory-only snapshot.
const rootfsDiskScratchBytes = 512 << 20

// rootfsDiskGeometry walks srcDir and returns the ext4 image size (MiB) and the
// inode count to build a writable rootfs disk holding that tree plus headroom for
// ext4 metadata and the actor's in-activation scratch writes. Both are
// DETERMINISTIC functions of the tree's apparent contents (summed regular-file
// sizes and entry count, NOT host block allocation), so the cold-boot build and
// the restore-time rebuild from the same OCI image produce an identically-sized
// disk — required because the guest resumes with the ext4 superblock cached in RAM.
func rootfsDiskGeometry(srcDir string) (sizeMiB int, inodes int64, err error) {
	var contentBytes, entries int64
	if werr := filepath.WalkDir(srcDir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		entries++ // every entry (file, dir, symlink, device) needs an inode
		if d.Type().IsRegular() {
			info, ierr := d.Info()
			if ierr != nil {
				return ierr
			}
			contentBytes += info.Size()
		}
		return nil
	}); werr != nil {
		return 0, 0, werr
	}

	const (
		mib            = 1 << 20
		inodeSizeBytes = 256 // ext4 default; over-estimates the table if it's 128
	)
	// One inode per entry plus 25% and a fixed reserve, so the actor can create new
	// files during its activation without exhausting inodes (the default
	// size-derived ratio can starve a file-heavy rootfs).
	inodes = entries + entries/4 + 8192
	// Contents + the eagerly-written inode table + ~6% for bitmaps/directory/extent
	// metadata + the scratch reserve. Unused space stays sparse (holes).
	sizeBytes := contentBytes + inodes*inodeSizeBytes + contentBytes/16 + rootfsDiskScratchBytes
	sizeMiB = int((sizeBytes + mib - 1) / mib)
	return sizeMiB, inodes, nil
}

// BuildExt4Image creates a raw ext4 disk image at outPath, sized dynamically from
// srcDir (see rootfsDiskGeometry), pre-populated with srcDir's contents in a single
// mkfs pass (`mkfs.ext4 -d <srcDir> ...`). This is how the ateom-owned-boot path
// turns the actor's OCI bundle rootfs into a writable virtio-blk disk (/dev/vdb):
// the guest mounts it as the container rootfs, so rootfs writes land on this
// host-backed file (off guest RAM) -> memory-only CH snapshot, no balloon.
//
// The size is a deterministic function of srcDir's contents, so the cold-boot
// build and the restore-time rebuild from the same OCI image agree (the guest
// resumes with the ext4 superblock cached in RAM, which must match the disk).
//
// Requires mkfs.ext4 (e2fsprogs) on PATH in the worker image. The image is
// recreated from scratch each call (reset-to-golden recreates it from the golden
// bundle), so any prior file at outPath is truncated.
//
// mkfs.ext4 -d copies srcDir's tree (perms, ownership, symlinks, xattrs) into the
// new filesystem without needing a loop mount or root's mount privileges — it
// writes the filesystem structures directly to the image file.
func BuildExt4Image(ctx context.Context, srcDir, outPath string) error {
	if fi, err := os.Stat(srcDir); err != nil || !fi.IsDir() {
		return fmt.Errorf("BuildExt4Image: source %q is not a directory: %v", srcDir, err)
	}
	sizeMiB, inodes, err := rootfsDiskGeometry(srcDir)
	if err != nil {
		return fmt.Errorf("BuildExt4Image: sizing from %q: %w", srcDir, err)
	}

	// Truncate to size first so mkfs writes into a sparse file of the right size
	// (mkfs.ext4 also accepts a size argument, but a pre-sized file is unambiguous
	// and keeps the on-disk size predictable for the snapshot config).
	if err := os.Remove(outPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("BuildExt4Image: removing stale image %q: %w", outPath, err)
	}
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("BuildExt4Image: creating image %q: %w", outPath, err)
	}
	if err := f.Truncate(int64(sizeMiB) * 1024 * 1024); err != nil {
		f.Close()
		return fmt.Errorf("BuildExt4Image: sizing image %q: %w", outPath, err)
	}
	f.Close()

	// -F: don't prompt (operating on a regular file, not a block device).
	// -q: quiet. -d: populate from srcDir. -N: fix the inode count to the tree's
	// entries + slack (the default size-derived ratio can starve a file-heavy
	// rootfs of inodes). -E lazy_*=0: write tables eagerly so the image is fully
	// materialized (deterministic on-disk bytes, important for the reset-to-golden
	// "verbatim copy" approach). -O ^has_journal: a reset-each-restore rootfs gains
	// nothing from a journal and it adds nondeterminism.
	args := []string{
		"-F", "-q",
		"-N", strconv.FormatInt(inodes, 10),
		"-E", "lazy_itable_init=0,lazy_journal_init=0",
		"-O", "^has_journal",
		"-d", srcDir,
		outPath,
		strconv.Itoa(sizeMiB) + "M",
	}
	cmd := exec.CommandContext(ctx, "mkfs.ext4", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("BuildExt4Image: mkfs.ext4 %v: %w: %s", args, err, out)
	}
	return nil
}
