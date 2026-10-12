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

package ch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// MergeSparseOverlay reconstructs a COMPLETE memory snapshot from an OnDemand
// (userfaultfd) restore. CH's new snapshot (deltaFile) contains only the pages
// the guest faulted in since the OnDemand restore; every other page is unchanged
// from the snapshot it restored FROM (baseFile). So the complete current memory
// = baseFile, with deltaFile's populated pages overlaid.
//
// It writes outFile = a sparse copy of baseFile, then overlays every DATA region
// of deltaFile (located via SEEK_DATA/SEEK_HOLE, so holes — the un-faulted pages —
// are skipped) at the same byte offsets. baseFile and deltaFile MUST be flat images
// of identical size and layout (CH memory-ranges of the same guest + CH version),
// which holds across a restore/snapshot of one actor. This is a Firecracker-style
// differential snapshot implemented on top of CH (which has no native diff
// snapshot): it keeps OnDemand's fast, non-densifying restore while still producing
// complete, re-restorable snapshots for the suspend/resume chain.
func MergeSparseOverlay(ctx context.Context, baseFile, deltaFile, outFile string) error {
	bi, err := os.Stat(baseFile)
	if err != nil {
		return fmt.Errorf("stat base %q: %w", baseFile, err)
	}
	// outFile := sparse copy of baseFile (preserves holes so it stays sparse).
	tmp := outFile + ".merge.tmp"
	_ = os.Remove(tmp)
	if err := copySparseFile(ctx, baseFile, tmp); err != nil {
		return fmt.Errorf("sparse copy base->tmp: %w", err)
	}

	d, err := os.Open(deltaFile)
	if err != nil {
		return fmt.Errorf("open delta %q: %w", deltaFile, err)
	}
	defer d.Close()
	di, err := d.Stat()
	if err != nil {
		return err
	}
	if di.Size() != bi.Size() {
		// Same guest => identical memory-ranges length. A mismatch means the overlay
		// offsets wouldn't line up, so refuse rather than corrupt.
		return fmt.Errorf("MergeSparseOverlay: size mismatch base=%d delta=%d", bi.Size(), di.Size())
	}

	o, err := os.OpenFile(tmp, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer o.Close()

	if _, err := copySparseRegions(d, o); err != nil {
		return err
	}
	// No fsync: atelet ships the merged image to GCS (the durability point), so a
	// partial local file after a node crash is just discarded + the suspend retried;
	// paying an ~150MiB fsync on the suspend critical path buys nothing.
	if err := o.Close(); err != nil {
		return err
	}
	// Put the merged image at outFile's name. Unlink the old one FIRST, then rename
	// onto the now-free name, for the same reason as MergeDeltaIntoBase's final
	// rename below: renaming OVER an existing file makes ext4 (data=ordered)
	// synchronously write back the renamed file's dirty pages, and `tmp` carries the
	// whole merged image. Renaming to a non-existent name skips that flush (the
	// dirty pages stay in page cache for atelet to ship), taking the merge
	// ~1140ms→~115ms. Unlike there, outFile need not exist: this is also called to
	// write a merged image to a fresh path.
	if err := os.Remove(outFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old out: %w", err)
	}
	return os.Rename(tmp, outFile)
}

// MergeDeltaIntoBase overlays deltaFile's populated pages onto baseFile in place
// and leaves the complete merged snapshot at deltaFile's path — the same result as
// MergeSparseOverlay, but WITHOUT copying baseFile's working set on every suspend.
//
// baseFile is the per-actor restore staging file (restore-state/memory-ranges),
// demand-paged only by the now-paused CH we are about to tear down and discarded
// afterward. So rather than `cp`-ing its whole working set (e.g. ~150MiB of a 2GiB
// guest, ~0.8s on the suspend critical path), we rename baseFile next to deltaFile,
// overlay deltaFile's (small) faulted pages onto it, and swap it into deltaFile's
// place — turning an O(working-set) copy into an O(delta) write plus two renames.
//
// baseFile and deltaFile are siblings under the actor dir (restore-state/ and
// checkpoint-state/), so the renames are same-filesystem (metadata-only). If they
// straddle a mount boundary (EXDEV) it falls back to the copying MergeSparseOverlay
// (baseFile is untouched until the first rename succeeds), as it does when baseFile
// carries a second link and so cannot be overlaid in place.
func MergeDeltaIntoBase(ctx context.Context, baseFile, deltaFile string) error {
	bi, err := os.Stat(baseFile)
	if err != nil {
		return fmt.Errorf("stat base %q: %w", baseFile, err)
	}
	di, err := os.Stat(deltaFile)
	if err != nil {
		return fmt.Errorf("stat delta %q: %w", deltaFile, err)
	}
	if di.Size() != bi.Size() {
		// Same guest => identical memory-ranges length; a mismatch would misalign the
		// overlay offsets, so refuse rather than corrupt.
		return fmt.Errorf("MergeDeltaIntoBase: size mismatch base=%d delta=%d", bi.Size(), di.Size())
	}

	// The fast path below renames baseFile and mutates its inode in place, so it
	// is only for an expendable staging file. Preserved snapshots use
	// MergeSparseOverlay directly; a shared inode (Nlink > 1) falls back to it too.
	if st, ok := bi.Sys().(*syscall.Stat_t); ok && st.Nlink > 1 {
		return MergeSparseOverlay(ctx, baseFile, deltaFile, deltaFile)
	}

	// Move baseFile (with its already-on-disk working set) next to deltaFile. If this
	// fails with EXDEV the two are on different filesystems and baseFile is still
	// intact, so fall back to the copying merge.
	merged := deltaFile + ".merged.tmp"
	_ = os.Remove(merged)
	if err := os.Rename(baseFile, merged); err != nil {
		if errors.Is(err, unix.EXDEV) {
			return MergeSparseOverlay(ctx, baseFile, deltaFile, deltaFile)
		}
		return fmt.Errorf("rename base->merged: %w", err)
	}

	d, err := os.Open(deltaFile)
	if err != nil {
		return fmt.Errorf("open delta %q: %w", deltaFile, err)
	}
	defer d.Close()
	m, err := os.OpenFile(merged, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer m.Close()
	if _, err := copySparseRegions(d, m); err != nil {
		return err
	}
	// No fsync: atelet ships the merged image to GCS (the durability point), so a
	// partial local file after a crash is just discarded + the suspend retried.
	if err := m.Close(); err != nil {
		return err
	}
	// Put the merged image at deltaFile's name. Unlink the old delta FIRST, then
	// rename onto the now-free name: renaming OVER an existing file makes ext4
	// (data=ordered) synchronously write back the renamed file's dirty pages, and
	// `merged` carries ~150MiB of dirty download pages, so a replace-rename costs
	// ~0.5-0.8s. Renaming to a non-existent name skips that flush (the dirty pages
	// stay in page cache for atelet to ship), taking the merge ~840ms→~5ms.
	if err := os.Remove(deltaFile); err != nil {
		return fmt.Errorf("remove old delta: %w", err)
	}
	return os.Rename(merged, deltaFile)
}

// copySparseRegions overwrites dst with every populated (non-hole) region of src
// at the same byte offsets, leaving dst's other bytes untouched. Holes in src are
// located via SEEK_DATA/SEEK_HOLE and skipped. src and dst are assumed to be the
// same logical size (the caller validates this).
func copySparseRegions(src, dst *os.File) (copied int64, err error) {
	si, err := src.Stat()
	if err != nil {
		return 0, err
	}
	size := si.Size()
	sfd := int(src.Fd())
	buf := make([]byte, 1<<20)
	off := int64(0)
	for off < size {
		// Next populated region [ds, de) in src.
		ds, err := unix.Seek(sfd, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				break // no more data
			}
			return copied, fmt.Errorf("SEEK_DATA: %w", err)
		}
		de, err := unix.Seek(sfd, ds, unix.SEEK_HOLE)
		if err != nil {
			return copied, fmt.Errorf("SEEK_HOLE: %w", err)
		}
		if _, err := src.Seek(ds, io.SeekStart); err != nil {
			return copied, err
		}
		if _, err := dst.Seek(ds, io.SeekStart); err != nil {
			return copied, err
		}
		remaining := de - ds
		for remaining > 0 {
			n := int64(len(buf))
			if n > remaining {
				n = remaining
			}
			r, err := io.ReadFull(src, buf[:n])
			if r > 0 {
				if _, werr := dst.Write(buf[:r]); werr != nil {
					return copied, werr
				}
				copied += int64(r)
			}
			if err != nil {
				return copied, fmt.Errorf("reading data region: %w", err)
			}
			remaining -= int64(r)
		}
		off = de
	}
	return copied, nil
}

// copySparseFile creates dstPath as a sparse copy of srcPath, like
// cp --sparse=always: holes in srcPath stay holes, and so does any all-zero
// block inside its data regions.
func copySparseFile(ctx context.Context, srcPath, dstPath string) error {
	s, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer s.Close()
	si, err := s.Stat()
	if err != nil {
		return err
	}
	d, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer d.Close()
	// A fresh file of the full size reads as zeros, so a skipped block needs no write.
	if err := d.Truncate(si.Size()); err != nil {
		return err
	}
	if err := copyNonZeroRegions(ctx, s, d, si.Size()); err != nil {
		return err
	}
	return d.Close()
}

// sparseBlock is the granularity at which copyNonZeroRegions leaves zeros as holes.
const sparseBlock = 4096

// copyNonZeroRegions writes every block of src's data regions that is not all
// zeros to the same offset in dst. dst must read as zeros wherever nothing is
// written, as a freshly truncated file does, so unlike copySparseRegions this
// cannot overlay onto existing data: a skipped zero block would leave the old
// bytes in place.
func copyNonZeroRegions(ctx context.Context, src, dst *os.File, size int64) error {
	sfd := int(src.Fd())
	buf := make([]byte, 1<<20)
	for off := int64(0); off < size; {
		ds, err := unix.Seek(sfd, off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			return nil // no more data
		}
		if err != nil {
			return fmt.Errorf("SEEK_DATA: %w", err)
		}
		de, err := unix.Seek(sfd, ds, unix.SEEK_HOLE)
		if err != nil {
			return fmt.Errorf("SEEK_HOLE: %w", err)
		}
		for pos := ds; pos < de; {
			if err := ctx.Err(); err != nil {
				return err
			}
			n := min(int64(len(buf)), de-pos)
			if _, err := src.ReadAt(buf[:n], pos); err != nil {
				return fmt.Errorf("reading data region: %w", err)
			}
			if err := writeNonZero(dst, buf[:n], pos); err != nil {
				return err
			}
			pos += n
		}
		off = de
	}
	return nil
}

// writeNonZero writes p to dst at off, skipping all-zero blocks and coalescing
// the rest into as few writes as possible.
func writeNonZero(dst *os.File, p []byte, off int64) error {
	start := -1 // start of the current run of non-zero blocks
	for i := 0; i < len(p); i += sparseBlock {
		if !allZero(p[i:min(i+sparseBlock, len(p))]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			if _, err := dst.WriteAt(p[start:i], off+int64(start)); err != nil {
				return err
			}
			start = -1
		}
	}
	if start >= 0 {
		if _, err := dst.WriteAt(p[start:], off+int64(start)); err != nil {
			return err
		}
	}
	return nil
}

// zeroBlock is what allZero compares against: bytes.Equal is vectorized, so it
// beats checking byte by byte.
var zeroBlock = make([]byte, sparseBlock)

func allZero(b []byte) bool {
	return bytes.Equal(b, zeroBlock[:len(b)])
}
