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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"

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
	if o, err := exec.CommandContext(ctx, "cp", "--sparse=always", baseFile, tmp).CombinedOutput(); err != nil {
		return fmt.Errorf("cp base->tmp: %w: %s", err, o)
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
// (baseFile is untouched until the first rename succeeds).
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
