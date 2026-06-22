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
// (userfaultfd) restore. CH's new snapshot (delta) contains only the pages the
// guest faulted in since the OnDemand restore; every other page is unchanged from
// the snapshot it restored FROM (base). So the complete current memory =
// base, with delta's populated pages overlaid.
//
// It writes out = a sparse copy of base, then overlays every DATA region of delta
// (located via SEEK_DATA/SEEK_HOLE, so holes — the un-faulted pages — are skipped)
// at the same byte offsets. base and delta MUST be flat images of identical size
// and layout (CH memory-ranges of the same guest + CH version), which holds across
// a restore/snapshot of one actor. This is a Firecracker-style differential
// snapshot implemented on top of CH (which has no native diff snapshot): it lets us
// keep OnDemand's fast, non-densifying restore while still producing complete,
// re-restorable snapshots for the suspend/resume chain.
func MergeSparseOverlay(ctx context.Context, base, delta, out string) error {
	bi, err := os.Stat(base)
	if err != nil {
		return fmt.Errorf("stat base %q: %w", base, err)
	}
	// out := sparse copy of base (preserves holes so the merged image stays sparse).
	tmp := out + ".merge.tmp"
	_ = os.Remove(tmp)
	if o, err := exec.CommandContext(ctx, "cp", "--sparse=always", base, tmp).CombinedOutput(); err != nil {
		return fmt.Errorf("cp base->tmp: %w: %s", err, o)
	}

	d, err := os.Open(delta)
	if err != nil {
		return fmt.Errorf("open delta %q: %w", delta, err)
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

	if _, err := overlayDataRegions(d, o); err != nil {
		return err
	}
	// No fsync: the merged image is consumed in-process by atelet on this same node
	// (page-cache coherent) and shipped to GCS — that upload is the durability point.
	// A partial local file after a node crash is just discarded + the suspend retried,
	// so paying an ~150MiB fsync on the suspend critical path buys nothing.
	if err := o.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, out)
}

// MergeDeltaIntoBase produces the same COMPLETE merged snapshot as
// MergeSparseOverlay(base, delta, delta) — base with delta's populated pages
// overlaid — but WITHOUT copying base's working set on every suspend.
//
// MergeSparseOverlay starts by `cp`-ing the whole base working set (e.g. ~150MiB
// of a 2GiB guest) into a temp file; on the suspend critical path that dominates
// the merge (~0.8s on a 2GiB guest, GKE). But base is the per-actor restore
// staging file (restore-state/memory-ranges), demand-paged ONLY by the now-paused
// CH we are about to tear down, and discarded afterward — so instead of copying it
// we rename it next to delta, overlay delta's (small) faulted pages onto it, and
// swap it into delta's place. That turns an O(working-set) copy into an O(delta)
// write plus two metadata renames.
//
// base and delta are siblings under the actor dir (restore-state/ and
// checkpoint-state/), so the renames are same-filesystem (metadata-only). On the
// off chance they straddle a mount boundary (EXDEV), it falls back to the copying
// MergeSparseOverlay (base is untouched until the first rename succeeds).
func MergeDeltaIntoBase(ctx context.Context, base, delta string) error {
	bi, err := os.Stat(base)
	if err != nil {
		return fmt.Errorf("stat base %q: %w", base, err)
	}
	di, err := os.Stat(delta)
	if err != nil {
		return fmt.Errorf("stat delta %q: %w", delta, err)
	}
	if di.Size() != bi.Size() {
		// Same guest => identical memory-ranges length; a mismatch would misalign the
		// overlay offsets, so refuse rather than corrupt.
		return fmt.Errorf("MergeDeltaIntoBase: size mismatch base=%d delta=%d", bi.Size(), di.Size())
	}

	// Move base (with its already-on-disk working set) next to delta. If this fails
	// with EXDEV the two are on different filesystems and base is still intact, so
	// fall back to the copying merge.
	merged := delta + ".merged.tmp"
	_ = os.Remove(merged)
	if err := os.Rename(base, merged); err != nil {
		if errors.Is(err, unix.EXDEV) {
			return MergeSparseOverlay(ctx, base, delta, delta)
		}
		return fmt.Errorf("rename base->merged: %w", err)
	}

	d, err := os.Open(delta)
	if err != nil {
		return fmt.Errorf("open delta %q: %w", delta, err)
	}
	defer d.Close()
	m, err := os.OpenFile(merged, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer m.Close()
	if _, err := overlayDataRegions(d, m); err != nil {
		return err
	}
	// No fsync: atelet reads the merged image back from page cache on this same node
	// and ships it to GCS — that upload is the durability point, so an ~150MiB fsync
	// on the suspend critical path buys nothing.
	if err := m.Close(); err != nil {
		return err
	}
	// Put the merged image at delta's name. We UNLINK CH's old delta FIRST, then
	// rename onto the now-free name. Renaming OVER an existing file makes ext4
	// (data=ordered) synchronously write back the renamed file's dirty pages before
	// committing — and `merged` (the former restore source) carries ~150MiB of dirty
	// pages from the download, so a replace-rename costs ~0.5-0.8s (measured on GKE).
	// Renaming to a name that does NOT exist skips that flush; the dirty pages stay in
	// page cache for atelet to read + ship. This is what takes the merge ~840ms→~5ms.
	if err := os.Remove(delta); err != nil {
		return fmt.Errorf("remove old delta: %w", err)
	}
	return os.Rename(merged, delta)
}

// overlayDataRegions copies every populated (non-hole) region of src onto dst at
// the same byte offsets, leaving dst's other bytes untouched. Holes in src are
// located via SEEK_DATA/SEEK_HOLE and skipped. src and dst are assumed to be the
// same logical size (the caller validates this).
func overlayDataRegions(src, dst *os.File) (copied int64, err error) {
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
