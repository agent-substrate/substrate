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
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// region is a populated byte range written into a sparse test image.
type region struct {
	off  int64
	data []byte
}

// writeSparse creates a sparse file of logical size with the given populated
// regions (the gaps are holes). It mirrors a CH memory-ranges image: scattered
// resident pages in a sea of zero (free) RAM.
func writeSparse(t *testing.T, path string, size int64, regions []region) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for _, r := range regions {
		if _, err := f.WriteAt(r.data, r.off); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

// fill returns n bytes of nonzero pseudo-data keyed by seed (never zero, so the
// regions are genuinely "data" and distinguishable from holes).
func fill(seed, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i+seed)%251 + 1)
	}
	return b
}

// TestMergeDeltaIntoBase asserts the fast (rename+overlay) merge produces exactly
// base-with-delta-overlaid — byte-identical to the reference copying merge
// (MergeSparseOverlay). This is the guest memory image, so any divergence = a dead
// or corrupted guest on resume.
func TestMergeDeltaIntoBase(t *testing.T) {
	const size = 8 << 20 // 8 MiB logical

	// base = a complete restore source: data at three scattered offsets.
	baseRegions := []region{
		{off: 0, data: fill(1, 4096)},           // first page
		{off: 1 << 20, data: fill(2, 70000)},    // interior, crosses 64KiB boundaries
		{off: size - 8192, data: fill(3, 4096)}, // near the end
	}
	// delta = CH's post-restore faulted pages: one OVERLAPS base (newer content,
	// must win), one lands in a base HOLE (newly faulted page), rest holes.
	deltaRegions := []region{
		{off: 1 << 20, data: fill(99, 70000)}, // overwrites base's interior region
		{off: 4 << 20, data: fill(42, 12345)}, // new page where base had a hole
	}

	// Build the expected merged image in memory: base, with delta overlaid.
	want := make([]byte, size)
	for _, r := range baseRegions {
		copy(want[r.off:], r.data)
	}
	for _, r := range deltaRegions {
		copy(want[r.off:], r.data)
	}

	ctx := context.Background()

	// --- fast path: MergeDeltaIntoBase (consumes base, result lands in delta) ---
	dir := t.TempDir()
	base := filepath.Join(dir, "restore-state", "memory-ranges")
	delta := filepath.Join(dir, "checkpoint-state", "memory-ranges")
	if err := os.MkdirAll(filepath.Dir(base), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(delta), 0o700); err != nil {
		t.Fatal(err)
	}
	writeSparse(t, base, size, baseRegions)
	writeSparse(t, delta, size, deltaRegions)

	if err := MergeDeltaIntoBase(ctx, base, delta); err != nil {
		t.Fatalf("MergeDeltaIntoBase: %v", err)
	}
	got, err := os.ReadFile(delta)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("MergeDeltaIntoBase result != expected (len got=%d want=%d)", len(got), len(want))
	}
	// base must have been consumed (renamed away), not left behind as a stale copy.
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Errorf("base still present after MergeDeltaIntoBase (err=%v); expected it consumed", err)
	}
	// The merged image must stay sparse (the holes between regions are not allocated).
	if fi, err := os.Stat(delta); err == nil {
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Blocks > 0 {
			if st.Blocks*512 >= size {
				t.Logf("note: merged not sparse on this fs (actual=%d >= logical=%d) — correctness still holds", st.Blocks*512, size)
			}
		}
	}

	// --- reference path: MergeSparseOverlay must produce the identical bytes ---
	rdir := t.TempDir()
	rbase := filepath.Join(rdir, "memory-ranges-base")
	rdelta := filepath.Join(rdir, "memory-ranges-delta")
	writeSparse(t, rbase, size, baseRegions)
	writeSparse(t, rdelta, size, deltaRegions)
	if err := MergeSparseOverlay(ctx, rbase, rdelta, rdelta); err != nil {
		t.Fatalf("MergeSparseOverlay: %v", err)
	}
	ref, err := os.ReadFile(rdelta)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ref, want) {
		t.Fatalf("MergeSparseOverlay result != expected (sanity check on the reference)")
	}
	if !bytes.Equal(got, ref) {
		t.Fatalf("MergeDeltaIntoBase and MergeSparseOverlay disagree")
	}
}

// TestMergeDeltaIntoBaseSizeMismatch verifies a base/delta size mismatch is
// refused (misaligned overlay would corrupt the image) rather than silently
// producing garbage.
func TestMergeDeltaIntoBaseSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base")
	delta := filepath.Join(dir, "delta")
	writeSparse(t, base, 1<<20, []region{{off: 0, data: fill(1, 4096)}})
	writeSparse(t, delta, 2<<20, []region{{off: 0, data: fill(2, 4096)}})
	if err := MergeDeltaIntoBase(context.Background(), base, delta); err == nil {
		t.Fatal("expected size-mismatch error, got nil")
	}
	// base must be untouched (no destructive rename happened before the check).
	if _, err := os.Stat(base); err != nil {
		t.Errorf("base should be intact after a refused merge: %v", err)
	}
}
