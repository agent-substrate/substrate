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

package sparsefile

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	want := []byte("checkpoint pages")
	if err := os.WriteFile(src, want, 0o600); err != nil {
		t.Fatalf("seeding src: %v", err)
	}

	dst := filepath.Join(dir, "dst")
	n, err := CopyFile(src, dst)
	if err != nil {
		t.Fatalf("CopyFile: %v", err)
	}
	if n != int64(len(want)) {
		t.Errorf("copied %d bytes, want %d", n, len(want))
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("reading dst: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("dst content = %q, want %q", got, want)
	}

	if _, err := CopyFile(dir, filepath.Join(dir, "dst2")); err == nil {
		t.Error("CopyFile(directory, ...) succeeded, want error")
	}
}

// allocatedBytes reports how much disk a file actually occupies, which is less than its
// logical size when it has holes.
func allocatedBytes(t *testing.T, path string) int64 {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	return st.Blocks * 512
}

func TestCopyFilePreservesHoles(t *testing.T) {
	const (
		size     = 32 << 20
		markerAt = 16 << 20
	)
	dir := t.TempDir()
	src := filepath.Join(dir, "memory-ranges")

	// A stand-in for a guest memory image: mostly hole, with data at both the start
	// and the middle.
	f, err := os.Create(src)
	if err != nil {
		t.Fatalf("creating src: %v", err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatalf("sizing src: %v", err)
	}
	head := bytes.Repeat([]byte{0xAB}, 4<<10)
	middle := bytes.Repeat([]byte{0xCD}, 4<<10)
	if _, err := f.WriteAt(head, 0); err != nil {
		t.Fatalf("writing head: %v", err)
	}
	if _, err := f.WriteAt(middle, markerAt); err != nil {
		t.Fatalf("writing middle: %v", err)
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		t.Fatalf("flushing src: %v", err)
	}

	dst := filepath.Join(dir, "copied")
	n, err := CopyFile(src, dst)
	if err != nil {
		t.Fatalf("CopyFile: %v", err)
	}
	if n != size {
		t.Errorf("copied %d logical bytes, want %d", n, size)
	}

	// The copy must be byte-identical, holes included.
	want, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("reading src: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("reading dst: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Fatal("copy differs from source")
	}

	srcAlloc, dstAlloc := allocatedBytes(t, src), allocatedBytes(t, dst)
	if srcAlloc >= size/2 {
		t.Skipf("source did not end up sparse (%d of %d bytes allocated); "+
			"this filesystem cannot report holes", srcAlloc, int64(size))
	}
	// A dense copy would allocate the full logical size; a hole-preserving one stays
	// near the source's footprint.
	if dstAlloc > srcAlloc*4 {
		t.Errorf("copy allocated %d bytes for a %d-byte source (logical %d): holes were filled in",
			dstAlloc, srcAlloc, int64(size))
	}
}

func TestCopyFileAllHoles(t *testing.T) {
	const size = 8 << 20
	dir := t.TempDir()
	src := filepath.Join(dir, "empty")
	f, err := os.Create(src)
	if err != nil {
		t.Fatalf("creating src: %v", err)
	}
	if err := errors.Join(f.Truncate(size), f.Close()); err != nil {
		t.Fatalf("sizing src: %v", err)
	}

	dst := filepath.Join(dir, "copied")
	if _, err := CopyFile(src, dst); err != nil {
		t.Fatalf("CopyFile: %v", err)
	}
	st, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("stat dst: %v", err)
	}
	if st.Size() != size {
		t.Errorf("copy is %d bytes, want %d", st.Size(), int64(size))
	}
}

// TestCopyFilePreservesHolesAcrossFilesystems covers the userspace fallback:
// with source and destination on different filesystems, copy_file_range
// fails with EXDEV and the extents are copied through userspace instead. It
// needs a second real filesystem, so it runs where one is available
// (/dev/shm on Linux) and skips elsewhere — on platforms with no kernel
// copy at all, the plain holes test above already exercises userspace.
func TestCopyFilePreservesHolesAcrossFilesystems(t *testing.T) {
	otherFS, err := os.MkdirTemp("/dev/shm", "sparsefile-test-")
	if err != nil {
		t.Skipf("no second filesystem available for a cross-filesystem copy: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(otherFS) })

	const size = 32 << 20
	dir := t.TempDir()
	src := filepath.Join(dir, "memory-ranges")
	f, err := os.Create(src)
	if err != nil {
		t.Fatalf("creating src: %v", err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatalf("sizing src: %v", err)
	}
	marker := bytes.Repeat([]byte{0xEF}, 4<<10)
	if _, err := f.WriteAt(marker, 8<<20); err != nil {
		t.Fatalf("writing marker: %v", err)
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		t.Fatalf("flushing src: %v", err)
	}
	// Prove the directories really are on different filesystems; same-mount
	// tmpdirs (some CI images) would silently test the kernel path instead.
	if err := os.Link(src, filepath.Join(otherFS, "probe")); err == nil {
		t.Skip("test dirs share a filesystem; cannot force the userspace fallback")
	}

	dst := filepath.Join(otherFS, "copied")
	if _, err := CopyFile(src, dst); err != nil {
		t.Fatalf("CopyFile: %v", err)
	}

	want, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("reading src: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("reading dst: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Fatal("cross-filesystem copy differs from source")
	}

	srcAlloc, dstAlloc := allocatedBytes(t, src), allocatedBytes(t, dst)
	if srcAlloc >= size/2 {
		t.Skipf("source did not end up sparse (%d of %d bytes allocated)", srcAlloc, int64(size))
	}
	if dstAlloc > srcAlloc*4 {
		t.Errorf("cross-filesystem copy allocated %d bytes for a %d-byte source: holes were filled in",
			dstAlloc, srcAlloc)
	}
}

// TestCopyRejectsNonEmptyDestination pins Copy's freshness contract: holes
// are never written, so an all-hole source copied over existing bytes would
// "succeed" while the stale bytes show through. Copy must refuse instead.
func TestCopyRejectsNonEmptyDestination(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src")
	f, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(f.Truncate(1<<20), f.Close()); err != nil {
		t.Fatal(err)
	}
	src, err := os.Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	dstPath := filepath.Join(dir, "dst")
	if err := os.WriteFile(dstPath, []byte("stale bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	dst, err := os.OpenFile(dstPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()

	if _, err := Copy(src, dst); err == nil {
		t.Fatal("Copy accepted a non-empty destination")
	}
	if got, err := os.ReadFile(dstPath); err != nil || string(got) != "stale bytes" {
		t.Errorf("rejected copy modified the destination: %q, %v", got, err)
	}
}

// TestCopyHandles covers the open-handle entry point: sparse source copied
// through caller-owned handles (O_EXCL destination), byte-identical result,
// holes preserved, and a source whose name is gone mid-copy still copies —
// the open handle is the contract.
func TestCopyHandles(t *testing.T) {
	const size = 16 << 20
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src")
	f, err := os.Create(srcPath)
	if err != nil {
		t.Fatalf("creating src: %v", err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatalf("sizing src: %v", err)
	}
	marker := bytes.Repeat([]byte{0x42}, 4<<10)
	if _, err := f.WriteAt(marker, 4<<20); err != nil {
		t.Fatalf("writing marker: %v", err)
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		t.Fatalf("flushing src: %v", err)
	}

	src, err := os.Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	// Unlink the source name before copying: Copy exists for callers whose
	// source can be evicted mid-copy, so only the handle may matter.
	if err := os.Remove(srcPath); err != nil {
		t.Fatal(err)
	}

	dstPath := filepath.Join(dir, "dst")
	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	n, err := Copy(src, dst)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	if n != size {
		t.Errorf("copied %d logical bytes, want %d", n, size)
	}

	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(got)) != size || !bytes.Equal(got[4<<20:4<<20+len(marker)], marker) {
		t.Error("handle copy differs from source")
	}
	for _, b := range got[:4<<20] {
		if b != 0 {
			t.Fatal("hole region contains data")
		}
	}
}
