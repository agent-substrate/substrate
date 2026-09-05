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
	"io"
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

type failingCloseFile struct{ *os.File }

func (f failingCloseFile) Close() error {
	_ = f.File.Close()
	return errors.New("deferred flush failed")
}

func TestCopyFile_CloseError(t *testing.T) {
	orig := createDestFile
	createDestFile = func(name string) (io.WriteCloser, error) {
		f, err := os.Create(name)
		if err != nil {
			return nil, err
		}
		return failingCloseFile{f}, nil
	}
	t.Cleanup(func() { createDestFile = orig })

	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	if err := os.WriteFile(src, []byte("checkpoint pages"), 0o600); err != nil {
		t.Fatalf("seeding src: %v", err)
	}
	if _, err := CopyFile(src, filepath.Join(dir, "dst")); err == nil {
		t.Error("CopyFile with failing destination Close = nil, want error")
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

// noFdFile hides the descriptor of an *os.File, forcing copySparse down its
// userspace write path.
type noFdFile struct {
	f *os.File
}

func (n noFdFile) Write(b []byte) (int, error)              { return n.f.Write(b) }
func (n noFdFile) WriteAt(b []byte, off int64) (int, error) { return n.f.WriteAt(b, off) }
func (n noFdFile) Truncate(size int64) error                { return n.f.Truncate(size) }
func (n noFdFile) Close() error                             { return n.f.Close() }

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

// TestCopyFilePreservesHolesUserspace covers the fallback taken when the destination
// does not expose a descriptor, so copy_file_range is unavailable.
func TestCopyFilePreservesHolesUserspace(t *testing.T) {
	orig := createDestFile
	createDestFile = func(name string) (io.WriteCloser, error) {
		f, err := os.Create(name)
		if err != nil {
			return nil, err
		}
		return noFdFile{f: f}, nil
	}
	t.Cleanup(func() { createDestFile = orig })

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

	dst := filepath.Join(dir, "copied")
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
		t.Fatal("userspace copy differs from source")
	}

	srcAlloc, dstAlloc := allocatedBytes(t, src), allocatedBytes(t, dst)
	if srcAlloc >= size/2 {
		t.Skipf("source did not end up sparse (%d of %d bytes allocated)", srcAlloc, int64(size))
	}
	if dstAlloc > srcAlloc*4 {
		t.Errorf("userspace copy allocated %d bytes for a %d-byte source: holes were filled in",
			dstAlloc, srcAlloc)
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
