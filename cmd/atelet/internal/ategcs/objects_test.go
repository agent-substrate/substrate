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

package ategcs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// memStore is an in-memory ObjectStorage for round-trip tests.
type memStore struct{ m map[string][]byte }

func newMemStore() *memStore { return &memStore{m: map[string][]byte{}} }

func (s *memStore) PutObject(_ context.Context, bucket, object string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.m[bucket+"/"+object] = b
	return nil
}

func (s *memStore) GetObject(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	b, ok := s.m[bucket+"/"+object]
	if !ok {
		return nil, fmt.Errorf("object %q/%q not found", bucket, object)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

// streamingMemStore is a memStore that advertises streaming PutObject support, so
// sendZstd takes the pipe (compress∥upload overlap) path used for GCS
// instead of staging a seekable temp file.
type streamingMemStore struct{ *memStore }

func (s *streamingMemStore) supportsStreamingPut() {}

// TestSparseUploadStreamingRoundTrip drives the STREAMING upload path (GCS-like
// backend) end-to-end through the real entry points: the object must still be the
// sparse-extent format (magic) and download byte-exact. This guards the pipe path
// that overlaps compression with the upload.
func TestSparseUploadStreamingRoundTrip(t *testing.T) {
	const size = 8 << 20
	want := make([]byte, size)
	regions := [][2]int{{0, 4096}, {2 << 20, 70000}, {size - 9000, 5000}}
	for _, e := range regions {
		for i := e[0]; i < e[0]+e[1]; i++ {
			want[i] = byte((i*7)%251 + 1)
		}
	}
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "memory-ranges")
	src, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for _, e := range regions {
		if _, err := src.WriteAt(want[e[0]:e[0]+e[1]], int64(e[0])); err != nil {
			t.Fatal(err)
		}
	}
	src.Close()

	store := &streamingMemStore{newMemStore()}
	ctx := context.Background()
	const gsURL = "gs://bucket/snap/memory-ranges.zstd"
	if err := SendLocalFileToGCSWithZstd(ctx, store, gsURL, srcPath); err != nil {
		t.Fatalf("streaming upload: %v", err)
	}
	stored := store.m["bucket/snap/memory-ranges.zstd"]
	if len(stored) < len(sparseMagic) || string(stored[:len(sparseMagic)]) != sparseMagic {
		t.Fatalf("streaming-stored object is not sparse-extent format (magic=%q)", stored[:min(len(stored), len(sparseMagic))])
	}
	if int64(len(stored)) >= size/2 {
		t.Errorf("stored %d bytes; expected far less than logical %d (holes not skipped)", len(stored), size)
	}
	dstPath := filepath.Join(dir, "restored")
	if err := FetchLocalFileFromGCSWithZstd(ctx, store, gsURL, dstPath); err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("streaming round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
	}
}

// TestSparseUploadDownloadRoundTrip drives the real upload+download entry points
// through an in-memory store: a genuinely sparse source file (multiple data extents
// separated by holes) must upload in the sparse-extent format (magic) and download
// byte-exact AND sparse on disk. This is the guest memory image, so correctness is
// non-negotiable.
func TestSparseUploadDownloadRoundTrip(t *testing.T) {
	const size = 8 << 20 // 8 MiB logical
	want := make([]byte, size)
	// Three populated extents at varied offsets/sizes (aligned + unaligned), the
	// rest holes — mirrors scattered resident pages in free RAM.
	fill := func(start, n int) {
		for i := start; i < start+n; i++ {
			want[i] = byte((i*7)%251 + 1) // never zero
		}
	}
	fill(0, 4096)         // first page
	fill(2<<20, 70000)    // interior, crosses 64KiB boundaries
	fill(size-9000, 5000) // near end, leaving a trailing hole

	// Write the source as a genuinely sparse file (holes between the extents).
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "memory-ranges")
	src, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for _, e := range [][2]int{{0, 4096}, {2 << 20, 70000}, {size - 9000, 5000}} {
		if _, err := src.WriteAt(want[e[0]:e[0]+e[1]], int64(e[0])); err != nil {
			t.Fatal(err)
		}
	}
	src.Close()

	store := newMemStore()
	ctx := context.Background()
	const gsURL = "gs://bucket/snap/memory-ranges.zstd"
	if err := SendLocalFileToGCSWithZstd(ctx, store, gsURL, srcPath); err != nil {
		t.Fatalf("upload: %v", err)
	}
	// The stored object must use the sparse-extent format (magic header).
	stored := store.m["bucket/snap/memory-ranges.zstd"]
	if len(stored) < len(sparseMagic) || string(stored[:len(sparseMagic)]) != sparseMagic {
		t.Fatalf("stored object is not sparse-extent format (magic=%q)", stored[:min(len(stored), len(sparseMagic))])
	}
	// The compressed object must be far smaller than the logical size (holes skipped).
	if int64(len(stored)) >= size/2 {
		t.Errorf("stored %d bytes; expected far less than logical %d (holes not skipped)", len(stored), size)
	}

	dstPath := filepath.Join(dir, "restored")
	if err := FetchLocalFileFromGCSWithZstd(ctx, store, gsURL, dstPath); err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
	}
	if fi, err := os.Stat(dstPath); err == nil {
		if blk := diskBlocks(fi); blk > 0 {
			t.Logf("restored sparse: apparent=%d actual=%d", size, blk*512)
			if blk*512 >= int64(size) {
				t.Logf("note: restored file not sparse on this fs — correctness still holds")
			}
		}
	}
}

// TestSparseVersionRejected confirms the reader refuses a sparse snapshot whose
// format version it doesn't understand (rather than misparsing it). This is the
// guest memory image, so a future incompatible layout must fail loudly.
func TestSparseVersionRejected(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "memory-ranges")
	f, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(1 << 20); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("hello sparse"), 4096); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, _, err := writeSparseZstd(&buf, f); err != nil {
		t.Fatalf("writeSparseZstd: %v", err)
	}
	f.Close()

	blob := buf.Bytes()
	// The version is the little-endian uint32 immediately after the 8-byte magic;
	// corrupt it so it no longer matches sparseVersion.
	blob[len(sparseMagic)] ^= 0xFF

	out, err := os.Create(filepath.Join(dir, "out"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	// readSparseZstd is called with the reader positioned just after the magic.
	if _, err := readSparseZstd(out, bytes.NewReader(blob[len(sparseMagic):])); err == nil {
		t.Fatal("expected an unsupported-version error, got nil")
	}
}

// TestWriteSparseZstdSkipsHoles asserts the encoder feeds ONLY the populated
// extents to zstd (not the whole logical image). Gated on the fs actually reporting
// the source as sparse (ext4/Linux does; macOS/APFS in CI dev may not — skipped there).
func TestWriteSparseZstdSkipsHoles(t *testing.T) {
	const size = 8 << 20
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "memory-ranges")
	src, err := os.Create(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Truncate(size); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 70000)
	for i := range data {
		data[i] = byte(i%251 + 1)
	}
	if _, err := src.WriteAt(data, 2<<20); err != nil { // one ~70KB extent in a sea of holes
		t.Fatal(err)
	}
	if err := src.Sync(); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(srcPath); err != nil {
		t.Fatal(err)
	} else if blk := diskBlocks(fi); blk == 0 || blk*512 >= int64(size) {
		t.Skipf("fs did not make the source sparse (actual=%d, logical=%d) — can't assert hole-skipping here", blk*512, size)
	}
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	logical, dataBytes, err := writeSparseZstd(&buf, src)
	if err != nil {
		t.Fatalf("writeSparseZstd: %v", err)
	}
	if logical != size {
		t.Errorf("logical=%d, want %d", logical, size)
	}
	if dataBytes >= int64(size)/2 {
		t.Errorf("dataBytes=%d fed to zstd; expected ~70KB (holes not skipped)", dataBytes)
	}
	t.Logf("fed %d data bytes for a %d logical image (holes skipped)", dataBytes, size)
}

// TestPlainZstdBackwardCompatRoundTrip drives the NON-file upload path (plain zstd,
// no magic) and confirms the download auto-detects + restores it — i.e. snapshots
// written before the sparse-extent format still restore.
func TestPlainZstdBackwardCompatRoundTrip(t *testing.T) {
	want := bytes.Repeat([]byte("agent-substrate snapshot payload\n"), 4096)
	store := newMemStore()
	ctx := context.Background()
	const gsURL = "gs://bucket/snap/config.json.zstd"
	// SendBytesToGCS is uncompressed; use sendZstd with a non-file reader to
	// hit the plain-zstd branch (no magic).
	if err := sendZstd(ctx, store, gsURL, bytes.NewReader(want)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	stored := store.m["bucket/snap/config.json.zstd"]
	if len(stored) >= len(sparseMagic) && string(stored[:len(sparseMagic)]) == sparseMagic {
		t.Fatal("non-file upload unexpectedly used the sparse-extent format")
	}
	dir := t.TempDir()
	dstPath := filepath.Join(dir, "config.json")
	if err := FetchLocalFileFromGCSWithZstd(ctx, store, gsURL, dstPath); err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("plain round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
	}
}

// diskBlocks returns the number of 512-byte blocks the file occupies on disk (for
// the sparseness check), or 0 if unavailable on this platform/fs.
func diskBlocks(fi os.FileInfo) int64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return int64(st.Blocks)
	}
	return 0
}

// TestCopyZstdSparse checks the sparse decompress is byte-exact (it's the guest
// memory image — corruption = a dead guest) and actually punches holes for the
// zero regions.
func TestCopyZstdSparse(t *testing.T) {
	// A mostly-zero image with non-zero data at the start, an interior block, the
	// tail-but-not-end, and aligned + unaligned sizes. Mirrors a guest memory-ranges:
	// scattered resident pages in a sea of zero (free) RAM.
	const size = 4 << 20 // 4 MiB
	want := make([]byte, size)
	for i := 0; i < 4096; i++ { // first page
		want[i] = byte(i%251 + 1)
	}
	for i := 1 << 20; i < (1<<20)+9000; i++ { // interior, crosses a 64KiB block boundary
		want[i] = byte(i%253 + 1)
	}
	for i := size - 5000; i < size-1000; i++ { // near the end, leaving a trailing zero hole
		want[i] = byte(i%249 + 1)
	}

	dir := t.TempDir()
	out := filepath.Join(dir, "memory-ranges")
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	size64, written, err := copyZstdSparse(f, bytes.NewReader(want))
	if err != nil {
		t.Fatalf("copyZstdSparse: %v", err)
	}
	if size64 != int64(len(want)) {
		t.Errorf("logical size = %d, want %d", size64, len(want))
	}
	if written >= int64(len(want)) {
		t.Errorf("written %d bytes; expected far less than %d for a mostly-zero image (not sparse)", written, len(want))
	}

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
	}

	// Best-effort sparseness check: the file should occupy fewer 512-byte blocks than
	// its apparent size (holes punched). fs-dependent, so only log if unavailable.
	if fi, err := os.Stat(out); err == nil {
		if st := diskBlocks(fi); st > 0 {
			actual := st * 512
			t.Logf("sparse: apparent=%d actual=%d written=%d", len(want), actual, written)
			if actual >= int64(len(want)) {
				t.Logf("note: file not sparse on this fs (actual=%d >= apparent=%d) — correctness still holds", actual, len(want))
			}
		}
	}
}

// TestWriteDecodeContentRoundTrip exercises the io-only compress/decompress halves
// (writeContent / decodeContent) directly — no object store — for both the sparse
// (file source) and plain (non-file reader) paths.
func TestWriteDecodeContentRoundTrip(t *testing.T) {
	dir := t.TempDir()

	t.Run("sparse file", func(t *testing.T) {
		const size = 8 << 20
		srcPath := filepath.Join(dir, "src")
		src, err := os.Create(srcPath)
		if err != nil {
			t.Fatal(err)
		}
		defer src.Close()
		if err := src.Truncate(size); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, 70000)
		for i := range data {
			data[i] = byte(i%251 + 1)
		}
		if _, err := src.WriteAt(data, 2<<20); err != nil { // one extent in a sea of holes
			t.Fatal(err)
		}
		if err := src.Sync(); err != nil {
			t.Fatal(err)
		}
		if _, err := src.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		wres, err := writeContent(&buf, src)
		if err != nil {
			t.Fatalf("writeContent: %v", err)
		}
		if !wres.sparse {
			t.Error("writeContent: sparse=false for a file source")
		}
		if wres.logicalBytes != size {
			t.Errorf("writeContent logicalBytes=%d, want %d", wres.logicalBytes, size)
		}

		dstPath := filepath.Join(dir, "dst")
		dst, err := os.Create(dstPath)
		if err != nil {
			t.Fatal(err)
		}
		defer dst.Close()
		dres, err := decodeContent(dst, &buf)
		if err != nil {
			t.Fatalf("decodeContent: %v", err)
		}
		if !dres.sparse {
			t.Error("decodeContent: sparse=false for a sparse-extent stream")
		}
		if dres.logicalBytes != size {
			t.Errorf("decodeContent logicalBytes=%d, want %d", dres.logicalBytes, size)
		}

		want, err := os.ReadFile(srcPath)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(dstPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("sparse round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
		}
	})

	t.Run("plain reader", func(t *testing.T) {
		want := bytes.Repeat([]byte("substrate payload\n"), 1000)
		var buf bytes.Buffer
		wres, err := writeContent(&buf, bytes.NewReader(want))
		if err != nil {
			t.Fatalf("writeContent: %v", err)
		}
		if wres.sparse {
			t.Error("writeContent: sparse=true for a non-file reader")
		}
		var out bytes.Buffer
		if _, err := decodeContent(&out, &buf); err != nil {
			t.Fatalf("decodeContent: %v", err)
		}
		if !bytes.Equal(out.Bytes(), want) {
			t.Fatalf("plain round-trip mismatch: len(got)=%d len(want)=%d", out.Len(), len(want))
		}
	})
}

// TestCopyZstdSparseClearsStaleData exercises the defensive dst.Truncate(0): a dst
// that already holds bytes — here larger than and different from the new content —
// must come out byte-exact, with the would-be holes reading back as zero (not the
// stale bytes) and the file shrunk to the new logical size.
func TestCopyZstdSparseClearsStaleData(t *testing.T) {
	const size = 2 << 20
	want := make([]byte, size)
	for i := 0; i < 4096; i++ { // first page
		want[i] = byte(i%251 + 1)
	}
	for i := 1 << 20; i < (1<<20)+4096; i++ { // an interior page, rest stays a hole
		want[i] = byte(i%253 + 1)
	}

	out := filepath.Join(t.TempDir(), "dst")
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// Pre-fill with stale non-zero bytes, larger than the new content, so the test
	// also covers shrinking to the exact logical size.
	stale := make([]byte, size+size/2)
	for i := range stale {
		stale[i] = 0xFF
	}
	if _, err := f.Write(stale); err != nil {
		t.Fatal(err)
	}

	if _, _, err := copyZstdSparse(f, bytes.NewReader(want)); err != nil {
		t.Fatalf("copyZstdSparse: %v", err)
	}

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("stale data not cleared / wrong size: len(got)=%d len(want)=%d", len(got), len(want))
	}
}
