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

package objectstorage

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func randomBytes(seed int64, n int) []byte {
	b := make([]byte, n)
	rng := rand.New(rand.NewSource(seed))
	fillSnapshotData(rng, b)
	return b
}

// writeInPieces feeds data to w in uneven writes, so chunk boundaries fall inside them.
func writeInPieces(t *testing.T, w io.Writer, data []byte) {
	t.Helper()
	rng := rand.New(rand.NewSource(int64(len(data))))
	for len(data) > 0 {
		n := min(len(data), 1+rng.Intn(3<<20))
		if _, err := w.Write(data[:n]); err != nil {
			t.Fatal(err)
		}
		data = data[n:]
	}
}

func zstdDecodeAll(t *testing.T, stream []byte) []byte {
	t.Helper()
	zr, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	out, err := zr.DecodeAll(stream, nil)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// withGOMAXPROCS runs the rest of the test with at least n Ps, so newParZstd(w, n)
// really starts n workers instead of falling back to inline mode on a small runner.
func withGOMAXPROCS(t *testing.T, n int) {
	t.Helper()
	if prev := runtime.GOMAXPROCS(0); prev < n {
		runtime.GOMAXPROCS(n)
		t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	}
}

// TestParZstdInlineWritesSizedFrames checks parZstd's inline mode (one worker)
// round-trips and cuts its output into frames of at most parZstdChunk that each
// declare their size, which is what lets a restore decode them in parallel.
func TestParZstdInlineWritesSizedFrames(t *testing.T) {
	for _, n := range []int{0, 1, parZstdChunk - 1, parZstdChunk, parZstdChunk + 1, 3*parZstdChunk + 4321} {
		data := randomBytes(int64(n), n)
		var out bytes.Buffer
		zw := newParZstd(&out, 1)
		if !zw.inline {
			t.Fatal("newParZstd(w, 1) is not inline")
		}
		writeInPieces(t, zw, data)
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		if got := zstdDecodeAll(t, out.Bytes()); !bytes.Equal(got, data) {
			t.Fatalf("n=%d: round trip differs (%d vs %d bytes)", n, len(got), len(data))
		}
		frames, sized, maxSize := zstdFrameStats(t, out.Bytes())
		if want := (n + parZstdChunk - 1) / parZstdChunk; frames != want {
			t.Errorf("n=%d: %d frames, want %d", n, frames, want)
		}
		if sized != frames {
			t.Errorf("n=%d: %d of %d frames declare their size", n, sized, frames)
		}
		if maxSize > parZstdChunk {
			t.Errorf("n=%d: frame of %d bytes, want at most %d", n, maxSize, parZstdChunk)
		}
	}
}

// TestParZstdMatchesChunkedEncodeAll pins parZstd's output, in both modes, to
// EncodeAll of each parZstdChunk of input with newChunkEncoder, concatenated: the
// sized-frame format parallel restore relies on.
func TestParZstdMatchesChunkedEncodeAll(t *testing.T) {
	withGOMAXPROCS(t, 2)
	data := randomBytes(7, 2*parZstdChunk+parZstdChunk/2+123)
	enc := newChunkEncoder()
	defer enc.Close()
	var want []byte
	for rest := data; len(rest) > 0; {
		n := min(len(rest), parZstdChunk)
		want = enc.EncodeAll(rest[:n], want)
		rest = rest[n:]
	}
	for _, workers := range []int{1, 2} {
		var out bytes.Buffer
		zw := newParZstd(&out, workers)
		writeInPieces(t, zw, data)
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out.Bytes(), want) {
			t.Errorf("workers=%d: output differs from chunked EncodeAll (%d vs %d bytes)", workers, out.Len(), len(want))
		}
	}
}

// TestPooledEncodersDoNotLeakData runs uploads back to back and side by side with
// different data through both modes. Pooled buffers are reused, so a later upload
// must never carry bytes from an earlier one.
func TestPooledEncodersDoNotLeakData(t *testing.T) {
	withGOMAXPROCS(t, 2)
	encoders := map[string]func(io.Writer) io.WriteCloser{
		"inline": func(w io.Writer) io.WriteCloser { return newParZstd(w, 1) },
		"par":    func(w io.Writer) io.WriteCloser { return newParZstd(w, 2) },
	}
	for name, newEnc := range encoders {
		t.Run(name, func(t *testing.T) {
			upload := func(t *testing.T, seed int64, n int) {
				data := randomBytes(seed, n)
				var out bytes.Buffer
				zw := newEnc(&out)
				writeInPieces(t, zw, data)
				if err := zw.Close(); err != nil {
					t.Error(err)
					return
				}
				if got := zstdDecodeAll(t, out.Bytes()); !bytes.Equal(got, data) {
					t.Errorf("seed %d: decoded %d bytes, want %d matching the input", seed, len(got), len(data))
				}
			}
			// A big upload fills the pooled buffers; a smaller one after it must not
			// pick up the rest of them.
			upload(t, 1, 3*parZstdChunk+parZstdChunk/2)
			upload(t, 2, parZstdChunk+parZstdChunk/4)
			upload(t, 3, 100)
			var wg sync.WaitGroup
			for i := range 6 {
				wg.Go(func() { upload(t, int64(10+i), (i+1)*parZstdChunk/2) })
			}
			wg.Wait()
		})
	}
}

type failAfter struct {
	n   int
	err error
}

func (f *failAfter) Write(p []byte) (int, error) {
	if f.n == 0 {
		return 0, f.err
	}
	f.n--
	return len(p), nil
}

// TestParZstdInlineReportsWriteErrors checks a failing destination stops the inline
// encoder and surfaces from Write and Close, and that Close is safe to call twice.
func TestParZstdInlineReportsWriteErrors(t *testing.T) {
	boom := errors.New("boom")
	zw := newParZstd(&failAfter{n: 1, err: boom}, 1)
	data := randomBytes(5, 3*parZstdChunk)
	var werr error
	for len(data) > 0 && werr == nil {
		n := min(len(data), 1<<20)
		_, werr = zw.Write(data[:n])
		data = data[n:]
	}
	if !errors.Is(werr, boom) {
		t.Errorf("Write error = %v, want %v", werr, boom)
	}
	if err := zw.Close(); !errors.Is(err, boom) {
		t.Errorf("Close error = %v, want %v", err, boom)
	}
	if err := zw.Close(); !errors.Is(err, boom) {
		t.Errorf("second Close error = %v, want %v", err, boom)
	}
}

// TestWriteSparseZstdOneWorkerDstFails checks the streamed path with one (inline)
// worker returns the destination's error, whether it comes from Close (a tail-only
// stream) or from Write (a full chunk, after which fail() closes the encoder). The
// 9 MiB source is random, so its first frame is a full-size incompressible chunk.
func TestWriteSparseZstdOneWorkerDstFails(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name string
		size int
	}{
		{"tail_4KiB", 4 << 10},
		{"chunk_9MiB", 9 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)
			rand.New(rand.NewSource(int64(tc.size))).Read(data)
			path := filepath.Join(t.TempDir(), "src")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			src, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			// The first write (magic + version) succeeds; the first frame fails.
			if _, _, err := writeSparseZstd(t.Context(), &failAfter{n: 1, err: boom}, src, 1); !errors.Is(err, boom) {
				t.Errorf("writeSparseZstd error = %v, want %v", err, boom)
			}
		})
	}
}

// TestSparsePartsWriteSizedFrames checks the upload parts are made of sized frames
// of at most parZstdChunk, several per part, rather than one streaming frame each.
func TestSparsePartsWriteSizedFrames(t *testing.T) {
	const logical, populated, extent = 80 << 20, 40 << 20, 4 << 20
	src := writeSnapshotSource(t, t.TempDir(), logical, populated, extent)
	exts, pop, err := sparseExtents(src, logical)
	if err != nil {
		t.Fatal(err)
	}
	groups := planSparseParts(exts, pop, 2)
	frames, sized, maxSize := zstdFrameStats(t, encodeSnapshot(t, src, logical, groups))
	if frames <= len(groups) {
		t.Errorf("%d frames for %d parts of %d MiB, want several per part", frames, len(groups), pop>>20/int64(len(groups)))
	}
	if sized != frames {
		t.Errorf("%d of %d frames declare their size", sized, frames)
	}
	if maxSize > parZstdChunk {
		t.Errorf("frame of %d bytes, want at most %d", maxSize, parZstdChunk)
	}
}

// TestWriteAfterClose checks Write and a second Close fail fast with os.ErrClosed in
// both modes, rather than spinning on (inline) or panicking over (workers) the
// released state.
func TestWriteAfterClose(t *testing.T) {
	withGOMAXPROCS(t, 2)
	for _, workers := range []int{1, 2} {
		zw := newParZstd(io.Discard, workers)
		if got := zw.inline; got != (workers == 1) {
			t.Fatalf("workers=%d: inline = %v", workers, got)
		}
		if _, err := zw.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := zw.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
			t.Errorf("workers=%d: Write after Close = %v, want os.ErrClosed", workers, err)
		}
		if err := zw.Close(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("workers=%d: second Close = %v, want os.ErrClosed", workers, err)
		}
	}
}
