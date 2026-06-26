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
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// region is a run of identical non-zero bytes written at off in a test source.
type region struct {
	off  int64
	len  int64
	fill byte
}

// writeSparseSource creates a file of logical size `size` with `regions` written
// in (the gaps stay zero, becoming holes on a sparse fs — but the round-trip is
// byte-exact regardless of whether the fs actually punches holes).
func writeSparseSource(t *testing.T, path string, size int64, regions []region) {
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
		buf := make([]byte, r.len)
		for i := range buf {
			buf[i] = r.fill
		}
		if _, err := f.WriteAt(buf, r.off); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

// TestSparseZstdRoundTrip checks writeSparseZstd -> readSparseZstd is byte-exact
// across hole/data layouts (the correctness property is fs-independent: whether or
// not the fs makes real holes, the decoded file must equal the source).
func TestSparseZstdRoundTrip(t *testing.T) {
	const M = 1 << 20
	cases := []struct {
		name    string
		size    int64
		regions []region
	}{
		{"empty", 0, nil},
		{"all-hole", 4 * M, nil},
		{"all-data", 256 << 10, []region{{0, 256 << 10, 0xAB}}},
		{"leading-hole", 2 * M, []region{{1 * M, 64 << 10, 0x11}}},
		{"trailing-hole", 2 * M, []region{{0, 64 << 10, 0x22}}},
		{"single-extent-midfile", 4 * M, []region{{2 * M, 70000, 0x33}}},
		{"multi-extent", 8 * M, []region{
			{0, 4096, 0x44}, {2 * M, 100000, 0x55}, {5 * M, 8192, 0x66},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			srcPath := filepath.Join(dir, "src")
			writeSparseSource(t, srcPath, tc.size, tc.regions)
			src, err := os.Open(srcPath)
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()

			var buf bytes.Buffer
			logical, _, err := writeSparseZstd(&buf, src)
			if err != nil {
				t.Fatalf("writeSparseZstd: %v", err)
			}
			if logical != tc.size {
				t.Errorf("writeSparseZstd logical=%d, want %d", logical, tc.size)
			}
			if got := buf.Bytes(); len(got) < len(sparseMagic) || string(got[:len(sparseMagic)]) != sparseMagic {
				t.Fatal("output missing sparse magic")
			}

			dstPath := filepath.Join(dir, "dst")
			dst, err := os.Create(dstPath)
			if err != nil {
				t.Fatal(err)
			}
			defer dst.Close()
			size, err := readSparseZstd(dst, bytes.NewReader(buf.Bytes()[len(sparseMagic):]))
			if err != nil {
				t.Fatalf("readSparseZstd: %v", err)
			}
			if size != tc.size {
				t.Errorf("readSparseZstd size=%d, want %d", size, tc.size)
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
				t.Fatalf("round-trip mismatch (len got=%d want=%d)", len(got), len(want))
			}
		})
	}
}

// craftSparseStream builds a sparse-extent stream (after the magic) with arbitrary
// header values, so tests can feed malformed input to readSparseZstd. Each frame is
// (off, len) followed by len zero bytes; an end sentinel is appended when asked.
func craftSparseStream(t *testing.T, version uint32, size int64, frames [][2]int64, sentinel bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, version); err != nil {
		t.Fatal(err)
	}
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite := func(v int64) {
		if err := binary.Write(zw, binary.LittleEndian, v); err != nil {
			zw.Close()
			t.Fatal(err)
		}
	}
	mustWrite(size)
	for _, f := range frames {
		mustWrite(f[0])
		mustWrite(f[1])
		if f[1] > 0 {
			if _, err := zw.Write(make([]byte, f[1])); err != nil {
				zw.Close()
				t.Fatal(err)
			}
		}
	}
	if sentinel {
		mustWrite(sparseEndOffset)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestReadSparseZstdRejectsMalformed(t *testing.T) {
	cases := []struct {
		name   string
		stream []byte
		want   string
	}{
		{
			name:   "wrong version",
			stream: craftSparseStream(t, sparseVersion+1, 0, nil, true),
			want:   "unsupported sparse snapshot format version",
		},
		{
			name:   "negative size",
			stream: craftSparseStream(t, sparseVersion, -1, nil, true),
			want:   "negative totalSize",
		},
		{
			name:   "extent past end",
			stream: craftSparseStream(t, sparseVersion, 100, [][2]int64{{50, 100}}, true),
			want:   "out of range",
		},
		{
			name:   "negative extent offset (not sentinel)",
			stream: craftSparseStream(t, sparseVersion, 100, [][2]int64{{-5, 1}}, true),
			want:   "out of range",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dst, err := os.Create(filepath.Join(t.TempDir(), "dst"))
			if err != nil {
				t.Fatal(err)
			}
			defer dst.Close()
			_, err = readSparseZstd(dst, bytes.NewReader(tc.stream))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tc.want)
			}
		})
	}
}

// TestReadSparseZstdTruncated confirms a stream cut short (no end sentinel / partial
// extent) is reported as an error rather than silently producing a short file.
func TestReadSparseZstdTruncated(t *testing.T) {
	// A valid stream with one extent but NO end sentinel: the reader should hit EOF
	// looking for the next frame offset.
	stream := craftSparseStream(t, sparseVersion, 1<<20, [][2]int64{{0, 4096}}, false)
	dst, err := os.Create(filepath.Join(t.TempDir(), "dst"))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if _, err := readSparseZstd(dst, bytes.NewReader(stream)); err == nil {
		t.Fatal("expected an error for a stream missing its end sentinel, got nil")
	}
}
