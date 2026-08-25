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
	"io"
	"math/rand"
	"os"
	"testing"
)

// TestSparsePartsRoundTrip checks the range-split upload: the parts concatenated must
// decode to exactly the source file, for any number of parts.
func TestSparsePartsRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name   string
		size   int64
		writes []int64 // offsets of 1 MiB populated extents
		parts  int
	}{
		{name: "one part", size: 8 << 20, writes: []int64{0, 4 << 20}, parts: 1},
		{name: "two parts", size: 8 << 20, writes: []int64{0, 4 << 20}, parts: 2},
		{name: "more parts than extents", size: 16 << 20, writes: []int64{1 << 20, 9 << 20}, parts: 5},
		{name: "leading and trailing holes", size: 32 << 20, writes: []int64{8 << 20}, parts: 3},
		{name: "adjacent extents", size: 16 << 20, writes: []int64{0, 1 << 20, 2 << 20}, parts: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := sparseFileWith(t, tc.size, tc.writes)
			exts, populated, err := sparseExtents(src, tc.size)
			if err != nil {
				t.Fatal(err)
			}
			if want := int64(len(tc.writes)) << 20; populated < want {
				t.Fatalf("populated = %d, want at least %d", populated, want)
			}

			groups := planSparseParts(exts, populated, tc.parts)
			var got bytes.Buffer
			for i, g := range groups {
				if err := writeSparsePart(&got, src, tc.size, g, i == 0, i == len(groups)-1); err != nil {
					t.Fatalf("part %d: %v", i, err)
				}
			}

			// The parts must land the same bytes as the single-stream writer.
			var want bytes.Buffer
			if _, _, err := writeSparseZstd(&want, src); err != nil {
				t.Fatal(err)
			}
			if g, w := decodeSparse(t, &got), decodeSparse(t, &want); !bytes.Equal(g, w) {
				t.Errorf("range-split output differs from single stream (%d vs %d bytes)", len(g), len(w))
			}
		})
	}
}

// TestPlanSparsePartsBalances checks every populated byte lands in exactly one part,
// in file order.
func TestPlanSparsePartsBalances(t *testing.T) {
	exts := []extentRange{{off: 0, length: 10}, {off: 100, length: 30}, {off: 500, length: 60}}
	const populated = 100
	for n := 1; n <= 8; n++ {
		groups := planSparseParts(exts, populated, n)
		var total int64
		var last int64 = -1
		for _, g := range groups {
			for _, r := range g {
				if r.off < last {
					t.Fatalf("n=%d: ranges out of order at %d after %d", n, r.off, last)
				}
				last = r.off
				total += r.length
			}
		}
		if total != populated {
			t.Errorf("n=%d: covered %d bytes, want %d", n, total, populated)
		}
		if len(groups) > n {
			t.Errorf("n=%d: got %d groups", n, len(groups))
		}
	}
}

// sparseFileWith builds a sparse file of size with 1 MiB of random data at each offset.
func sparseFileWith(t *testing.T, size int64, offsets []int64) *os.File {
	t.Helper()
	f, err := os.CreateTemp("", "sparseparts-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(f.Name()); f.Close() })
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(7))
	buf := make([]byte, 1<<20)
	for _, off := range offsets {
		rng.Read(buf)
		if _, err := f.WriteAt(buf, off); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	return f
}

// decodeSparse expands an encoded sparse stream back to the full file contents.
func decodeSparse(t *testing.T, r io.Reader) []byte {
	t.Helper()
	magic := make([]byte, len(sparseMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		t.Fatal(err)
	}
	if string(magic) != sparseMagic {
		t.Fatalf("bad magic %q", magic)
	}
	out, err := os.CreateTemp("", "sparsedecode-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(out.Name())
	defer out.Close()
	if _, err := readSparseZstd(out, r); err != nil {
		t.Fatal(err)
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(out)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
