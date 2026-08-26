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
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// Sizing for the range-parallel sparse upload. Measured on a GKE worker node writing
// 64 MiB objects to GCS: one stream 87 MB/s, 4 streams 254, 8 streams 518, 12 streams
// 622 — so the node is nowhere near a ceiling and what limits a snapshot upload is how
// early the streams can all start.
const (
	// sparsePartTarget is the populated source bytes per part. Parts smaller than this
	// stop amortizing the ~0.25s a part upload costs before it transfers anything.
	sparsePartTarget = 64 << 20
	// sparseMaxParts caps the fan-out: past a dozen streams per-stream throughput falls
	// faster than the extra parallelism gains.
	sparseMaxParts = 12
)

// extentRange is a populated byte range of the source file.
type extentRange struct{ off, length int64 }

// sparseExtents lists the populated extents of src via SEEK_DATA/SEEK_HOLE, and their
// total size. A guest memory image is mostly holes, so this is what actually has to be
// compressed and uploaded.
func sparseExtents(src *os.File, size int64) (exts []extentRange, populated int64, err error) {
	fd := int(src.Fd())
	for off := int64(0); off < size; {
		ds, serr := unix.Seek(fd, off, unix.SEEK_DATA)
		if serr != nil {
			if serr == unix.ENXIO { // the rest of the file is a hole
				break
			}
			return nil, 0, fmt.Errorf("SEEK_DATA: %w", serr)
		}
		de, serr := unix.Seek(fd, ds, unix.SEEK_HOLE)
		if serr != nil {
			return nil, 0, fmt.Errorf("SEEK_HOLE: %w", serr)
		}
		exts = append(exts, extentRange{off: ds, length: de - ds})
		populated += de - ds
		off = de
	}
	return exts, populated, nil
}

// planSparseParts splits extents into n groups of roughly equal populated bytes,
// splitting an extent across a boundary when that balances better. Groups keep file
// order, so concatenating the parts reproduces the single-stream byte order.
func planSparseParts(exts []extentRange, populated int64, n int) [][]extentRange {
	if n < 1 {
		n = 1
	}
	per := populated / int64(n)
	if per < 1 {
		per = 1
	}
	groups := make([][]extentRange, 0, n)
	cur := []extentRange{}
	var curBytes int64
	for _, e := range exts {
		for e.length > 0 {
			room := per - curBytes
			// The last group takes whatever is left rather than starting an n+1th.
			if len(groups) == n-1 {
				room = e.length
			}
			take := min(room, e.length)
			cur = append(cur, extentRange{off: e.off, length: take})
			curBytes += take
			e.off += take
			e.length -= take
			if curBytes >= per && len(groups) < n-1 {
				groups = append(groups, cur)
				cur, curBytes = []extentRange{}, 0
			}
		}
	}
	if len(cur) > 0 {
		groups = append(groups, cur)
	}
	return groups
}

// sparsePartCount picks how many parts to split a snapshot into.
func sparsePartCount(populated int64) int {
	n := int(populated / sparsePartTarget)
	if n < 1 {
		n = 1
	}
	return min(n, sparseMaxParts)
}

// writeSparsePart writes one part's share of the sparse-extent stream: the header and
// totalSize when it is the first part, its own extents, and the end sentinel when it is
// the last. Reads use ReadAt so every part can read the same file concurrently.
func writeSparsePart(dst io.Writer, src *os.File, size int64, ranges []extentRange, first, last bool) error {
	if first {
		if _, err := io.WriteString(dst, sparseMagic); err != nil {
			return err
		}
		if err := binary.Write(dst, binary.LittleEndian, sparseVersion); err != nil {
			return err
		}
	}
	zw := newSerialZstd(dst)
	if first {
		if err := binary.Write(zw, binary.LittleEndian, size); err != nil {
			zw.Close()
			return err
		}
	}
	buf := make([]byte, 1<<20)
	for _, e := range ranges {
		if err := binary.Write(zw, binary.LittleEndian, e.off); err != nil {
			zw.Close()
			return err
		}
		if err := binary.Write(zw, binary.LittleEndian, e.length); err != nil {
			zw.Close()
			return err
		}
		for at, end := e.off, e.off+e.length; at < end; {
			n := min(int64(len(buf)), end-at)
			read, err := src.ReadAt(buf[:n], at)
			if read > 0 {
				if _, werr := zw.Write(buf[:read]); werr != nil {
					zw.Close()
					return werr
				}
				at += int64(read)
			}
			if err != nil {
				zw.Close()
				return fmt.Errorf("reading extent @%d+%d: %w", e.off, e.length, err)
			}
		}
	}
	if last {
		if err := binary.Write(zw, binary.LittleEndian, sparseEndOffset); err != nil {
			zw.Close()
			return err
		}
	}
	return zw.Close()
}
