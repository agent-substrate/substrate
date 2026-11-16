//go:build linux

// Copyright 2026 The Agent Substrate Authors
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

package tarutil

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// paxHoles is the PAX record that lists a sparse file's holes, as
// comma-separated offset,length pairs in file order. archive/tar can't write
// sparse entries, so the entry still holds zeros for the holes. Extract skips
// writing them, so the file comes back sparse. Other readers ignore the
// record and write the zeros.
const paxHoles = "ATE.holes"

// maxHoles caps the holes recorded for one file, so the record stays far
// below the 1 MiB archive/tar allows for a PAX header. Extract writes any
// further holes as zeros.
const maxHoles = 4096

// hole is a range of a file that has no data.
type hole struct {
	off, length int64
}

// recordHoles adds a paxHoles record to hdr if f has holes. Like GNU tar, it
// looks for them only when info shows fewer blocks allocated than the size.
// SEEK_HOLE can also report fallocated blocks that were never written as
// holes, while none of their pages are cached. So a fully fallocated file
// stays allocated, but in a sparse file such blocks may come back as holes.
func recordHoles(hdr *tar.Header, f *os.File, info os.FileInfo) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Blocks*512 >= hdr.Size {
		return nil
	}
	holes, err := fileHoles(f, hdr.Size)
	if err != nil {
		return err
	}
	if len(holes) == 0 {
		return nil
	}
	if hdr.PAXRecords == nil {
		hdr.PAXRecords = map[string]string{}
	}
	hdr.PAXRecords[paxHoles] = formatHoles(holes)
	return nil
}

// fileHoles returns up to maxHoles holes in the first size bytes of f, as
// SEEK_DATA and SEEK_HOLE report them, and rewinds f.
func fileHoles(f *os.File, size int64) ([]hole, error) {
	var holes []hole
	for off := int64(0); off < size && len(holes) < maxHoles; {
		data, err := f.Seek(off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			// no data at or after off
			data = size
		} else if err != nil {
			return nil, err
		}
		data = min(data, size)
		if data > off {
			holes = append(holes, hole{off: off, length: data - off})
		}
		if data == size {
			break
		}
		end, err := f.Seek(data, unix.SEEK_HOLE)
		if err != nil {
			return nil, err
		}
		// Every pass must move forward, or a broken filesystem would loop here.
		if end <= off {
			return nil, fmt.Errorf("SEEK_HOLE from %d returned %d", data, end)
		}
		off = end
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return holes, nil
}

func formatHoles(holes []hole) string {
	var b []byte
	for i, h := range holes {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendInt(b, h.off, 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, h.length, 10)
	}
	return string(b)
}

// parseHoles returns the holes in hdr's paxHoles record, or nil if it has
// none. The holes must be in order, must not overlap, and must lie inside the
// file.
func parseHoles(hdr *tar.Header) ([]hole, error) {
	rec, ok := hdr.PAXRecords[paxHoles]
	if !ok {
		return nil, nil
	}
	fields := strings.Split(rec, ",")
	if len(fields)%2 != 0 {
		return nil, errors.New("odd number of fields")
	}
	holes := make([]hole, 0, len(fields)/2)
	var end int64
	for i := 0; i < len(fields); i += 2 {
		off, err := strconv.ParseInt(fields[i], 10, 64)
		if err != nil {
			return nil, err
		}
		length, err := strconv.ParseInt(fields[i+1], 10, 64)
		if err != nil {
			return nil, err
		}
		if off < end || length <= 0 || length > hdr.Size-off {
			return nil, fmt.Errorf("invalid hole %d,%d in a %d byte file", off, length, hdr.Size)
		}
		holes = append(holes, hole{off: off, length: length})
		end = off + length
	}
	return holes, nil
}

// copySparse writes size bytes from src to dst, which must be empty. Inside
// holes it skips the chunks that are all zeros, so they stay unallocated. It
// writes any other chunk, so dst always gets exactly the bytes src holds.
func copySparse(dst *os.File, src io.Reader, size int64, holes []hole) error {
	if err := dst.Truncate(size); err != nil {
		return err
	}
	bp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bp)
	for off := int64(0); off < size; {
		n, inHole := min(int64(len(*bp)), size-off), false
		if len(holes) > 0 {
			if h := holes[0]; off < h.off {
				n = min(n, h.off-off)
			} else {
				n, inHole = min(n, h.off+h.length-off), true
			}
		}
		chunk := (*bp)[:n]
		if _, err := io.ReadFull(src, chunk); err != nil {
			return err
		}
		if !inHole || !bytes.Equal(chunk, zeros[:n]) {
			if _, err := dst.WriteAt(chunk, off); err != nil {
				return err
			}
		}
		off += n
		if len(holes) > 0 && off == holes[0].off+holes[0].length {
			holes = holes[1:]
		}
	}
	return nil
}

// zeros is what copySparse compares chunks against, since bytes.Equal is
// vectorized.
var zeros [copyBufSize]byte
