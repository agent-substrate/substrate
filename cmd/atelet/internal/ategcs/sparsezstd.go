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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"
)

// sparseMagic marks the sparse-extent snapshot format (see writeSparseZstd). It is
// 8 bytes and deliberately NOT a valid zstd frame magic, so a reader can tell this
// format from a plain zstd stream (no magic) by the first 8 bytes. The magic is
// version-neutral; the format version follows it (see sparseVersion).
const sparseMagic = "ATESPRSE"

// sparseVersion is the sparse-extent format version, written as a little-endian
// uint32 immediately after sparseMagic (in the clear, before the zstd stream). Bump
// it on any incompatible layout change so readers reject snapshots they don't
// understand instead of misparsing them.
const sparseVersion uint32 = 2

// sparseEndOffset is the end-of-stream sentinel: an extent-frame offset of -1 marks
// the end of the frames (a real extent offset is always >= 0). Using an end marker
// keeps the format streamable — the writer need not know the extent count up front
// and the reader stops when it sees the sentinel.
const sparseEndOffset int64 = -1

// writeSparseZstd encodes a sparse file src to dst in the sparse-extent format:
//
//	magic[8] | version:u32 | zstd( totalSize:i64 | (off:i64, len:i64, data[len])* | -1:i64 )
//
// The magic + version are in the clear so a reader can dispatch on them; everything
// after is a single zstd stream of the metadata interleaved with ONLY the populated
// extents' data (holes are neither read nor compressed), terminated by the
// end-offset sentinel. The extents are discovered and emitted incrementally
// (SEEK_DATA/SEEK_HOLE), so the format is streamable — no extent count is written
// up front.
//
// This is the upload mirror of the sparse DOWNLOAD: a guest memory-ranges image is
// mostly holes (free RAM), so feeding only the real extents to zstd cuts the
// compress from "scan the whole logical image" (e.g. 2GiB) to "scan the resident
// set" (e.g. ~150MiB). Returns the logical size and the populated (pre-compression)
// byte count. All integers are little-endian.
func writeSparseZstd(dst io.Writer, src *os.File) (logical, dataBytes int64, err error) {
	fi, err := src.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := fi.Size()

	// magic + version in the clear (buffered: a couple of tiny writes).
	bw := bufio.NewWriter(dst)
	if _, err := bw.WriteString(sparseMagic); err != nil {
		return 0, 0, err
	}
	if err := binary.Write(bw, binary.LittleEndian, sparseVersion); err != nil {
		return 0, 0, err
	}
	if err := bw.Flush(); err != nil {
		return 0, 0, err
	}

	zw, err := zstd.NewWriter(dst,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return 0, 0, err
	}
	// fail closes the encoder before returning err (Close flushes/frees state).
	fail := func(e error) (int64, int64, error) {
		zw.Close()
		return 0, 0, e
	}
	if err := binary.Write(zw, binary.LittleEndian, size); err != nil {
		return fail(err)
	}

	fd := int(src.Fd())
	off := int64(0)
	for off < size {
		ds, serr := unix.Seek(fd, off, unix.SEEK_DATA)
		if serr != nil {
			if serr == unix.ENXIO { // no more data: the rest is a hole
				break
			}
			return fail(fmt.Errorf("SEEK_DATA: %w", serr))
		}
		de, serr := unix.Seek(fd, ds, unix.SEEK_HOLE)
		if serr != nil {
			return fail(fmt.Errorf("SEEK_HOLE: %w", serr))
		}
		length := de - ds
		if err := binary.Write(zw, binary.LittleEndian, ds); err != nil {
			return fail(err)
		}
		if err := binary.Write(zw, binary.LittleEndian, length); err != nil {
			return fail(err)
		}
		if _, err := src.Seek(ds, io.SeekStart); err != nil {
			return fail(err)
		}
		n, cerr := io.CopyN(zw, src, length)
		dataBytes += n
		if cerr != nil {
			return fail(fmt.Errorf("reading extent @%d+%d: %w", ds, length, cerr))
		}
		off = de
	}
	if err := binary.Write(zw, binary.LittleEndian, sparseEndOffset); err != nil {
		return fail(err)
	}
	if err := zw.Close(); err != nil {
		return 0, 0, err
	}
	return size, dataBytes, nil
}

// readSparseZstd decodes the sparse-extent format into dst, which becomes a sparse
// file (the holes between extents are never written). src must be positioned just
// AFTER the magic (the caller reads + dispatches on it). dst is truncated to the
// logical size so trailing holes + the exact size are represented.
func readSparseZstd(dst *os.File, src io.Reader) (logical int64, err error) {
	var ver uint32
	if err := binary.Read(src, binary.LittleEndian, &ver); err != nil {
		return 0, fmt.Errorf("reading sparse format version: %w", err)
	}
	if ver != sparseVersion {
		return 0, fmt.Errorf("unsupported sparse snapshot format version %d (this build supports %d)", ver, sparseVersion)
	}

	zr, err := zstd.NewReader(src, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return 0, err
	}
	defer zr.Close()

	var size int64
	if err := binary.Read(zr, binary.LittleEndian, &size); err != nil {
		return 0, fmt.Errorf("reading totalSize: %w", err)
	}
	if size < 0 {
		return 0, fmt.Errorf("negative totalSize %d", size)
	}
	if err := dst.Truncate(size); err != nil {
		return 0, err
	}

	// Replay the extent frames written by writeSparseZstd. Each frame is an offset
	// (i64), a length (i64), then that many data bytes; the stream ends with a
	// terminator frame whose offset is sparseEndOffset (-1) and carries no len/data.
	for {
		var off int64
		if err := binary.Read(zr, binary.LittleEndian, &off); err != nil {
			return 0, fmt.Errorf("reading extent offset: %w", err)
		}
		if off == sparseEndOffset {
			break
		}
		var length int64
		if err := binary.Read(zr, binary.LittleEndian, &length); err != nil {
			return 0, fmt.Errorf("reading extent length: %w", err)
		}
		// Validate against the declared size (the stream is the downloaded snapshot):
		// an out-of-range extent would seek/write past the file or wrap on the
		// off+length arithmetic. size-off is safe because off <= size.
		if off < 0 || length < 0 || off > size || length > size-off {
			return 0, fmt.Errorf("sparse extent out of range (off=%d len=%d size=%d)", off, length, size)
		}
		if _, err := dst.Seek(off, io.SeekStart); err != nil {
			return 0, err
		}
		if _, err := io.CopyN(dst, zr, length); err != nil {
			return 0, fmt.Errorf("writing extent @%d+%d: %w", off, length, err)
		}
	}
	return size, nil
}
