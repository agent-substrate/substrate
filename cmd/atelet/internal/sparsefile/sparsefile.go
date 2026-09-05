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

// Package sparsefile copies files while preserving holes.
//
// The biggest thing atelet copies is a guest memory image, which is mostly
// unallocated: a plain io.Copy reads holes as zeroes and writes them as
// data, inflating a snapshot to its full logical size. That costs disk on
// every copy, and it destroys the sparseness later stages rely on to tell
// which parts of guest RAM actually hold anything.
package sparsefile

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// createDestFile is a test seam for CopyFile's destination.
var createDestFile = func(name string) (io.WriteCloser, error) { return os.Create(name) }

// sparseDest is the part of *os.File a hole-preserving copy needs. Destinations that
// do not implement it are copied densely instead.
type sparseDest interface {
	Truncate(size int64) error
	WriteAt(b []byte, off int64) (int, error)
}

// errSparseUnsupported means the source filesystem cannot report holes, so the caller
// should fall back to a dense copy.
var errSparseUnsupported = errors.New("filesystem cannot report holes")

// errKernelCopyUnsupported means this platform, kernel or filesystem cannot copy a
// range in the kernel, so the caller should copy through userspace instead.
var errKernelCopyUnsupported = errors.New("kernel range copy unsupported")

// CopyFile copies src to dst, preserving holes where it can, and returns the number of
// logical bytes copied. dst is created (truncated if it exists), like os.Create.
func CopyFile(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := createDestFile(dst)
	if err != nil {
		return 0, err
	}

	n, err := copyTo(source, destination, sourceFileStat.Size())
	return n, errors.Join(err, destination.Close())
}

// Copy copies the open handle src into the open handle dst, preserving holes
// where it can, and returns the number of logical bytes copied. Neither
// handle is closed, and both handles' offsets are clobbered. It exists for
// callers that must control how the endpoints are opened — e.g. a source
// opened before its directory entry could vanish, or a destination created
// with O_EXCL.
func Copy(src, dst *os.File) (int64, error) {
	fi, err := src.Stat()
	if err != nil {
		return 0, err
	}
	if !fi.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src.Name())
	}
	return copyTo(src, dst, fi.Size())
}

// copyTo is the shared engine: a hole-preserving copy when both ends allow
// it, a dense copy otherwise.
func copyTo(source *os.File, destination io.Writer, size int64) (int64, error) {
	if sd, ok := destination.(sparseDest); ok {
		switch err := copySparse(source, sd, size); {
		case err == nil:
			return size, nil
		case !errors.Is(err, errSparseUnsupported):
			return 0, err
		}
		// Unsupported: nothing has been written yet, but probing moved the read
		// offset, so rewind before the dense copy below.
		if _, err := source.Seek(0, io.SeekStart); err != nil {
			return 0, err
		}
	}
	return io.Copy(destination, source)
}

// copySparse writes only src's populated extents to dst, located with SEEK_DATA and
// SEEK_HOLE, leaving the rest of dst unallocated. It reports errSparseUnsupported
// before writing anything if the filesystem cannot report holes.
//
// Extents are copied in the kernel where possible. The dense io.Copy this replaces got
// that for free (os.File's ReadFrom uses copy_file_range), so without it a fully
// populated file — a guest that really did touch all its RAM — would copy slower than
// before.
func copySparse(src *os.File, dst sparseDest, size int64) error {
	fd := int(src.Fd())

	// Probe first so an unsupported filesystem falls back with dst untouched. ENXIO
	// means the seek ran but found no data at all, i.e. the file is one big hole.
	if _, err := unix.Seek(fd, 0, unix.SEEK_DATA); err != nil {
		if errors.Is(err, unix.ENXIO) {
			return dst.Truncate(size)
		}
		return errSparseUnsupported
	}
	if err := dst.Truncate(size); err != nil {
		return err
	}

	// A destination that exposes its descriptor can be written by the kernel; anything
	// else (the test seam substitutes plain writers) goes through userspace.
	dstFd := -1
	if f, ok := dst.(interface{ Fd() uintptr }); ok {
		dstFd = int(f.Fd())
	}
	var buf []byte

	for off := int64(0); off < size; {
		dataOff, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				break // no data past off; the tail is a hole
			}
			return fmt.Errorf("seeking to data at %d: %w", off, err)
		}
		if dataOff >= size {
			break // data starts past the size we were asked to copy
		}
		holeOff, err := unix.Seek(fd, dataOff, unix.SEEK_HOLE)
		if err != nil {
			return fmt.Errorf("seeking to hole at %d: %w", dataOff, err)
		}
		// Refuse to spin: every iteration must move off forward, which a
		// filesystem reporting a hole at or before where we started would not.
		if holeOff <= off {
			return fmt.Errorf("seeking to hole at %d returned non-advancing offset %d", dataOff, holeOff)
		}
		if holeOff > size {
			holeOff = size
		}
		for pos := dataOff; pos < holeOff; {
			if dstFd >= 0 {
				copied, err := kernelCopyRange(fd, dstFd, pos, holeOff-pos)
				if err == nil {
					pos += copied
					continue
				}
				if !errors.Is(err, errKernelCopyUnsupported) {
					return fmt.Errorf("copying %d bytes at %d: %w", holeOff-pos, pos, err)
				}
				// Give up on the kernel path for the rest of this file, but redo
				// this chunk below: nothing was copied.
				dstFd = -1
			}
			if buf == nil {
				buf = make([]byte, 4<<20)
			}
			n := int64(len(buf))
			if rem := holeOff - pos; rem < n {
				n = rem
			}
			if _, err := src.ReadAt(buf[:n], pos); err != nil {
				return fmt.Errorf("reading %d bytes at %d: %w", n, pos, err)
			}
			if _, err := dst.WriteAt(buf[:n], pos); err != nil {
				return fmt.Errorf("writing %d bytes at %d: %w", n, pos, err)
			}
			pos += n
		}
		off = holeOff
	}
	return nil
}
