//go:build linux

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

package ch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	diffMagic            = "ADIF"
	diffVersion   uint32 = 1
	diffPageSize  int64  = 4096
	diffChunkSize int64  = 256 * 1024
)

// SparseExtent describes a contiguous byte range [Offset, Offset+Length) in a guest memory image.
type SparseExtent struct {
	Offset int64
	Length int64
}

// openSameSizePair opens baseFile for reading and targetFile with targetFlags,
// verifying that both files have identical byte sizes.
func openSameSizePair(baseFile, targetFile string, targetFlags int) (baseF, targetF *os.File, size int64, err error) {
	if baseF, err = os.Open(baseFile); err != nil {
		return nil, nil, 0, err
	}
	if targetF, err = os.OpenFile(targetFile, targetFlags, 0); err != nil {
		baseF.Close()
		return nil, nil, 0, err
	}
	bfi, berr := baseF.Stat()
	tfi, terr := targetF.Stat()
	if err = errors.Join(berr, terr); err == nil && bfi.Size() != tfi.Size() {
		err = fmt.Errorf("size mismatch (%d vs %d)", bfi.Size(), tfi.Size())
	}
	if err != nil {
		baseF.Close()
		targetF.Close()
		return nil, nil, 0, err
	}
	return baseF, targetF, tfi.Size(), nil
}

// seekDataExtent locates the next allocated data extent [start, end) at or after off using SEEK_DATA and SEEK_HOLE.
func seekDataExtent(fd int, off int64) (start, end int64, eof bool, err error) {
	if start, err = unix.Seek(fd, off, unix.SEEK_DATA); err != nil {
		if errors.Is(err, unix.ENXIO) {
			return 0, 0, true, nil
		}
		return 0, 0, false, err
	}
	end, err = unix.Seek(fd, start, unix.SEEK_HOLE)
	return start, end, false, err
}

// scanTargetPages compares 4 KiB pages across allocated extents in targetFile against baseFile
// and invokes visit for each page and at the end of each extent (with flush set to true).
func scanTargetPages(ctx context.Context, baseFile, targetFile string, targetFlags int, visit func(targetFd int, off, pLen int64, page []byte, same, flush bool) error) (int64, error) {
	baseF, targetF, size, err := openSameSizePair(baseFile, targetFile, targetFlags)
	if err != nil {
		return 0, err
	}
	defer baseF.Close()
	defer targetF.Close()

	targetFd := int(targetF.Fd())
	baseBuf, targetBuf := make([]byte, diffChunkSize), make([]byte, diffChunkSize)
	for off := int64(0); off < size; {
		if err := ctx.Err(); err != nil {
			return size, err
		}
		dataStart, dataEnd, eof, err := seekDataExtent(targetFd, off)
		if err != nil || eof {
			return size, err
		}
		for pos := dataStart; pos < dataEnd; {
			if err := ctx.Err(); err != nil {
				return size, err
			}
			n := min(diffChunkSize, dataEnd-pos)
			_, rerr1 := targetF.ReadAt(targetBuf[:n], pos)
			_, rerr2 := baseF.ReadAt(baseBuf[:n], pos)
			if err := errors.Join(rerr1, rerr2); err != nil {
				return size, err
			}
			for p := int64(0); p < n; p += diffPageSize {
				pLen := min(diffPageSize, n-p)
				if err := visit(targetFd, pos+p, pLen, targetBuf[p:p+pLen], bytes.Equal(targetBuf[p:p+pLen], baseBuf[p:p+pLen]), false); err != nil {
					return size, err
				}
			}
			pos += n
		}
		if err := visit(targetFd, dataEnd, 0, nil, false, true); err != nil {
			return size, err
		}
		off = dataEnd
	}
	return size, nil
}

// DeflateAgainstBase punches holes in targetFile wherever 4 KiB pages match baseFile.
// If targetFile shares an inode with another hard link (Nlink > 1), it breaks the link
// first so hole punching does not mutate the other link.
func DeflateAgainstBase(ctx context.Context, baseFile, targetFile string) (punchedBytes, keptBytes int64, err error) {
	if tfi, serr := os.Stat(targetFile); serr == nil {
		if st, ok := tfi.Sys().(*syscall.Stat_t); ok && st.Nlink > 1 {
			tmpFile := targetFile + ".deflate-unlinked"
			_ = os.Remove(tmpFile)
			if cerr := CopySparseFileFast(ctx, targetFile, tmpFile); cerr != nil {
				return 0, 0, cerr
			}
			if rerr := os.Rename(tmpFile, targetFile); rerr != nil {
				_ = os.Remove(tmpFile)
				return 0, 0, rerr
			}
		}
	}
	var punchStart, punchLen int64
	_, err = scanTargetPages(ctx, baseFile, targetFile, os.O_RDWR, func(targetFd int, off, pLen int64, _ []byte, same, flush bool) error {
		if same && !flush {
			if punchLen == 0 {
				punchStart = off
			}
			punchLen += pLen
			return nil
		}
		if punchLen > 0 {
			if err := unix.Fallocate(targetFd, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, punchStart, punchLen); err != nil {
				return err
			}
			punchedBytes += punchLen
			punchLen = 0
		}
		if !flush {
			keptBytes += pLen
		}
		return nil
	})
	return punchedBytes, keptBytes, err
}

// CreateDeltaAgainstBase writes modified 4 KiB pages into metaFile (ADIF v1) and dataFile.
func CreateDeltaAgainstBase(ctx context.Context, baseFile, targetFile, metaFile, dataFile string) (keptBytes int64, err error) {
	dataF, err := os.OpenFile(dataFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = dataF.Close()
		}
		if err != nil {
			_ = os.Remove(dataFile)
			_ = os.Remove(metaFile)
		}
	}()
	dataBw := bufio.NewWriterSize(dataF, 1<<20)
	var extents []SparseExtent

	size, err := scanTargetPages(ctx, baseFile, targetFile, os.O_RDONLY, func(_ int, off, pLen int64, page []byte, same, flush bool) error {
		if flush || same {
			return nil
		}
		if n := len(extents); n > 0 && extents[n-1].Offset+extents[n-1].Length == off {
			extents[n-1].Length += pLen
		} else {
			extents = append(extents, SparseExtent{Offset: off, Length: pLen})
		}
		_, werr := dataBw.Write(page)
		keptBytes += pLen
		return werr
	})
	if err != nil {
		return 0, err
	}
	closed = true
	if err = errors.Join(dataBw.Flush(), dataF.Close()); err != nil {
		return 0, err
	}

	header := make([]byte, 24+len(extents)*16)
	copy(header[0:4], diffMagic)
	binary.LittleEndian.PutUint32(header[4:8], diffVersion)
	binary.LittleEndian.PutUint64(header[8:16], uint64(size))
	binary.LittleEndian.PutUint64(header[16:24], uint64(len(extents)))
	for i, ext := range extents {
		b := 24 + i*16
		binary.LittleEndian.PutUint64(header[b:b+8], uint64(ext.Offset))
		binary.LittleEndian.PutUint64(header[b+8:b+16], uint64(ext.Length))
	}
	return keptBytes, os.WriteFile(metaFile, header, 0o600)
}

// parseDeltaMeta parses and validates an ADIF v1 differential metadata header and its extent table.
func parseDeltaMeta(metaBytes []byte) (targetSize int64, extents []SparseExtent, err error) {
	if len(metaBytes) < 24 || string(metaBytes[0:4]) != diffMagic || binary.LittleEndian.Uint32(metaBytes[4:8]) != diffVersion {
		return 0, nil, fmt.Errorf("invalid delta meta header")
	}
	rawTargetSize := binary.LittleEndian.Uint64(metaBytes[8:16])
	rawNumExtents := binary.LittleEndian.Uint64(metaBytes[16:24])
	payloadLen := len(metaBytes) - 24
	if rawTargetSize > math.MaxInt64 || payloadLen%16 != 0 || rawNumExtents != uint64(payloadLen/16) {
		return 0, nil, fmt.Errorf("corrupt delta meta size (%d bytes for %d extents)", len(metaBytes), rawNumExtents)
	}
	targetSize = int64(rawTargetSize)
	numExtents := payloadLen / 16
	extents = make([]SparseExtent, numExtents)
	for i := range numExtents {
		b := 24 + i*16
		extents[i] = SparseExtent{
			Offset: int64(binary.LittleEndian.Uint64(metaBytes[b : b+8])),
			Length: int64(binary.LittleEndian.Uint64(metaBytes[b+8 : b+16])),
		}
	}
	return targetSize, extents, nil
}

// RestoreFromDelta reconstructs outFile from baseFile + ADIF v1 metaFile and dataFile.
func RestoreFromDelta(ctx context.Context, baseFile, metaFile, dataFile, outFile string) error {
	tmpFile := outFile + ".reconstructed"
	_ = os.Remove(tmpFile)
	if err := CopySparseFileFast(ctx, baseFile, tmpFile); err != nil {
		_ = os.Remove(tmpFile)
		return err
	}
	if _, _, err := OverlayDeltaFilesRecorded(ctx, metaFile, dataFile, tmpFile); err != nil {
		_ = os.Remove(tmpFile)
		return err
	}
	_ = os.Remove(outFile)
	return os.Rename(tmpFile, outFile)
}

// OverlayDeltaFilesRecorded overlays ADIF v1 dirty extents onto dstFile in place.
func OverlayDeltaFilesRecorded(ctx context.Context, metaFile, dataFile, dstFile string) ([]SparseExtent, int64, error) {
	metaBytes, err := os.ReadFile(metaFile)
	if err != nil {
		return nil, 0, err
	}
	targetSize, extents, err := parseDeltaMeta(metaBytes)
	if err != nil {
		return nil, 0, err
	}
	dataF, err := os.Open(dataFile)
	if err != nil {
		return nil, 0, err
	}
	defer dataF.Close()
	dataFi, err := dataF.Stat()
	if err != nil {
		return nil, 0, err
	}
	dstF, err := os.OpenFile(dstFile, os.O_WRONLY, 0)
	if err != nil {
		return nil, 0, err
	}
	defer dstF.Close()
	if fi, err := dstF.Stat(); err != nil || fi.Size() != targetSize {
		return nil, 0, errors.Join(err, fmt.Errorf("dst size mismatch (want %d)", targetSize))
	}

	buf := make([]byte, diffChunkSize)
	var dataPos, totalBytes int64
	for _, ext := range extents {
		if err := ctx.Err(); err != nil {
			return nil, totalBytes, err
		}
		if ext.Offset < 0 || ext.Length < 0 || ext.Offset > targetSize || ext.Length > targetSize-ext.Offset {
			return nil, totalBytes, fmt.Errorf("delta extent out of bounds (off=%d len=%d size=%d)", ext.Offset, ext.Length, targetSize)
		}
		if ext.Length > dataFi.Size()-dataPos {
			return nil, totalBytes, io.ErrUnexpectedEOF
		}
		if err := copyRangeOrReadWrite(dataF, dstF, dataPos, ext.Offset, ext.Length, buf); err != nil {
			return nil, totalBytes, err
		}
		dataPos += ext.Length
		totalBytes += ext.Length
	}
	if dataPos != dataFi.Size() {
		return nil, totalBytes, fmt.Errorf("delta data size mismatch (read %d bytes, file has %d)", dataPos, dataFi.Size())
	}
	return extents, totalBytes, nil
}

// CopySparseFileFast copies a sparse file preserving holes via SEEK_DATA/SEEK_HOLE and copy_file_range.
func CopySparseFileFast(ctx context.Context, src, dst string) error {
	fi, err := os.Stat(src)
	if err != nil {
		return err
	}
	df, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := errors.Join(df.Truncate(fi.Size()), df.Close()); err != nil {
		_ = os.Remove(dst)
		return err
	}
	if _, _, err := OverlaySparseRegionsRecorded(ctx, src, dst); err != nil {
		_ = os.Remove(dst)
		return err
	}
	return nil
}

// OverlaySparseRegionsRecorded overlays populated regions from deltaFile onto dstFile in place.
func OverlaySparseRegionsRecorded(ctx context.Context, deltaFile, dstFile string) ([]SparseExtent, int64, error) {
	df, tf, size, err := openSameSizePair(deltaFile, dstFile, os.O_WRONLY)
	if err != nil {
		return nil, 0, err
	}
	defer df.Close()
	defer tf.Close()

	dfd, buf := int(df.Fd()), make([]byte, diffChunkSize)
	var extents []SparseExtent
	var totalBytes int64
	for off := int64(0); off < size; {
		if err := ctx.Err(); err != nil {
			return nil, totalBytes, err
		}
		dataStart, dataEnd, eof, err := seekDataExtent(dfd, off)
		if err != nil || eof {
			return extents, totalBytes, err
		}
		extLen := dataEnd - dataStart
		extents = append(extents, SparseExtent{Offset: dataStart, Length: extLen})
		totalBytes += extLen
		if err := copyRangeOrReadWrite(df, tf, dataStart, dataStart, extLen, buf); err != nil {
			return nil, totalBytes, err
		}
		off = dataEnd
	}
	return extents, totalBytes, nil
}

// RestoreSparseRegions restores the specified byte extents in dstFile from srcFile in place.
func RestoreSparseRegions(ctx context.Context, srcFile, dstFile string, extents []SparseExtent) error {
	if len(extents) == 0 {
		return nil
	}
	sf, df, size, err := openSameSizePair(srcFile, dstFile, os.O_WRONLY)
	if err != nil {
		return err
	}
	defer sf.Close()
	defer df.Close()

	sfd, buf := int(sf.Fd()), make([]byte, diffChunkSize)
	for _, ext := range extents {
		if err := ctx.Err(); err != nil {
			return err
		}
		if ext.Offset < 0 || ext.Length < 0 || ext.Offset > size || ext.Length > size-ext.Offset {
			return fmt.Errorf("restore extent out of bounds (off=%d len=%d size=%d)", ext.Offset, ext.Length, size)
		}
		for pos, extEnd := ext.Offset, ext.Offset+ext.Length; pos < extEnd; {
			ds, de, eof, serr := seekDataExtent(sfd, pos)
			if serr != nil {
				return serr
			}
			if eof || ds >= extEnd {
				if err := punchOrZero(df, buf, pos, extEnd-pos); err != nil {
					return err
				}
				break
			}
			if ds > pos {
				if err := punchOrZero(df, buf, pos, ds-pos); err != nil {
					return err
				}
				pos = ds
			}
			de = min(de, extEnd)
			if err := copyReadWrite(sf, df, pos, pos, de-pos, buf); err != nil {
				return err
			}
			pos = de
		}
	}
	return nil
}

// punchOrZero punches a sparse hole at [off, off+length) in df, falling back to writing zeros if hole punching is unsupported.
func punchOrZero(df *os.File, scratch []byte, off, length int64) error {
	if length <= 0 {
		return nil
	}
	err := unix.Fallocate(int(df.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, off, length)
	if err == nil || (!errors.Is(err, syscall.EOPNOTSUPP) && !errors.Is(err, syscall.ENOSYS)) {
		return err
	}
	clear(scratch)
	for pos := off; pos < off+length; {
		n := min(int64(len(scratch)), (off+length)-pos)
		if _, err := df.WriteAt(scratch[:n], pos); err != nil {
			return err
		}
		pos += n
	}
	return nil
}

// copyRangeOrReadWrite copies length bytes from srcF at srcOff to dstF at dstOff using copy_file_range with a pread/pwrite fallback.
func copyRangeOrReadWrite(srcF, dstF *os.File, srcOff, dstOff, length int64, buf []byte) error {
	for sfd, dfd, rem := int(srcF.Fd()), int(dstF.Fd()), length; rem > 0; {
		rOff, wOff := srcOff, dstOff
		chunk := int(min(rem, 1<<30))
		n, err := unix.CopyFileRange(sfd, &rOff, dfd, &wOff, chunk, 0)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) {
			return copyReadWrite(srcF, dstF, srcOff, dstOff, rem, buf)
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		srcOff, dstOff, rem = rOff, wOff, rem-int64(n)
	}
	return nil
}

// copyReadWrite copies length bytes from srcF at srcOff to dstF at dstOff using ReadAt and WriteAt.
func copyReadWrite(srcF, dstF *os.File, srcOff, dstOff, length int64, buf []byte) error {
	for rem := length; rem > 0; {
		chunk := min(int64(len(buf)), rem)
		if _, err := srcF.ReadAt(buf[:chunk], srcOff); err != nil {
			return err
		}
		if _, err := dstF.WriteAt(buf[:chunk], dstOff); err != nil {
			return err
		}
		srcOff, dstOff, rem = srcOff+chunk, dstOff+chunk, rem-chunk
	}
	return nil
}
