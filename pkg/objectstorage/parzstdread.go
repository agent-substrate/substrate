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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstd decodes one frame on one core however many it is given, which capped a
// snapshot download at one core of decompression: on the counter demo, ~95ms to
// decode a 71MB memory image that took ~10ms to fetch. The upload side already
// writes independent frames (parZstd), so parZstdReader splits the compressed
// stream at frame boundaries and decodes the frames concurrently, handing their
// output back in order.
//
// Only frames whose decompressed size is known to be at most parZstdMaxFrame are
// decoded in parallel: each is decoded whole into memory, so the size bounds what
// a worker holds. The size is the one the frame header declares or, when it
// declares none, the frame's block count times the largest a block decodes to. At
// the first frame that does not qualify (one written by a streaming encoder, or a
// huge one), the rest of the stream is decoded serially.
const (
	// parZstdMaxFrame is the largest frame decoded in parallel: twice parZstdChunk,
	// so every frame parZstd writes qualifies.
	parZstdMaxFrame = 2 * parZstdChunk
	// parZstdMaxDecoders caps the decode workers, and with them the memory held in
	// decoded frames (about 3*parZstdMaxDecoders*parZstdMaxFrame at most).
	parZstdMaxDecoders = 8
)

const (
	// zstdMaxBlockOut is the most a single block decodes to (RFC 8878 3.1.1.2.4).
	zstdMaxBlockOut        = 128 << 10
	zstdFrameMagic         = 0xFD2FB528
	zstdSkippableMagicMask = 0xFFFFFFF0
	zstdSkippableMagic     = 0x184D2A50
)

// parZstdReader is an io.ReadCloser of the decompressed contents of a stream of
// concatenated zstd frames.
type parZstdReader struct {
	ordered chan chan parZstdResult
	cur     []byte
	serial  io.Reader
	serialD *zstd.Decoder
	err     error

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// parZstdResult is one frame's output, or the reader to decode the rest of the
// stream from serially (tail), or a split error.
type parZstdResult struct {
	data []byte
	tail io.Reader
	err  error
}

type parZstdDecodeJob struct {
	frame []byte
	// size is the decoded size the frame header declares, or -1.
	size int
	// maxSize bounds the decoded size.
	maxSize int
	out     chan parZstdResult
}

// newParZstdReader starts splitting src into frames and decoding them, on up to
// workers goroutines (capped at GOMAXPROCS and parZstdMaxDecoders).
func newParZstdReader(src io.Reader, workers int) *parZstdReader {
	workers = min(max(workers, 1), runtime.GOMAXPROCS(0), parZstdMaxDecoders)
	r := &parZstdReader{
		ordered: make(chan chan parZstdResult, 2*workers),
		stop:    make(chan struct{}),
	}
	jobs := make(chan parZstdDecodeJob, workers)
	for range workers {
		r.wg.Add(1)
		go r.decodeWorker(jobs)
	}
	r.wg.Add(1)
	go r.split(src, jobs)
	return r
}

func (r *parZstdReader) decodeWorker(jobs <-chan parZstdDecodeJob) {
	defer r.wg.Done()
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(parZstdMaxFrame))
	if err != nil {
		// NewReader only fails on bad options, which are compile-time constants here.
		panic(err)
	}
	defer dec.Close()
	for j := range jobs {
		data, err := dec.DecodeAll(j.frame, make([]byte, 0, j.maxSize))
		if err == nil && j.size >= 0 && len(data) != j.size {
			err = fmt.Errorf("zstd frame decoded to %d bytes, header declared %d", len(data), j.size)
		}
		j.out <- parZstdResult{data: data, err: err}
	}
}

// split reads src one frame at a time and dispatches each, in order. It hands
// the remaining stream to the reader as a tail at the first frame that cannot be
// decoded in parallel.
func (r *parZstdReader) split(src io.Reader, jobs chan<- parZstdDecodeJob) {
	defer r.wg.Done()
	defer close(r.ordered)
	defer close(jobs)
	emit := func(res parZstdResult) bool {
		out := make(chan parZstdResult, 1)
		out <- res
		select {
		case r.ordered <- out:
			return true
		case <-r.stop:
			return false
		}
	}
	for {
		frame, size, maxSize, ok, err := readZstdFrame(src)
		switch {
		case errors.Is(err, io.EOF) && len(frame) == 0:
			return
		case err != nil:
			emit(parZstdResult{err: err})
			return
		case !ok:
			emit(parZstdResult{tail: io.MultiReader(bytes.NewReader(frame), src)})
			return
		case frame == nil:
			continue // skippable frame
		}
		out := make(chan parZstdResult, 1)
		select {
		case r.ordered <- out:
		case <-r.stop:
			return
		}
		select {
		case jobs <- parZstdDecodeJob{frame: frame, size: size, maxSize: maxSize, out: out}:
		case <-r.stop:
			return
		}
	}
}

func (r *parZstdReader) Read(p []byte) (int, error) {
	for len(r.cur) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if r.serial != nil {
			n, err := r.serial.Read(p)
			if err != nil {
				r.err = err
			}
			return n, err
		}
		out, ok := <-r.ordered
		if !ok {
			r.err = io.EOF
			continue
		}
		res := <-out
		switch {
		case res.err != nil:
			r.err = res.err
		case res.tail != nil:
			r.serialD, r.err = zstd.NewReader(res.tail, zstd.WithDecoderConcurrency(1))
			r.serial = r.serialD
		default:
			r.cur = res.data
		}
	}
	n := copy(p, r.cur)
	r.cur = r.cur[n:]
	return n, nil
}

// Close stops the splitter and the workers. It does not close the source. Workers
// never block on a result (each has a buffered channel), so they exit once the
// splitter stops dispatching.
func (r *parZstdReader) Close() error {
	r.stopOnce.Do(func() { close(r.stop) })
	r.wg.Wait()
	if r.serialD != nil {
		r.serialD.Close()
	}
	return nil
}

// readZstdFrame reads the next frame from src. It returns:
//   - the frame bytes, its declared decompressed size (-1 if it declares none)
//     and a bound on it, ok=true, when the frame can be decoded in parallel;
//   - nil, ok=true for a skippable frame (consumed and dropped);
//   - the bytes read so far and ok=false when the frame may decode to more than
//     parZstdMaxFrame: the caller decodes the rest of the stream serially,
//     starting with those bytes.
//
// At a clean end of stream it returns io.EOF with no bytes.
func readZstdFrame(src io.Reader) (frame []byte, size, maxSize int, ok bool, err error) {
	var magic [4]byte
	if n, err := io.ReadFull(src, magic[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, 0, false, io.EOF
		}
		return magic[:n], 0, 0, false, fmt.Errorf("reading zstd frame magic: %w", err)
	}
	m := binary.LittleEndian.Uint32(magic[:])
	if m&zstdSkippableMagicMask == zstdSkippableMagic {
		var sz [4]byte
		if _, err := io.ReadFull(src, sz[:]); err != nil {
			return nil, 0, 0, false, fmt.Errorf("reading skippable frame size: %w", noEOF(err))
		}
		if _, err := io.CopyN(io.Discard, src, int64(binary.LittleEndian.Uint32(sz[:]))); err != nil {
			return nil, 0, 0, false, fmt.Errorf("skipping skippable frame: %w", noEOF(err))
		}
		return nil, 0, 0, true, nil
	}
	if m != zstdFrameMagic {
		return magic[:], 0, 0, false, fmt.Errorf("not a zstd frame (magic %#x)", m)
	}
	buf := bytes.NewBuffer(make([]byte, 0, parZstdChunk/2))
	buf.Write(magic[:])
	read := func(n int) ([]byte, error) {
		start := buf.Len()
		if _, err := io.CopyN(buf, src, int64(n)); err != nil {
			return nil, noEOF(err)
		}
		return buf.Bytes()[start:], nil
	}

	// Frame header (RFC 8878 section 3.1.1.1).
	fhd, err := read(1)
	if err != nil {
		return buf.Bytes(), 0, 0, false, fmt.Errorf("reading zstd frame header: %w", err)
	}
	desc := fhd[0]
	fcsFlag := desc >> 6
	singleSegment := desc&0x20 != 0
	hasChecksum := desc&0x04 != 0
	dictIDSize := [4]int{0, 1, 2, 4}[desc&0x03]
	fcsSize := [4]int{0, 2, 4, 8}[fcsFlag]
	if fcsFlag == 0 && singleSegment {
		fcsSize = 1
	}
	rest := dictIDSize + fcsSize
	if !singleSegment {
		rest++ // window descriptor
	}
	hdr, err := read(rest)
	if err != nil {
		return buf.Bytes(), 0, 0, false, fmt.Errorf("reading zstd frame header: %w", err)
	}
	size = -1
	var fcs uint64
	fcsBytes := hdr[len(hdr)-fcsSize:]
	switch fcsSize {
	case 1:
		fcs = uint64(fcsBytes[0])
	case 2:
		fcs = uint64(binary.LittleEndian.Uint16(fcsBytes)) + 256
	case 4:
		fcs = uint64(binary.LittleEndian.Uint32(fcsBytes))
	case 8:
		fcs = binary.LittleEndian.Uint64(fcsBytes)
	}
	if fcsSize > 0 {
		if fcs > parZstdMaxFrame {
			return buf.Bytes(), 0, 0, false, nil
		}
		size = int(fcs)
	}

	// Blocks (section 3.1.1.2), up to and including the last one.
	for {
		bh, err := read(3)
		if err != nil {
			return nil, 0, 0, false, fmt.Errorf("reading zstd block header: %w", err)
		}
		h := uint32(bh[0]) | uint32(bh[1])<<8 | uint32(bh[2])<<16
		last := h&1 != 0
		n := int(h >> 3)
		switch (h >> 1) & 3 {
		case 1: // RLE: one byte, repeated
			n = 1
		case 3:
			return nil, 0, 0, false, errors.New("reserved zstd block type")
		}
		if n > parZstdMaxFrame {
			return nil, 0, 0, false, fmt.Errorf("zstd block of %d bytes exceeds the frame limit", n)
		}
		if _, err := read(n); err != nil {
			return nil, 0, 0, false, fmt.Errorf("reading zstd block: %w", err)
		}
		maxSize += zstdMaxBlockOut
		if size < 0 && maxSize > parZstdMaxFrame && !last {
			return buf.Bytes(), 0, 0, false, nil
		}
		if last {
			break
		}
	}
	if hasChecksum {
		if _, err := read(4); err != nil {
			return nil, 0, 0, false, fmt.Errorf("reading zstd frame checksum: %w", err)
		}
	}
	if size >= 0 {
		maxSize = size
	}
	return buf.Bytes(), size, maxSize, true, nil
}

// noEOF turns a clean EOF in the middle of a frame into ErrUnexpectedEOF.
func noEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
