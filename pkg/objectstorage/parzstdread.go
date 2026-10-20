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
	"slices"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstd decodes a frame on one core. parZstdReader splits a stream of concatenated
// frames, as parZstd writes, at frame boundaries, decodes the frames concurrently
// and returns their output in order.
//
// Each frame is decoded whole into memory, so a frame is decoded in parallel only
// when it decodes to at most parZstdMaxFrame and its window is no larger. The
// decoded size is bounded by the size the header declares, and by the frame's
// block count times zstdMaxBlockOut. From the first frame that does not qualify
// (one written by a streaming encoder, or a huge one), the rest of the stream is
// decoded serially.
//
// A frame holds a decode token from when the splitter starts reading it until its
// output has been read. Each reader has one token of its own, so it always makes
// progress, and takes any others from parZstdDecodeTokens, which all readers
// share. Across concurrent downloads, this bounds the frames in memory, and the
// decode workers busy, to parZstdDecoders plus one per reader.
const (
	// parZstdMaxFrame is the largest frame decoded in parallel: twice parZstdChunk,
	// so every frame parZstd writes qualifies.
	parZstdMaxFrame = 2 * parZstdChunk
	// parZstdDecoders is the number of shared decode tokens, and the most decode
	// workers one reader starts. More did not decode any faster into a file.
	parZstdDecoders = 4
)

var parZstdDecodeTokens = make(chan struct{}, parZstdDecoders)

const (
	// zstdMaxBlockOut is the most a single block decodes to, and the largest a
	// block may be (RFC 8878 3.1.1.2.4).
	zstdMaxBlockOut        = 128 << 10
	zstdFrameMagic         = 0xFD2FB528
	zstdSkippableMagicMask = 0xFFFFFFF0
	zstdSkippableMagic     = 0x184D2A50
)

// parZstdReader is an io.ReadCloser of the decompressed contents of a stream of
// concatenated zstd frames. Close must be called to return its decode tokens.
type parZstdReader struct {
	ordered chan chan parZstdResult
	// own is the reader's own decode token.
	own     chan struct{}
	held    parZstdResult
	cur     []byte
	serial  io.Reader
	serialD *zstd.Decoder
	err     error

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// parZstdResult is one frame's output, or the reader to decode the rest of the
// stream from serially (tail), or a split error. Each holds a decode token.
type parZstdResult struct {
	data []byte
	tail io.Reader
	err  error
	// sem is the channel the frame's decode token came from, or nil.
	sem chan struct{}
}

// release returns the result's decode token and output buffer.
func (res parZstdResult) release() {
	if res.sem != nil {
		<-res.sem
	}
	putChunk(res.data)
}

type parZstdDecodeJob struct {
	frame []byte
	// size is the decoded size the frame header declares, or -1.
	size int
	// maxSize bounds the decoded size.
	maxSize int
	sem     chan struct{}
	out     chan parZstdResult
}

// newParZstdReader starts splitting src into frames and decoding them, on up to
// min(GOMAXPROCS, parZstdDecoders) goroutines.
func newParZstdReader(src io.Reader) *parZstdReader {
	workers := min(runtime.GOMAXPROCS(0), parZstdDecoders)
	// The splitter blocks on ordered, so a reader holds at most workers+1 frames:
	// one being read from src, and workers dispatched but not yet read by the
	// caller. Sends to jobs never block.
	r := &parZstdReader{
		ordered: make(chan chan parZstdResult, workers-1),
		own:     make(chan struct{}, 1),
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
		res := parZstdResult{sem: j.sem}
		select {
		case <-r.stop:
			// Close discards the output.
		default:
			res.data, res.err = dec.DecodeAll(j.frame, getBuf(j.maxSize, parZstdChunk, getChunk))
			if res.err == nil && j.size >= 0 && len(res.data) != j.size {
				res.err = fmt.Errorf("zstd frame decoded to %d bytes, header declared %d", len(res.data), j.size)
			}
		}
		putFrame(j.frame)
		j.out <- res
	}
}

// acquire takes a decode token for the next frame: the reader's own if it is
// free, else a shared one. It returns nil once the reader is closed.
func (r *parZstdReader) acquire() chan struct{} {
	select {
	case r.own <- struct{}{}:
		return r.own
	default:
	}
	select {
	case r.own <- struct{}{}:
		return r.own
	case parZstdDecodeTokens <- struct{}{}:
		return parZstdDecodeTokens
	case <-r.stop:
		return nil
	}
}

// split reads src one frame at a time and dispatches each, in order. At the first
// frame that cannot be decoded in parallel, it hands the rest of the stream to
// the reader as a tail.
func (r *parZstdReader) split(src io.Reader, jobs chan<- parZstdDecodeJob) {
	defer r.wg.Done()
	defer close(r.ordered)
	defer close(jobs)
	for {
		sem := r.acquire()
		if sem == nil {
			return
		}
		frame, size, maxSize, ok, err := readZstdFrame(src)
		res := parZstdResult{sem: sem}
		switch {
		case err == io.EOF:
			<-sem
			return
		case err != nil:
			res.err = err
		case !ok:
			res.tail = io.MultiReader(bytes.NewReader(frame), src)
		}
		out := make(chan parZstdResult, 1)
		select {
		case r.ordered <- out:
		case <-r.stop:
			<-sem
			putFrame(frame)
			return
		}
		if res.err != nil || res.tail != nil {
			out <- res
			return
		}
		jobs <- parZstdDecodeJob{frame: frame, size: size, maxSize: maxSize, sem: sem, out: out}
	}
}

func (r *parZstdReader) Read(p []byte) (int, error) {
	for len(r.cur) == 0 {
		r.held.release()
		r.held = parZstdResult{}
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
		r.held = <-out
		switch {
		case r.held.err != nil:
			r.err = r.held.err
		case r.held.tail != nil:
			r.serialD, r.err = zstd.NewReader(r.held.tail, zstd.WithDecoderConcurrency(1))
			r.serial = r.serialD
		default:
			r.cur = r.held.data
		}
	}
	n := copy(p, r.cur)
	r.cur = r.cur[n:]
	return n, nil
}

// Close stops the splitter and the workers, and returns the decode tokens of
// frames not yet read. It waits for the splitter to finish reading the frame it
// is on from src, but does not close src.
func (r *parZstdReader) Close() error {
	r.stopOnce.Do(func() { close(r.stop) })
	r.wg.Wait()
	r.held.release()
	r.held = parZstdResult{}
	r.cur = nil
	r.err = zstd.ErrDecoderClosed
	// Every job has a result by now: the workers drained jobs before exiting.
	for out := range r.ordered {
		(<-out).release()
	}
	if r.serialD != nil {
		r.serialD.Close()
		r.serialD = nil
	}
	return nil
}

// readZstdFrame reads the next frame from src, skipping skippable frames. It
// returns:
//   - the frame, the decoded size its header declares (-1 if none) and a bound
//     on the decoded size, with ok=true, when the frame can be decoded in
//     parallel;
//   - the bytes read so far, with ok=false, when it cannot: the caller decodes
//     the rest of the stream serially, starting with those bytes.
//
// At a clean end of stream it returns io.EOF and no bytes.
func readZstdFrame(src io.Reader) (frame []byte, size, maxSize int, ok bool, err error) {
	var hdr [18]byte // magic, then the frame header (RFC 8878 3.1.1.1)
	for {
		if _, err := io.ReadFull(src, hdr[:4]); err != nil {
			if err == io.EOF {
				return nil, 0, 0, false, io.EOF
			}
			return nil, 0, 0, false, fmt.Errorf("reading zstd frame magic: %w", err)
		}
		m := binary.LittleEndian.Uint32(hdr[:4])
		if m == zstdFrameMagic {
			break
		}
		if m&zstdSkippableMagicMask != zstdSkippableMagic {
			return nil, 0, 0, false, fmt.Errorf("not a zstd frame (magic %#x)", m)
		}
		if _, err := io.ReadFull(src, hdr[4:8]); err != nil {
			return nil, 0, 0, false, fmt.Errorf("reading skippable frame size: %w", noEOF(err))
		}
		if _, err := io.CopyN(io.Discard, src, int64(binary.LittleEndian.Uint32(hdr[4:8]))); err != nil {
			return nil, 0, 0, false, fmt.Errorf("skipping skippable frame: %w", noEOF(err))
		}
	}

	if _, err := io.ReadFull(src, hdr[4:5]); err != nil {
		return nil, 0, 0, false, fmt.Errorf("reading zstd frame header: %w", noEOF(err))
	}
	desc := hdr[4]
	fcsFlag := desc >> 6
	singleSegment := desc&0x20 != 0
	hasChecksum := desc&0x04 != 0
	dictIDSize := [4]int{0, 1, 2, 4}[desc&0x03]
	fcsSize := [4]int{0, 2, 4, 8}[fcsFlag]
	if fcsFlag == 0 && singleSegment {
		fcsSize = 1
	}
	hdrLen := 5 + dictIDSize + fcsSize
	if !singleSegment {
		hdrLen++ // window descriptor
	}
	if _, err := io.ReadFull(src, hdr[5:hdrLen]); err != nil {
		return nil, 0, 0, false, fmt.Errorf("reading zstd frame header: %w", noEOF(err))
	}
	if !singleSegment {
		// A single-segment frame's window is its declared size, checked below.
		wd := hdr[5]
		base := uint64(1) << (10 + wd>>3)
		if base+base/8*uint64(wd&7) > parZstdMaxFrame {
			return slices.Clone(hdr[:hdrLen]), 0, 0, false, nil
		}
	}
	size = -1
	var fcs uint64
	fcsBytes := hdr[hdrLen-fcsSize : hdrLen]
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
			return slices.Clone(hdr[:hdrLen]), 0, 0, false, nil
		}
		size = int(fcs)
	}

	// A frame compresses to at most about its decoded size.
	buf := append(getBuf(size, parZstdChunk, getFrame), hdr[:hdrLen]...)
	read := func(n int) ([]byte, error) {
		start := len(buf)
		buf = slices.Grow(buf, n)[:start+n]
		if _, err := io.ReadFull(src, buf[start:]); err != nil {
			return nil, noEOF(err)
		}
		return buf[start:], nil
	}

	// Blocks (section 3.1.1.2), up to and including the last one.
	for last := false; !last; {
		bh, err := read(3)
		if err != nil {
			return nil, 0, 0, false, fmt.Errorf("reading zstd block header: %w", err)
		}
		h := uint32(bh[0]) | uint32(bh[1])<<8 | uint32(bh[2])<<16
		last = h&1 != 0
		n := int(h >> 3)
		if n > zstdMaxBlockOut {
			return nil, 0, 0, false, fmt.Errorf("zstd block of %d bytes exceeds the maximum of %d", n, zstdMaxBlockOut)
		}
		switch (h >> 1) & 3 {
		case 1: // RLE: one byte, repeated
			n = 1
		case 3:
			return nil, 0, 0, false, errors.New("reserved zstd block type")
		}
		maxSize += zstdMaxBlockOut
		if maxSize > parZstdMaxFrame {
			return buf, 0, 0, false, nil
		}
		if _, err := read(n); err != nil {
			return nil, 0, 0, false, fmt.Errorf("reading zstd block: %w", err)
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
	return buf, size, maxSize, true, nil
}

// getBuf returns an empty buffer for about n bytes: a pooled one from get when n
// is in (limit/2, limit], else a new one of capacity n.
func getBuf(n, limit int, get func() []byte) []byte {
	if n > limit/2 && n <= limit {
		return get()
	}
	return make([]byte, 0, max(n, 0))
}

// noEOF turns a clean EOF in the middle of a frame into ErrUnexpectedEOF.
func noEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
