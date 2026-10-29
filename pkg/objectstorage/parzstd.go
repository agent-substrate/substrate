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
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstd's streaming encoder compresses one block at a time however many cores it is
// given — WithEncoderConcurrency only lets the next block be filled while the
// current one encodes. That caps a snapshot upload at one core of compression
// (measured on a GKE worker node: ~190 MB/s of compressed output, below what the
// parallel part upload underneath it can absorb, so the uploader starved).
//
// parZstd instead cuts the stream into chunks and compresses each as its own zstd
// FRAME on its own core. Concatenated frames decode as one stream, so the output
// still reads back through a plain zstd reader and the snapshot format is unchanged.
// Independent chunks cost a little ratio (no matches across a boundary), which
// parZstdChunk is sized to keep negligible.
const (
	// parZstdChunk is the maximum per-frame chunk size.
	parZstdChunk = 8 << 20
	// parZstdMinChunk is the smallest per-frame chunk size, covering a fast
	// encoder's fixed tables, block buffers, and minimum history.
	parZstdMinChunk = 1 << 20
	// parZstdQueue is how many chunks may be outstanding per worker, so the producer
	// can run ahead while workers compress.
	parZstdQueue = 2
	// parZstdFrameCap is the capacity of a pooled output buffer: one chunk's frame
	// even when the chunk does not compress.
	parZstdFrameCap = parZstdChunk + parZstdChunk/16
)

// Chunk and frame buffers are pooled across uploads rather than allocated for each
// one: a snapshot otherwise allocates tens of MiB of fresh buffers per upload, and a
// node checkpointing many actors at once spends its time page-faulting them in. Only
// buffers of the pooled capacity go back, so a pool never hands out something smaller
// than its callers expect. Encoders are not pooled: an Encoder makes an internal
// channel on first use, which ties it to the synctest bubble (in tests) that first
// used it.
var (
	parZstdChunkPool = sync.Pool{New: func() any { b := make([]byte, 0, parZstdChunk); return &b }}
	parZstdFramePool = sync.Pool{New: func() any { b := make([]byte, 0, parZstdFrameCap); return &b }}
)

// parZstdChunkSize returns the per-frame chunk size for a sparse file with
// populated data bytes, as a power of two between parZstdMinChunk and parZstdChunk.
func parZstdChunkSize(populated int64) int64 {
	chunk := int64(parZstdMinChunk)
	for chunk < parZstdChunk && chunk < populated {
		chunk <<= 1
	}
	return chunk
}

// parZstdWorkerMemory returns the memory reservation per worker for a given
// frame chunk size.
func parZstdWorkerMemory(chunk int64) int64 {
	return (parZstdQueue*3 + 2) * chunk
}

// newChunkEncoder returns an encoder for EncodeAll use. Single-segment frames always
// declare their content size, even the short last frame of a stream (zstd otherwise
// leaves it out under 256 bytes), so a reader can size every frame without having
// to bound it; a chunk is at most parZstdChunk, so the frame never needs a larger
// window than that.
func newChunkEncoder() *zstd.Encoder {
	return newChunkEncoderSized(parZstdChunk)
}

func newChunkEncoderSized(chunk int64) *zstd.Encoder {
	opts := []zstd.EOption{
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithSingleSegment(true),
	}
	if chunk > 0 && chunk < 4<<20 {
		opts = append(opts, zstd.WithWindowSize(int(max(chunk, parZstdMinChunk))))
	}
	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		// NewWriter only fails on bad options, which are bounded powers of two here.
		panic(err)
	}
	return enc
}

func getChunk() []byte { return (*parZstdChunkPool.Get().(*[]byte))[:0] }
func getFrame() []byte { return (*parZstdFramePool.Get().(*[]byte))[:0] }

func getChunkSized(chunk int64) []byte {
	if chunk == parZstdChunk {
		return getChunk()
	}
	return make([]byte, 0, chunk)
}

func putChunk(b []byte) {
	if cap(b) == parZstdChunk {
		b = b[:0]
		parZstdChunkPool.Put(&b)
	}
}

func putFrame(b []byte) {
	if cap(b) == parZstdFrameCap {
		b = b[:0]
		parZstdFramePool.Put(&b)
	}
}

// parZstd is an io.WriteCloser that compresses what it is given as independent zstd
// frames of at most parZstdChunk input bytes, each declaring its decompressed size,
// written to dst in order. Sized frames can be split and decoded in parallel on
// restore. Close flushes the tail and reports the first error from any worker or
// from dst.
//
// With more than one worker, chunks are compressed on a pool of goroutines. With one
// (inline mode) there is no pool: each chunk is compressed on the caller's goroutine
// when it fills. Inline mode holds one pooled chunk, one encoder, and a pooled frame
// only once a full chunk is compressed, which keeps an upload part within
// serialWorkerMemory; it suits callers that already run one encoder per parallel
// unit of work (an upload part) and small files. Inline Write returns dst's error
// as soon as it happens, so a failed upload stops reading its source.
type parZstd struct {
	dst     io.Writer
	workers int
	chunk   int64

	buf      []byte
	free     chan []byte
	jobs     chan parZstdJob
	ordered  chan chan []byte
	done     chan struct{}
	err      error
	workerWG sync.WaitGroup

	// Inline mode (workers == 1) only.
	inline bool
	enc    *zstd.Encoder
	frame  []byte
}

type parZstdJob struct {
	buf []byte
	out chan []byte
}

// newParZstd starts the worker pool, capped at GOMAXPROCS (workers <= 0 asks for it).
// One worker, after the cap, means inline mode with no goroutines, so with
// GOMAXPROCS=1 every caller compresses inline.
func newParZstd(dst io.Writer, workers int) *parZstd {
	return newParZstdChunk(dst, workers, parZstdChunk)
}

func newParZstdChunk(dst io.Writer, workers int, chunk int64) *parZstd {
	if workers <= 0 || workers > runtime.GOMAXPROCS(0) {
		workers = runtime.GOMAXPROCS(0)
	}
	if chunk <= 0 || chunk > parZstdChunk {
		chunk = parZstdChunk
	}
	if workers == 1 {
		return &parZstd{
			dst:     dst,
			workers: 1,
			chunk:   chunk,
			inline:  true,
			enc:     newChunkEncoderSized(chunk),
			buf:     getChunkSized(chunk),
		}
	}
	p := &parZstd{
		dst:     dst,
		workers: workers,
		chunk:   chunk,
		free:    make(chan []byte, workers*parZstdQueue),
		jobs:    make(chan parZstdJob, workers),
		ordered: make(chan chan []byte, workers*parZstdQueue),
		done:    make(chan struct{}),
	}
	for range workers * parZstdQueue {
		p.free <- getChunkSized(chunk)
	}
	for range workers {
		p.workerWG.Go(p.worker)
	}
	go p.writer()
	p.buf = <-p.free
	return p
}

// worker compresses whole chunks. Each holds its own encoder: the encoders are
// single-shot EncodeAll users, so one per worker keeps their state private.
func (p *parZstd) worker() {
	enc := newChunkEncoderSized(p.chunk)
	defer enc.Close()
	for j := range p.jobs {
		// Only a full chunk takes a pooled frame. A partial one (a small file, or
		// the tail of a stream) gets a buffer its own size, so many small uploads
		// at once don't each hold a full-size frame.
		var dst []byte
		if len(j.buf) == parZstdChunk {
			dst = getFrame()
		} else {
			dst = make([]byte, 0, len(j.buf)+len(j.buf)/16)
		}
		j.out <- enc.EncodeAll(j.buf, dst)
		p.free <- j.buf[:0]
	}
}

// writer drains the per-chunk result channels in dispatch order, so the frames land
// in the same order the bytes arrived.
func (p *parZstd) writer() {
	defer close(p.done)
	for out := range p.ordered {
		frame := <-out
		if p.err == nil {
			_, p.err = p.dst.Write(frame)
		}
		putFrame(frame)
	}
}

func (p *parZstd) Write(b []byte) (int, error) {
	if p.buf == nil {
		return 0, os.ErrClosed
	}
	if p.inline {
		return p.writeInline(b)
	}
	total := len(b)
	for len(b) > 0 {
		n := min(cap(p.buf)-len(p.buf), len(b))
		p.buf = append(p.buf, b[:n]...)
		b = b[n:]
		if len(p.buf) == cap(p.buf) {
			p.flush()
		}
	}
	return total, nil
}

// writeInline is Write in inline mode. It stops at the first error from dst, so the
// caller stops feeding a failed upload. (With workers, dst's error is only known to
// the writer goroutine and surfaces from Close.)
func (p *parZstd) writeInline(b []byte) (int, error) {
	total := len(b)
	for len(b) > 0 && p.err == nil {
		n := min(cap(p.buf)-len(p.buf), len(b))
		p.buf = append(p.buf, b[:n]...)
		b = b[n:]
		if len(p.buf) == cap(p.buf) {
			p.flush()
		}
	}
	if p.err != nil {
		return total - len(b), p.err
	}
	return total, nil
}

// flush dispatches the current chunk and takes a fresh buffer or, inline,
// compresses and writes it here.
func (p *parZstd) flush() {
	if len(p.buf) == 0 {
		return
	}
	if p.inline {
		p.flushInline()
		return
	}
	out := make(chan []byte, 1)
	p.ordered <- out
	p.jobs <- parZstdJob{buf: p.buf, out: out}
	p.buf = <-p.free
}

// flushInline compresses the buffered chunk into one frame and writes it. Only a
// full chunk takes the pooled frame, which is then kept for the rest of the stream;
// a stream that is only a partial chunk (a small file) gets a frame its own size.
// Write flushes only full chunks, so a partial one is always the tail, from Close.
func (p *parZstd) flushInline() {
	if p.err != nil {
		return
	}
	out := p.frame
	if out == nil {
		if len(p.buf) == parZstdChunk {
			p.frame = getFrame()
			out = p.frame
		} else {
			out = make([]byte, 0, len(p.buf)+len(p.buf)/16)
		}
	}
	out = p.enc.EncodeAll(p.buf, out[:0])
	_, p.err = p.dst.Write(out)
	p.buf = p.buf[:0]
}

// Close flushes the tail, returns the buffers to their pools and reports the first
// error. Later calls to Write or Close return that error, or os.ErrClosed.
func (p *parZstd) Close() error {
	if p.buf == nil {
		return p.err
	}
	p.flush()
	if p.inline {
		p.enc.Close()
		putChunk(p.buf)
		putFrame(p.frame)
		p.enc, p.buf, p.frame = nil, nil, nil
		return p.markClosed()
	}
	close(p.jobs)
	close(p.ordered)
	<-p.done
	p.workerWG.Wait()
	// Every chunk is idle now: the one in hand and the rest back in free.
	putChunk(p.buf)
	p.buf = nil
	for {
		select {
		case b := <-p.free:
			putChunk(b)
		default:
			return p.markClosed()
		}
	}
}

// markClosed returns the first error and records os.ErrClosed if there was none, so
// a stray Write after Close fails fast instead of spinning on the nil buffer.
func (p *parZstd) markClosed() error {
	err := p.err
	if err == nil {
		p.err = os.ErrClosed
	}
	return err
}
