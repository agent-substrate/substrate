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
	"io"
	"runtime"

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
	// parZstdChunk is the per-frame chunk size.
	parZstdChunk = 8 << 20
	// parZstdQueue is how many chunks may be outstanding per worker, so the producer
	// can run ahead while workers compress.
	parZstdQueue = 2
)

// parZstd is an io.WriteCloser that compresses what it is given as parallel zstd
// frames, written to dst in order. Close flushes the tail and reports the first
// error from any worker or from dst.
type parZstd struct {
	dst     io.Writer
	workers int

	buf     []byte
	free    chan []byte
	jobs    chan parZstdJob
	ordered chan chan []byte
	done    chan struct{}
	err     error
}

type parZstdJob struct {
	buf []byte
	out chan []byte
}

// newParZstd starts the worker pool, capped at GOMAXPROCS (workers <= 0 asks for it).
func newParZstd(dst io.Writer, workers int) *parZstd {
	if workers <= 0 || workers > runtime.GOMAXPROCS(0) {
		workers = runtime.GOMAXPROCS(0)
	}
	p := &parZstd{
		dst:     dst,
		workers: workers,
		free:    make(chan []byte, workers*parZstdQueue),
		jobs:    make(chan parZstdJob, workers),
		ordered: make(chan chan []byte, workers*parZstdQueue),
		done:    make(chan struct{}),
	}
	for range workers * parZstdQueue {
		p.free <- make([]byte, 0, parZstdChunk)
	}
	for range workers {
		go p.worker()
	}
	go p.writer()
	p.buf = <-p.free
	return p
}

// worker compresses whole chunks. Each holds its own encoder: the encoders are
// single-shot EncodeAll users, so one per worker keeps their state private.
func (p *parZstd) worker() {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		// NewWriter only fails on bad options, which are compile-time constants here.
		panic(err)
	}
	defer enc.Close()
	for j := range p.jobs {
		j.out <- enc.EncodeAll(j.buf, make([]byte, 0, len(j.buf)+len(j.buf)/16))
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
	}
}

func (p *parZstd) Write(b []byte) (int, error) {
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

// flush dispatches the current chunk and takes a fresh buffer.
func (p *parZstd) flush() {
	if len(p.buf) == 0 {
		return
	}
	out := make(chan []byte, 1)
	p.ordered <- out
	p.jobs <- parZstdJob{buf: p.buf, out: out}
	p.buf = <-p.free
}

func (p *parZstd) Close() error {
	p.flush()
	close(p.jobs)
	close(p.ordered)
	<-p.done
	return p.err
}

// newSerialZstd is a plain streaming encoder, for callers that are already running one
// per parallel unit of work (an upload part) and so need no fan-out of their own.
func newSerialZstd(dst io.Writer) *zstd.Encoder {
	// NewWriter only errors on bad options, which are compile-time constants here.
	enc, err := zstd.NewWriter(dst,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		panic(err)
	}
	return enc
}
