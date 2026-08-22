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
	"context"
	"fmt"
	"io"
)

// A restore spends most of its download blocked on the socket: 450 MB of snapshot
// measured on a GKE worker node took 2.05-2.34s, of which 1.30-1.60s was waiting for
// bytes (~0.45s decompress, ~0.27s writing the file). One stream is the limit, the
// same way it was for the upload, so an object is fetched as consecutive byte RANGES
// in parallel and handed to the decoder in order.
//
// This reads nothing from the object's contents: the ranges are the original byte
// stream again before anything parses it, so it needs no format change and works on
// objects written by any earlier version.
const (
	// downloadChunkSize is how much one ranged request fetches. Peak buffering is
	// this times downloadConcurrency.
	downloadChunkSize = 16 << 20
	// downloadConcurrency is how many ranges are in flight.
	downloadConcurrency = 8
)

// fetchRangeFunc reads the object's [off, off+n) into buf. i is the range's index,
// which backends whose client holds a single connection use to spread ranges over a
// pool (see gcsClient.poolClient).
type fetchRangeFunc func(ctx context.Context, i int, off, n int64, buf []byte) error

// rangedReader reassembles parallel ranged reads into one ordered stream. head, if
// set, is the already-open body of the first range, so the size probe is not wasted.
type rangedReader struct {
	cancel  context.CancelFunc
	ordered chan chan rangeResult
	free    chan []byte
	cur     []byte
	curFull []byte
	err     error
}

type rangeResult struct {
	buf []byte
	err error
}

// newRangedReader starts fetching size bytes as chunks, at most downloadConcurrency
// of them in flight. head supplies the first chunk when the caller already opened it.
func newRangedReader(ctx context.Context, size int64, head io.Reader, fetch fetchRangeFunc) *rangedReader {
	ctx, cancel := context.WithCancel(ctx)
	r := &rangedReader{
		cancel:  cancel,
		ordered: make(chan chan rangeResult, downloadConcurrency),
		free:    make(chan []byte, downloadConcurrency),
	}
	for range downloadConcurrency {
		r.free <- make([]byte, downloadChunkSize)
	}
	go r.schedule(ctx, size, head, fetch)
	return r
}

// schedule issues the ranges in order, bounded by the free buffers.
func (r *rangedReader) schedule(ctx context.Context, size int64, head io.Reader, fetch fetchRangeFunc) {
	defer close(r.ordered)
	for i, off := 0, int64(0); off < size; i, off = i+1, off+downloadChunkSize {
		var buf []byte
		select {
		case buf = <-r.free:
		case <-ctx.Done():
			return
		}
		out := make(chan rangeResult, 1)
		select {
		case r.ordered <- out:
		case <-ctx.Done():
			return
		}
		n := min(int64(downloadChunkSize), size-off)
		src := head
		if i > 0 {
			src = nil
		}
		go func(i int, off, n int64, buf []byte, src io.Reader) {
			if src != nil {
				if _, err := io.ReadFull(src, buf[:n]); err != nil {
					out <- rangeResult{err: fmt.Errorf("while reading range %d+%d: %w", off, n, err)}
					return
				}
			} else if err := fetch(ctx, i, off, n, buf[:n]); err != nil {
				out <- rangeResult{err: err}
				return
			}
			out <- rangeResult{buf: buf[:n]}
		}(i, off, n, buf, src)
	}
}

func (r *rangedReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	for len(r.cur) == 0 {
		out, ok := <-r.ordered
		if !ok {
			r.err = io.EOF
			return 0, r.err
		}
		res := <-out
		if res.err != nil {
			r.err = res.err
			return 0, r.err
		}
		r.cur = res.buf
		r.curFull = res.buf[:cap(res.buf)]
	}
	n := copy(p, r.cur)
	r.cur = r.cur[n:]
	if len(r.cur) == 0 && r.curFull != nil {
		// Recycle the whole buffer, not the consumed tail.
		select {
		case r.free <- r.curFull:
		default:
		}
		r.curFull = nil
	}
	return n, nil
}

func (r *rangedReader) Close() error {
	r.cancel()
	return nil
}
