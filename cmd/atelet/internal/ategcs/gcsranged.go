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
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/substrate/internal/ateerrors"
)

// A restore spends most of its download blocked on the socket: 450 MB of snapshot
// measured on a GKE worker node took 2.05s, of which 1.30s was waiting for bytes
// (0.46s decompress, 0.28s writing the file). One stream is the limit, the same way
// it was for the upload, so the object is fetched as consecutive byte RANGES in
// parallel, each on its own client/connection, and handed to the decoder in order.
//
// This needs nothing from the object's contents: the ranges are reassembled into the
// original byte stream before anything parses it, so any object -- however it was
// written -- downloads this way.
const (
	// downloadChunkSize is how much one ranged GET fetches. Peak buffering is this
	// times downloadConcurrency.
	downloadChunkSize = 16 << 20
	// downloadConcurrency is how many ranges are in flight.
	downloadConcurrency = 8
)

// GetObject streams the object. Objects larger than one chunk are fetched as parallel
// ranges; smaller ones are a single request, as before.
func (g *gcsClient) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	obj := g.client.Bucket(bucket).Object(object)
	// The first chunk doubles as the size probe: a range read reports the whole
	// object's size in its attrs, so nothing pays an extra round trip for it.
	head, err := obj.NewRangeReader(ctx, 0, downloadChunkSize)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, storage.ErrBucketNotExist) {
			return nil, fmt.Errorf("%w: Bucket:%q, Object:%q", ateerrors.ReasonFailedGetExternalObject, bucket, object)
		}
		return nil, err
	}
	if head.Attrs.Size <= downloadChunkSize {
		return head, nil
	}
	return newRangedReader(ctx, g, bucket, object, head, head.Attrs.Size), nil
}

// rangedReader reassembles parallel ranged GETs into one ordered stream.
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

func newRangedReader(ctx context.Context, g *gcsClient, bucket, object string, head *storage.Reader, size int64) *rangedReader {
	ctx, cancel := context.WithCancel(ctx)
	r := &rangedReader{
		cancel:  cancel,
		ordered: make(chan chan rangeResult, downloadConcurrency),
		free:    make(chan []byte, downloadConcurrency),
	}
	for range downloadConcurrency {
		r.free <- make([]byte, downloadChunkSize)
	}
	go r.schedule(ctx, g, bucket, object, head, size)
	return r
}

// schedule issues the ranges in order, bounded by the free buffers, so at most
// downloadConcurrency of them are in flight.
func (r *rangedReader) schedule(ctx context.Context, g *gcsClient, bucket, object string, head *storage.Reader, size int64) {
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
		// The first chunk is already in flight from the size probe; the rest get a
		// client each so they do not share one HTTP/2 connection.
		src := head
		if i > 0 {
			src = nil
		}
		go r.fetch(ctx, g, bucket, object, i, off, n, buf, src, out)
	}
}

func (r *rangedReader) fetch(ctx context.Context, g *gcsClient, bucket, object string, i int, off, n int64, buf []byte, head *storage.Reader, out chan<- rangeResult) {
	rc := head
	if rc == nil {
		var err error
		rc, err = g.uploadClient(ctx, i).Bucket(bucket).Object(object).NewRangeReader(ctx, off, n)
		if err != nil {
			out <- rangeResult{err: fmt.Errorf("while reading range %d+%d of %q: %w", off, n, object, err)}
			return
		}
	}
	defer rc.Close()
	if _, err := io.ReadFull(rc, buf[:n]); err != nil {
		out <- rangeResult{err: fmt.Errorf("while reading range %d+%d of %q: %w", off, n, object, err)}
		return
	}
	out <- rangeResult{buf: buf[:n]}
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
