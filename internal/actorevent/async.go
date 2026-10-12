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

package actorevent

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// AsyncHandler writes records through another handler from its own goroutine,
// so a slow stdout cannot block the caller. When the queue is full it drops the
// record and counts it, and the writer reports the count through that same
// handler once it catches up, so a level that silences the default logger
// cannot hide the loss.
type AsyncHandler struct {
	inner slog.Handler
	q     *asyncQueue
}

type asyncQueue struct {
	// base writes the drop reports.
	base    slog.Handler
	mu      sync.RWMutex
	closed  bool
	ch      chan asyncItem
	done    chan struct{}
	dropped atomic.Uint64
}

type asyncItem struct {
	ctx context.Context
	h   slog.Handler
	r   slog.Record
}

// NewAsyncHandler starts the writer. capacity is how many records may wait.
func NewAsyncHandler(inner slog.Handler, capacity int) *AsyncHandler {
	q := &asyncQueue{base: inner, ch: make(chan asyncItem, capacity), done: make(chan struct{})}
	go q.run()
	return &AsyncHandler{inner: inner, q: q}
}

func (q *asyncQueue) run() {
	defer close(q.done)
	var reported uint64
	report := func() {
		if d := q.dropped.Load(); d > reported {
			r := slog.NewRecord(time.Now(), slog.LevelWarn, "Dropped actor event records from stdout", 0)
			r.AddAttrs(slog.Uint64("dropped", d-reported))
			_ = q.base.Handle(context.Background(), r)
			reported = d
		}
	}
	for it := range q.ch {
		_ = it.h.Handle(it.ctx, it.r)
		report()
	}
	report()
}

// Enabled reports the inner handler's level.
func (h *AsyncHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

// Handle queues r and returns at once.
func (h *AsyncHandler) Handle(ctx context.Context, r slog.Record) error {
	h.q.mu.RLock()
	defer h.q.mu.RUnlock()
	if h.q.closed {
		// The writer is gone; a late record, as at shutdown, is written here.
		return h.inner.Handle(ctx, r)
	}
	select {
	case h.q.ch <- asyncItem{ctx: context.WithoutCancel(ctx), h: h.inner, r: r.Clone()}:
	default:
		h.q.dropped.Add(1)
	}
	return nil
}

// WithAttrs shares the queue with h.
func (h *AsyncHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &AsyncHandler{inner: h.inner.WithAttrs(attrs), q: h.q}
}

// WithGroup shares the queue with h.
func (h *AsyncHandler) WithGroup(name string) slog.Handler {
	return &AsyncHandler{inner: h.inner.WithGroup(name), q: h.q}
}

// Close writes the queued records and stops the writer. A record handled after
// Close is written on the caller's goroutine.
func (h *AsyncHandler) Close() {
	h.q.mu.Lock()
	if !h.q.closed {
		h.q.closed = true
		close(h.q.ch)
	}
	h.q.mu.Unlock()
	<-h.q.done
}
