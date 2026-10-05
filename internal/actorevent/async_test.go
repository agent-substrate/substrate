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
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// gateHandler blocks every Handle until release is closed.
type gateHandler struct {
	release chan struct{}
	mu      sync.Mutex
	msgs    []string
}

func (g *gateHandler) Enabled(context.Context, slog.Level) bool { return true }
func (g *gateHandler) Handle(_ context.Context, r slog.Record) error {
	<-g.release
	g.mu.Lock()
	defer g.mu.Unlock()
	g.msgs = append(g.msgs, r.Message)
	return nil
}
func (g *gateHandler) WithAttrs([]slog.Attr) slog.Handler { return g }
func (g *gateHandler) WithGroup(string) slog.Handler      { return g }

func TestAsyncHandlerWritesInOrder(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	h := NewAsyncHandler(slog.NewTextHandler(&buf, nil), 8)
	for _, msg := range []string{"one", "two", "three"} {
		_ = h.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, msg, 0))
	}
	h.Close()
	out := buf.String()
	if i, j, k := strings.Index(out, "one"), strings.Index(out, "two"), strings.Index(out, "three"); i < 0 || i > j || j > k {
		t.Errorf("output out of order or incomplete:\n%s", out)
	}
}

// TestAsyncHandlerDropsWhenFull holds the writer on its first record, fills
// the queue, and checks that the overflow is counted rather than blocking.
func TestAsyncHandlerDropsWhenFull(t *testing.T) {
	t.Parallel()
	inner := &gateHandler{release: make(chan struct{})}
	h := NewAsyncHandler(inner, 2)

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		for range 10 {
			_ = h.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "m", 0))
		}
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Handle blocked on a full queue")
	}
	close(inner.release)
	h.Close()
	// One record is held by the writer and two wait; the rest are dropped, and
	// the loss is reported.
	if got := slices.Index(inner.msgs, "Dropped actor event records from stdout"); got < 0 {
		t.Errorf("no drop report among %v", inner.msgs)
	}
	written := 0
	for _, m := range inner.msgs {
		if m == "m" {
			written++
		}
	}
	if written < 1 || written > 3 {
		t.Errorf("wrote %d records, want 1 to 3 of the 10", written)
	}
}

func TestAsyncHandlerAfterClose(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	h := NewAsyncHandler(slog.NewTextHandler(&buf, nil), 1)
	h.Close()
	h.Close()
	_ = h.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "late", 0))
	if !strings.Contains(buf.String(), "late") {
		t.Errorf("a record handled after Close was lost: %q", buf.String())
	}
}

// TestNewEmitterToBypassesDefault pins that the stdout record goes to the given
// handler, so --log-level on slog.Default() does not reach it.
func TestNewEmitterToBypassesDefault(t *testing.T) {
	quiet := &captureHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	lp := sdklog.NewLoggerProvider()
	t.Cleanup(func() { _ = lp.Shutdown(context.Background()) })
	NewEmitterTo(lp, quiet).Log(context.Background(), UsageSampled, usageSampledAttrs())

	if len(quiet.records) != 1 {
		t.Errorf("given handler got %d records, want 1", len(quiet.records))
	}
}

// TestAsyncHandlerReportsDropsThroughItsHandler pins that the drop report goes
// through the handler the records use, not slog.Default(), so a quiet default
// logger cannot hide it.
func TestAsyncHandlerReportsDropsThroughItsHandler(t *testing.T) {
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	inner := &gateHandler{release: make(chan struct{})}
	h := NewAsyncHandler(inner, 1)
	for range 5 {
		_ = h.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "m", 0))
	}
	close(inner.release)
	h.Close()
	if !slices.Contains(inner.msgs, "Dropped actor event records from stdout") {
		t.Errorf("records' handler got %v, want a drop report among them", inner.msgs)
	}
}
