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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/serverboot"
)

// eventSample is the fully-populated sample the event tests emit; distinct
// values per field so a crossed wire is visible in the JSON.
func eventSample() *ateompb.WorkloadStatsSample {
	return &ateompb.WorkloadStatsSample{
		Atespace:              "space-a",
		ActorName:             "actor-a",
		ActorUid:              "uid-a",
		ActorTemplateAtespace: "ns-a",
		ActorTemplateName:     "template-a",
		SandboxClass:          ateompb.SandboxClass_SANDBOX_CLASS_MICROVM,
		Source:                ateompb.StatsSource_STATS_SOURCE_GUEST_AGENT,
		MemoryCurrentBytes:    1000,
		MemoryPeakBytes:       2000,
		MemoryWorkingSetBytes: 700,
		CpuUsageUsec:          1234,
		ObservedAtUnixNano:    42,
	}
}

// syncBuffer is a mutex-guarded buffer, so tests stay valid if an emitter
// call site ever moves onto a goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *syncBuffer) String() string { return string(b.Bytes()) }

// newBufferEmitter returns an emitter writing JSON records into buf. The
// labels-key closure stands in for defaultLabelsKey, whose metadata-server
// probe has no place in a unit test.
func newBufferEmitter(buf *syncBuffer, isOnGCE bool) *statsEventEmitter {
	return newStatsEventEmitter(buf, func() string { return actorlog.LabelsKey(isOnGCE) })
}

func TestStatsEventEmitterEmit(t *testing.T) {
	var buf syncBuffer
	e := newBufferEmitter(&buf, false)

	e.emit(context.Background(), eventKindPeriodic, eventSample(), workerPoolRef{})

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("event is not one JSON record: %v (%q)", err, buf.String())
	}
	if got := rec["msg"]; got != usageSampleMsg {
		t.Errorf("msg = %v, want %q", got, usageSampleMsg)
	}
	if got := rec["kind"]; got != "periodic" {
		t.Errorf("kind = %v, want periodic", got)
	}
	// The identity travels as the actorlog-style label group, so usage events
	// join lifecycle events and container logs under the same keys.
	labels, ok := rec["labels"].(map[string]any)
	if !ok {
		t.Fatalf("labels group missing or wrong shape: %v", rec["labels"])
	}
	for key, want := range map[string]string{
		"ate.atespace":          "space-a",
		"ate.actor.name":        "actor-a",
		"ate.actor.uid":         "uid-a",
		"ate.template.atespace": "ns-a",
		"ate.template.name":     "template-a",
	} {
		if got := labels[key]; got != want {
			t.Errorf("labels[%q] = %v, want %q", key, got, want)
		}
	}
	for key, want := range map[string]float64{
		"memory_current_bytes":     1000,
		"memory_peak_bytes":        2000,
		"memory_working_set_bytes": 700,
		"cpu_usage_usec":           1234,
		"observed_at_unix_nano":    42,
	} {
		if got := rec[key]; got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
	if got := rec["sandbox_class"]; got != "microvm" {
		t.Errorf("sandbox_class = %v, want microvm", got)
	}
	if got := rec["source"]; got != "guest-agent" {
		t.Errorf("source = %v, want guest-agent", got)
	}
	// An unresolved pool omits the pair rather than emitting empty strings.
	for _, key := range []string{"ate.workerpool.namespace", "ate.workerpool.name"} {
		if _, ok := labels[key]; ok {
			t.Errorf("zero pool ref emitted label %q: %v", key, labels[key])
		}
	}
}

// TestStatsEventEmitterPoolLabels pins the pool enrichment on events: a
// resolved pod's events carry the same ate.workerpool label pair as the
// metric channel, inside the promoted label group.
func TestStatsEventEmitterPoolLabels(t *testing.T) {
	var buf syncBuffer
	e := newBufferEmitter(&buf, false)

	e.emit(context.Background(), eventKindPeriodic, eventSample(), workerPoolRef{namespace: "pool-ns", name: "pool-a"})

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	labels, ok := rec["labels"].(map[string]any)
	if !ok {
		t.Fatalf("labels group missing or wrong shape: %v", rec["labels"])
	}
	if got := labels["ate.workerpool.namespace"]; got != "pool-ns" {
		t.Errorf("labels[ate.workerpool.namespace] = %v, want pool-ns", got)
	}
	if got := labels["ate.workerpool.name"]; got != "pool-a" {
		t.Errorf("labels[ate.workerpool.name] = %v, want pool-a", got)
	}
}

// TestStatsEventEmitterGCELabelsKey pins the label-group spelling split: on
// GCE the group must sit under the key Cloud Logging promotes.
func TestStatsEventEmitterGCELabelsKey(t *testing.T) {
	var buf syncBuffer
	e := newBufferEmitter(&buf, true)

	e.emit(context.Background(), eventKindPeriodic, eventSample(), workerPoolRef{})

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := rec["logging.googleapis.com/labels"]; !ok {
		t.Errorf("GCE emitter did not use the promoted labels key; record keys: %v", buf.String())
	}
}

// TestStatsEventEmitterIgnoresLogLevel pins the emitter's independence from
// the serverboot verbosity knob: usage events are a data feed, and quieting
// the node with --log-level=warn (or error) must not silently sever them.
func TestStatsEventEmitterIgnoresLogLevel(t *testing.T) {
	if err := serverboot.SetLogLevel("error"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := serverboot.SetLogLevel("info"); err != nil {
			t.Fatal(err)
		}
	})

	var buf syncBuffer
	newBufferEmitter(&buf, false).emit(context.Background(), eventKindPeriodic, eventSample(), workerPoolRef{})

	if buf.Len() == 0 {
		t.Error("raising the serverboot log level suppressed a usage event")
	}
}

// TestStatsEventEmitterNil: a nil emitter and a nil sample are both valid
// no-ops, so call sites stay unconditional.
func TestStatsEventEmitterNil(t *testing.T) {
	var e *statsEventEmitter
	e.emit(context.Background(), eventKindPeriodic, eventSample(), workerPoolRef{}) // must not panic

	var buf syncBuffer
	newBufferEmitter(&buf, false).emit(context.Background(), eventKindPeriodic, nil, workerPoolRef{})
	if buf.Len() != 0 {
		t.Errorf("nil sample emitted a record: %q", buf.String())
	}
}

// stallableWriter blocks every Write until released -- the stalled log
// consumer the asyncWriter exists for.
type stallableWriter struct {
	gate  chan struct{}
	wrote syncBuffer
}

func (w *stallableWriter) Write(p []byte) (int, error) {
	<-w.gate
	return w.wrote.Write(p)
}

// TestAsyncWriterNeverBlocks pins the contract: with the underlying
// writer wedged, every Write returns immediately -- overflow drops and
// counts instead of blocking -- and releasing the wedge drains what the
// queue held.
func TestAsyncWriterNeverBlocks(t *testing.T) {
	under := &stallableWriter{gate: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	const depth = 4
	aw := newAsyncWriter(ctx, under, depth)

	// The writer goroutine dequeues one record and wedges on it; the queue
	// holds depth more. Everything past that must drop, not block. The
	// in-flight handoff is asynchronous, so tolerate one record of slack.
	const writes = depth + 8
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < writes; i++ {
			if _, err := aw.Write([]byte("x")); err != nil {
				t.Errorf("Write returned %v, want nil", err)
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Write blocked on a wedged log stream")
	}
	dropped := int(aw.dropped.Load())
	if dropped < writes-depth-1 || dropped > writes-depth {
		t.Errorf("dropped = %d, want %d or %d", dropped, writes-depth-1, writes-depth)
	}

	// Conservation: everything not dropped -- the wedged in-flight record
	// plus the queue -- drains once the stream recovers, and the drop report
	// arrives over the same sink, unkillable by any verbosity knob.
	close(under.gate)
	wantWritten := writes - dropped
	waitFor(t, func() bool { return bytes.Count(under.wrote.Bytes(), []byte("x")) >= wantWritten })
	if got := bytes.Count(under.wrote.Bytes(), []byte("x")); got != wantWritten {
		t.Errorf("drained %d records, want %d", got, wantWritten)
	}
	waitFor(t, func() bool { return bytes.Contains(under.wrote.Bytes(), []byte("Usage events dropped")) })
}

// TestAsyncWriterCopies: the slog handler reuses its buffer after
// Write returns, so the queue must hold copies, not aliases.
func TestAsyncWriterCopies(t *testing.T) {
	under := &stallableWriter{gate: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	aw := newAsyncWriter(ctx, under, 4)

	p := []byte("original")
	if _, err := aw.Write(p); err != nil {
		t.Fatal(err)
	}
	copy(p, "clobber!")

	close(under.gate)
	waitFor(t, func() bool { return under.wrote.Len() > 0 })
	if got := under.wrote.String(); got != "original" {
		t.Errorf("record = %q, want the copy taken at Write time", got)
	}
}

// waitFor polls until cond returns true or the deadline passes.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not reached before deadline")
}
