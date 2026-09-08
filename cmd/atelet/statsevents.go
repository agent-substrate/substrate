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
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/compute/metadata"

	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// The events channel is the per-actor half of the usage telemetry split: the
// metrics (statspoller.go) aggregate to the bounded template-level label set,
// and everything with actor or atespace identity travels here instead, as
// structured log events -- the same stream and label vocabulary as actorlog's
// lifecycle events, never a TSDB series.

// usageSampleMsg is the message every usage event carries; consumers filter on
// it plus the "kind" field.
const usageSampleMsg = "Actor usage sample"

// eventKindPeriodic marks the events riding the poller's sweep -- today the
// only kind, carried on the wire so future kinds (lifecycle brackets, say)
// can join without reshaping the record.
const eventKindPeriodic = "periodic"

// defaultLabelsKey resolves the actor-identity label group's spelling --
// actorlog's, so usage events and lifecycle events promote into Cloud
// Logging labels the same way. metadata.OnGCE probes the metadata server, so
// startStatsPoller warms this off the boot path on a throwaway goroutine;
// sync.OnceValue makes an emit that arrives first wait for the in-flight
// probe. That wait is first-tick-only and bounded: milliseconds on GCE, the
// transport's 2s dial timeout off GCE, a 5s cap on the one pathological
// branch (SMBIOS says GCE, probes disagree) -- and no request path ever
// touches it.
var defaultLabelsKey = sync.OnceValue(func() string {
	return actorlog.LabelsKey(metadata.OnGCE())
})

// statsEventEmitter writes per-actor usage events to the process log stream.
// A nil emitter is a valid no-op, so call sites need no guard.
type statsEventEmitter struct {
	log       *slog.Logger
	labelsKey func() string
}

// newStatsEventEmitter builds an emitter over its own fixed-level handler on
// w rather than the serverboot logger: these records are a data feed, not
// leveled diagnostics, and quieting the node with --log-level=warn must not
// silently sever them -- the subsystem's one off-switch is
// --actor-stats-poll-interval=0. The records still carry level INFO on the
// wire, so nothing downstream changes. In production w is an asyncWriter
// over the process's synchronized stdout writer (see startStatsPoller).
func newStatsEventEmitter(w io.Writer, labelsKey func() string) *statsEventEmitter {
	return &statsEventEmitter{
		log:       slog.New(contextlogging.NewHandler(slog.NewJSONHandler(w, nil))),
		labelsKey: labelsKey,
	}
}

// usageEventQueueDepth is the asyncWriter's buffer, in records: about one
// tick of a packed node. A healthy drain outruns the RPC-paced producers by
// orders of magnitude, so the queue only fills once the pipe is dead --
// where any finite depth drops, and the choice is cosmetic.
const usageEventQueueDepth = 256

// asyncWriter decouples event emission from the log consumer's health. A
// write to a full stdout pipe blocks forever, and emit runs inside the
// sweep's errgroup, so one stalled log consumer (wedged rotation, disk-full
// fallout) would otherwise park a probe, wedge the sweep, and silently
// freeze the metrics channel that shares it. Writes land in a bounded queue
// drained by one goroutine; a full queue drops the record and counts it.
// Dropping is safe: the samples are point-in-time readings the next healthy
// tick repairs, and when stdout is dead the events are lost either way --
// the choice is whether the metrics die with them.
type asyncWriter struct {
	w  io.Writer
	ch chan []byte

	// report carries the drop warning over the same fixed-level pipeline as
	// the records: the loss signal is part of the feed's integrity, so it
	// must be exactly as unkillable by --log-level as the feed itself.
	report *slog.Logger

	dropped atomic.Int64
}

func newAsyncWriter(ctx context.Context, w io.Writer, depth int) *asyncWriter {
	aw := &asyncWriter{
		w:      w,
		ch:     make(chan []byte, depth),
		report: slog.New(contextlogging.NewHandler(slog.NewJSONHandler(w, nil))),
	}
	go aw.run(ctx)
	return aw
}

func (aw *asyncWriter) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-aw.ch:
			// Best-effort: a failed write has nobody to report to.
			_, _ = aw.w.Write(b)
			// Report drops only after a successful write, when the stream
			// can carry the warning -- written directly, so the drain does
			// not queue behind itself.
			if n := aw.dropped.Swap(0); n > 0 {
				aw.report.WarnContext(ctx, "Usage events dropped while the log stream stalled", slog.Int64("count", n))
			}
		}
	}
}

// Write queues one record without ever blocking. It reports full success
// even on a drop: the emitter has no recovery to offer, and the drop is
// already counted for the writer goroutine to report.
func (aw *asyncWriter) Write(p []byte) (int, error) {
	// The slog handler reuses its buffer after Write returns, so the queue
	// must own a copy.
	b := append([]byte(nil), p...)
	select {
	case aw.ch <- b:
	default:
		aw.dropped.Add(1)
	}
	return len(p), nil
}

// emit writes one usage event. The identity comes solely from the sample's
// echo, per the stats RPCs' attribution contract. pool is the caller's
// pod-to-pool resolution -- the same enrichment the metric labels carry, so a
// pool-level metric spike can pivot to the actors behind it. A zero-valued
// pool omits the label pair rather than emitting empty strings, following the
// metric channel's rule.
func (e *statsEventEmitter) emit(ctx context.Context, kind string, s *ateompb.WorkloadStatsSample, pool workerPoolRef) {
	if e == nil || s == nil {
		return
	}
	a := resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: s.GetAtespace(), Name: s.GetActorName()},
		UID:              s.GetActorUid(),
		TemplateAtespace: s.GetActorTemplateAtespace(),
		TemplateName:     s.GetActorTemplateName(),
	}
	labels := ateattr.ActorLogLabels(a, "")
	if pool != (workerPoolRef{}) {
		labels[string(ateattr.WorkerPoolNamespaceKey)] = pool.namespace
		labels[string(ateattr.WorkerPoolNameKey)] = pool.name
	}
	e.log.LogAttrs(ctx, slog.LevelInfo, usageSampleMsg,
		slog.Any(e.labelsKey(), labels),
		slog.String("kind", kind),
		slog.String("sandbox_class", sandboxClassLabel(s.GetSandboxClass())),
		slog.String("source", statsSourceLabel(s.GetSource())),
		slog.Uint64("memory_current_bytes", s.GetMemoryCurrentBytes()),
		slog.Uint64("memory_peak_bytes", s.GetMemoryPeakBytes()),
		slog.Uint64("memory_working_set_bytes", s.GetMemoryWorkingSetBytes()),
		slog.Uint64("cpu_usage_usec", s.GetCpuUsageUsec()),
		slog.Int64("observed_at_unix_nano", s.GetObservedAtUnixNano()),
	)
}
