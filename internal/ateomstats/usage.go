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

package ateomstats

import (
	"context"
	"io"
	"log/slog"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel/log/noop"
	sdklog "go.opentelemetry.io/otel/sdk/log"

	"github.com/agent-substrate/substrate/internal/actorevent"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// finalFlushTimeout bounds the flush after a final record. The ateom stays up
// after a checkpoint, so the flush buys promptness, not delivery.
const finalFlushTimeout = 500 * time.Millisecond

// Pool is the WorkerPool of the ateom's pod.
type Pool struct {
	Namespace string
	Name      string
}

// UsageEmitter writes ate.actor.usage_sampled records. A nil *UsageEmitter
// writes nothing.
type UsageEmitter struct {
	pool   Pool
	events *actorevent.Emitter
	// flush pushes queued OTLP records out after a final record; nil when the
	// OTLP copy is off.
	flush func(context.Context) error
}

// NewUsageEmitter writes the records' stdout copy through stdout and their OTLP
// copy through lp, the provider InitLogging returned; nil lp means the OTLP
// copy is off.
func NewUsageEmitter(lp *sdklog.LoggerProvider, stdout slog.Handler, pool Pool) *UsageEmitter {
	if lp == nil {
		return &UsageEmitter{pool: pool, events: actorevent.NewEmitterTo(noop.NewLoggerProvider(), stdout)}
	}
	return &UsageEmitter{pool: pool, events: actorevent.NewEmitterTo(lp, stdout), flush: lp.ForceFlush}
}

// Emit writes one record for s, dated when s was read. kind is one of the
// ateattr.StatsKind values.
func (e *UsageEmitter) Emit(ctx context.Context, kind string, s *ateompb.WorkloadStatsSample) {
	if e == nil {
		return
	}
	at := time.Unix(0, s.GetObservedAtUnixNano())
	e.events.LogAt(ctx, actorevent.UsageSampled, at, UsageAttrs(e.pool, kind, s))
}

// EmitFinal writes the final record of an activation and flushes it in the
// background, off the checkpoint's path.
func (e *UsageEmitter) EmitFinal(ctx context.Context, s *ateompb.WorkloadStatsSample) {
	if e == nil {
		return
	}
	e.Emit(ctx, ateattr.StatsKindFinal, s)
	if e.flush == nil {
		return
	}
	go func() {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalFlushTimeout)
		defer cancel()
		if err := e.flush(flushCtx); err != nil {
			slog.DebugContext(ctx, "Usage records not flushed after the final sample", slog.Any("err", err))
		}
	}()
}

// UsageAttrs is the attribute set of ate.actor.usage_sampled for s. The
// measurements are absent while s has no source, and the peak also when the
// source reports none.
func UsageAttrs(pool Pool, kind string, s *ateompb.WorkloadStatsSample) []slog.Attr {
	attrs := append(ateattr.ActorLogAttrs(resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: s.GetAtespace(), Name: s.GetActorName()},
		UID:              s.GetActorUid(),
		TemplateAtespace: s.GetActorTemplateAtespace(),
		TemplateName:     s.GetActorTemplateName(),
	}),
		slog.String(string(ateattr.WorkerPoolNamespaceKey), pool.Namespace),
		slog.String(string(ateattr.WorkerPoolNameKey), pool.Name),
		slog.String(string(ateattr.SandboxClassKey), SandboxClassLabel(s.GetSandboxClass())),
		slog.String(string(ateattr.StatsSourceKey), StatsSourceLabel(s.GetSource())),
		slog.String(string(ateattr.StatsKindKey), kind),
		slog.Int64(string(ateattr.ActorEpochKey), s.GetEpochUnixNano()),
	)
	if s.GetSource() == ateompb.StatsSource_STATS_SOURCE_UNSPECIFIED {
		return attrs
	}
	attrs = append(attrs,
		slog.Uint64(string(ateattr.StatsMemoryUsageKey), s.GetMemoryCurrentBytes()),
		slog.Uint64(string(ateattr.StatsMemoryWorkingSetKey), s.GetMemoryWorkingSetBytes()),
		slog.Float64(string(ateattr.StatsCPUTimeKey), float64(s.GetCpuUsageUsec())/1e6),
	)
	if peak := s.GetMemoryPeakBytes(); peak > 0 {
		attrs = append(attrs, slog.Uint64(string(ateattr.StatsMemoryPeakKey), peak))
	}
	return attrs
}

// SandboxClassLabel maps the wire enum to the ate.sandbox.class values.
func SandboxClassLabel(c ateompb.SandboxClass) string {
	switch c {
	case ateompb.SandboxClass_SANDBOX_CLASS_GVISOR:
		return "gvisor"
	case ateompb.SandboxClass_SANDBOX_CLASS_MICROVM:
		return "microvm"
	default:
		return ateattr.SandboxClassUnknown
	}
}

// StatsSourceLabel maps the wire enum to the ate.stats.source values.
func StatsSourceLabel(s ateompb.StatsSource) string {
	switch s {
	case ateompb.StatsSource_STATS_SOURCE_CGROUP:
		return ateattr.StatsSourceCgroup
	case ateompb.StatsSource_STATS_SOURCE_GUEST_AGENT:
		return ateattr.StatsSourceGuestAgent
	default:
		return ateattr.StatsSourceUnspecified
	}
}

// stdoutQueue is how many stdout records may wait. One sweep writes one per
// hosted actor.
const stdoutQueue = 4096

// NewStdoutHandler writes the stdout copy of the usage records to w in the
// ateom's JSON format, at a fixed level so --log-level does not silence it, and
// off the caller's goroutine.
func NewStdoutHandler(w io.Writer) *actorevent.AsyncHandler {
	json := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	return actorevent.NewAsyncHandler(contextlogging.NewHandler(json), stdoutQueue)
}

// StartSampler calls sweep every interval on its own goroutine and returns a
// function that stops it and waits for the sweep in flight. Defer the stop
// after the stdout handler's Close and the LoggerProvider's shutdown, so it runs
// before them.
func StartSampler(ctx context.Context, interval time.Duration, sweep func(context.Context)) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			sweepOnce(ctx, sweep)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// sweepOnce runs one sweep. It recovers from panics: the sampler is a
// background job, and a bug in it must not take the ateom down and every actor
// on the worker with it.
func sweepOnce(ctx context.Context, sweep func(context.Context)) {
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Usage sweep panicked; skipping this tick",
				slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		}
	}()
	sweep(ctx)
}
