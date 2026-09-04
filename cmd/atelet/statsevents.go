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

	"cloud.google.com/go/compute/metadata"

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

// defaultLabelsKey resolves the actor-identity label group's spelling:
// actorlog's GCE / plain split, so usage events and lifecycle events promote
// into Cloud Logging labels the same way. Resolved once, at first use --
// metadata.OnGCE probes the metadata server (seconds of timeout off GCE),
// which must not bill atelet's boot path; the first emit pays it once, on
// the poller's sweep goroutine, which nobody waits on.
var defaultLabelsKey = sync.OnceValue(func() string {
	if metadata.OnGCE() {
		return "logging.googleapis.com/labels"
	}
	return "labels"
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
// wire, so nothing downstream changes; sharing atelet's stdout is safe
// because a JSON handler writes each record in a single Write and the
// records are small.
func newStatsEventEmitter(w io.Writer, labelsKey func() string) *statsEventEmitter {
	return &statsEventEmitter{
		log:       slog.New(contextlogging.NewHandler(slog.NewJSONHandler(w, nil))),
		labelsKey: labelsKey,
	}
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
