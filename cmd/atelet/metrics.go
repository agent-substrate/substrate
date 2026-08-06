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
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	restoreDurationMetric    = "ate.actor.restore.duration"
	checkpointDurationMetric = "ate.actor.checkpoint.duration"
)

// snapshotPhaseBuckets have to cover both ends of a phase breakdown: a warm OCI
// unpack or a local rename lands in single-digit milliseconds, while a cold node
// fetching a multi-GiB snapshot runs for tens of seconds.
var snapshotPhaseBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60}

// Instruments holds atelet's cold-start histograms. A nil *Instruments is a
// valid no-op, so call sites need no guard.
type Instruments struct {
	restoreDuration    metric.Float64Histogram
	checkpointDuration metric.Float64Histogram
}

func NewInstruments(meter metric.Meter) (*Instruments, error) {
	restoreDuration, err := meter.Float64Histogram(
		restoreDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of one phase of an actor restore on atelet. Phases overlap, so they are independent observations rather than a partition of the total phase."),
		metric.WithExplicitBucketBoundaries(snapshotPhaseBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", restoreDurationMetric, err)
	}

	checkpointDuration, err := meter.Float64Histogram(
		checkpointDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of one phase of an actor checkpoint on atelet. Phases overlap, so they are independent observations rather than a partition of the total phase."),
		metric.WithExplicitBucketBoundaries(snapshotPhaseBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", checkpointDurationMetric, err)
	}

	return &Instruments{
		restoreDuration:    restoreDuration,
		checkpointDuration: checkpointDuration,
	}, nil
}

// snapshotOp is the dimension set shared by every phase of one restore or
// checkpoint.
type snapshotOp struct {
	templateNamespace string
	templateName      string
	kind              string
	scope             string
	sandboxClass      string
}

// attrs omits kind and sandbox class while they are unknown (a restore that
// failed before reading the snapshot manifest) rather than emitting an
// empty-string series. error.type is set from the gRPC status; its absence
// marks success.
func (o snapshotOp) attrs(err error) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 6)
	attrs = append(attrs,
		ateattr.TemplateNamespaceKey.String(o.templateNamespace),
		ateattr.TemplateNameKey.String(o.templateName),
		ateattr.SnapshotScopeKey.String(o.scope),
	)
	if o.kind != "" {
		attrs = append(attrs, ateattr.SnapshotKindKey.String(o.kind))
	}
	if o.sandboxClass != "" {
		attrs = append(attrs, ateattr.SandboxClassKey.String(ateattr.NormalizeSandboxClass(o.sandboxClass)))
	}
	if err != nil {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(status.Code(err).String()))
	}
	return attrs
}

// phase is one timed step of a snapshot operation.
type phase struct {
	name string
	d    time.Duration
}

func (i *Instruments) recordRestore(ctx context.Context, op snapshotOp, err error, phases ...phase) {
	if i == nil || i.restoreDuration == nil {
		return
	}
	recordPhases(ctx, i.restoreDuration, op.attrs(err), phases)
}

func (i *Instruments) recordCheckpoint(ctx context.Context, op snapshotOp, err error, phases ...phase) {
	if i == nil || i.checkpointDuration == nil {
		return
	}
	recordPhases(ctx, i.checkpointDuration, op.attrs(err), phases)
}

// recordPhases skips zero-valued phases: those never ran, because the operation
// failed before reaching them, and reporting them as instantaneous would drag
// every percentile down.
func recordPhases(ctx context.Context, h metric.Float64Histogram, base []attribute.KeyValue, phases []phase) {
	for _, p := range phases {
		if p.d == 0 {
			continue
		}
		attrs := make([]attribute.KeyValue, 0, len(base)+1)
		attrs = append(attrs, base...)
		attrs = append(attrs, ateattr.SnapshotPhaseKey.String(p.name))
		h.Record(ctx, p.d.Seconds(), metric.WithAttributes(attrs...))
	}
}

// restoreSnapshotKind classifies which snapshot a restore reads. A local
// restore is evident from the wire; golden and latest both arrive as an external
// URI prefix, so they are told apart by the identity the manifest records for
// the actor that wrote the snapshot. An empty result means the manifest has not
// been read yet, so the kind is not knowable.
func restoreSnapshotKind(req *ateletpb.RestoreRequest, rec *sandboxAssetsRecord) string {
	if req.GetType() == ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL {
		return ateattr.SnapshotKindLocal
	}
	if rec == nil {
		return ""
	}
	// Manifests written before the identity fields existed carry no atespace and
	// fall through to latest, which is the common case for them anyway.
	if rec.Atespace == resources.GoldenActorAtespace {
		return ateattr.SnapshotKindGolden
	}
	return ateattr.SnapshotKindLatest
}

// checkpointSnapshotKind classifies which snapshot a checkpoint writes: a pause
// writes the node-local one, a suspend the actor's durable latest, and a commit
// by an actor in the golden atespace the template's golden image.
func checkpointSnapshotKind(req *ateletpb.CheckpointRequest) string {
	if req.GetType() == ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL {
		return ateattr.SnapshotKindLocal
	}
	if req.GetAtespace() == resources.GoldenActorAtespace {
		return ateattr.SnapshotKindGolden
	}
	return ateattr.SnapshotKindLatest
}
