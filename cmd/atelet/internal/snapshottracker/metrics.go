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

package snapshottracker

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
)

const (
	uploadDurationMetric        = "atelet.snapshot.upload.duration"
	detachDurationMetric        = "atelet.snapshot.detach.duration"
	uploadOverdueDurationMetric = "atelet.snapshot.upload.overdue.duration"
	detachOverdueDurationMetric = "atelet.snapshot.detach.overdue.duration"
)

// opDurationBuckets cover background uploads and detaches: deleting local
// snapshots or uploading a small volumes snapshot takes milliseconds, while
// uploading a multi-GiB memory snapshot runs for tens of seconds.
var opDurationBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60}

// overdueBuckets cover how late background work starts: within 100ms of its
// due time while a worker is free, and up to an hour late while every worker
// is busy with a backlog of memory snapshot uploads, or for work that came due
// while atelet was down.
var overdueBuckets = []float64{0.1, 1, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600}

// Instruments holds the snapshot tracker's metric instruments. A nil
// *Instruments is a valid no-op, so call sites need no guard.
type Instruments struct {
	uploadDuration        metric.Float64Histogram
	detachDuration        metric.Float64Histogram
	uploadOverdueDuration metric.Float64Histogram
	detachOverdueDuration metric.Float64Histogram
}

// NewInstruments creates the snapshot tracker's instruments against meter.
func NewInstruments(meter metric.Meter) (*Instruments, error) {
	uploadDuration, err := meter.Float64Histogram(
		uploadDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of one background upload of a local snapshot to object storage on atelet."),
		metric.WithExplicitBucketBoundaries(opDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", uploadDurationMetric, err)
	}
	detachDuration, err := meter.Float64Histogram(
		detachDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of deleting an actor's local snapshots during a background detach on atelet, excluding any snapshot upload."),
		metric.WithExplicitBucketBoundaries(opDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", detachDurationMetric, err)
	}
	uploadOverdueDuration, err := meter.Float64Histogram(
		uploadOverdueDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Time from when a background upload of a local snapshot on atelet is due to when a worker starts it."),
		metric.WithExplicitBucketBoundaries(overdueBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", uploadOverdueDurationMetric, err)
	}
	detachOverdueDuration, err := meter.Float64Histogram(
		detachOverdueDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Time from when a background detach of an actor on atelet is due to when a worker starts it."),
		metric.WithExplicitBucketBoundaries(overdueBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", detachOverdueDurationMetric, err)
	}
	return &Instruments{
		uploadDuration:        uploadDuration,
		detachDuration:        detachDuration,
		uploadOverdueDuration: uploadOverdueDuration,
		detachOverdueDuration: detachOverdueDuration,
	}, nil
}

// recordUpload records one background upload attempt. A probe that found the
// snapshot's manifest committed already without uploading (err == nil &&
// !uploaded) is skipped so metadata probes do not skew upload percentiles.
func (i *Instruments) recordUpload(ctx context.Context, target SnapshotRef, elapsed time.Duration, uploaded bool, err error) {
	if i == nil || i.uploadDuration == nil {
		return
	}
	if err == nil && !uploaded {
		return
	}
	i.uploadDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(outcomeAttrs(ctx, target, err)...))
}

// recordDetach records one background detach once target is durable, excluding
// any upload that ran first.
func (i *Instruments) recordDetach(ctx context.Context, target SnapshotRef, elapsed time.Duration, err error) {
	if i == nil || i.detachDuration == nil {
		return
	}
	i.detachDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(outcomeAttrs(ctx, target, err)...))
}

// recordUploadOverdue records how long after a background upload came due a
// worker began it.
func (i *Instruments) recordUploadOverdue(ctx context.Context, overdue time.Duration) {
	if i == nil || i.uploadOverdueDuration == nil {
		return
	}
	i.uploadOverdueDuration.Record(ctx, overdue.Seconds())
}

// recordDetachOverdue records how long after a background detach came due a
// worker began it.
func (i *Instruments) recordDetachOverdue(ctx context.Context, overdue time.Duration) {
	if i == nil || i.detachOverdueDuration == nil {
		return
	}
	i.detachOverdueDuration.Record(ctx, overdue.Seconds())
}

// outcomeAttrs returns the labels of a finished upload or detach of target:
// its fidelity, and error.type if it failed.
func outcomeAttrs(ctx context.Context, target SnapshotRef, err error) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 2)
	attrs = append(attrs, ateattr.SnapshotFidelityKey.String(ateattr.SnapshotFidelityValue(target.Snapshot.GetFidelity())))
	if err != nil {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(errorType(ctx, err)))
	}
	return attrs
}

// errorType returns the error.type of a failed upload or detach: Canceled
// once ctx has ended, as when Terminate or an atelet shutdown cancels the
// operation, and otherwise the code an apierror or a plugin's gRPC status
// carries, or Internal.
func errorType(ctx context.Context, err error) string {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Code().String()
	}
	if s, ok := apierror.FromError(err); ok {
		return s.Code().String()
	}
	if s, ok := status.FromError(err); ok {
		return s.Code().String()
	}
	return codes.Internal.String()
}
