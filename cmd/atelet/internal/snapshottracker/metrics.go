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

const uploadDurationMetric = "atelet.snapshot.upload.duration"

// uploadDurationBuckets match snapshotPhaseBuckets in cmd/atelet/metrics.go:
// a small volumes snapshot uploads in tens of milliseconds, while a multi-GiB
// memory snapshot runs for tens of seconds.
var uploadDurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60}

// Instruments holds the snapshot tracker's metric instruments. A nil
// *Instruments is a valid no-op, so call sites need no guard.
type Instruments struct {
	uploadDuration metric.Float64Histogram
}

// NewInstruments creates the snapshot tracker's instruments against meter.
func NewInstruments(meter metric.Meter) (*Instruments, error) {
	uploadDuration, err := meter.Float64Histogram(
		uploadDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of one background upload of a local snapshot to object storage on atelet."),
		metric.WithExplicitBucketBoundaries(uploadDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", uploadDurationMetric, err)
	}
	return &Instruments{uploadDuration: uploadDuration}, nil
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
	attrs := make([]attribute.KeyValue, 0, 2)
	attrs = append(attrs, ateattr.SnapshotFidelityKey.String(ateattr.SnapshotFidelityValue(target.Snapshot.GetFidelity())))
	if err != nil {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(uploadErrorType(ctx, err)))
	}
	i.uploadDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(attrs...))
}

// uploadErrorType returns the error.type of a failed upload: Canceled once ctx
// has ended, as when Terminate or an atelet shutdown cancels the upload, and
// otherwise the code an apierror or a plugin's gRPC status carries, or
// Internal.
func uploadErrorType(ctx context.Context, err error) string {
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
