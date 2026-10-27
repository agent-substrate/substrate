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
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/clock"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
)

const (
	uploadDurationMetric        = "atelet.snapshot.upload.duration"
	detachDurationMetric        = "atelet.snapshot.detach.duration"
	uploadOverdueDurationMetric = "atelet.snapshot.upload.overdue.duration"
	detachOverdueDurationMetric = "atelet.snapshot.detach.overdue.duration"
	uploadOverdueActorsMetric   = "atelet.snapshot.upload.overdue.actors"
	undurableAgeMaxMetric       = "atelet.snapshot.undurable.age.max"
)

// overdueBand indexes the ate.snapshot.upload_overdue_band values.
type overdueBand int

const (
	overdueBandUnder1m overdueBand = iota
	overdueBand1mTo5m
	overdueBand5mTo15m
	overdueBandOver15m
	numOverdueBands
)

var overdueBandValues = [numOverdueBands]string{
	overdueBandUnder1m: ateattr.SnapshotUploadOverdueBandUnder1m,
	overdueBand1mTo5m:  ateattr.SnapshotUploadOverdueBand1mTo5m,
	overdueBand5mTo15m: ateattr.SnapshotUploadOverdueBand5mTo15m,
	overdueBandOver15m: ateattr.SnapshotUploadOverdueBandOver15m,
}

func classifyOverdueBand(overdue time.Duration) overdueBand {
	switch {
	case overdue <= time.Minute:
		return overdueBandUnder1m
	case overdue <= 5*time.Minute:
		return overdueBand1mTo5m
	case overdue <= 15*time.Minute:
		return overdueBand5mTo15m
	default:
		return overdueBandOver15m
	}
}

// uploadDurationBuckets cover background uploads: uploading a small volumes
// snapshot takes milliseconds, uploading a multi-GiB memory snapshot runs for
// tens of seconds, and an upload of a large snapshot can run for minutes
// before its timeout, which is at least --snapshot-upload-timeout.
var uploadDurationBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600}

// detachDurationBuckets cover deleting an actor's local snapshots and node
// files during a background detach, which takes milliseconds to seconds. The
// upload a detach runs first is recorded on the upload histogram.
var detachDurationBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60}

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

	// undurableActorsTally is the tally the observable instruments report.
	// NewInstruments registers their callback before New creates the tracker
	// and attaches its tally, so a metric reader can invoke the callback while
	// New stores the pointer.
	undurableActorsTally atomic.Pointer[undurableActorsTally]
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
	detachDuration, err := meter.Float64Histogram(
		detachDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of deleting an actor's local snapshots and node files during a background detach on atelet, excluding any snapshot upload."),
		metric.WithExplicitBucketBoundaries(detachDurationBuckets...),
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
	// The tracker owns the tally of actors with overdue uploads, and the
	// overdue bands partition it, so this is an UpDownCounter and not a gauge.
	uploadOverdueActors, err := meter.Int64ObservableUpDownCounter(
		uploadOverdueActorsMetric,
		metric.WithUnit("{actor}"),
		metric.WithDescription("Number of actors on atelet's node whose background snapshot upload is overdue, by how long it has been overdue."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s updowncounter: %w", uploadOverdueActorsMetric, err)
	}
	undurableAgeMax, err := meter.Float64ObservableGauge(
		undurableAgeMaxMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Age of the oldest local snapshot on atelet's node that object storage does not hold yet, or zero if there is none."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s gauge: %w", undurableAgeMaxMetric, err)
	}

	i := &Instruments{
		uploadDuration:        uploadDuration,
		detachDuration:        detachDuration,
		uploadOverdueDuration: uploadOverdueDuration,
		detachOverdueDuration: detachOverdueDuration,
	}
	var bandAttrs [numOverdueBands]metric.ObserveOption
	for b, v := range overdueBandValues {
		bandAttrs[b] = metric.WithAttributes(ateattr.SnapshotUploadOverdueBandKey.String(v))
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		tally := i.undurableActorsTally.Load()
		if tally == nil {
			return nil
		}
		c := tally.counts()
		// Every overdue band is observed even at zero, so alerts read 0 rather
		// than a missing series.
		for b, count := range c.overdueBands {
			o.ObserveInt64(uploadOverdueActors, count, bandAttrs[b])
		}
		o.ObserveFloat64(undurableAgeMax, c.oldestUndurable.Seconds())
		return nil
	}, uploadOverdueActors, undurableAgeMax)
	if err != nil {
		return nil, fmt.Errorf("register undurable actors callback: %w", err)
	}
	return i, nil
}

// watch makes tally the one the observable instruments report.
func (i *Instruments) watch(tally *undurableActorsTally) {
	if i == nil {
		return
	}
	i.undurableActorsTally.Store(tally)
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

// errorType returns the error.type of a failed upload or detach: Canceled or
// DeadlineExceeded once ctx has ended (when Terminate, an atelet shutdown, a
// newer durable snapshot, or the background upload timeout ends it), and
// otherwise the code an apierror or a plugin's gRPC status carries, or
// Internal.
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

// undurableActorsTally tracks, for the observable instruments, the actors that
// hold an undurable local snapshot on this node. It follows what each actor's
// snapshot state file records, so unlike Tracker.actors it covers every actor
// atelet tracks, in memory or not.
type undurableActorsTally struct {
	clock       clock.PassiveClock
	uploadDelay time.Duration
	hasStore    bool

	mu sync.Mutex
	// actors maps the UID of each actor with an undurable local snapshot to
	// what the tally keeps of that snapshot.
	actors map[string]undurableActorSummary
}

// undurableActorSummary is what an undurableActorsTally keeps of an actor's
// undurable local snapshot.
type undurableActorSummary struct {
	// undurableSince is when the actor's state on this node stopped being
	// durable, as uploadDue measures from. Zero if unknown.
	undurableSince time.Time
	// uploadable is set when atelet has an object storage location to upload
	// the actor's undurable snapshot to.
	uploadable bool
}

// undurableActorsCounts is one reading of an undurableActorsTally.
type undurableActorsCounts struct {
	// overdueBands counts the actors whose background upload is past
	// uploadDelay, by how long past it they are.
	overdueBands [numOverdueBands]int64
	// oldestUndurable is the age of the oldest undurable local snapshot: zero
	// when there is none.
	oldestUndurable time.Duration
}

func newUndurableActorsTally(clk clock.PassiveClock, uploadDelay time.Duration, hasStore bool) *undurableActorsTally {
	return &undurableActorsTally{
		clock:       clk,
		uploadDelay: uploadDelay,
		hasStore:    hasStore,
		actors:      map[string]undurableActorSummary{},
	}
}

// observe records the actor's undurable local snapshot, or drops the actor
// once its latest local snapshot is durable or detached. The caller holds the
// actor's lock.
func (l *undurableActorsTally) observe(a *actorSnapshots) {
	if l == nil {
		return
	}
	if a.LatestLocal == nil || !a.uploadNeeded(*a.LatestLocal) {
		l.forget(a.actorUID)
		return
	}
	keepUploading := a.keepUploading()
	since := a.UndurableSince
	if since.IsZero() && keepUploading {
		since = a.Uploading.CommitTime
	}
	if since.IsZero() {
		since = a.LatestLocal.CommitTime
	}
	s := undurableActorSummary{
		undurableSince: since,
		uploadable:     l.hasStore && (a.canUpload(a.LatestLocal) || keepUploading),
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.actors[a.actorUID] = s
}

// forget drops the actor from the tally.
func (l *undurableActorsTally) forget(actorUID string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.actors, actorUID)
}

// counts reads the tally.
func (l *undurableActorsTally) counts() undurableActorsCounts {
	now := l.clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	var c undurableActorsCounts
	for _, s := range l.actors {
		if s.undurableSince.IsZero() {
			continue
		}
		age := now.Sub(s.undurableSince)
		c.oldestUndurable = max(c.oldestUndurable, age)
		if s.uploadable && age > l.uploadDelay {
			c.overdueBands[classifyOverdueBand(age-l.uploadDelay)]++
		}
	}
	return c
}
