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
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

func newTestInstruments(t *testing.T) (*Instruments, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := NewInstruments(mp.Meter("atelet"))
	if err != nil {
		t.Fatalf("NewInstruments: %v", err)
	}
	return inst, reader
}

// collectMetric returns the instrument named name from reader, if collected.
func collectMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) (metricdata.Metrics, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func collectUploadDuration(t *testing.T, reader *sdkmetric.ManualReader) (metricdata.Metrics, bool) {
	t.Helper()
	return collectMetric(t, reader, uploadDurationMetric)
}

// recordedErrorTypes returns the error.type of each upload recorded on reader,
// "" for one that succeeded.
func recordedErrorTypes(t *testing.T, reader *sdkmetric.ManualReader) []string {
	t.Helper()
	return errorTypesOf(t, reader, uploadDurationMetric)
}

// errorTypesOf returns the error.type of each operation recorded on the
// histogram named name, "" for one that succeeded.
func errorTypesOf(t *testing.T, reader *sdkmetric.ManualReader, name string) []string {
	t.Helper()
	m, ok := collectMetric(t, reader, name)
	if !ok {
		return nil
	}
	var types []string
	for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
		v, _ := dp.Attributes.Value(ateattr.ErrorTypeKey)
		for range dp.Count {
			types = append(types, v.AsString())
		}
	}
	return types
}

func TestUploadDurationShape(t *testing.T) {
	inst, reader := newTestInstruments(t)
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	useActorDir(t)
	tr := newStoreTracker(t, store)
	tr.instruments = inst
	tr.Start()

	ref := testRef("s1", 1, tr.clock.Now())
	ref.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	tr.CommitLocal(trackedActorUID, ref)
	done := startUpload(t, tr, store)
	stepTo(tr, tr.clock.Now().Add(2*time.Second))
	store.release <- nil
	waitClosed(t, done, "the upload")

	m, ok := collectUploadDuration(t, reader)
	if !ok {
		t.Fatalf("metric %q not collected", uploadDurationMetric)
	}
	if m.Unit != "s" {
		t.Errorf("unit = %q, want %q", m.Unit, "s")
	}
	if m.Description == "" {
		t.Error("description is empty")
	}
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s is %T, want a float64 histogram", m.Name, m.Data)
	}
	if len(hist.DataPoints) != 1 {
		t.Fatalf("datapoints = %d, want 1", len(hist.DataPoints))
	}
	dp := hist.DataPoints[0]
	if dp.Count != 1 || dp.Sum != 2 {
		t.Errorf("count = %d, sum = %v, want 1 and 2", dp.Count, dp.Sum)
	}
	if got, _ := dp.Attributes.Value(ateattr.SnapshotFidelityKey); got.AsString() != ateattr.SnapshotFidelityVolumes {
		t.Errorf("fidelity = %q, want %q", got.AsString(), ateattr.SnapshotFidelityVolumes)
	}
	if _, ok := dp.Attributes.Value(ateattr.ErrorTypeKey); ok {
		t.Error("error.type present on a successful upload")
	}
	if _, ok := dp.Attributes.Value(attribute.Key("ate.actor.uid")); ok {
		t.Error("actor identity must never reach a metric datapoint")
	}
}

func TestUploadDurationFailureAndUnknownFidelity(t *testing.T) {
	inst, reader := newTestInstruments(t)
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	useActorDir(t)
	tr := newStoreTracker(t, store)
	tr.instruments = inst
	tr.Start()

	ref := testRef("s1", 1, tr.clock.Now())
	ref.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
	tr.CommitLocal(trackedActorUID, ref)
	uploadNow(t, tr, store, apierror.Unavailable("object store down"))

	m, ok := collectUploadDuration(t, reader)
	if !ok {
		t.Fatalf("metric %q not collected", uploadDurationMetric)
	}
	hist := m.Data.(metricdata.Histogram[float64])
	if len(hist.DataPoints) != 1 {
		t.Fatalf("datapoints = %d, want 1", len(hist.DataPoints))
	}
	dp := hist.DataPoints[0]
	if dp.Count != 1 {
		t.Errorf("count = %d, want 1", dp.Count)
	}
	if got, _ := dp.Attributes.Value(ateattr.SnapshotFidelityKey); got.AsString() != "unknown" {
		t.Errorf("fidelity = %q, want unknown", got.AsString())
	}
	if got, _ := dp.Attributes.Value(ateattr.ErrorTypeKey); got.AsString() != codes.Unavailable.String() {
		t.Errorf("error.type = %q, want %q", got.AsString(), codes.Unavailable.String())
	}
}

// Background detaches that upload a snapshot record on atelet.snapshot.upload.duration,
// while foreground detaches, already-durable background detaches, and probes
// that find the remote manifest committed already do not.
func TestUploadDurationSkipsNonUploadingPaths(t *testing.T) {
	t.Run("background detach that uploads records", func(t *testing.T) {
		inst, reader := newTestInstruments(t)
		store := newFakeSnapshotStore()
		useActorDir(t)
		tr := newStoreTracker(t, store)
		tr.instruments = inst
		setWorkers(tr, 0, DefaultDetachWorkers)
		tr.Start()

		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		detachNow(t, tr)
		m, ok := collectUploadDuration(t, reader)
		if !ok {
			t.Fatalf("metric %q not collected after a background detach that uploaded", uploadDurationMetric)
		}
		var uploads uint64
		for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
			uploads += dp.Count
		}
		if uploads != 1 {
			t.Errorf("recorded uploads = %d, want 1", uploads)
		}
	})

	t.Run("foreground detach does not record", func(t *testing.T) {
		inst, reader := newTestInstruments(t)
		store := newFakeSnapshotStore()
		useActorDir(t)
		tr := newStoreTracker(t, store)
		tr.instruments = inst
		tr.Start()

		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		if _, err := tr.Detach(t.Context(), trackedActorUID); err != nil {
			t.Fatalf("Detach: %v", err)
		}
		if _, ok := collectUploadDuration(t, reader); ok {
			t.Error("foreground Detach recorded on atelet.snapshot.upload.duration")
		}
	})

	t.Run("already durable background detach does not record", func(t *testing.T) {
		inst, reader := newTestInstruments(t)
		store := newFakeSnapshotStore()
		useActorDir(t)
		tr := newStoreTracker(t, store)
		tr.instruments = inst
		tr.Start()

		ref := testRef("s1", 1, tr.clock.Now())
		tr.CommitDurable(trackedActorUID, ref)
		tr.CommitLocal(trackedActorUID, ref)
		detachNow(t, tr)
		if _, ok := collectUploadDuration(t, reader); ok {
			t.Error("already-durable background detach recorded on atelet.snapshot.upload.duration")
		}
	})

	t.Run("probe finding remote manifest committed does not record", func(t *testing.T) {
		inst, reader := newTestInstruments(t)
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		useActorDir(t)
		tr := newStoreTracker(t, store)
		tr.instruments = inst
		tr.Start()

		ref := testRef("s1", 1, tr.clock.Now())
		store.uploaded = map[string]bool{ref.uri(): true}
		tr.CommitLocal(trackedActorUID, ref)
		uploadNow(t, tr, store, nil)
		if _, ok := collectUploadDuration(t, reader); ok {
			t.Error("probe without upload recorded on atelet.snapshot.upload.duration")
		}
	})
}

// A failed upload's error.type is Canceled once its context has ended, as
// when Terminate or shutdown cancels it, even though the plugin reports a
// gRPC status, and otherwise the code an apierror or a wrapped gRPC status
// carries.
func TestErrorType(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want codes.Code
	}{
		{
			name: "canceled",
			ctx:  canceled,
			err:  fmt.Errorf("while uploading snapshot files: %w", status.Error(codes.Canceled, "context canceled")),
			want: codes.Canceled,
		},
		{
			name: "apierror",
			ctx:  context.Background(),
			err:  apierror.Unavailable("plugin: unreachable"),
			want: codes.Unavailable,
		},
		{
			name: "plugin status",
			ctx:  context.Background(),
			err:  fmt.Errorf("while uploading snapshot files: %w", status.Error(codes.PermissionDenied, "denied")),
			want: codes.PermissionDenied,
		},
		{
			name: "plain error",
			ctx:  context.Background(),
			err:  errors.New("boom"),
			want: codes.Internal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := errorType(tc.ctx, tc.err); got != tc.want.String() {
				t.Errorf("errorType = %q, want %q", got, tc.want.String())
			}
		})
	}
}

// histogramOf returns the float64 histogram named name from reader, failing
// the test unless it was collected with unit seconds.
func histogramOf(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Histogram[float64] {
	t.Helper()
	m, ok := collectMetric(t, reader, name)
	if !ok {
		t.Fatalf("metric %q not collected", name)
	}
	if m.Unit != "s" {
		t.Errorf("%s unit = %q, want %q", name, m.Unit, "s")
	}
	if m.Description == "" {
		t.Errorf("%s description is empty", name)
	}
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s is %T, want a float64 histogram", name, m.Data)
	}
	for _, dp := range hist.DataPoints {
		if _, ok := dp.Attributes.Value(attribute.Key("ate.actor.uid")); ok {
			t.Errorf("%s carries actor identity, which must never reach a metric datapoint", name)
		}
	}
	return hist
}

// waitDetachEnded waits until a background detach of the tracked actor has
// ended, and recorded how: its operation is over and it either dropped the
// local snapshot or set a retry.
func waitDetachEnded(t *testing.T, tr *Tracker) {
	t.Helper()
	eventually(t, "the detach ends", func() bool {
		tr.lock(trackedActorUID)
		defer tr.unlock(trackedActorUID)
		a := tr.findLocked(trackedActorUID)
		return a != nil && a.op == nil && (a.LatestLocal == nil || !a.retryAt.IsZero())
	})
}

// A background detach records how long its local cleanup took, labeled with
// the snapshot's fidelity, and excludes any time spent uploading the snapshot
// first (which atelet.snapshot.upload.duration records instead).
func TestDetachDurationShape(t *testing.T) {
	inst, reader := newTestInstruments(t)
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	useActorDir(t)
	tr := newStoreTracker(t, store)
	tr.instruments = inst
	setWorkers(tr, 0, DefaultDetachWorkers)
	tr.Start()

	ref := testRef("s1", 1, tr.clock.Now())
	ref.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	tr.CommitLocal(trackedActorUID, ref)
	stepTo(tr, detachDueOf(tr, trackedActorUID))
	waitStarted(t, store)
	done := opDone(t, tr, trackedActorUID)
	stepTo(tr, tr.clock.Now().Add(3*time.Second))
	store.release <- nil
	waitClosed(t, done, "the detach")

	uploadHist := histogramOf(t, reader, uploadDurationMetric)
	if len(uploadHist.DataPoints) != 1 || uploadHist.DataPoints[0].Count != 1 || uploadHist.DataPoints[0].Sum != 3 {
		t.Errorf("upload histogram = %+v, want count=1 sum=3", uploadHist.DataPoints)
	}

	hist := histogramOf(t, reader, detachDurationMetric)
	if len(hist.DataPoints) != 1 {
		t.Fatalf("datapoints = %d, want 1", len(hist.DataPoints))
	}
	dp := hist.DataPoints[0]
	if dp.Count != 1 || dp.Sum != 0 {
		t.Errorf("count = %d, sum = %v, want 1 and 0 (upload time excluded)", dp.Count, dp.Sum)
	}
	if got, _ := dp.Attributes.Value(ateattr.SnapshotFidelityKey); got.AsString() != ateattr.SnapshotFidelityVolumes {
		t.Errorf("fidelity = %q, want %q", got.AsString(), ateattr.SnapshotFidelityVolumes)
	}
	if _, ok := dp.Attributes.Value(ateattr.ErrorTypeKey); ok {
		t.Error("error.type present on a successful detach")
	}
}

// A background detach whose local cleanup fails, or that Terminate cancels,
// records error.type; one whose upload fails stops there and records only on
// atelet.snapshot.upload.duration; a foreground Detach records nothing.
func TestDetachDurationOutcomes(t *testing.T) {
	newTracker := func(t *testing.T, store *fakeSnapshotStore) (*Tracker, *sdkmetric.ManualReader) {
		t.Helper()
		inst, reader := newTestInstruments(t)
		useActorDir(t)
		tr := newStoreTracker(t, store)
		tr.instruments = inst
		setWorkers(tr, 0, DefaultDetachWorkers)
		tr.Start()
		return tr, reader
	}

	t.Run("failed", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr, reader := newTracker(t, store)
		ref := testRef("s1", 1, tr.clock.Now())
		tr.CommitDurable(trackedActorUID, ref)
		tr.CommitLocal(trackedActorUID, ref)
		// A file where the local checkpoint directory belongs fails the prune.
		if err := os.WriteFile(ateletpath.LocalCheckpointsDir(trackedActorUID), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		stepTo(tr, detachDueOf(tr, trackedActorUID))
		waitDetachEnded(t, tr)
		if got, want := errorTypesOf(t, reader, detachDurationMetric), []string{codes.Internal.String()}; !slices.Equal(got, want) {
			t.Errorf("error types = %v, want %v", got, want)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		inst, reader := newTestInstruments(t)
		store := newFakeSnapshotStore()
		useActorDir(t)
		tr := newStoreTracker(t, store, WithInstruments(inst))
		ref := testRef("s1", 1, tr.clock.Now())
		tr.CommitDurable(trackedActorUID, ref)
		tr.CommitLocal(trackedActorUID, ref)
		stepTo(tr, detachDueOf(tr, trackedActorUID))
		run := tr.beginDueWork(trackedActorUID, opDetach)
		if run == nil {
			t.Fatal("beginDueWork returned nil, want due detach")
		}
		if err := tr.Forget(trackedActorUID); err != nil {
			t.Fatalf("Forget: %v", err)
		}
		run()
		if got, want := errorTypesOf(t, reader, detachDurationMetric), []string{codes.Canceled.String()}; !slices.Equal(got, want) {
			t.Errorf("error types = %v, want %v", got, want)
		}
	})

	t.Run("upload failure does not record", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr, reader := newTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		stepTo(tr, detachDueOf(tr, trackedActorUID))
		waitStarted(t, store)
		done := opDone(t, tr, trackedActorUID)
		store.release <- apierror.Unavailable("object store down")
		waitClosed(t, done, "the detach")
		if got, want := errorTypesOf(t, reader, uploadDurationMetric), []string{codes.Unavailable.String()}; !slices.Equal(got, want) {
			t.Errorf("upload error types = %v, want %v", got, want)
		}
		if _, ok := collectMetric(t, reader, detachDurationMetric); ok {
			t.Errorf("failed upload during detach recorded on %s", detachDurationMetric)
		}
	})

	t.Run("already durable", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr, reader := newTracker(t, store)
		ref := testRef("s1", 1, tr.clock.Now())
		tr.CommitDurable(trackedActorUID, ref)
		tr.CommitLocal(trackedActorUID, ref)
		detachNow(t, tr)
		if got, want := errorTypesOf(t, reader, detachDurationMetric), []string{""}; !slices.Equal(got, want) {
			t.Errorf("error types = %v, want %v", got, want)
		}
		if _, ok := collectUploadDuration(t, reader); ok {
			t.Error("a detach that uploaded nothing recorded an upload")
		}
	})

	t.Run("foreground", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr, reader := newTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		if _, err := tr.Detach(t.Context(), trackedActorUID); err != nil {
			t.Fatalf("Detach: %v", err)
		}
		if _, ok := collectMetric(t, reader, detachDurationMetric); ok {
			t.Errorf("foreground Detach recorded on %s", detachDurationMetric)
		}
	})
}

// Each background upload and detach records how long after it came due a
// worker started it, on its own histogram.
func TestOverdueDuration(t *testing.T) {
	inst, reader := newTestInstruments(t)
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	useActorDir(t)
	tr := newStoreTracker(t, store)
	tr.instruments = inst
	tr.Start()

	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	uploadDue := timeOf(tr.State(trackedActorUID).GetUploadDueTime())
	stepTo(tr, uploadDue.Add(5*time.Second))
	waitStarted(t, store)
	done := opDone(t, tr, trackedActorUID)
	store.release <- nil
	waitClosed(t, done, "the upload")

	// The snapshot is durable now, so the detach only deletes it.
	stepTo(tr, detachDueOf(tr, trackedActorUID).Add(7*time.Second))
	waitDetachEnded(t, tr)

	for _, tc := range []struct {
		metric string
		want   float64
	}{
		{metric: uploadOverdueDurationMetric, want: 5},
		{metric: detachOverdueDurationMetric, want: 7},
	} {
		hist := histogramOf(t, reader, tc.metric)
		if len(hist.DataPoints) != 1 {
			t.Fatalf("%s datapoints = %d, want 1", tc.metric, len(hist.DataPoints))
		}
		dp := hist.DataPoints[0]
		if dp.Attributes.Len() != 0 {
			t.Errorf("%s labels = %v, want none", tc.metric, dp.Attributes.ToSlice())
		}
		if dp.Count != 1 || dp.Sum != tc.want {
			t.Errorf("%s count = %d, sum = %v, want 1 and %v", tc.metric, dp.Count, dp.Sum, tc.want)
		}
	}
}
