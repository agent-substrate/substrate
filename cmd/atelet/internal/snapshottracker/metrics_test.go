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

func collectUploadDuration(t *testing.T, reader *sdkmetric.ManualReader) (metricdata.Metrics, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == uploadDurationMetric {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

// recordedErrorTypes returns the error.type of each upload recorded on reader,
// "" for one that succeeded.
func recordedErrorTypes(t *testing.T, reader *sdkmetric.ManualReader) []string {
	t.Helper()
	m, ok := collectUploadDuration(t, reader)
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
func TestUploadErrorType(t *testing.T) {
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
			if got := uploadErrorType(tc.ctx, tc.err); got != tc.want.String() {
				t.Errorf("uploadErrorType = %q, want %q", got, tc.want.String())
			}
		})
	}
}
