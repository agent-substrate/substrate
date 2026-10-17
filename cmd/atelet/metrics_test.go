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
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

const (
	testTemplateNamespace = "ate-agents"
	testTemplateName      = "support-agent"
)

// newTestInstruments builds the histograms against a local ManualReader so tests
// stay parallel-safe and never touch the global meter provider.
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

func collectHistogram(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %q not collected", name)
	return metricdata.Metrics{}
}

// phaseValues maps each recorded phase to the attribute set it carries.
func phaseValues(t *testing.T, m metricdata.Metrics) map[string]attribute.Set {
	t.Helper()
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s is %T, want a float64 histogram", m.Name, m.Data)
	}
	byPhase := make(map[string]attribute.Set, len(hist.DataPoints))
	for _, dp := range hist.DataPoints {
		v, ok := dp.Attributes.Value(ateattr.SnapshotPhaseKey)
		if !ok {
			t.Errorf("datapoint without a phase attribute: %v", dp.Attributes.ToSlice())
			continue
		}
		byPhase[v.AsString()] = dp.Attributes
	}
	return byPhase
}

func attrString(t *testing.T, set attribute.Set, k attribute.Key) string {
	t.Helper()
	v, ok := set.Value(k)
	if !ok {
		t.Errorf("missing attribute %s in %v", k, set.ToSlice())
		return ""
	}
	return v.AsString()
}

func TestRestoreDurationShape(t *testing.T) {
	inst, reader := newTestInstruments(t)

	op := snapshotOp{
		templateNamespace: testTemplateNamespace,
		templateName:      testTemplateName,
		kind:              ateattr.SnapshotKindLatest,
		fidelity:          ateattr.SnapshotFidelityVolumes,
		sandboxClass:      "gvisor",
	}
	inst.recordRestore(context.Background(), op,
		phase{ateattr.SnapshotPhaseDownload, 2 * time.Second},
		phase{ateattr.SnapshotPhaseTotal, 3 * time.Second})

	m := collectHistogram(t, reader, restoreDurationMetric)
	if m.Unit != "s" {
		t.Errorf("unit = %q, want %q", m.Unit, "s")
	}
	if m.Description == "" {
		t.Error("description is empty")
	}

	byPhase := phaseValues(t, m)
	if len(byPhase) != 2 {
		t.Fatalf("recorded %d phases, want download and total", len(byPhase))
	}
	got := byPhase[ateattr.SnapshotPhaseDownload]
	for _, tc := range []struct {
		key  attribute.Key
		want string
	}{
		{ateattr.TemplateAtespaceKey, testTemplateNamespace},
		{ateattr.TemplateNameKey, testTemplateName},
		{ateattr.SnapshotKindKey, ateattr.SnapshotKindLatest},
		{ateattr.SnapshotFidelityKey, ateattr.SnapshotFidelityVolumes},
		{ateattr.SandboxClassKey, "gvisor"},
	} {
		if v := attrString(t, got, tc.key); v != tc.want {
			t.Errorf("%s = %q, want %q", tc.key, v, tc.want)
		}
	}
	if _, ok := got.Value(ateattr.ErrorTypeKey); ok {
		t.Error("error.type present on a successful restore")
	}
	if _, ok := got.Value(ateattr.ActorNameKey); ok {
		t.Error("actor identity must never reach a metric datapoint")
	}
}

func TestCheckpointDurationShape(t *testing.T) {
	inst, reader := newTestInstruments(t)

	inst.recordCheckpoint(context.Background(), snapshotOp{
		templateNamespace: testTemplateNamespace,
		templateName:      testTemplateName,
		kind:              ateattr.SnapshotKindLocal,
		fidelity:          ateattr.SnapshotFidelityMemory,
		sandboxClass:      "microvm",
	},
		phase{ateattr.SnapshotPhasePersist, time.Second},
		phase{ateattr.SnapshotPhaseCleanup, 20 * time.Millisecond},
	)

	m := collectHistogram(t, reader, checkpointDurationMetric)
	if m.Unit != "s" {
		t.Errorf("unit = %q, want %q", m.Unit, "s")
	}
	byPhase := phaseValues(t, m)
	for _, wantPhase := range []string{ateattr.SnapshotPhasePersist, ateattr.SnapshotPhaseCleanup} {
		set, ok := byPhase[wantPhase]
		if !ok {
			t.Fatalf("missing phase %q", wantPhase)
		}
		if v := attrString(t, set, ateattr.SnapshotKindKey); v != ateattr.SnapshotKindLocal {
			t.Errorf("%s snapshot kind = %q, want %q", wantPhase, v, ateattr.SnapshotKindLocal)
		}
		if v := attrString(t, set, ateattr.SandboxClassKey); v != "microvm" {
			t.Errorf("%s sandbox class = %q, want microvm", wantPhase, v)
		}
	}
}

// TestCheckpointRecordsCleanup drives a Checkpoint: the node cleanup it does
// once the snapshot commits lands in the cleanup phase, a failed upload returns
// before it, and the phases fit in the total.
func TestCheckpointRecordsCleanup(t *testing.T) {
	for _, tc := range []struct {
		name        string
		putErr      error
		wantCleanup bool
	}{
		{name: "succeeded", wantCleanup: true},
		{name: "upload failed", putErr: errors.New("object store unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := runPluginActor(t)
			var reader *sdkmetric.ManualReader
			a.s.instruments, reader = newTestInstruments(t)
			a.store.putErr = tc.putErr
			_, err := a.checkpoint(t, a.snapshot("suspend-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE)
			if (err != nil) != (tc.putErr != nil) {
				t.Fatalf("Checkpoint = %v, want an error only from the upload", err)
			}

			sums := phaseSums(t, collectHistogram(t, reader, checkpointDurationMetric))
			if timed := sums[ateattr.SnapshotPhaseCleanup] > 0; timed != tc.wantCleanup {
				t.Errorf("cleanup = %vs, want it timed: %v", sums[ateattr.SnapshotPhaseCleanup], tc.wantCleanup)
			}
			var parts float64
			for name, v := range sums {
				if name != ateattr.SnapshotPhaseTotal {
					parts += v
				}
			}
			if total := sums[ateattr.SnapshotPhaseTotal]; parts > total {
				t.Errorf("phases sum to %vs, more than the %vs total", parts, total)
			}
		})
	}
}

// phaseSums maps each recorded phase to the sum of its observations.
func phaseSums(t *testing.T, m metricdata.Metrics) map[string]float64 {
	t.Helper()
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s is %T, want a float64 histogram", m.Name, m.Data)
	}
	sums := make(map[string]float64, len(hist.DataPoints))
	for _, dp := range hist.DataPoints {
		if v, ok := dp.Attributes.Value(ateattr.SnapshotPhaseKey); ok {
			sums[v.AsString()] += dp.Sum
		}
	}
	return sums
}

// TestRecordPhasesSkipsZeroPhases pins the absence rule: a phase that never
// started stays absent instead of landing in the percentiles as instantaneous.
func TestRecordPhasesSkipsZeroPhases(t *testing.T) {
	inst, reader := newTestInstruments(t)

	inst.recordRestore(context.Background(),
		snapshotOp{fidelity: ateattr.SnapshotFidelityMemory},
		phase{ateattr.SnapshotPhaseManifestFetch, 50 * time.Millisecond},
		phase{ateattr.SnapshotPhaseDownload, 2 * time.Second},
		phase{ateattr.SnapshotPhaseWorkerRestore, 0},
		phase{ateattr.SnapshotPhaseTotal, 2 * time.Second})

	byPhase := phaseValues(t, collectHistogram(t, reader, restoreDurationMetric))
	if _, ok := byPhase[ateattr.SnapshotPhaseWorkerRestore]; ok {
		t.Error("a phase that never started was recorded as a zero observation")
	}
	if len(byPhase) != 3 {
		t.Errorf("recorded %d phases, want 3", len(byPhase))
	}
}

// TestSnapshotOpAttrsOmitsUnknownDimensions covers a restore that fails before
// the manifest resolves: kind and sandbox class are unknowable there, and an
// empty-string series would be indistinguishable from a real one.
func TestSnapshotOpAttrsOmitsUnknownDimensions(t *testing.T) {
	attrs := snapshotOp{
		templateNamespace: testTemplateNamespace,
		templateName:      testTemplateName,
		fidelity:          ateattr.SnapshotFidelityMemory,
	}.attrs()
	for _, kv := range attrs {
		if kv.Key == ateattr.SnapshotKindKey || kv.Key == ateattr.SandboxClassKey {
			t.Errorf("attribute %s must be omitted while unknown, got %q", kv.Key, kv.Value.AsString())
		}
	}
}

// TestSnapshotOpAttrsNormalizesSandboxClass keeps an unvalidated manifest value
// from becoming an unbounded label.
func TestSnapshotOpAttrsNormalizesSandboxClass(t *testing.T) {
	attrs := snapshotOp{sandboxClass: "definitely-not-a-runtime"}.attrs()
	for _, kv := range attrs {
		if kv.Key == ateattr.SandboxClassKey && kv.Value.AsString() != ateattr.SandboxClassUnknown {
			t.Errorf("sandbox class = %q, want %q", kv.Value.AsString(), ateattr.SandboxClassUnknown)
		}
	}
}

// TestIsCollateral guards the durations: a leg cancelled by the other leg's
// failure recorded a partial duration, which would land in the healthy-path
// percentiles as a fast success and drag them down on every failed restore.
func TestIsCollateral(t *testing.T) {
	owner := errors.New("prepare bundles: no entrypoint")
	cancelled := errors.New("context canceled")

	tests := []struct {
		name     string
		groupErr error
		legErr   error
		want     bool
	}{
		{name: "leg that owns the error keeps its duration", groupErr: owner, legErr: owner, want: false},
		{name: "leg cancelled as collateral drops its duration", groupErr: owner, legErr: cancelled, want: true},
		{name: "leg that succeeded keeps its duration", groupErr: owner, legErr: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isCollateral(tt.groupErr, tt.legErr); got != tt.want {
				t.Errorf("isCollateral() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestAssetsAfterCollateral covers the other half of the collateral rule: a leg
// cancelled during the unpack had already finished fetching its assets, and
// since absence means "never ran", zeroing that reports a step that completed
// as one that never started.
func TestAssetsAfterCollateral(t *testing.T) {
	const assets = 1200 * time.Millisecond

	tests := []struct {
		name            string
		prepFailedPhase string
		want            time.Duration
	}{
		{
			name:            "cancelled during the asset fetch truncates it",
			prepFailedPhase: ateattr.SnapshotPhaseSandboxAssets,
			want:            0,
		},
		{
			name:            "cancelled during the unpack keeps the completed asset fetch",
			prepFailedPhase: ateattr.SnapshotPhaseOCIUnpack,
			want:            assets,
		},
		{
			name:            "an unattributed prep failure keeps it rather than guessing",
			prepFailedPhase: "",
			want:            assets,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := assetsAfterCollateral(tt.prepFailedPhase, assets); got != tt.want {
				t.Errorf("assetsAfterCollateral() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRestoreSnapshotKind(t *testing.T) {
	tests := []struct {
		name        string
		directLocal bool
		rec         *sandboxAssetsRecord
		want        string
	}{
		{
			name:        "restored from the local copy",
			directLocal: true,
			rec:         &sandboxAssetsRecord{Atespace: "team-a"},
			want:        ateattr.SnapshotKindLocal,
		},
		{
			name:        "a snapshot from another atespace served locally is still local",
			directLocal: true,
			rec:         &sandboxAssetsRecord{Atespace: "team-b"},
			want:        ateattr.SnapshotKindLocal,
		},
		{
			name: "external snapshot written in another atespace",
			rec:  &sandboxAssetsRecord{Atespace: "team-b"},
			want: ateattr.SnapshotKindLatest,
		},
		{
			name: "external snapshot written by a tenant actor",
			rec:  &sandboxAssetsRecord{Atespace: "team-a"},
			want: ateattr.SnapshotKindLatest,
		},
		{
			name: "manifest predating the identity fields",
			rec:  &sandboxAssetsRecord{},
			want: ateattr.SnapshotKindLatest,
		},
		{
			name: "external kind is unknowable until the manifest is read",
			rec:  nil,
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := restoreSnapshotKind(tt.directLocal, tt.rec); got != tt.want {
				t.Errorf("restoreSnapshotKind() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCheckpointSnapshotKind(t *testing.T) {
	deferred := &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED}
	immediate := &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE}
	tests := []struct {
		name string
		req  *ateletpb.CheckpointRequest
		want string
	}{
		{
			name: "pause writes the node-local snapshot",
			req:  &ateletpb.CheckpointRequest{UploadConfig: deferred, Atespace: "team-a"},
			want: ateattr.SnapshotKindLocal,
		},
		{
			name: "suspend writes the actor's durable snapshot",
			req:  &ateletpb.CheckpointRequest{UploadConfig: immediate, Atespace: "team-a"},
			want: ateattr.SnapshotKindLatest,
		},
		{
			name: "a durable checkpoint in another atespace is latest",
			req:  &ateletpb.CheckpointRequest{UploadConfig: immediate, Atespace: "team-b"},
			want: ateattr.SnapshotKindLatest,
		},
		{
			name: "a local checkpoint in another atespace is still local",
			req:  &ateletpb.CheckpointRequest{UploadConfig: deferred, Atespace: "team-b"},
			want: ateattr.SnapshotKindLocal,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkpointSnapshotKind(tt.req); got != tt.want {
				t.Errorf("checkpointSnapshotKind() = %q, want %q", got, tt.want)
			}
		})
	}
}
