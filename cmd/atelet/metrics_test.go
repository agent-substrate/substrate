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
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
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
		scope:             ateattr.SnapshotScopeData,
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
		{ateattr.SnapshotScopeKey, ateattr.SnapshotScopeData},
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
		scope:             ateattr.SnapshotScopeFull,
		sandboxClass:      "microvm",
	}, phase{ateattr.SnapshotPhasePersist, time.Second})

	m := collectHistogram(t, reader, checkpointDurationMetric)
	if m.Unit != "s" {
		t.Errorf("unit = %q, want %q", m.Unit, "s")
	}
	set := phaseValues(t, m)[ateattr.SnapshotPhasePersist]
	if v := attrString(t, set, ateattr.SnapshotKindKey); v != ateattr.SnapshotKindLocal {
		t.Errorf("snapshot kind = %q, want %q", v, ateattr.SnapshotKindLocal)
	}
	if v := attrString(t, set, ateattr.SandboxClassKey); v != "microvm" {
		t.Errorf("sandbox class = %q, want microvm", v)
	}
}

// TestRecordPhasesSkipsZeroPhases pins the absence rule: a phase that never
// started stays absent instead of landing in the percentiles as instantaneous.
func TestRecordPhasesSkipsZeroPhases(t *testing.T) {
	inst, reader := newTestInstruments(t)

	inst.recordRestore(context.Background(),
		snapshotOp{scope: ateattr.SnapshotScopeFull},
		phase{ateattr.SnapshotPhaseVolumeMount, 50 * time.Millisecond},
		phase{ateattr.SnapshotPhaseDownload, 2 * time.Second},
		phase{ateattr.SnapshotPhaseAteomRestore, 0},
		phase{ateattr.SnapshotPhaseTotal, 2 * time.Second})

	byPhase := phaseValues(t, collectHistogram(t, reader, restoreDurationMetric))
	if _, ok := byPhase[ateattr.SnapshotPhaseAteomRestore]; ok {
		t.Error("a phase that never started was recorded as a zero observation")
	}
	if len(byPhase) != 3 {
		t.Errorf("recorded %d phases, want 3", len(byPhase))
	}
}

// TestSnapshotOpAttrsOmitsUnknownDimensions covers an operation whose kind
// and sandbox class are unknown: an empty-string series would be
// indistinguishable from a real one.
func TestSnapshotOpAttrsOmitsUnknownDimensions(t *testing.T) {
	attrs := snapshotOp{
		templateNamespace: testTemplateNamespace,
		templateName:      testTemplateName,
		scope:             ateattr.SnapshotScopeFull,
	}.attrs()
	for _, kv := range attrs {
		if kv.Key == ateattr.SnapshotKindKey || kv.Key == ateattr.SandboxClassKey {
			t.Errorf("attribute %s must be omitted while unknown, got %q", kv.Key, kv.Value.AsString())
		}
	}
}

// TestSnapshotOpAttrsNormalizesSandboxClass keeps an unvalidated sandbox class
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
	external := func(uri string) *ateletpb.RestoreRequest {
		return &ateletpb.RestoreRequest{
			Type:   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
			Config: &ateletpb.RestoreRequest_ExternalConfig{ExternalConfig: &ateletpb.ExternalRestoreConfiguration{SnapshotUri: uri}},
		}
	}
	tests := []struct {
		name string
		req  *ateletpb.RestoreRequest
		want string
	}{
		{
			name: "local pause snapshot",
			req:  &ateletpb.RestoreRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL},
			want: ateattr.SnapshotKindLocal,
		},
		{
			name: "golden tag an actor borrows",
			req:  external("gs://bucket/root/atespaces/" + resources.GoldenActorAtespace + "/tags/" + snapshotOwnerUID),
			want: ateattr.SnapshotKindGolden,
		},
		{
			name: "golden actor's own snapshot",
			req:  external("gs://bucket/root/atespaces/" + resources.GoldenActorAtespace + "/actors/" + snapshotOwnerUID + "/snapshots/snap-1"),
			want: ateattr.SnapshotKindGolden,
		},
		{
			name: "user tag an actor borrows",
			req:  external("gs://bucket/root/atespaces/ate-demo/tags/" + snapshotOwnerUID),
			want: ateattr.SnapshotKindLatest,
		},
		{
			name: "external snapshot of the actor",
			req:  external(testSnapshotURI),
			want: ateattr.SnapshotKindLatest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := restoreSnapshotKind(tt.req); got != tt.want {
				t.Errorf("restoreSnapshotKind() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCheckpointSnapshotKind(t *testing.T) {
	tests := []struct {
		name string
		req  *ateletpb.CheckpointRequest
		want string
	}{
		{
			name: "pause writes the node-local snapshot",
			req:  &ateletpb.CheckpointRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL, Atespace: "team-a"},
			want: ateattr.SnapshotKindLocal,
		},
		{
			name: "suspend writes the actor's durable snapshot",
			req:  &ateletpb.CheckpointRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL, Atespace: "team-a"},
			want: ateattr.SnapshotKindLatest,
		},
		{
			name: "a golden actor's commit writes the template's golden",
			req:  &ateletpb.CheckpointRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL, Atespace: resources.GoldenActorAtespace},
			want: ateattr.SnapshotKindGolden,
		},
		{
			name: "a local checkpoint in the golden atespace is still local",
			req:  &ateletpb.CheckpointRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL, Atespace: resources.GoldenActorAtespace},
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

// patternBytes returns a byte slice of length n filled with a repeating
// non-zero byte pattern so test data is incompressible and not treated as zero
// blocks.
func patternBytes(n int64) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i%251 + 1)
	}
	return b
}

func TestAllocatedBytesSparseImage(t *testing.T) {
	const (
		apparent = 2 << 30 // 2 GiB
		written  = 1 << 20 // 1 MiB
	)

	path := filepath.Join(t.TempDir(), "memory-ranges")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create sparse image: %v", err)
	}
	if _, err := f.Write(patternBytes(written)); err != nil {
		f.Close()
		t.Fatalf("write initial pages: %v", err)
	}
	if err := f.Truncate(apparent); err != nil {
		f.Close()
		t.Fatalf("extend sparse image: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close sparse image: %v", err)
	}

	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("lstat sparse image: %v", err)
	}
	if fi.Size() != apparent {
		t.Fatalf("fi.Size() = %d, want %d", fi.Size(), int64(apparent))
	}

	got := allocatedBytes(fi)
	if got >= apparent/2 {
		t.Skipf("file did not end up sparse (%d of %d bytes allocated); "+
			"this filesystem cannot report holes", got, int64(apparent))
	}
	if got == 0 || got > written*16 {
		t.Errorf("allocatedBytes() = %d, want > 0 and << %d (wrote %d)", got, int64(apparent), int64(written))
	}
}

func TestAllocatedBytesDenseImage(t *testing.T) {
	const size = 64 << 10

	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, patternBytes(size), 0o600); err != nil {
		t.Fatalf("write dense image: %v", err)
	}
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("lstat dense image: %v", err)
	}

	got := allocatedBytes(fi)
	if got < size || got > size*2 {
		t.Errorf("allocatedBytes() = %d, want ~%d", got, size)
	}
}
