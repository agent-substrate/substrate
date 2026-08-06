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
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
		scope:             ateattr.SnapshotScopeDataOnGolden,
		sandboxClass:      "gvisor",
	}
	inst.recordRestore(context.Background(), op, nil,
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
		{ateattr.TemplateNamespaceKey, testTemplateNamespace},
		{ateattr.TemplateNameKey, testTemplateName},
		{ateattr.SnapshotKindKey, ateattr.SnapshotKindLatest},
		{ateattr.SnapshotScopeKey, ateattr.SnapshotScopeDataOnGolden},
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
	}, nil, phase{ateattr.SnapshotPhasePersist, time.Second})

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

// TestRecordPhasesSkipsPhasesThatNeverRan is the failure-path contract: a
// restore that dies in the download still reports the phases it completed, with
// error.type set, and does not report the later ones as instantaneous.
func TestRecordPhasesSkipsPhasesThatNeverRan(t *testing.T) {
	inst, reader := newTestInstruments(t)

	inst.recordRestore(context.Background(), snapshotOp{scope: ateattr.SnapshotScopeFull},
		status.Error(codes.DataLoss, "snapshot gone"),
		phase{ateattr.SnapshotPhaseManifestFetch, 50 * time.Millisecond},
		phase{ateattr.SnapshotPhaseDownload, 2 * time.Second},
		phase{ateattr.SnapshotPhaseAteomRestore, 0},
		phase{ateattr.SnapshotPhaseTotal, 2 * time.Second})

	byPhase := phaseValues(t, collectHistogram(t, reader, restoreDurationMetric))
	if _, ok := byPhase[ateattr.SnapshotPhaseAteomRestore]; ok {
		t.Error("a phase that never ran was recorded as a zero observation")
	}
	for _, want := range []string{ateattr.SnapshotPhaseManifestFetch, ateattr.SnapshotPhaseDownload, ateattr.SnapshotPhaseTotal} {
		set, ok := byPhase[want]
		if !ok {
			t.Errorf("phase %q missing", want)
			continue
		}
		if v := attrString(t, set, ateattr.ErrorTypeKey); v != codes.DataLoss.String() {
			t.Errorf("phase %q error.type = %q, want %q", want, v, codes.DataLoss)
		}
	}
}

// TestSnapshotOpAttrsOmitsUnknownDimensions covers a restore that fails before
// the manifest resolves: kind and sandbox class are unknowable there, and an
// empty-string series would be indistinguishable from a real one.
func TestSnapshotOpAttrsOmitsUnknownDimensions(t *testing.T) {
	attrs := snapshotOp{
		templateNamespace: testTemplateNamespace,
		templateName:      testTemplateName,
		scope:             ateattr.SnapshotScopeFull,
	}.attrs(nil)
	for _, kv := range attrs {
		if kv.Key == ateattr.SnapshotKindKey || kv.Key == ateattr.SandboxClassKey {
			t.Errorf("attribute %s must be omitted while unknown, got %q", kv.Key, kv.Value.AsString())
		}
	}
}

// TestSnapshotOpAttrsNormalizesSandboxClass keeps an unvalidated manifest value
// from becoming an unbounded label.
func TestSnapshotOpAttrsNormalizesSandboxClass(t *testing.T) {
	attrs := snapshotOp{sandboxClass: "definitely-not-a-runtime"}.attrs(nil)
	for _, kv := range attrs {
		if kv.Key == ateattr.SandboxClassKey && kv.Value.AsString() != ateattr.SandboxClassUnknown {
			t.Errorf("sandbox class = %q, want %q", kv.Value.AsString(), ateattr.SandboxClassUnknown)
		}
	}
}

func TestRestoreSnapshotKind(t *testing.T) {
	tests := []struct {
		name string
		req  *ateletpb.RestoreRequest
		rec  *sandboxAssetsRecord
		want string
	}{
		{
			name: "local pause snapshot",
			req:  &ateletpb.RestoreRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL},
			rec:  &sandboxAssetsRecord{Atespace: "team-a"},
			want: ateattr.SnapshotKindLocal,
		},
		{
			name: "local restore is classifiable before the manifest is read",
			req:  &ateletpb.RestoreRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL},
			rec:  nil,
			want: ateattr.SnapshotKindLocal,
		},
		{
			name: "external snapshot written by a golden actor",
			req:  &ateletpb.RestoreRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL},
			rec:  &sandboxAssetsRecord{Atespace: resources.GoldenActorAtespace},
			want: ateattr.SnapshotKindGolden,
		},
		{
			name: "external snapshot written by a tenant actor",
			req:  &ateletpb.RestoreRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL},
			rec:  &sandboxAssetsRecord{Atespace: "team-a"},
			want: ateattr.SnapshotKindLatest,
		},
		{
			name: "manifest predating the identity fields",
			req:  &ateletpb.RestoreRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL},
			rec:  &sandboxAssetsRecord{},
			want: ateattr.SnapshotKindLatest,
		},
		{
			name: "external kind is unknowable until the manifest is read",
			req:  &ateletpb.RestoreRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL},
			rec:  nil,
			want: "",
		},
		{
			name: "data on golden keeps the actor snapshot's own kind",
			req: &ateletpb.RestoreRequest{
				Type:  ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
				Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
			},
			rec:  &sandboxAssetsRecord{Atespace: "team-a"},
			want: ateattr.SnapshotKindLocal,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := restoreSnapshotKind(tt.req, tt.rec); got != tt.want {
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
