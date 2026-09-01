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
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateerrors"
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
		{ateattr.TemplateAtespaceKey, testTemplateNamespace},
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

// TestRecordPhasesFailurePath is the failure-path contract: a restore that dies
// in the download marks ate.failure.reason on that phase and on the total,
// leaves the phases that already succeeded unlabeled so their latency stays
// queryable, and does not report phases that never started as instantaneous.
func TestRecordPhasesFailurePath(t *testing.T) {
	inst, reader := newTestInstruments(t)

	downloadErr := fmt.Errorf("%w: while downloading snapshot", ateerrors.ReasonFailedGetExternalObject)
	inst.recordRestore(context.Background(),
		snapshotOp{scope: ateattr.SnapshotScopeFull, failedPhase: ateattr.SnapshotPhaseDownload},
		downloadErr,
		phase{ateattr.SnapshotPhaseManifestFetch, 50 * time.Millisecond},
		phase{ateattr.SnapshotPhaseDownload, 2 * time.Second},
		phase{ateattr.SnapshotPhaseAteomRestore, 0},
		phase{ateattr.SnapshotPhaseTotal, 2 * time.Second})

	byPhase := phaseValues(t, collectHistogram(t, reader, restoreDurationMetric))
	if _, ok := byPhase[ateattr.SnapshotPhaseAteomRestore]; ok {
		t.Error("a phase that never started was recorded as a zero observation")
	}

	wantReason := string(ateerrors.ReasonFailedGetExternalObject)
	tests := []struct {
		phase      string
		wantReason string // empty means ate.failure.reason must be absent
	}{
		{phase: ateattr.SnapshotPhaseDownload, wantReason: wantReason},
		{phase: ateattr.SnapshotPhaseTotal, wantReason: wantReason},
		{phase: ateattr.SnapshotPhaseManifestFetch, wantReason: ""},
	}
	for _, tt := range tests {
		t.Run(tt.phase, func(t *testing.T) {
			set, ok := byPhase[tt.phase]
			if !ok {
				t.Fatalf("phase %q missing", tt.phase)
			}
			got, present := set.Value(ateattr.FailureReasonKey)
			if tt.wantReason == "" {
				if present {
					t.Errorf("ate.failure.reason = %q on a phase that succeeded, want absent", got.AsString())
				}
				return
			}
			if !present || got.AsString() != tt.wantReason {
				t.Errorf("ate.failure.reason = %q (present=%v), want %q", got.AsString(), present, tt.wantReason)
			}
		})
	}
}

// TestRecordPhasesUnclassifiedFailure covers the infrastructure failures that
// carry no ateerrors.Reason (a dead object-storage endpoint, say): they must
// collapse onto UNKNOWN rather than leaking an error message into the label.
func TestRecordPhasesUnclassifiedFailure(t *testing.T) {
	inst, reader := newTestInstruments(t)

	inst.recordRestore(context.Background(),
		snapshotOp{scope: ateattr.SnapshotScopeFull, failedPhase: ateattr.SnapshotPhaseManifestFetch},
		fmt.Errorf("dial tcp 10.96.192.187:9000: connect: connection refused"),
		phase{ateattr.SnapshotPhaseManifestFetch, 30 * time.Millisecond},
		phase{ateattr.SnapshotPhaseTotal, 30 * time.Millisecond})

	set := phaseValues(t, collectHistogram(t, reader, restoreDurationMetric))[ateattr.SnapshotPhaseManifestFetch]
	if v := attrString(t, set, ateattr.FailureReasonKey); v != ateattr.ReasonUnknown {
		t.Errorf("ate.failure.reason = %q, want %q", v, ateattr.ReasonUnknown)
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

// TestGroupFailedPhase covers the concurrent leg of a restore: whichever
// goroutine fails first cancels the shared context, so the other one also
// returns an error, and only the one whose error errgroup actually surfaced may
// claim the phase.
func TestGroupFailedPhase(t *testing.T) {
	download := errors.New("download: connection reset")
	prep := errors.New("prepare bundles: no entrypoint")
	cancelled := errors.New("context canceled")

	tests := []struct {
		name        string
		err         error
		downloadErr error
		prepErr     error
		prepPhase   string
		want        string
	}{
		{
			name:        "download failed alone",
			err:         download,
			downloadErr: download,
			want:        ateattr.SnapshotPhaseDownload,
		},
		{
			name:      "prep failed alone during the asset fetch",
			err:       prep,
			prepErr:   prep,
			prepPhase: ateattr.SnapshotPhaseSandboxAssets,
			want:      ateattr.SnapshotPhaseSandboxAssets,
		},
		{
			name:        "prep failed first and the in-flight download was collateral",
			err:         prep,
			downloadErr: cancelled,
			prepErr:     prep,
			prepPhase:   ateattr.SnapshotPhaseOCIUnpack,
			want:        ateattr.SnapshotPhaseOCIUnpack,
		},
		{
			name:        "download failed first and prep was collateral",
			err:         download,
			downloadErr: download,
			prepErr:     cancelled,
			prepPhase:   ateattr.SnapshotPhaseSandboxAssets,
			want:        ateattr.SnapshotPhaseDownload,
		},
		{
			name: "error from neither leg claims no phase",
			err:  errors.New("something else entirely"),
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := groupFailedPhase(tt.err, tt.downloadErr, tt.prepErr, tt.prepPhase); got != tt.want {
				t.Errorf("groupFailedPhase() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestIsCollateral guards the durations the same way TestGroupFailedPhase
// guards the label: a leg cancelled by the other leg's failure recorded an
// unlabeled partial duration, which would land in the healthy-path percentiles
// as a fast success and drag them down on every failed restore.
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
