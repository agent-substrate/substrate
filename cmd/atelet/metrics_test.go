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
	"sort"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newAteletReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	// Assign the package vars directly rather than swapping the global provider,
	// so the test never leaks a MeterProvider into other tests.
	var err error
	restoreDuration, err = buildDurationHistogram(mp.Meter("atelet"), restoreDurationMetric, "restore")
	if err != nil {
		t.Fatalf("build restore histogram: %v", err)
	}
	checkpointDuration, err = buildDurationHistogram(mp.Meter("atelet"), checkpointDurationMetric, "checkpoint")
	if err != nil {
		t.Fatalf("build checkpoint histogram: %v", err)
	}
	t.Cleanup(func() { restoreDuration, checkpointDuration = nil, nil })
	return reader
}

func ateletMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Metrics {
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

func histoAttrKeys(set attribute.Set) []string {
	ks := make([]string, 0, set.Len())
	for _, kv := range set.ToSlice() {
		ks = append(ks, string(kv.Key))
	}
	sort.Strings(ks)
	return ks
}

func wantKeys(keys ...attribute.Key) []string {
	ks := make([]string, 0, len(keys))
	for _, k := range keys {
		ks = append(ks, string(k))
	}
	sort.Strings(ks)
	return ks
}

func TestRestoreDurationShape(t *testing.T) {
	reader := newAteletReader(t)
	recordRestorePhase(context.Background(), ateattr.SnapshotPhaseDownload, 1500*time.Millisecond,
		"ns", "tmpl", "gvisor", ateattr.SnapshotKindGolden)

	m := ateletMetric(t, reader, restoreDurationMetric)
	if m.Unit != "s" {
		t.Errorf("unit = %q, want s", m.Unit)
	}
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("data type = %T, want Histogram[float64]", m.Data)
	}
	if len(h.DataPoints) != 1 {
		t.Fatalf("got %d datapoints, want 1", len(h.DataPoints))
	}
	dp := h.DataPoints[0]
	want := wantKeys(ateattr.SnapshotPhaseKey, ateattr.ActorTemplateNamespaceKey,
		ateattr.ActorTemplateNameKey, ateattr.SandboxClassKey, ateattr.SnapshotKindKey)
	if got := histoAttrKeys(dp.Attributes); !equalStrings(got, want) {
		t.Errorf("attribute keys = %v, want %v", got, want)
	}
	if kind, _ := dp.Attributes.Value(ateattr.SnapshotKindKey); kind.AsString() != ateattr.SnapshotKindGolden {
		t.Errorf("snapshot.kind = %q, want %q", kind.AsString(), ateattr.SnapshotKindGolden)
	}
}

func TestCheckpointDurationShape(t *testing.T) {
	reader := newAteletReader(t)
	recordCheckpointPhase(context.Background(), ateattr.SnapshotPhaseUpload, 800*time.Millisecond,
		"ns", "tmpl", "microvm")

	m := ateletMetric(t, reader, checkpointDurationMetric)
	if m.Unit != "s" {
		t.Errorf("unit = %q, want s", m.Unit)
	}
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("data type = %T, want Histogram[float64]", m.Data)
	}
	if len(h.DataPoints) != 1 {
		t.Fatalf("got %d datapoints, want 1", len(h.DataPoints))
	}
	dp := h.DataPoints[0]
	// No ate.snapshot.kind on checkpoint.
	want := wantKeys(ateattr.SnapshotPhaseKey, ateattr.ActorTemplateNamespaceKey,
		ateattr.ActorTemplateNameKey, ateattr.SandboxClassKey)
	if got := histoAttrKeys(dp.Attributes); !equalStrings(got, want) {
		t.Errorf("attribute keys = %v, want %v", got, want)
	}
	if _, ok := dp.Attributes.Value(ateattr.SnapshotKindKey); ok {
		t.Errorf("checkpoint must not carry %s", ateattr.SnapshotKindKey)
	}
}

func TestRestorePhaseClampsSnapshotKind(t *testing.T) {
	reader := newAteletReader(t)
	recordRestorePhase(context.Background(), ateattr.SnapshotPhaseDownload, time.Second, "ns", "tmpl", "gvisor", "$(unbounded-attacker-value)")

	m := ateletMetric(t, reader, restoreDurationMetric)
	h := m.Data.(metricdata.Histogram[float64])
	kind, _ := h.DataPoints[0].Attributes.Value(ateattr.SnapshotKindKey)
	if kind.AsString() != ateattr.SnapshotKindUnknown {
		t.Errorf("snapshot.kind = %q, want %q (clamped)", kind.AsString(), ateattr.SnapshotKindUnknown)
	}
}

func TestRestorePhaseValues(t *testing.T) {
	reader := newAteletReader(t)
	phases := []string{ateattr.SnapshotPhaseDownload, ateattr.SnapshotPhaseOCIUnpack, ateattr.SnapshotPhaseAteomRestore}
	for _, p := range phases {
		recordRestorePhase(context.Background(), p, time.Second, "ns", "tmpl", "gvisor", ateattr.SnapshotKindLatest)
	}

	m := ateletMetric(t, reader, restoreDurationMetric)
	h := m.Data.(metricdata.Histogram[float64])
	got := make(map[string]bool)
	for _, dp := range h.DataPoints {
		v, _ := dp.Attributes.Value(ateattr.SnapshotPhaseKey)
		got[v.AsString()] = true
	}
	for _, p := range phases {
		if !got[p] {
			t.Errorf("missing restore phase %q; got %v", p, got)
		}
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
