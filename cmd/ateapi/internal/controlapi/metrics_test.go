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

package controlapi

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// newTestInstruments builds Instruments against a local ManualReader-backed
// provider so tests stay parallel-safe and never touch the global provider.
func newTestInstruments(t *testing.T, workers func() ([]*ateapipb.Worker, error), listPools func(labels.Selector) ([]*atev1alpha1.WorkerPool, error)) (*Instruments, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := NewInstruments(mp.Meter("ateapi"), workers, listPools)
	if err != nil {
		t.Fatalf("NewInstruments: %v", err)
	}
	return inst, reader
}

func noWorkers() ([]*ateapipb.Worker, error) { return nil, nil }

func noPools(labels.Selector) ([]*atev1alpha1.WorkerPool, error) { return nil, nil }

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

func mustMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Metrics {
	t.Helper()
	m, ok := collectMetric(t, reader, name)
	if !ok {
		t.Fatalf("metric %q not collected", name)
	}
	return m
}

func attrKeys(set attribute.Set) []string {
	ks := make([]string, 0, set.Len())
	for _, kv := range set.ToSlice() {
		ks = append(ks, string(kv.Key))
	}
	sort.Strings(ks)
	return ks
}

func equalKeys(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	w := append([]string(nil), want...)
	sort.Strings(w)
	for i := range got {
		if got[i] != w[i] {
			return false
		}
	}
	return true
}

func TestLifecycleOpDurationShape(t *testing.T) {
	inst, reader := newTestInstruments(t, noWorkers, noPools)

	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "a1"},
		ActorTemplateNamespace: "ns",
		ActorTemplateName:      "tmpl",
		WorkerPoolName:         "pool-a",
	}
	template := &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor}}
	inst.recordLifecycleOp(context.Background(), ateattr.OperationResume, time.Now(), nil,
		lifecycleOpAttrs(actor, template, ateattr.SnapshotKindGolden)...)

	m := mustMetric(t, reader, lifecycleOpDurationMetric)
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
	wantKeys := []string{
		string(ateattr.ActorOperationNameKey),
		string(ateattr.TemplateNameKey),
		string(ateattr.TemplateNamespaceKey),
		string(ateattr.WorkerPoolNameKey),
		string(ateattr.SandboxClassKey),
		string(ateattr.SnapshotKindKey),
	}
	if got := attrKeys(h.DataPoints[0].Attributes); !equalKeys(got, wantKeys) {
		t.Errorf("attribute keys = %v, want %v", got, wantKeys)
	}
}

func TestRecordLifecycleOp_OutcomeClassification(t *testing.T) {
	baseTmplAttrs := []attribute.KeyValue{
		ateattr.TemplateNameKey.String("tmpl"),
		ateattr.TemplateNamespaceKey.String("ns"),
	}
	tests := []struct {
		name          string
		op            string
		err           error
		extra         []attribute.KeyValue
		wantKeys      []string
		wantErrorType string
	}{
		{
			name:  "create success carries no error.type",
			op:    ateattr.OperationCreate,
			err:   nil,
			extra: baseTmplAttrs,
			wantKeys: []string{
				string(ateattr.ActorOperationNameKey),
				string(ateattr.TemplateNameKey),
				string(ateattr.TemplateNamespaceKey),
			},
		},
		{
			name:  "create NotFound",
			op:    ateattr.OperationCreate,
			err:   status.Error(codes.NotFound, "missing"),
			extra: baseTmplAttrs,
			wantKeys: []string{
				string(ateattr.ActorOperationNameKey),
				string(ateattr.TemplateNameKey),
				string(ateattr.TemplateNamespaceKey),
				string(ateattr.ErrorTypeKey),
			},
			wantErrorType: "NotFound",
		},
		{
			name:  "resume Aborted",
			op:    ateattr.OperationResume,
			err:   status.Error(codes.Aborted, "locked"),
			extra: baseTmplAttrs,
			wantKeys: []string{
				string(ateattr.ActorOperationNameKey),
				string(ateattr.TemplateNameKey),
				string(ateattr.TemplateNamespaceKey),
				string(ateattr.ErrorTypeKey),
			},
			wantErrorType: "Aborted",
		},
		{
			name:  "crash maps to DataLoss",
			op:    ateattr.OperationResume,
			err:   status.Errorf(codes.DataLoss, "actor crashed"),
			extra: baseTmplAttrs,
			wantKeys: []string{
				string(ateattr.ActorOperationNameKey),
				string(ateattr.TemplateNameKey),
				string(ateattr.TemplateNamespaceKey),
				string(ateattr.ErrorTypeKey),
			},
			wantErrorType: "DataLoss",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst, reader := newTestInstruments(t, noWorkers, noPools)
			inst.recordLifecycleOp(context.Background(), tt.op, time.Now(), tt.err, tt.extra...)

			m := mustMetric(t, reader, lifecycleOpDurationMetric)
			h := m.Data.(metricdata.Histogram[float64])
			if len(h.DataPoints) != 1 {
				t.Fatalf("got %d datapoints, want 1", len(h.DataPoints))
			}
			dp := h.DataPoints[0]
			if got := attrKeys(dp.Attributes); !equalKeys(got, tt.wantKeys) {
				t.Fatalf("attribute keys = %v, want %v", got, tt.wantKeys)
			}
			if op, _ := dp.Attributes.Value(ateattr.ActorOperationNameKey); op.AsString() != tt.op {
				t.Errorf("operation = %q, want %q", op.AsString(), tt.op)
			}
			gotErrType, hasErrType := dp.Attributes.Value(ateattr.ErrorTypeKey)
			if tt.wantErrorType == "" {
				if hasErrType {
					t.Errorf("unexpected error.type = %q", gotErrType.AsString())
				}
			} else if gotErrType.AsString() != tt.wantErrorType {
				t.Errorf("error.type = %q, want %q", gotErrType.AsString(), tt.wantErrorType)
			}
		})
	}
}

func TestSchedulerAssignmentShapeAndOutcomes(t *testing.T) {
	tests := []struct {
		name          string
		outcome       string
		pool          string
		err           error
		wantKeys      []string
		wantErrorType string
	}{
		{
			name:    "assigned stamps pool, no error.type",
			outcome: ateattr.SchedulerOutcomeAssigned,
			pool:    "pool-a",
			wantKeys: []string{
				string(ateattr.SchedulerOutcomeKey),
				string(ateattr.WorkerPoolNameKey),
			},
		},
		{
			name:    "no_free_worker has neither pool nor error.type",
			outcome: ateattr.SchedulerOutcomeNoFreeWorker,
			pool:    "",
			wantKeys: []string{
				string(ateattr.SchedulerOutcomeKey),
			},
		},
		{
			name:    "error stamps error.type but no pool",
			outcome: ateattr.SchedulerOutcomeError,
			pool:    "",
			err:     status.Error(codes.Internal, "boom"),
			wantKeys: []string{
				string(ateattr.SchedulerOutcomeKey),
				string(ateattr.ErrorTypeKey),
			},
			wantErrorType: "Internal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst, reader := newTestInstruments(t, noWorkers, noPools)
			inst.recordSchedulerAssignment(context.Background(), time.Now(), tt.outcome, tt.pool, tt.err)

			m := mustMetric(t, reader, schedulerAssignmentMetric)
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
			if got := attrKeys(dp.Attributes); !equalKeys(got, tt.wantKeys) {
				t.Fatalf("attribute keys = %v, want %v", got, tt.wantKeys)
			}
			if _, hasErrType := dp.Attributes.Value(ateattr.ErrorTypeKey); tt.wantErrorType == "" && hasErrType {
				t.Errorf("unexpected error.type on outcome %q", tt.outcome)
			}
		})
	}
}

func worker(pool, class string, assigned bool) *ateapipb.Worker {
	w := &ateapipb.Worker{WorkerPool: pool, SandboxClass: class}
	if assigned {
		w.Assignment = &ateapipb.Assignment{}
	}
	return w
}

func TestWorkerCountTally(t *testing.T) {
	workers := func() ([]*ateapipb.Worker, error) {
		return []*ateapipb.Worker{
			worker("pool-a", "gvisor", false),
			worker("pool-a", "gvisor", false),
			worker("pool-a", "gvisor", true),
			worker("pool-b", "microvm", false),
		}, nil
	}
	_, reader := newTestInstruments(t, workers, noPools)

	m := mustMetric(t, reader, workerpoolWorkersMetric)
	if m.Unit != "{worker}" {
		t.Errorf("unit = %q, want {worker}", m.Unit)
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("data type = %T, want Sum[int64]", m.Data)
	}
	if sum.IsMonotonic {
		t.Errorf("IsMonotonic = true, want false (updowncounter, not counter)")
	}

	type key struct{ pool, state, class string }
	got := make(map[key]int64)
	for _, dp := range sum.DataPoints {
		pool, _ := dp.Attributes.Value(ateattr.WorkerPoolNameKey)
		state, _ := dp.Attributes.Value(ateattr.WorkerStateKey)
		class, _ := dp.Attributes.Value(ateattr.SandboxClassKey)
		got[key{pool.AsString(), state.AsString(), class.AsString()}] = dp.Value
	}
	want := map[key]int64{
		{"pool-a", ateattr.WorkerStateIdle, "gvisor"}:     2,
		{"pool-a", ateattr.WorkerStateAssigned, "gvisor"}: 1,
		{"pool-b", ateattr.WorkerStateIdle, "microvm"}:    1,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d series, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("series %v = %d, want %d", k, got[k], v)
		}
	}
}

// TestWorkerCountSkipsWhenCacheNotReady asserts the callback emits nothing while
// the cache is warming up, so we never publish misleading zero-valued points.
func TestWorkerCountSkipsWhenCacheNotReady(t *testing.T) {
	notReady := func() ([]*ateapipb.Worker, error) {
		return nil, context.DeadlineExceeded
	}
	_, reader := newTestInstruments(t, notReady, noPools)

	if _, ok := collectMetric(t, reader, workerpoolWorkersMetric); ok {
		t.Errorf("%s was collected, want no datapoints while cache not ready", workerpoolWorkersMetric)
	}
}

func workerPool(name string, class atev1alpha1.SandboxClass) *atev1alpha1.WorkerPool {
	return &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       atev1alpha1.WorkerPoolSpec{SandboxClass: class},
	}
}

// TestWorkerCountSeedsZeroForKnownPools covers the saturation cases: a pool whose
// only state has no workers, and a pool with no workers at all, both report 0
// rather than an absent series. Empty pool class defaults to gvisor.
func TestWorkerCountSeedsZeroForKnownPools(t *testing.T) {
	pools := func(labels.Selector) ([]*atev1alpha1.WorkerPool, error) {
		return []*atev1alpha1.WorkerPool{
			workerPool("pool-a", ""),
			workerPool("pool-c", atev1alpha1.SandboxClassMicroVM),
		}, nil
	}
	workers := func() ([]*ateapipb.Worker, error) {
		return []*ateapipb.Worker{worker("pool-a", "gvisor", true)}, nil
	}
	_, reader := newTestInstruments(t, workers, pools)

	sum := mustMetric(t, reader, workerpoolWorkersMetric).Data.(metricdata.Sum[int64])
	type key struct{ pool, state, class string }
	got := make(map[key]int64)
	for _, dp := range sum.DataPoints {
		pool, _ := dp.Attributes.Value(ateattr.WorkerPoolNameKey)
		state, _ := dp.Attributes.Value(ateattr.WorkerStateKey)
		class, _ := dp.Attributes.Value(ateattr.SandboxClassKey)
		got[key{pool.AsString(), state.AsString(), class.AsString()}] = dp.Value
	}
	want := map[key]int64{
		{"pool-a", ateattr.WorkerStateIdle, "gvisor"}:      0,
		{"pool-a", ateattr.WorkerStateAssigned, "gvisor"}:  1,
		{"pool-c", ateattr.WorkerStateIdle, "microvm"}:     0,
		{"pool-c", ateattr.WorkerStateAssigned, "microvm"}: 0,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d series, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if gv, ok := got[k]; !ok || gv != v {
			t.Errorf("series %v = %d (present=%v), want %d", k, gv, ok, v)
		}
	}
}
