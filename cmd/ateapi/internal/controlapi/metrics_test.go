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

// newWorkerCountReader registers ate.workerpool.workers against a local
// ManualReader-backed provider so tests stay parallel-safe and never touch the
// global meter provider.
func newWorkerCountReader(t *testing.T, workers func() ([]*ateapipb.Worker, error), listPools func(labels.Selector) ([]*atev1alpha1.WorkerPool, error)) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	if err := RegisterWorkerCount(mp.Meter("ateapi"), workers, listPools); err != nil {
		t.Fatalf("RegisterWorkerCount: %v", err)
	}
	return reader
}

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

func worker(namespace, pool, class string, assigned bool) *ateapipb.Worker {
	w := &ateapipb.Worker{WorkerNamespace: namespace, WorkerPool: pool, SandboxClass: class, Status: &ateapipb.WorkerStatus{}}
	if assigned {
		w.Status.Assignment = &ateapipb.ActorAssignment{}
	}
	return w
}

type series struct{ namespace, pool, state, class string }

func seriesCounts(sum metricdata.Sum[int64]) map[series]int64 {
	got := make(map[series]int64)
	for _, dp := range sum.DataPoints {
		namespace, _ := dp.Attributes.Value(ateattr.WorkerPoolNamespaceKey)
		pool, _ := dp.Attributes.Value(ateattr.WorkerPoolNameKey)
		state, _ := dp.Attributes.Value(ateattr.WorkerStateKey)
		class, _ := dp.Attributes.Value(ateattr.SandboxClassKey)
		got[series{namespace.AsString(), pool.AsString(), state.AsString(), class.AsString()}] = dp.Value
	}
	return got
}

// TestWorkerCountTally covers the tally, including two pools that share a name in
// different namespaces: a WorkerPool is namespaced, so those must stay two series.
func TestWorkerCountTally(t *testing.T) {
	workers := func() ([]*ateapipb.Worker, error) {
		return []*ateapipb.Worker{
			worker("ns-1", "pool-a", "gvisor", false),
			worker("ns-1", "pool-a", "gvisor", false),
			worker("ns-1", "pool-a", "gvisor", true),
			worker("ns-1", "pool-b", "microvm", false),
			worker("ns-2", "pool-a", "gvisor", false),
		}, nil
	}
	reader := newWorkerCountReader(t, workers, noPools)

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

	got := seriesCounts(sum)
	want := map[series]int64{
		{"ns-1", "pool-a", ateattr.WorkerStateIdle, "gvisor"}:     2,
		{"ns-1", "pool-a", ateattr.WorkerStateAssigned, "gvisor"}: 1,
		{"ns-1", "pool-b", ateattr.WorkerStateIdle, "microvm"}:    1,
		{"ns-2", "pool-a", ateattr.WorkerStateIdle, "gvisor"}:     1,
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
	reader := newWorkerCountReader(t, notReady, noPools)

	if _, ok := collectMetric(t, reader, workerpoolWorkersMetric); ok {
		t.Errorf("%s was collected, want no datapoints while cache not ready", workerpoolWorkersMetric)
	}
}

// newTestInstruments builds the lifecycle/scheduler histograms on a local
// ManualReader-backed provider so tests stay parallel-safe and never touch the
// global meter provider.
func newTestInstruments(t *testing.T) (*Instruments, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := NewInstruments(mp.Meter("ateapi"))
	if err != nil {
		t.Fatalf("NewInstruments: %v", err)
	}
	return inst, reader
}

// singleHistogramDP asserts name is a float histogram in seconds with exactly one
// datapoint and returns it.
func singleHistogramDP(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	m := mustMetric(t, reader, name)
	if m.Unit != "s" {
		t.Errorf("%s unit = %q, want s", name, m.Unit)
	}
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s data type = %T, want Histogram[float64]", name, m.Data)
	}
	if len(hist.DataPoints) != 1 {
		t.Fatalf("%s: got %d datapoints, want 1", name, len(hist.DataPoints))
	}
	return hist.DataPoints[0]
}

// assertAttrKeys asserts the datapoint carries exactly want, order-independent.
func assertAttrKeys(t *testing.T, dp metricdata.HistogramDataPoint[float64], want ...attribute.Key) {
	t.Helper()
	got := make(map[attribute.Key]bool, dp.Attributes.Len())
	for _, kv := range dp.Attributes.ToSlice() {
		got[kv.Key] = true
	}
	if len(got) != len(want) {
		t.Errorf("attribute keys = %v, want %v", dp.Attributes.ToSlice(), want)
	}
	for _, k := range want {
		if !got[k] {
			t.Errorf("missing attribute key %s; got %v", k, dp.Attributes.ToSlice())
		}
	}
}

// attrString returns the string value of key k and whether it is present.
func attrString(dp metricdata.HistogramDataPoint[float64], k attribute.Key) (string, bool) {
	v, ok := dp.Attributes.Value(k)
	return v.AsString(), ok
}

func TestLifecycleOpDurationShape(t *testing.T) {
	inst, reader := newTestInstruments(t)

	actor := &ateapipb.Actor{
		ActorTemplateName:      "support-agent",
		ActorTemplateNamespace: "ate-agents",
		Status: &ateapipb.ActorStatus{
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerNamespace: "ate-workers", WorkerPool: "pool-a"},
		},
	}
	template := mustTemplateFromCRD(&atev1alpha1.ActorTemplate{
		Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor},
	})
	inst.recordLifecycleOp(context.Background(), ateattr.OperationResume, time.Now(), nil,
		lifecycleOpAttrs(actor, template, ateattr.SnapshotKindLatest, ateattr.SnapshotScopeDataOnGolden)...)

	dp := singleHistogramDP(t, reader, lifecycleOpDurationMetric)
	assertAttrKeys(t, dp,
		ateattr.ActorOperationNameKey,
		ateattr.TemplateNameKey,
		ateattr.TemplateNamespaceKey,
		ateattr.WorkerPoolNamespaceKey,
		ateattr.WorkerPoolNameKey,
		ateattr.SandboxClassKey,
		ateattr.SnapshotKindKey,
		ateattr.SnapshotScopeKey,
	)
	if op, _ := attrString(dp, ateattr.ActorOperationNameKey); op != ateattr.OperationResume {
		t.Errorf("operation = %q, want %q", op, ateattr.OperationResume)
	}
	// The pool is only identified by the pair: two pools may share a name in
	// different namespaces.
	if ns, _ := attrString(dp, ateattr.WorkerPoolNamespaceKey); ns != "ate-workers" {
		t.Errorf("worker pool namespace = %q, want %q", ns, "ate-workers")
	}
	// Kind and scope are independent: a data_on_golden restore of the actor's
	// own latest snapshot must stay distinguishable from one of a local snapshot.
	if scope, _ := attrString(dp, ateattr.SnapshotScopeKey); scope != ateattr.SnapshotScopeDataOnGolden {
		t.Errorf("snapshot scope = %q, want %q", scope, ateattr.SnapshotScopeDataOnGolden)
	}
	if kind, _ := attrString(dp, ateattr.SnapshotKindKey); kind != ateattr.SnapshotKindLatest {
		t.Errorf("snapshot kind = %q, want %q", kind, ateattr.SnapshotKindLatest)
	}
}

// TestLifecycleOpAttrsOmitsUnknownScope guards the failure path: a resume that
// dies before the restore request is built has no scope, and an empty-string
// series would be indistinguishable from a real one. The unassigned actor also
// pins the pool pair: a failure before the assign step emits neither key.
func TestLifecycleOpAttrsOmitsUnknownScope(t *testing.T) {
	actor := &ateapipb.Actor{ActorTemplateName: "support-agent", ActorTemplateNamespace: "ate-agents"}
	for _, kv := range lifecycleOpAttrs(actor, nil, "", "") {
		switch kv.Key {
		case ateattr.SnapshotScopeKey, ateattr.SnapshotKindKey, ateattr.WorkerPoolNamespaceKey, ateattr.WorkerPoolNameKey:
			t.Errorf("attribute %s must be omitted while unknown, got %q", kv.Key, kv.Value.AsString())
		}
	}
}

// TestRecordLifecycleOp_OutcomeClassification asserts success omits error.type and
// each gRPC failure maps onto its status-code string; the absence of error.type is
// the success signal, so there is no separate failure counter.
func TestRecordLifecycleOp_OutcomeClassification(t *testing.T) {
	tests := []struct {
		name          string
		op            string
		err           error
		wantErrorType string // empty means error.type must be absent
	}{
		{name: "create success", op: ateattr.OperationCreate, err: nil, wantErrorType: ""},
		{name: "create not found", op: ateattr.OperationCreate, err: status.Error(codes.NotFound, "missing"), wantErrorType: "NotFound"},
		{name: "resume aborted", op: ateattr.OperationResume, err: status.Error(codes.Aborted, "conflict"), wantErrorType: "Aborted"},
		{name: "resume crash", op: ateattr.OperationResume, err: status.Error(codes.DataLoss, "crashed"), wantErrorType: "DataLoss"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst, reader := newTestInstruments(t)
			inst.recordLifecycleOp(context.Background(), tt.op, time.Now(), tt.err,
				ateattr.TemplateNameKey.String("support-agent"),
				ateattr.TemplateNamespaceKey.String("ate-agents"),
			)

			dp := singleHistogramDP(t, reader, lifecycleOpDurationMetric)
			if op, _ := attrString(dp, ateattr.ActorOperationNameKey); op != tt.op {
				t.Errorf("operation = %q, want %q", op, tt.op)
			}
			gotErrType, hasErrType := attrString(dp, ateattr.ErrorTypeKey)
			if tt.wantErrorType == "" {
				if hasErrType {
					t.Errorf("error.type = %q, want absent", gotErrType)
				}
			} else if !hasErrType || gotErrType != tt.wantErrorType {
				t.Errorf("error.type = %q (present=%v), want %q", gotErrType, hasErrType, tt.wantErrorType)
			}
		})
	}
}

// TestSchedulerAssignmentShapeAndOutcomes asserts the assignment histogram stamps
// the pool pair only when a worker was assigned and error.type only for the error
// outcome, so no_free_worker (a capacity signal) carries neither.
func TestSchedulerAssignmentShapeAndOutcomes(t *testing.T) {
	tests := []struct {
		name          string
		outcome       string
		poolNamespace string
		pool          string
		class         string
		err           error
		wantKeys      []attribute.Key
		wantErrorType string
	}{
		{
			name:          "assigned stamps the pool pair and class, no error.type",
			outcome:       ateattr.SchedulerOutcomeAssigned,
			poolNamespace: "ate-workers",
			pool:          "pool-a",
			class:         "gvisor",
			err:           nil,
			wantKeys:      []attribute.Key{ateattr.SchedulerOutcomeKey, ateattr.WorkerPoolNamespaceKey, ateattr.WorkerPoolNameKey, ateattr.SandboxClassKey},
		},
		{
			name:     "no_free_worker carries class but neither pool key nor error.type",
			outcome:  ateattr.SchedulerOutcomeNoFreeWorker,
			pool:     "",
			class:    "gvisor",
			err:      status.Error(codes.FailedPrecondition, "no free workers available"),
			wantKeys: []attribute.Key{ateattr.SchedulerOutcomeKey, ateattr.SandboxClassKey},
		},
		{
			name:          "error carries error.type, no pool keys, class omitted when unknown",
			outcome:       ateattr.SchedulerOutcomeError,
			pool:          "",
			class:         "",
			err:           status.Error(codes.Internal, "boom"),
			wantKeys:      []attribute.Key{ateattr.SchedulerOutcomeKey, ateattr.ErrorTypeKey},
			wantErrorType: "Internal",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst, reader := newTestInstruments(t)
			inst.recordSchedulerAssignment(context.Background(), time.Now(), tt.outcome, tt.poolNamespace, tt.pool, tt.class, tt.err)

			dp := singleHistogramDP(t, reader, schedulerAssignmentMetric)
			assertAttrKeys(t, dp, tt.wantKeys...)
			if o, _ := attrString(dp, ateattr.SchedulerOutcomeKey); o != tt.outcome {
				t.Errorf("outcome = %q, want %q", o, tt.outcome)
			}
			if tt.wantErrorType != "" {
				if et, ok := attrString(dp, ateattr.ErrorTypeKey); !ok || et != tt.wantErrorType {
					t.Errorf("error.type = %q (present=%v), want %q", et, ok, tt.wantErrorType)
				}
			}
		})
	}
}

func workerPool(namespace, name string, class atev1alpha1.SandboxClass) *atev1alpha1.WorkerPool {
	return &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       atev1alpha1.WorkerPoolSpec{SandboxClass: class},
	}
}

// TestWorkerCountSeedsZeroForKnownPools covers the saturation cases: a pool whose
// only state has no workers, and a pool with no workers at all, both report 0
// rather than an absent series. Empty pool class defaults to gvisor.
func TestWorkerCountSeedsZeroForKnownPools(t *testing.T) {
	pools := func(labels.Selector) ([]*atev1alpha1.WorkerPool, error) {
		return []*atev1alpha1.WorkerPool{
			workerPool("ns-1", "pool-a", ""),
			workerPool("ns-2", "pool-a", atev1alpha1.SandboxClassMicroVM),
		}, nil
	}
	workers := func() ([]*ateapipb.Worker, error) {
		return []*ateapipb.Worker{worker("ns-1", "pool-a", "gvisor", true)}, nil
	}
	reader := newWorkerCountReader(t, workers, pools)

	sum := mustMetric(t, reader, workerpoolWorkersMetric).Data.(metricdata.Sum[int64])
	got := seriesCounts(sum)
	want := map[series]int64{
		{"ns-1", "pool-a", ateattr.WorkerStateIdle, "gvisor"}:      0,
		{"ns-1", "pool-a", ateattr.WorkerStateAssigned, "gvisor"}:  1,
		{"ns-2", "pool-a", ateattr.WorkerStateIdle, "microvm"}:     0,
		{"ns-2", "pool-a", ateattr.WorkerStateAssigned, "microvm"}: 0,
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
