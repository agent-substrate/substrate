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

	"github.com/agent-substrate/substrate/internal/ateattr"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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
	reader := newWorkerCountReader(t, notReady, noPools)

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
	reader := newWorkerCountReader(t, workers, pools)

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
