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

package workercache_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/util/wait"
)

func TestCache_NotReadyBeforeStart(t *testing.T) {
	c := newWorkerCache(t, newFakeStore(), time.Hour)
	_, err := c.Workers()
	if err == nil {
		t.Fatal("expected error from Workers before Start, got nil")
	}
}

func TestCache_SyncsOnStart(t *testing.T) {
	ctx := t.Context()
	w1 := makeWorker("ns", "pod1", 1)
	w2 := makeWorker("ns", "pod2", 1)

	c := newWorkerCache(t, newFakeStore(w1, w2), time.Hour)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	got, err := c.Workers()
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if diff := cmp.Diff([]*ateapipb.Worker{w1, w2}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers mismatch (-want +got):\n%s", diff)
	}
}

func TestCache_CreatedEvent(t *testing.T) {
	ctx := t.Context()
	fs := newFakeStore()
	c := newWorkerCache(t, fs, time.Hour)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	w := makeWorker("ns", "pod1", 1)
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: w})

	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 1
	}, 2*time.Second)

	got, _ := c.Workers()
	if diff := cmp.Diff([]*ateapipb.Worker{w}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers mismatch (-want +got):\n%s", diff)
	}
}

func TestCache_UpdatedEvent_NewerVersionApplied(t *testing.T) {
	ctx := t.Context()
	w := makeWorker("ns", "pod1", 1)
	fs := newFakeStore(w)
	c := newWorkerCache(t, fs, time.Hour)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	updated := makeWorker("ns", "pod1", 2)
	updated.ActorId = "actor-1"
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: updated})

	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 1 && workers[0].GetActorId() == "actor-1"
	}, 2*time.Second)

	got, _ := c.Workers()
	if diff := cmp.Diff([]*ateapipb.Worker{updated}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers mismatch (-want +got):\n%s", diff)
	}
}

func TestCache_UpdatedEvent_OlderVersionIgnored(t *testing.T) {
	ctx := t.Context()
	w := makeWorker("ns", "pod1", 5)
	fs := newFakeStore(w)
	c := newWorkerCache(t, fs, time.Hour)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send a stale update followed by a sentinel we can detect.
	stale := makeWorker("ns", "pod1", 3)
	stale.ActorId = "stale-actor"
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: stale})

	sentinel := makeWorker("ns", "pod2", 1)
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: sentinel})

	// Wait for the sentinel to be processed so we know the stale event was also handled.
	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 2
	}, 2*time.Second)

	got, _ := c.Workers()
	if diff := cmp.Diff([]*ateapipb.Worker{w, sentinel}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers mismatch (-want +got):\n%s", diff)
	}
}

func TestCache_DeletedEvent(t *testing.T) {
	ctx := t.Context()
	w := makeWorker("ns", "pod1", 1)
	fs := newFakeStore(w)
	c := newWorkerCache(t, fs, time.Hour)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fs.send(store.WorkerEvent{
		Type:   store.WorkerEventDeleted,
		Worker: &ateapipb.Worker{WorkerNamespace: "ns", WorkerPod: "pod1"},
	})

	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 0
	}, 2*time.Second)
}

func TestCache_Disconnect_ResyncsWithFreshSnapshot(t *testing.T) {
	ctx := t.Context()
	w1 := makeWorker("ns", "pod1", 1)
	fs := newFakeStore(w1)
	c := newWorkerCache(t, fs, time.Hour)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Add a worker to the store snapshot and disconnect to trigger resync.
	w2 := makeWorker("ns", "pod2", 1)
	fs.setWorkers(w1, w2)
	fs.disconnect()

	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 2
	}, 2*time.Second)

	got, _ := c.Workers()
	if diff := cmp.Diff([]*ateapipb.Worker{w1, w2}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers after resync (-want +got):\n%s", diff)
	}
}

func TestCache_MultipleDisconnects(t *testing.T) {
	ctx := t.Context()
	fs := newFakeStore()
	c := newWorkerCache(t, fs, time.Hour)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for i := range 3 {
		pod := makeWorker("ns", string(rune('a'+i)), 1)
		fs.setWorkers(append(fs.workers[:i], pod)...)
		fs.disconnect()

		want := i + 1
		eventually(t, func() bool {
			workers, err := c.Workers()
			return err == nil && len(workers) == want
		}, 2*time.Second)
	}
}

func TestCache_WatchClosedOnListWorkersFailure(t *testing.T) {
	ctx := t.Context()
	fs := newFakeStore()
	fs.listErr = errors.New("valkey unavailable")
	c := newWorkerCache(t, fs, time.Hour)

	if err := c.Start(ctx); err == nil {
		t.Fatal("expected Start to fail when ListWorkers errors")
	}

	fs.mu.Lock()
	closes := fs.closes
	fs.mu.Unlock()
	if closes != 1 {
		t.Fatalf("expected watch to be closed once on sync failure, got %d closes", closes)
	}
}

func TestCache_WatchClosedOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	fs := newFakeStore()
	c := newWorkerCache(t, fs, time.Hour)

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cancel()

	eventually(t, func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return fs.closes == 1
	}, 2*time.Second)
}

func TestCache_WatchClosedOnDisconnectAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	fs := newFakeStore()
	c := newWorkerCache(t, fs, time.Hour)

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fs.disconnect()
	eventually(t, func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return fs.closes == 1
	}, 2*time.Second)

	cancel()
	eventually(t, func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return fs.closes == 2
	}, 2*time.Second)
}

func TestCache_Relist_PicksUpSilentCreate(t *testing.T) {
	ctx := t.Context()
	w1 := makeWorker("ns", "pod1", 1)
	fs := newFakeStore(w1)
	c := newWorkerCache(t, fs, 10*time.Millisecond)

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	w2 := makeWorker("ns", "pod2", 1)
	fs.setWorkers(w1, w2)

	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 2
	}, 2*time.Second)

	got, _ := c.Workers()
	if diff := cmp.Diff([]*ateapipb.Worker{w1, w2}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers after relist (-want +got):\n%s", diff)
	}
}

func TestCache_Relist_PicksUpSilentDelete(t *testing.T) {
	ctx := t.Context()
	w1 := makeWorker("ns", "pod1", 1)
	w2 := makeWorker("ns", "pod2", 1)
	fs := newFakeStore(w1, w2)
	c := newWorkerCache(t, fs, 10*time.Millisecond)

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fs.setWorkers(w1)

	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 1
	}, 2*time.Second)

	got, _ := c.Workers()
	if diff := cmp.Diff([]*ateapipb.Worker{w1}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers after relist (-want +got):\n%s", diff)
	}
}

func TestCache_Relist_FailureIsNonFatal(t *testing.T) {
	ctx := t.Context()
	w1 := makeWorker("ns", "pod1", 1)
	fs := newFakeStore(w1)
	c := newWorkerCache(t, fs, 10*time.Millisecond)

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fs.mu.Lock()
	fs.listErr = errors.New("valkey unavailable")
	fs.mu.Unlock()

	time.Sleep(50 * time.Millisecond)

	fs.mu.Lock()
	fs.listErr = nil
	fs.mu.Unlock()

	workers, err := c.Workers()
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if diff := cmp.Diff([]*ateapipb.Worker{w1}, workers, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers mismatch (-want +got):\n%s", diff)
	}
}

func TestCache_Metrics_WorkersGauge(t *testing.T) {
	ctx := t.Context()
	reader := newTestProvider(t)

	w1 := makeWorker("ns", "pod1", 1)
	w2 := makeWorker("ns", "pod2", 1)
	fs := newFakeStore(w1, w2)
	c := newWorkerCache(t, fs, time.Hour)

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	m, ok := collectMetric(t, reader, "cache.worker.count")
	if !ok {
		t.Fatal("cache.worker.count not found")
	}
	metricdatatest.AssertEqual(t, metricdata.Metrics{
		Name:        "cache.worker.count",
		Description: "Current number of workers in the cache.",
		Unit:        "{worker}",
		Data: metricdata.Gauge[int64]{
			DataPoints: []metricdata.DataPoint[int64]{{Value: 2}},
		},
	}, m, metricdatatest.IgnoreTimestamp())

	fs.send(store.WorkerEvent{
		Type:   store.WorkerEventDeleted,
		Worker: &ateapipb.Worker{WorkerNamespace: "ns", WorkerPod: "pod1"},
	})
	eventually(t, func() bool {
		m, ok := collectMetric(t, reader, "cache.worker.count")
		if !ok {
			return false
		}
		g := m.Data.(metricdata.Gauge[int64])
		return len(g.DataPoints) > 0 && g.DataPoints[len(g.DataPoints)-1].Value == 1
	}, 2*time.Second)
}

func TestCache_Metrics_ResyncsCounter(t *testing.T) {
	ctx := t.Context()
	reader := newTestProvider(t)

	fs := newFakeStore()
	c := newWorkerCache(t, fs, time.Hour)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fs.disconnect()

	eventually(t, func() bool {
		m, ok := collectMetric(t, reader, "cache.resyncs")
		if !ok {
			return false
		}
		sum := m.Data.(metricdata.Sum[int64])
		return len(sum.DataPoints) > 0 && sum.DataPoints[0].Value == 1
	}, 2*time.Second)
}

func TestCache_Metrics_NotReadyDurationCounter(t *testing.T) {
	ctx := t.Context()
	reader := newTestProvider(t)

	fs := newFakeStore()
	c := newWorkerCache(t, fs, time.Hour)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fs.disconnect()

	eventually(t, func() bool {
		m, ok := collectMetric(t, reader, "cache.not_ready.duration")
		if !ok {
			return false
		}
		sum := m.Data.(metricdata.Sum[float64])
		return len(sum.DataPoints) > 0 && sum.DataPoints[0].Value > 0
	}, 2*time.Second)
}

// fakeStore is a test double for store.Interface.
type fakeStore struct {
	store.Interface

	mu      sync.Mutex
	workers []*ateapipb.Worker
	watchCh chan store.WorkerEvent
	listErr error
	closes  int
}

func newFakeStore(workers ...*ateapipb.Worker) *fakeStore {
	return &fakeStore{
		workers: workers,
		watchCh: make(chan store.WorkerEvent, 16),
	}
}

func (f *fakeStore) WatchWorkers(_ context.Context) (*store.WorkerWatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return store.NewWorkerWatch(f.watchCh, func() {
		f.mu.Lock()
		f.closes++
		f.mu.Unlock()
	}), nil
}

func (f *fakeStore) ListWorkers(_ context.Context) ([]*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]*ateapipb.Worker, len(f.workers))
	copy(out, f.workers)
	return out, nil
}

func (f *fakeStore) send(event store.WorkerEvent) {
	f.mu.Lock()
	ch := f.watchCh
	f.mu.Unlock()
	ch <- event
}

func (f *fakeStore) setWorkers(workers ...*ateapipb.Worker) {
	f.mu.Lock()
	f.workers = workers
	f.mu.Unlock()
}

func (f *fakeStore) disconnect() {
	f.mu.Lock()
	old := f.watchCh
	f.watchCh = make(chan store.WorkerEvent, 16)
	f.mu.Unlock()
	close(old)
}

func makeWorker(namespace, pod string, version int64) *ateapipb.Worker {
	return &ateapipb.Worker{
		WorkerNamespace: namespace,
		WorkerPod:       pod,
		Version:         version,
	}
}

var workerSortOpt = cmpopts.SortSlices(func(a, b *ateapipb.Worker) bool {
	if a.GetWorkerNamespace() != b.GetWorkerNamespace() {
		return a.GetWorkerNamespace() < b.GetWorkerNamespace()
	}
	return a.GetWorkerPod() < b.GetWorkerPod()
})

func eventually(t *testing.T, condition func() bool, timeout time.Duration) {
	t.Helper()
	err := wait.PollUntilContextTimeout(t.Context(), 10*time.Millisecond, timeout, true, func(context.Context) (bool, error) {
		return condition(), nil
	})
	if err != nil {
		t.Fatal("condition not met within timeout")
	}
}

// newWorkerCache creates a Cache, failing the test if New returns an error.
func newWorkerCache(t *testing.T, fs *fakeStore, relistInterval time.Duration) *workercache.Cache {
	t.Helper()
	c, err := workercache.New(fs, relistInterval)
	if err != nil {
		t.Fatalf("workercache.New: %v", err)
	}
	return c
}

// newTestProvider installs a fresh ManualReader as the global MeterProvider for
// this test. Because New creates instruments from otel.GetMeterProvider() at
// call time (not package-init time), each test gets isolated metric state.
func newTestProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { mp.Shutdown(context.Background()) })
	return reader
}

// collectMetric collects all metrics from reader and returns the one named name.
func collectMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) (metricdata.Metrics, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
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
