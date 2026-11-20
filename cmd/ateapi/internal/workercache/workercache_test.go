// Copyright 2026 The Agent Substrate Authors
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

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/util/wait"
)

func TestCache_NotReadyBeforeStart(t *testing.T) {
	c := workercache.New(newFakeStore(), time.Hour)
	_, err := c.Workers()
	if err == nil {
		t.Fatal("expected error from Workers before Start, got nil")
	}
	if _, err := c.Worker(workerName("ns", "pod")); err == nil {
		t.Fatal("expected error from Worker before Start, got nil")
	}
}

func TestCache_SyncsOnStart(t *testing.T) {
	w1 := makeWorker("ns", "pod1", 1)
	w2 := makeWorker("ns", "pod2", 1)

	c := workercache.New(newFakeStore(w1, w2), time.Hour)
	ctx := t.Context()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	got, err := c.Workers()
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if diff := cmp.Diff([]*ateapi.Worker{w1, w2}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers mismatch (-want +got):\n%s", diff)
	}
}

func TestCache_Worker(t *testing.T) {
	want := makeWorker("ns", "pod", 1)
	c := workercache.New(newFakeStore(want), time.Hour)
	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	got, err := c.Worker(workerName("ns", "pod"))
	if err != nil {
		t.Fatalf("Worker: %v", err)
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("worker mismatch (-want +got):\n%s", diff)
	}
	if _, err := c.Worker(workerName("ns", "missing")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing Worker error = %v, want store.ErrNotFound", err)
	}
}

func TestCache_CreatedEvent(t *testing.T) {
	fs := newFakeStore()
	c := workercache.New(fs, time.Hour)
	ctx := t.Context()

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
	if diff := cmp.Diff([]*ateapi.Worker{w}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers mismatch (-want +got):\n%s", diff)
	}
}

func TestCache_UpdatedEvent_NewerVersionApplied(t *testing.T) {
	w := makeWorker("ns", "pod1", 1)
	fs := newFakeStore(w)
	c := workercache.New(fs, time.Hour)
	ctx := t.Context()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	updated := makeWorker("ns", "pod1", 2)
	updated.Status.Allocated = &ateapi.WorkerResources{Actors: 1}
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: updated})

	eventually(t, func() bool {
		workers, err := c.Workers()
		if err != nil || len(workers) != 1 {
			return false
		}
		return workers[0].GetStatus().GetAllocated().GetActors() == 1
	}, 2*time.Second)

	got, _ := c.Workers()
	if diff := cmp.Diff([]*ateapi.Worker{updated}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers mismatch (-want +got):\n%s", diff)
	}
}

func TestCache_UpdatedEvent_OlderVersionIgnored(t *testing.T) {
	w := makeWorker("ns", "pod1", 5)
	fs := newFakeStore(w)
	c := workercache.New(fs, time.Hour)
	ctx := t.Context()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send a stale update followed by a sentinel we can detect.
	stale := makeWorker("ns", "pod1", 3)
	stale.Status.Allocated = &ateapi.WorkerResources{Actors: 7}
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: stale})

	sentinel := makeWorker("ns", "pod2", 1)
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: sentinel})

	// Wait for the sentinel to be processed so we know the stale event was also handled.
	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 2
	}, 2*time.Second)

	got, _ := c.Workers()
	if diff := cmp.Diff([]*ateapi.Worker{w, sentinel}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers mismatch (-want +got):\n%s", diff)
	}
}

func TestCache_DeletedEvent(t *testing.T) {
	w := makeWorker("ns", "pod1", 1)
	fs := newFakeStore(w)
	c := workercache.New(fs, time.Hour)
	ctx := t.Context()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fs.send(store.WorkerEvent{Type: store.WorkerEventDeleted, Worker: w})

	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 0
	}, 2*time.Second)
}

func TestCache_Disconnect_ResyncsWithFreshSnapshot(t *testing.T) {
	w1 := makeWorker("ns", "pod1", 1)
	fs := newFakeStore(w1)
	c := workercache.New(fs, time.Hour)
	ctx := t.Context()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Add a worker to the store snapshot and disconnect to trigger resync.
	w2 := makeWorker("ns", "pod2", 1)
	fs.setWorkers(w1, w2)
	fs.disconnect()

	// After resync the cache should reflect the updated snapshot.
	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 2
	}, 2*time.Second)

	got, _ := c.Workers()
	if diff := cmp.Diff([]*ateapi.Worker{w1, w2}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers after resync (-want +got):\n%s", diff)
	}
}

func TestCache_MultipleDisconnects(t *testing.T) {
	fs := newFakeStore()
	c := workercache.New(fs, time.Hour)
	ctx := t.Context()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Disconnect three times, each time adding a worker to the snapshot.
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
	fs := newFakeStore()
	fs.listErr = errors.New("store unavailable")
	c := workercache.New(fs, time.Hour)

	if err := c.Start(t.Context()); err == nil {
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
	fs := newFakeStore()
	c := workercache.New(fs, time.Hour)
	ctx, cancel := context.WithCancel(t.Context())

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
	fs := newFakeStore()
	c := workercache.New(fs, time.Hour)
	ctx, cancel := context.WithCancel(t.Context())

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Disconnect: the old watch should be closed and a new one opened.
	fs.disconnect()
	eventually(t, func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return fs.closes == 1
	}, 2*time.Second)

	// Shutdown: the new watch should also be closed.
	cancel()
	eventually(t, func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return fs.closes == 2
	}, 2*time.Second)
}

func TestCache_Relist_RecoversFromMissedCreate(t *testing.T) {
	w1 := makeWorker("ns", "pod1", 1)
	fs := newFakeStore(w1)
	c := workercache.New(fs, 10*time.Millisecond)

	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Add a worker directly to the store without sending a watch event,
	// simulating a silent PUBLISH failure.
	w2 := makeWorker("ns", "pod2", 1)
	fs.setWorkers(w1, w2)

	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 2
	}, 2*time.Second)

	got, _ := c.Workers()
	if diff := cmp.Diff([]*ateapi.Worker{w1, w2}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers after relist (-want +got):\n%s", diff)
	}
}

func TestCache_Relist_RecoversFromMissedDelete(t *testing.T) {
	w1 := makeWorker("ns", "pod1", 1)
	w2 := makeWorker("ns", "pod2", 1)
	fs := newFakeStore(w1, w2)
	c := workercache.New(fs, 10*time.Millisecond)

	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Remove a worker from the store without a watch event,
	// simulating a silent PUBLISH failure on delete.
	fs.setWorkers(w1)

	eventually(t, func() bool {
		workers, err := c.Workers()
		return err == nil && len(workers) == 1
	}, 2*time.Second)

	got, _ := c.Workers()
	if diff := cmp.Diff([]*ateapi.Worker{w1}, got, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers after relist (-want +got):\n%s", diff)
	}
}

func TestCache_Relist_FailureIsNonFatal(t *testing.T) {
	w1 := makeWorker("ns", "pod1", 1)
	fs := newFakeStore(w1)
	c := workercache.New(fs, 10*time.Millisecond)

	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Make ListWorkers fail to simulate a transient store error.
	fs.mu.Lock()
	fs.listErr = errors.New("store unavailable")
	fs.mu.Unlock()

	// Wait long enough for at least one relist attempt.
	time.Sleep(50 * time.Millisecond)

	// Clear the error; the cache should still be usable with the old snapshot.
	fs.mu.Lock()
	fs.listErr = nil
	fs.mu.Unlock()

	workers, err := c.Workers()
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if diff := cmp.Diff([]*ateapi.Worker{w1}, workers, protocmp.Transform(), workerSortOpt); diff != "" {
		t.Errorf("workers mismatch (-want +got):\n%s", diff)
	}
}

type fakeStore struct {
	store.Interface

	mu      sync.Mutex
	workers []*ateapi.Worker
	watchCh chan store.WorkerEvent
	listErr error // if set, ListWorkers returns it
	closes  int   // number of times a returned watch was Closed
}

func newFakeStore(workers ...*ateapi.Worker) *fakeStore {
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

func (f *fakeStore) ListWorkers(_ context.Context, _ store.ListOptions) (store.ListResponse[*ateapi.Worker], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return store.ListResponse[*ateapi.Worker]{}, f.listErr
	}
	out := make([]*ateapi.Worker, len(f.workers))
	copy(out, f.workers)
	return store.ListResponse[*ateapi.Worker]{Items: out}, nil
}

func (f *fakeStore) send(event store.WorkerEvent) {
	f.mu.Lock()
	ch := f.watchCh
	f.mu.Unlock()
	ch <- event
}

func (f *fakeStore) setWorkers(workers ...*ateapi.Worker) {
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

// workerName stands in for the pod UID that names a real Worker. The tests only
// need it to be stable and unique per pod.
func workerName(namespace, pod string) string {
	return "uid-" + namespace + "-" + pod
}

func makeWorker(namespace, pod string, version int64) *ateapi.Worker {
	return &ateapi.Worker{
		Metadata: &ateapi.ResourceMetadata{
			Name:    workerName(namespace, pod),
			Uid:     workerName(namespace, pod),
			Version: version,
		},
		WorkerNamespace: namespace,
		WorkerPod:       pod,
		WorkerPodUid:    workerName(namespace, pod),
		Status:          &ateapi.WorkerStatus{},
	}
}

// workerSortOpt compares workers ignoring ordering.
var workerSortOpt = cmpopts.SortSlices(func(a, b *ateapi.Worker) bool {
	if a.GetWorkerNamespace() != b.GetWorkerNamespace() {
		return a.GetWorkerNamespace() < b.GetWorkerNamespace()
	}
	return a.GetWorkerPod() < b.GetWorkerPod()
})

// eventually polls condition every 10ms until it returns true or timeout elapses.
func eventually(t *testing.T, condition func() bool, timeout time.Duration) {
	t.Helper()
	err := wait.PollUntilContextTimeout(t.Context(), 10*time.Millisecond, timeout, true, func(context.Context) (bool, error) {
		return condition(), nil
	})
	if err != nil {
		t.Fatal("condition not met within timeout")
	}
}

// The watch may replay an event or deliver one out of order, so an event
// older than or already at what the cache holds must not regress it or
// resurrect a deleted worker.
func TestCache_WatchEventsAreFenced(t *testing.T) {
	w1 := makeWorker("ns", "pod1", 1)
	w1.Metadata.Uid = "incarnation-1"
	w3 := makeWorker("ns", "pod3", 2)
	w3.Metadata.Uid = "pod3-incarnation-1"
	fs := newFakeStore(w1, w3)
	c := workercache.New(fs, time.Hour)
	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	updated := makeWorker("ns", "pod1", 2)
	updated.Metadata.Uid = "incarnation-1"
	updated.Status.Allocated = &ateapi.WorkerResources{Actors: 1}
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: updated})
	eventually(t, func() bool {
		w, err := c.Worker(workerName("ns", "pod1"))
		return err == nil && w.GetMetadata().GetVersion() == 2 && w.GetStatus().GetAllocated().GetActors() == 1
	}, 2*time.Second)

	// A same-version replay and an older event must both be no-ops.
	dupV2 := makeWorker("ns", "pod1", 2)
	dupV2.Metadata.Uid = "incarnation-1"
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: dupV2})
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: w1})
	// Ordering on the channel means observing a later event proves the two
	// above were already applied.
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: makeWorker("ns", "pod2", 1)})
	eventually(t, func() bool {
		_, err := c.Worker(workerName("ns", "pod2"))
		return err == nil
	}, 2*time.Second)

	got, err := c.Worker(workerName("ns", "pod1"))
	if err != nil {
		t.Fatalf("Worker: %v", err)
	}
	if got.GetMetadata().GetVersion() != 2 || got.GetStatus().GetAllocated().GetActors() != 1 {
		t.Fatalf("stale or duplicate event overwrote the cache: version=%d actors=%d",
			got.GetMetadata().GetVersion(), got.GetStatus().GetAllocated().GetActors())
	}

	// Delete pod1 and Forget pod3, then replay earlier creates/updates for both
	// (simulating outbox replay after commit-time publishLocally or Forget).
	fs.send(store.WorkerEvent{Type: store.WorkerEventDeleted, Worker: updated})
	c.Forget(workerName("ns", "pod3"), w3.Metadata.Uid)
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: w1})
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: updated})
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: w3})
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: makeWorker("ns", "pod4", 1)})
	eventually(t, func() bool {
		_, err := c.Worker(workerName("ns", "pod4"))
		return err == nil
	}, 2*time.Second)

	if _, err := c.Worker(workerName("ns", "pod1")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("replayed event resurrected deleted pod1: err = %v, want ErrNotFound", err)
	}
	if _, err := c.Worker(workerName("ns", "pod3")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("replayed event resurrected forgotten pod3: err = %v, want ErrNotFound", err)
	}

	// Re-creating pod1 under a new UID starts at version 1 and must not be
	// blocked by incarnation-1's tombstone or clobbered by its replayed events.
	recreated := makeWorker("ns", "pod1", 1)
	recreated.Metadata.Uid = "incarnation-2"
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: recreated})
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: updated})
	fs.send(store.WorkerEvent{Type: store.WorkerEventDeleted, Worker: updated})
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: makeWorker("ns", "pod5", 1)})
	eventually(t, func() bool {
		_, err := c.Worker(workerName("ns", "pod5"))
		return err == nil
	}, 2*time.Second)

	got, err = c.Worker(workerName("ns", "pod1"))
	if err != nil {
		t.Fatalf("recreated pod1 missing: %v", err)
	}
	if got.GetMetadata().GetUid() != "incarnation-2" || got.GetMetadata().GetVersion() != 1 {
		t.Fatalf("recreated pod1 = (%q, v%d), want (incarnation-2, v1)",
			got.GetMetadata().GetUid(), got.GetMetadata().GetVersion())
	}

	// Forgetting incarnation-1 must neither remove nor fence incarnation-2.
	c.Forget(workerName("ns", "pod1"), "incarnation-1")
	recreatedV2 := makeWorker("ns", "pod1", 2)
	recreatedV2.Metadata.Uid = "incarnation-2"
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: recreatedV2})
	eventually(t, func() bool {
		w, err := c.Worker(workerName("ns", "pod1"))
		return err == nil && w.GetMetadata().GetUid() == "incarnation-2" && w.GetMetadata().GetVersion() == 2
	}, 2*time.Second)
}

// A relist that finds a tombstoned worker drops the tombstone, so the
// worker's later events apply again.
func TestCache_RelistClearsTombstoneOfListedWorker(t *testing.T) {
	fs := newFakeStore(makeWorker("ns", "pod1", 1))
	c := workercache.New(fs, time.Hour)
	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	c.Forget(workerName("ns", "pod1"), workerName("ns", "pod1"))
	fs.disconnect()
	eventually(t, func() bool {
		_, err := c.Worker(workerName("ns", "pod1"))
		return err == nil
	}, 5*time.Second)

	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: makeWorker("ns", "pod1", 2)})
	eventually(t, func() bool {
		w, err := c.Worker(workerName("ns", "pod1"))
		return err == nil && w.GetMetadata().GetVersion() == 2
	}, 2*time.Second)
}

// handlerRecorder records the Workers a Cache handler is called with.
type handlerRecorder struct {
	mu   sync.Mutex
	seen []*ateapi.Worker
}

func (h *handlerRecorder) handle(w *ateapi.Worker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = append(h.seen, w)
}

// versions returns the versions the handler saw for the named Worker, in order.
func (h *handlerRecorder) versions(name string) []int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []int64
	for _, w := range h.seen {
		if w.GetMetadata().GetName() == name {
			out = append(out, w.GetMetadata().GetVersion())
		}
	}
	return out
}

// A handler added after Start is first called with what the cache holds.
func TestCache_AddHandler_ReplaysCurrentWorkers(t *testing.T) {
	c := workercache.New(newFakeStore(makeWorker("ns", "pod1", 1), makeWorker("ns", "pod2", 3)), time.Hour)
	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	var h handlerRecorder
	c.AddHandler(h.handle)

	if diff := cmp.Diff([]int64{1}, h.versions(workerName("ns", "pod1"))); diff != "" {
		t.Errorf("pod1 versions (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]int64{3}, h.versions(workerName("ns", "pod2"))); diff != "" {
		t.Errorf("pod2 versions (-want +got):\n%s", diff)
	}
}

// A handler sees each change the cache applies, and not an event it fences
// off as stale, duplicate, or a delete.
func TestCache_AddHandler_SeesAppliedEvents(t *testing.T) {
	fs := newFakeStore(makeWorker("ns", "pod1", 2))
	c := workercache.New(fs, time.Hour)
	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	var h handlerRecorder
	c.AddHandler(h.handle)

	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: makeWorker("ns", "pod1", 1)})
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: makeWorker("ns", "pod1", 2)})
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: makeWorker("ns", "pod1", 3)})
	fs.send(store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: makeWorker("ns", "pod1", 3)})
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: makeWorker("ns", "pod2", 1)})
	fs.send(store.WorkerEvent{Type: store.WorkerEventDeleted, Worker: makeWorker("ns", "pod2", 1)})
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: makeWorker("ns", "pod2", 1)})
	fs.send(store.WorkerEvent{Type: store.WorkerEventCreated, Worker: makeWorker("ns", "pod3", 1)})
	eventually(t, func() bool {
		_, err := c.Worker(workerName("ns", "pod3"))
		return err == nil
	}, 2*time.Second)

	if diff := cmp.Diff([]int64{2, 3}, h.versions(workerName("ns", "pod1"))); diff != "" {
		t.Errorf("pod1 versions (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]int64{1}, h.versions(workerName("ns", "pod2"))); diff != "" {
		t.Errorf("pod2 versions (-want +got):\n%s", diff)
	}
}

// A relist hands the handler each Worker it finds new or changed, so a change
// whose event was missed still reaches it, and leaves the rest alone.
func TestCache_AddHandler_SeesRelistChanges(t *testing.T) {
	unchanged := makeWorker("ns", "pod2", 1)
	fs := newFakeStore(makeWorker("ns", "pod1", 1), unchanged)
	c := workercache.New(fs, 20*time.Millisecond)
	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	var h handlerRecorder
	c.AddHandler(h.handle)

	fs.setWorkers(makeWorker("ns", "pod1", 5), unchanged, makeWorker("ns", "pod3", 1))
	eventually(t, func() bool {
		return len(h.versions(workerName("ns", "pod1"))) == 2 && len(h.versions(workerName("ns", "pod3"))) == 1
	}, 2*time.Second)
	// Let a few more relists run over the now-unchanged store.
	time.Sleep(100 * time.Millisecond)

	if diff := cmp.Diff([]int64{1, 5}, h.versions(workerName("ns", "pod1"))); diff != "" {
		t.Errorf("pod1 versions (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]int64{1}, h.versions(workerName("ns", "pod2"))); diff != "" {
		t.Errorf("pod2 versions (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]int64{1}, h.versions(workerName("ns", "pod3"))); diff != "" {
		t.Errorf("pod3 versions (-want +got):\n%s", diff)
	}
}
