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
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/cmd/benchmarking/isolate/internal/fakeworker"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testRun = "r1"

// fakeControl is an in-memory Worker registry.
type fakeControl struct {
	mu      sync.Mutex
	workers map[string]*ateapipb.Worker
	creates int
	// lostReply, when set, is returned by CreateWorker after the Worker is
	// stored, as when the server commits a create whose reply never arrives.
	lostReply error
}

func newFakeControl() *fakeControl {
	return &fakeControl{workers: map[string]*ateapipb.Worker{}}
}

func (f *fakeControl) CreateWorker(_ context.Context, in *ateapipb.CreateWorkerRequest, _ ...grpc.CallOption) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates++
	name := in.GetWorker().GetMetadata().GetName()
	if _, ok := f.workers[name]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "Worker %s already exists", name)
	}
	w := proto.CloneOf(in.GetWorker())
	w.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}
	f.workers[name] = w
	if f.lostReply != nil {
		return nil, f.lostReply
	}
	return w, nil
}

func (f *fakeControl) DeleteWorker(_ context.Context, in *ateapipb.DeleteWorkerRequest, _ ...grpc.CallOption) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetWorker().GetName()
	w, ok := f.workers[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
	}
	delete(f.workers, name)
	return w, nil
}

func (f *fakeControl) ListWorkers(_ context.Context, _ *ateapipb.ListWorkersRequest, _ ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &ateapipb.ListWorkersResponse{Workers: slices.Collect(maps.Values(f.workers))}, nil
}

func (f *fakeControl) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.workers))
}

func (f *fakeControl) worker(name string) *ateapipb.Worker {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.workers[name]
}

// fakeCluster is the controller's Kubernetes, in memory.
type fakeCluster struct {
	mu       sync.Mutex
	pools    []*atev1alpha1.WorkerPool
	nodes    []node
	statuses map[string]atev1alpha1.WorkerPoolStatus // by pool name
	updates  int
}

func (f *fakeCluster) Pools() ([]*atev1alpha1.WorkerPool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.pools), nil
}

func (f *fakeCluster) Nodes(context.Context) ([]node, error) { return f.nodes, nil }

func (f *fakeCluster) UpdateStatus(_ context.Context, wp *atev1alpha1.WorkerPool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates++
	if f.statuses == nil {
		f.statuses = map[string]atev1alpha1.WorkerPoolStatus{}
	}
	f.statuses[wp.Name] = wp.Status
	// Like the API server: the next List returns the written status.
	for i, p := range f.pools {
		if p.Name == wp.Name && p.Namespace == wp.Namespace {
			f.pools[i] = wp.DeepCopy()
		}
	}
	return nil
}

func (f *fakeCluster) setReplicas(pool string, n int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, p := range f.pools {
		if p.Name == pool {
			c := p.DeepCopy()
			c.Spec.Replicas = n
			f.pools[i] = c
		}
	}
}

func pool(name string, replicas int32) *atev1alpha1.WorkerPool {
	return &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "benchmark-workloads", Name: name, Labels: map[string]string{"workload": name}},
		Spec:       atev1alpha1.WorkerPoolSpec{Replicas: replicas, SandboxClass: atev1alpha1.SandboxClass("gvisor")},
	}
}

func testNodes() []node {
	return []node{{name: "node-a"}, {name: "node-b"}}
}

func newTestController(pools ...*atev1alpha1.WorkerPool) (*controller, *fakeControl, *fakeCluster) {
	ctl := newFakeControl()
	cl := &fakeCluster{pools: pools, nodes: testNodes()}
	return newController(ctl, cl, testRun, 4), ctl, cl
}

func reconcile(t *testing.T, c *controller) error {
	t.Helper()
	return c.reconcile(context.Background())
}

func poolNames(name string, n int) []string {
	var out []string
	for i := range n {
		out = append(out, fakeworker.Name(testRun, "benchmark-workloads", name, i))
	}
	slices.Sort(out)
	return out
}

func TestReconcileHonorsReplicas(t *testing.T) {
	c, ctl, cl := newTestController(pool("bench", 3))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got, want := ctl.names(), poolNames("bench", 3); !slices.Equal(got, want) {
		t.Fatalf("registered %v, want %v", got, want)
	}

	name := fakeworker.Name(testRun, "benchmark-workloads", "bench", 1)
	w := ctl.worker(name)
	switch {
	case w.GetNodeName() != "node-b":
		t.Errorf("index 1 placed on %q, want node-b (round robin)", w.GetNodeName())
	case w.GetWorkerNamespace() != "benchmark-workloads" || w.GetWorkerPool() != "bench":
		t.Errorf("WorkerNamespace/WorkerPool = %q/%q", w.GetWorkerNamespace(), w.GetWorkerPool())
	case w.GetSandboxClass() != "gvisor":
		t.Errorf("SandboxClass = %q", w.GetSandboxClass())
	case w.GetLabels()["workload"] != "bench":
		t.Errorf("Labels = %v, want the pool's, which template selectors match", w.GetLabels())
	case w.GetWorkerPodUid() != fakeworker.PodUID(name) || w.GetEpoch() != 0:
		t.Errorf("WorkerPodUid/Epoch = %q/%d", w.GetWorkerPodUid(), w.GetEpoch())
	}

	if got, want := cl.statuses["bench"], (atev1alpha1.WorkerPoolStatus{Replicas: 3, ReadyReplicas: 3, Selector: "ate.dev/worker-pool=bench"}); got != want {
		t.Errorf("status = %+v, want %+v", got, want)
	}
}

// A steady fleet costs ate-api-server nothing: a second pass with nothing
// changed creates nothing and rewrites no status.
func TestReconcileIsIdempotent(t *testing.T) {
	c, ctl, cl := newTestController(pool("bench", 3))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	creates, updates := ctl.creates, cl.updates
	if err := reconcile(t, c); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if ctl.creates != creates || cl.updates != updates {
		t.Errorf("second pass: %d creates, %d status writes; want none", ctl.creates-creates, cl.updates-updates)
	}
}

func TestScaleDownDeletes(t *testing.T) {
	c, ctl, cl := newTestController(pool("bench", 4))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	cl.setReplicas("bench", 2)
	if err := reconcile(t, c); err != nil {
		t.Fatalf("scale-down reconcile: %v", err)
	}
	if got, want := ctl.names(), poolNames("bench", 2); !slices.Equal(got, want) {
		t.Errorf("registered %v, want %v", got, want)
	}
	if got := cl.statuses["bench"]; got.Replicas != 2 || got.ReadyReplicas != 2 {
		t.Errorf("status = %+v, want 2 replicas", got)
	}
}

// A create the server commits but whose reply is lost, as on a deadline or a
// SIGTERM mid-pass, must not strand the Worker: shutdown still deletes it.
func TestDeleteAllAfterALostCreateReply(t *testing.T) {
	c, ctl, cl := newTestController(pool("bench", 2))
	ctl.lostReply = status.Error(codes.Unavailable, "connection reset")
	if err := reconcile(t, c); err == nil {
		t.Fatal("reconcile succeeded with every create reply lost")
	}
	if got := cl.statuses["bench"]; got.Replicas != 0 {
		t.Errorf("status = %+v, want no replicas before a create is confirmed", got)
	}
	if err := c.deleteAll(context.Background()); err != nil {
		t.Fatalf("deleteAll: %v", err)
	}
	if got := ctl.names(); len(got) != 0 {
		t.Errorf("left %v registered after shutdown", got)
	}
}

// The same Worker, unwanted by the next pass, is retired rather than left
// registered; wanted, its create is retried and confirmed.
func TestLostCreateReplyIsRetiredOrConfirmed(t *testing.T) {
	c, ctl, cl := newTestController(pool("bench", 2))
	ctl.lostReply = status.Error(codes.DeadlineExceeded, "deadline exceeded")
	if err := reconcile(t, c); err == nil {
		t.Fatal("reconcile succeeded with every create reply lost")
	}
	ctl.lostReply = nil
	cl.setReplicas("bench", 1)
	if err := reconcile(t, c); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if got, want := ctl.names(), poolNames("bench", 1); !slices.Equal(got, want) {
		t.Errorf("registered %v, want %v", got, want)
	}
	if got := cl.statuses["bench"]; got.Replicas != 1 || got.ReadyReplicas != 1 {
		t.Errorf("status = %+v, want 1 replica, confirmed by the retried create", got)
	}
}

func TestDeletedPoolRetiresItsWorkers(t *testing.T) {
	c, ctl, cl := newTestController(pool("bench", 2), pool("other", 1))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	cl.pools = cl.pools[1:]
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile after delete: %v", err)
	}
	if got, want := ctl.names(), poolNames("other", 1); !slices.Equal(got, want) {
		t.Errorf("registered %v, want only the remaining pool's %v", got, want)
	}
}

// After a restart the controller adopts its run's Workers instead of creating
// them again, and leaves every other Worker alone.
func TestAdoptAfterRestart(t *testing.T) {
	c, ctl, cl := newTestController(pool("bench", 2))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	real := &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: "0c4f6c2e-real-pod-uid"}, WorkerNamespace: "benchmark-workloads", WorkerPool: "bench", Status: &ateapipb.WorkerStatus{}}
	otherRun := &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: fakeworker.Name("r2", "benchmark-workloads", "bench", 0)}, WorkerNamespace: "benchmark-workloads", WorkerPool: "bench", Status: &ateapipb.WorkerStatus{}}
	ctl.workers[real.Metadata.Name] = real
	ctl.workers[otherRun.Metadata.Name] = otherRun

	restarted := newController(ctl, cl, testRun, 4)
	if err := restarted.adopt(context.Background()); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if got := len(restarted.workers); got != 2 {
		t.Fatalf("adopted %d Workers, want this run's 2", got)
	}
	creates := ctl.creates
	if err := reconcile(t, restarted); err != nil {
		t.Fatalf("reconcile after restart: %v", err)
	}
	if ctl.creates != creates {
		t.Errorf("created %d Workers again after adopting them", ctl.creates-creates)
	}
	if !slices.Contains(ctl.names(), real.Metadata.Name) || !slices.Contains(ctl.names(), otherRun.Metadata.Name) {
		t.Error("a Worker that is not this run's was retired")
	}
}

func TestDeleteAll(t *testing.T) {
	c, ctl, _ := newTestController(pool("bench", 3))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// One already gone, as after a cut-short shutdown.
	delete(ctl.workers, fakeworker.Name(testRun, "benchmark-workloads", "bench", 0))
	if err := c.deleteAll(context.Background()); err != nil {
		t.Fatalf("deleteAll: %v", err)
	}
	if got := ctl.names(); len(got) != 0 {
		t.Errorf("left %v", got)
	}
}
