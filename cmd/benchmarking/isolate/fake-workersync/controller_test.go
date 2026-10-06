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
	"github.com/agent-substrate/substrate/internal/hardware"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

// fakeRelay records accepted reports, as ate-api-server's view of capacity,
// and refuses Workers listed in reject with their code.
type fakeRelay struct {
	mu       sync.Mutex
	reported map[string]*ateapipb.WorkerResources  // by Worker name
	hardware map[string]*ateapipb.HardwareIdentity // by Worker name
	via      map[string]string                     // relay address, by Worker name
	reject   map[string]codes.Code
	calls    int
	live     map[string]string // as last pruned to
}

func newFakeRelay() *fakeRelay {
	return &fakeRelay{reported: map[string]*ateapipb.WorkerResources{}, hardware: map[string]*ateapipb.HardwareIdentity{}, via: map[string]string{}, reject: map[string]codes.Code{}}
}

func (f *fakeRelay) Report(_ context.Context, addr string, req *ateapipb.RegisterWorkerRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	name := req.GetWorker().GetName()
	if code, ok := f.reject[name]; ok {
		return status.Error(code, "rejected")
	}
	f.reported[name] = req.GetCapacity()
	f.hardware[name] = req.GetHardware()
	f.via[name] = addr
	return nil
}

func (f *fakeRelay) Prune(live map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.live = maps.Clone(live)
}

// fakeCluster is the controller's Kubernetes, in memory.
type fakeCluster struct {
	mu       sync.Mutex
	pools    []*atev1alpha1.WorkerPool
	nodes    []node
	relays   map[string]string
	statuses map[string]atev1alpha1.WorkerPoolStatus // by pool name
	updates  int
}

func (f *fakeCluster) Pools() ([]*atev1alpha1.WorkerPool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.pools), nil
}

func (f *fakeCluster) Nodes(context.Context) ([]node, error) { return f.nodes, nil }

func (f *fakeCluster) Relays(context.Context) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.relays), nil
}

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

func pool(name string, replicas int32, limits corev1.ResourceList) *atev1alpha1.WorkerPool {
	wp := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "benchmark-workloads", Name: name, Labels: map[string]string{"workload": name}},
		Spec:       atev1alpha1.WorkerPoolSpec{Replicas: replicas, SandboxClass: atev1alpha1.SandboxClass("gvisor")},
	}
	if limits != nil {
		wp.Spec.Template = &atev1alpha1.WorkerPoolPodTemplate{Resources: &corev1.ResourceRequirements{Limits: limits}}
	}
	return wp
}

func testNodes() []node {
	alloc := func(cpu, mem string) corev1.ResourceList {
		return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(mem)}
	}
	return []node{{name: "node-a", allocatable: alloc("86", "160Gi")}, {name: "node-b", allocatable: alloc("44", "80Gi")}}
}

func newTestController(pools ...*atev1alpha1.WorkerPool) (*controller, *fakeControl, *fakeRelay, *fakeCluster) {
	ctl, rel := newFakeControl(), newFakeRelay()
	cl := &fakeCluster{pools: pools, nodes: testNodes(), relays: map[string]string{"node-a": "10.0.0.1:8086", "node-b": "10.0.0.2:8086"}}
	return newController(ctl, rel, cl, testRun, 4), ctl, rel, cl
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

func wantCapacity(actors int32, cpu, mem string) *ateapipb.WorkerResources {
	return &ateapipb.WorkerResources{Actors: actors, Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
		{Name: "cpu", Quantity: cpu}, {Name: "memory", Quantity: mem},
	}}}
}

func TestReconcileHonorsReplicasAndLimits(t *testing.T) {
	limits := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1500m"), corev1.ResourceMemory: resource.MustParse("4Gi")}
	c, ctl, rel, cl := newTestController(pool("bench", 3, limits))
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

	if got := rel.via[name]; got != "10.0.0.2:8086" {
		t.Errorf("capacity for %s sent via %q, want node-b's fake-atelet", name, got)
	}
	if got, want := rel.reported[name], wantCapacity(1000, "1500m", "4Gi"); !proto.Equal(got, want) {
		t.Errorf("reported %v, want %v from the pool's limits", got, want)
	}
	if got, want := rel.hardware[name], hardware.ProbeHost(); !proto.Equal(got, want) {
		t.Errorf("hardware %v, want %v, which ate-api-server requires", got, want)
	}
	if got, want := cl.statuses["bench"], (atev1alpha1.WorkerPoolStatus{Replicas: 3, ReadyReplicas: 3, Selector: "ate.dev/worker-pool=bench"}); got != want {
		t.Errorf("status = %+v, want %+v", got, want)
	}
}

// With no limit set, a real worker reports its node's allocatable, which is
// what the downward API projects for an unset limit.
func TestReconcileReportsNodeAllocatableWithoutLimits(t *testing.T) {
	c, _, rel, _ := newTestController(pool("bench", 2, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	onA, onB := fakeworker.Name(testRun, "benchmark-workloads", "bench", 0), fakeworker.Name(testRun, "benchmark-workloads", "bench", 1)
	if got, want := rel.reported[onA], wantCapacity(1000, "86", "160Gi"); !proto.Equal(got, want) {
		t.Errorf("node-a Worker reported %v, want %v", got, want)
	}
	if got, want := rel.reported[onB], wantCapacity(1000, "44", "80Gi"); !proto.Equal(got, want) {
		t.Errorf("node-b Worker reported %v, want %v", got, want)
	}
}

func TestReconcileHonorsMaxActorsAnnotation(t *testing.T) {
	wp := pool("bench", 1, nil)
	wp.Annotations = map[string]string{fakeworker.MaxActorsAnnotation: "5"}
	bad := pool("broken", 1, nil)
	bad.Annotations = map[string]string{fakeworker.MaxActorsAnnotation: "lots"}
	c, ctl, rel, _ := newTestController(wp, bad)
	if err := reconcile(t, c); err == nil {
		t.Error("reconcile succeeded with an unparseable annotation")
	}
	name := fakeworker.Name(testRun, "benchmark-workloads", "bench", 0)
	if got := rel.reported[name].GetActors(); got != 5 {
		t.Errorf("actors = %d, want 5 from the annotation", got)
	}
	if slices.Contains(ctl.names(), fakeworker.Name(testRun, "benchmark-workloads", "broken", 0)) {
		t.Error("registered a Worker for the pool with a bad annotation")
	}
}

// An actor count past 32 bits would wrap in the capacity report, so it holds
// the pool like any other bad annotation.
func TestReconcileRejectsAMaxActorsPast32Bits(t *testing.T) {
	wp := pool("bench", 1, nil)
	wp.Annotations = map[string]string{fakeworker.MaxActorsAnnotation: "3000000000"}
	c, ctl, rel, _ := newTestController(wp)
	if err := reconcile(t, c); err == nil {
		t.Error("reconcile succeeded with an actor count past 32 bits")
	}
	if len(ctl.names()) != 0 || len(rel.reported) != 0 {
		t.Errorf("registered %v and reported %v for the pool with a bad annotation", ctl.names(), rel.reported)
	}
}

// Each pass lets go of relays the pod list no longer has, as after a
// fake-atelet restarts under a new IP.
func TestReconcilePrunesGoneRelays(t *testing.T) {
	c, _, rel, cl := newTestController(pool("bench", 2, nil))
	delete(cl.relays, "node-b")
	_ = reconcile(t, c)
	if want := map[string]string{"node-a": "10.0.0.1:8086"}; !maps.Equal(rel.live, want) {
		t.Errorf("pruned to %v, want %v", rel.live, want)
	}
}

// An annotation typo on a running pool reports an error and leaves the pool's
// Workers as they are, rather than retiring them all.
func TestBadAnnotationLeavesThePoolsWorkers(t *testing.T) {
	c, ctl, _, cl := newTestController(pool("bench", 2, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	bad := cl.pools[0].DeepCopy()
	bad.Annotations = map[string]string{fakeworker.MaxActorsAnnotation: "abc"}
	cl.pools[0] = bad
	if err := reconcile(t, c); err == nil {
		t.Error("reconcile succeeded with an unparseable annotation")
	}
	if got, want := ctl.names(), poolNames("bench", 2); !slices.Equal(got, want) {
		t.Errorf("registered %v, want the pool's Workers kept, %v", got, want)
	}
}

// A Worker whose node has left the benchmark set keeps the capacity it last
// reported: with no allocatable to read, a report would drop its cpu and
// memory.
func TestReconcileKeepsCapacityWhenTheNodeLeaves(t *testing.T) {
	c, _, rel, cl := newTestController(pool("bench", 2, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	name := fakeworker.Name(testRun, "benchmark-workloads", "bench", 1)
	before := rel.reported[name]
	cl.nodes = cl.nodes[:1]
	// Changes what the Worker would report, so it is not skipped as unchanged.
	wp := cl.pools[0].DeepCopy()
	wp.Annotations = map[string]string{fakeworker.MaxActorsAnnotation: "5"}
	cl.pools[0] = wp
	if err := reconcile(t, c); err == nil {
		t.Error("reconcile succeeded with a Worker on a node that is not a benchmark node")
	}
	if got := rel.reported[name]; !proto.Equal(got, before) {
		t.Errorf("reported %v for the Worker on the departed node, want %v kept", got, before)
	}
}

// A Worker on a node whose fake-atelet is not up yet is registered but not
// ready; the next pass reports it once the relay is there.
func TestReconcileWaitsForTheNodesFakeAtelet(t *testing.T) {
	c, ctl, rel, cl := newTestController(pool("bench", 2, nil))
	delete(cl.relays, "node-b")
	if err := reconcile(t, c); err == nil {
		t.Error("reconcile reported success with a Worker left unreported")
	}
	if got := len(ctl.names()); got != 2 {
		t.Errorf("registered %d Workers, want 2", got)
	}
	if got := cl.statuses["bench"]; got.Replicas != 2 || got.ReadyReplicas != 1 {
		t.Errorf("status = %+v, want 2 replicas, 1 ready", got)
	}
	cl.relays["node-b"] = "10.0.0.2:8086"
	if err := reconcile(t, c); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if len(rel.reported) != 2 || cl.statuses["bench"].ReadyReplicas != 2 {
		t.Errorf("after the relay came up: %d reported, status %+v", len(rel.reported), cl.statuses["bench"])
	}
}

func TestReconcileRetriesARejectedReport(t *testing.T) {
	c, _, rel, cl := newTestController(pool("bench", 1, nil))
	name := fakeworker.Name(testRun, "benchmark-workloads", "bench", 0)
	rel.reject[name] = codes.NotFound
	if err := reconcile(t, c); err == nil {
		t.Error("reconcile succeeded with a rejected report")
	}
	delete(rel.reject, name)
	if err := reconcile(t, c); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if rel.reported[name] == nil || cl.statuses["bench"].ReadyReplicas != 1 {
		t.Error("rejected report not retried")
	}
}

// A steady fleet costs ate-api-server nothing: a second pass with nothing
// changed creates and reports nothing, and rewrites no status.
func TestReconcileIsIdempotent(t *testing.T) {
	c, ctl, rel, cl := newTestController(pool("bench", 3, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	creates, reports, updates := ctl.creates, rel.calls, cl.updates
	if err := reconcile(t, c); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if ctl.creates != creates || rel.calls != reports || cl.updates != updates {
		t.Errorf("second pass: %d creates, %d reports, %d status writes; want none", ctl.creates-creates, rel.calls-reports, cl.updates-updates)
	}
}

func TestReconcileReportsAgainWhenCapacityChanges(t *testing.T) {
	limits := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}
	c, _, rel, cl := newTestController(pool("bench", 1, limits))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	bigger := pool("bench", 1, corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi")})
	cl.pools = []*atev1alpha1.WorkerPool{bigger}
	if err := reconcile(t, c); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	name := fakeworker.Name(testRun, "benchmark-workloads", "bench", 0)
	if got, want := rel.reported[name], wantCapacity(1000, "4", "8Gi"); !proto.Equal(got, want) {
		t.Errorf("after the limits changed, reported %v, want %v", got, want)
	}
}

func TestScaleDownDeletes(t *testing.T) {
	c, ctl, _, cl := newTestController(pool("bench", 4, nil))
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
	c, ctl, _, cl := newTestController(pool("bench", 2, nil))
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
	c, ctl, _, cl := newTestController(pool("bench", 2, nil))
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
	c, ctl, _, cl := newTestController(pool("bench", 2, nil), pool("other", 1, nil))
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
	c, ctl, rel, cl := newTestController(pool("bench", 2, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	real := &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: "0c4f6c2e-real-pod-uid"}, WorkerNamespace: "benchmark-workloads", WorkerPool: "bench", Status: &ateapipb.WorkerStatus{}}
	otherRun := &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: fakeworker.Name("r2", "benchmark-workloads", "bench", 0)}, WorkerNamespace: "benchmark-workloads", WorkerPool: "bench", Status: &ateapipb.WorkerStatus{}}
	ctl.workers[real.Metadata.Name] = real
	ctl.workers[otherRun.Metadata.Name] = otherRun

	restarted := newController(ctl, rel, cl, testRun, 4)
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
	c, ctl, _, _ := newTestController(pool("bench", 3, nil))
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

func TestCapacityFormatsLikeAteom(t *testing.T) {
	got, err := capacity(pool("p", 1, corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0.5"), corev1.ResourceMemory: resource.MustParse("1G")}), nil)
	if err != nil {
		t.Fatal(err)
	}
	// ateom spells cpu as decimal milli-cores and memory as a binary-SI byte
	// count; 1G is not a power of two, so 1e9.
	if want := wantCapacity(1000, "500m", "1e9"); !proto.Equal(got, want) {
		t.Errorf("capacity = %v, want %v", got, want)
	}
	none, err := capacity(pool("p", 1, nil), nil)
	if err != nil || none.GetResources() != nil || none.GetActors() != 1000 {
		t.Errorf("no limits and no allocatable: %v, %v; want 1000 actors and no resources", none, err)
	}
}
