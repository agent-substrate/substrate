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
	"time"

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

const (
	testRun        = "r1"
	testDrainGrace = time.Minute
)

// fakeControl is an in-memory Worker registry.
type fakeControl struct {
	mu          sync.Mutex
	workers     map[string]*ateapipb.Worker
	assignments map[string]int // actors assigned, by Worker name
	creates     int
	updates     int
	// lostReply, when set, is returned by CreateWorker after the Worker is
	// stored, as when the server commits a create whose reply never arrives.
	lostReply error
}

func newFakeControl() *fakeControl {
	return &fakeControl{workers: map[string]*ateapipb.Worker{}, assignments: map[string]int{}}
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
	// Like ate-api-server: deleting a Worker releases its Actors.
	delete(f.assignments, name)
	return w, nil
}

func (f *fakeControl) DrainWorker(_ context.Context, in *ateapipb.DrainWorkerRequest, _ ...grpc.CallOption) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.workers[in.GetWorker().GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such Worker")
	}
	w.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
	return w, nil
}

func (f *fakeControl) GetWorker(_ context.Context, in *ateapipb.GetWorkerRequest, _ ...grpc.CallOption) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.workers[in.GetWorker().GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such Worker")
	}
	return proto.CloneOf(w), nil
}

// UpdateWorker allows only the labels to change, as ate-api-server's
// immutable-field validation does for the fields the controller sets.
func (f *fakeControl) UpdateWorker(_ context.Context, in *ateapipb.UpdateWorkerRequest, _ ...grpc.CallOption) (*ateapipb.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates++
	name := in.GetWorker().GetMetadata().GetName()
	old, ok := f.workers[name]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such Worker")
	}
	w := proto.CloneOf(in.GetWorker())
	cmpOld, cmpNew := proto.CloneOf(old), proto.CloneOf(w)
	cmpOld.Labels, cmpNew.Labels = nil, nil
	if !proto.Equal(cmpOld, cmpNew) {
		return nil, status.Errorf(codes.InvalidArgument, "Worker %s: only labels may change", name)
	}
	f.workers[name] = w
	return w, nil
}

func (f *fakeControl) ListWorkers(_ context.Context, _ *ateapipb.ListWorkersRequest, _ ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &ateapipb.ListWorkersResponse{Workers: slices.Collect(maps.Values(f.workers))}, nil
}

func (f *fakeControl) ListWorkerActorAssignments(_ context.Context, in *ateapipb.ListWorkerActorAssignmentsRequest, _ ...grpc.CallOption) (*ateapipb.ListWorkerActorAssignmentsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetWorker().GetName()
	if _, ok := f.workers[name]; !ok {
		return nil, status.Error(codes.NotFound, "no such Worker")
	}
	resp := &ateapipb.ListWorkerActorAssignmentsResponse{}
	for range f.assignments[name] {
		resp.ActorAssignments = append(resp.ActorAssignments, &ateapipb.ActorAssignment{})
	}
	return resp, nil
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
// and refuses Workers listed in reject with their code. An accepted report is
// also written to control's Worker, as RegisterWorker records it.
type fakeRelay struct {
	control  *fakeControl
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
	if f.control != nil {
		f.control.mu.Lock()
		if w, ok := f.control.workers[name]; ok {
			w.Status.Capacity, w.Status.Hardware = req.GetCapacity(), req.GetHardware()
		}
		f.control.mu.Unlock()
	}
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

// editPool replaces the named pool with a copy edit has changed.
func (f *fakeCluster) editPool(name string, edit func(*atev1alpha1.WorkerPool)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, p := range f.pools {
		if p.Name == name {
			c := p.DeepCopy()
			edit(c)
			f.pools[i] = c
		}
	}
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
	rel.control = ctl
	cl := &fakeCluster{pools: pools, nodes: testNodes(), relays: map[string]string{"node-a": "10.0.0.1:8086", "node-b": "10.0.0.2:8086"}}
	c := newController(ctl, rel, cl, testRun, 4, testDrainGrace)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return start }
	return c, ctl, rel, cl
}

// advance moves the controller's clock forward by d.
func advance(c *controller, d time.Duration) {
	t := c.now().Add(d)
	c.now = func() time.Time { return t }
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

// A limits edit replaces the pool's Workers, as it rolls a real pool's pods:
// ateom reads its limits once at startup, so a Worker's capacity never
// changes. The old Worker drains with the capacity it reported, and is not
// reported again.
func TestPoolLimitsChangeReplacesWorkers(t *testing.T) {
	limits := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}
	c, ctl, rel, cl := newTestController(pool("bench", 1, limits))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	name := fakeworker.Name(testRun, "benchmark-workloads", "bench", 0)
	ctl.assignments[name] = 1
	reports := rel.calls

	cl.editPool("bench", func(wp *atev1alpha1.WorkerPool) {
		wp.Spec.Template.Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi")}
	})
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile after the limits changed: %v", err)
	}
	if w := ctl.worker(name); w.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("Worker state = %v after the limits changed, want draining", w.GetStatus().GetState())
	}
	if got, want := rel.reported[name], wantCapacity(1000, "2", "4Gi"); !proto.Equal(got, want) || rel.calls != reports {
		t.Errorf("draining Worker reported %v in %d new reports, want %v kept and none", got, rel.calls-reports, want)
	}

	ctl.assignments[name] = 0
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile after the Actor left: %v", err)
	}
	if w := ctl.worker(name); w.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("replaced Worker state = %v, want active", w.GetStatus().GetState())
	}
	if got, want := rel.reported[name], wantCapacity(1000, "4", "8Gi"); !proto.Equal(got, want) {
		t.Errorf("replaced Worker reported %v, want %v", got, want)
	}
	if got := cl.statuses["bench"]; got.Replicas != 1 || got.ReadyReplicas != 1 {
		t.Errorf("status = %+v, want 1 replica, 1 ready", got)
	}
}

// ateom's actor count is a flag, so changing it rolls a real pool's pods too.
func TestMaxActorsAnnotationChangeReplacesWorkers(t *testing.T) {
	c, ctl, rel, cl := newTestController(pool("bench", 1, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	name := fakeworker.Name(testRun, "benchmark-workloads", "bench", 0)
	creates := ctl.creates
	cl.editPool("bench", func(wp *atev1alpha1.WorkerPool) {
		wp.Annotations = map[string]string{fakeworker.MaxActorsAnnotation: "5"}
	})
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile after the annotation changed: %v", err)
	}
	if ctl.creates != creates+1 {
		t.Errorf("%d creates after the actor count changed, want 1", ctl.creates-creates)
	}
	if got := rel.reported[name].GetActors(); got != 5 {
		t.Errorf("replaced Worker reported %d actors, want 5", got)
	}
}

// A node's allocatable reaches ateom once, at startup, so a later change
// leaves a pool with no limits as it is: no new Worker and no report.
func TestAllocatableChangeKeepsWorkerCapacity(t *testing.T) {
	c, ctl, rel, cl := newTestController(pool("bench", 2, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	creates, reports := ctl.creates, rel.calls
	cl.nodes[0].allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("64"), corev1.ResourceMemory: resource.MustParse("128Gi")}
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile after allocatable changed: %v", err)
	}
	if ctl.creates != creates || rel.calls != reports {
		t.Errorf("allocatable change cost %d creates and %d reports, want none", ctl.creates-creates, rel.calls-reports)
	}
	if got := cl.statuses["bench"]; got.Replicas != 2 || got.ReadyReplicas != 2 {
		t.Errorf("status = %+v, want 2 replicas, 2 ready", got)
	}
}

// Scaling down mirrors a real worker pod going away: drained first so nothing
// new lands on it, deleted only once no Actor is assigned to it.
func TestScaleDownDrainsThenDeletes(t *testing.T) {
	c, ctl, _, cl := newTestController(pool("bench", 4, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	busy := fakeworker.Name(testRun, "benchmark-workloads", "bench", 3)
	idle := fakeworker.Name(testRun, "benchmark-workloads", "bench", 2)
	ctl.assignments[busy] = 1

	cl.setReplicas("bench", 2)
	if err := reconcile(t, c); err != nil {
		t.Fatalf("scale-down reconcile: %v", err)
	}
	if slices.Contains(ctl.names(), idle) {
		t.Errorf("idle Worker %s not deleted", idle)
	}
	if w := ctl.worker(busy); w == nil || w.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Fatalf("busy Worker %s = %v, want kept and draining", busy, w)
	}
	if got := cl.statuses["bench"]; got.Replicas != 2 || got.ReadyReplicas != 2 {
		t.Errorf("status = %+v, want 2 replicas: a draining Worker is no replica", got)
	}

	ctl.assignments[busy] = 0
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile after the Actor left: %v", err)
	}
	if got, want := ctl.names(), poolNames("bench", 2); !slices.Equal(got, want) {
		t.Errorf("registered %v, want %v", got, want)
	}
}

// Nothing stops a fake Worker's Actors the way ateom stops a terminating
// pod's, so a Worker still held after the drain grace is deleted anyway,
// which releases its Actors, rather than left draining for good.
func TestDrainGraceDeletesAWorkerStillHeld(t *testing.T) {
	c, ctl, _, cl := newTestController(pool("bench", 2, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	busy := fakeworker.Name(testRun, "benchmark-workloads", "bench", 1)
	ctl.assignments[busy] = 1
	cl.setReplicas("bench", 1)
	if err := reconcile(t, c); err != nil {
		t.Fatalf("scale-down reconcile: %v", err)
	}
	advance(c, testDrainGrace-time.Second)
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile within the grace: %v", err)
	}
	if ctl.worker(busy) == nil {
		t.Fatalf("Worker %s deleted within the drain grace", busy)
	}
	advance(c, time.Second)
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile after the grace: %v", err)
	}
	if got, want := ctl.names(), poolNames("bench", 1); !slices.Equal(got, want) {
		t.Errorf("registered %v after the drain grace, want %v", got, want)
	}
	if n := ctl.assignments[busy]; n != 0 {
		t.Errorf("%d Actors still assigned to the deleted Worker", n)
	}
}

// A Worker scaled down while an Actor holds it, then wanted again, is deleted
// once the Actor leaves and registered afresh, rather than left draining.
func TestScaleDownThenUpReplacesTheDrainingWorker(t *testing.T) {
	c, ctl, _, cl := newTestController(pool("bench", 2, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	busy := fakeworker.Name(testRun, "benchmark-workloads", "bench", 1)
	ctl.assignments[busy] = 1
	cl.setReplicas("bench", 1)
	if err := reconcile(t, c); err != nil {
		t.Fatalf("scale-down reconcile: %v", err)
	}
	cl.setReplicas("bench", 2)
	if err := reconcile(t, c); err != nil {
		t.Fatalf("scale-up reconcile: %v", err)
	}
	if got := cl.statuses["bench"]; got.Replicas != 1 {
		t.Errorf("status = %+v while the Worker drains, want 1 replica", got)
	}

	ctl.assignments[busy] = 0
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile after the Actor left: %v", err)
	}
	if w := ctl.worker(busy); w.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("Worker %s state = %v, want registered afresh and active", busy, w.GetStatus().GetState())
	}
	if got := cl.statuses["bench"]; got.Replicas != 2 || got.ReadyReplicas != 2 {
		t.Errorf("status = %+v, want 2 replicas, 2 ready", got)
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

// A label edit on the pool reaches its Workers in place, as the worker syncer
// applies it, including Workers adopted after a restart.
func TestPoolLabelChangeUpdatesWorkers(t *testing.T) {
	c, ctl, rel, cl := newTestController(pool("bench", 2, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	relabel := func(v string) {
		cl.editPool("bench", func(wp *atev1alpha1.WorkerPool) { wp.Labels = map[string]string{"workload": v} })
	}

	relabel("v2")
	creates := ctl.creates
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile after relabel: %v", err)
	}
	for _, name := range poolNames("bench", 2) {
		if got := ctl.worker(name).GetLabels()["workload"]; got != "v2" {
			t.Errorf("Worker %s workload label = %q, want v2", name, got)
		}
	}
	if ctl.creates != creates || ctl.updates != 2 {
		t.Errorf("relabel cost %d creates and %d updates, want 0 and 2", ctl.creates-creates, ctl.updates)
	}

	restarted := newController(ctl, rel, cl, testRun, 4, testDrainGrace)
	if err := restarted.adopt(context.Background()); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	relabel("v3")
	if err := reconcile(t, restarted); err != nil {
		t.Fatalf("reconcile after restart and relabel: %v", err)
	}
	for _, name := range poolNames("bench", 2) {
		if got := ctl.worker(name).GetLabels()["workload"]; got != "v3" {
			t.Errorf("after restart, Worker %s workload label = %q, want v3", name, got)
		}
	}
}

// sandbox_class is immutable on a Worker, so a sandboxClass edit replaces the
// pool's Workers the way it replaces real worker pods: an idle Worker at once,
// a held one once its Actors leave.
func TestPoolSandboxClassChangeReplacesWorkers(t *testing.T) {
	c, ctl, _, cl := newTestController(pool("bench", 2, nil))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	idle := fakeworker.Name(testRun, "benchmark-workloads", "bench", 0)
	busy := fakeworker.Name(testRun, "benchmark-workloads", "bench", 1)
	ctl.assignments[busy] = 1

	cl.editPool("bench", func(wp *atev1alpha1.WorkerPool) { wp.Spec.SandboxClass = "microvm" })
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile after the class change: %v", err)
	}
	if got := ctl.worker(idle).GetSandboxClass(); got != "microvm" {
		t.Errorf("idle Worker sandbox class = %q, want microvm", got)
	}
	if w := ctl.worker(busy); w.GetSandboxClass() != "gvisor" || w.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("busy Worker = %q/%v, want gvisor and draining", w.GetSandboxClass(), w.GetStatus().GetState())
	}
	if got := cl.statuses["bench"]; got.Replicas != 1 {
		t.Errorf("status = %+v, want 1 replica while the old Worker drains", got)
	}

	ctl.assignments[busy] = 0
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile after the Actor left: %v", err)
	}
	if w := ctl.worker(busy); w.GetSandboxClass() != "microvm" || w.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("replaced Worker = %q/%v, want microvm and active", w.GetSandboxClass(), w.GetStatus().GetState())
	}
	if got := cl.statuses["bench"]; got.Replicas != 2 || got.ReadyReplicas != 2 {
		t.Errorf("status = %+v, want 2 replicas, 2 ready", got)
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

	restarted := newController(ctl, rel, cl, testRun, 4, testDrainGrace)
	if err := restarted.adopt(context.Background()); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if got := len(restarted.workers); got != 2 {
		t.Fatalf("adopted %d Workers, want this run's 2", got)
	}
	creates, reports := ctl.creates, rel.calls
	if err := reconcile(t, restarted); err != nil {
		t.Fatalf("reconcile after restart: %v", err)
	}
	if ctl.creates != creates || rel.calls != reports {
		t.Errorf("after adopting: %d creates and %d reports, want none", ctl.creates-creates, rel.calls-reports)
	}
	if !slices.Contains(ctl.names(), real.Metadata.Name) || !slices.Contains(ctl.names(), otherRun.Metadata.Name) {
		t.Error("a Worker that is not this run's was retired")
	}
}

// A limits edit made while fake-workersync was down still replaces the
// pool's Workers: an adopted Worker whose reported capacity is not what the
// pool now gives is from an older template.
func TestAdoptReplacesWorkersOfAPoolEditedWhileDown(t *testing.T) {
	limits := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}
	c, ctl, rel, cl := newTestController(pool("bench", 1, limits))
	if err := reconcile(t, c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	name := fakeworker.Name(testRun, "benchmark-workloads", "bench", 0)
	cl.editPool("bench", func(wp *atev1alpha1.WorkerPool) {
		wp.Spec.Template.Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("8Gi")}
	})

	restarted := newController(ctl, rel, cl, testRun, 4, testDrainGrace)
	if err := restarted.adopt(context.Background()); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	creates := ctl.creates
	if err := reconcile(t, restarted); err != nil {
		t.Fatalf("reconcile after restart: %v", err)
	}
	if ctl.creates != creates+1 {
		t.Errorf("%d creates after a limits edit made while down, want 1", ctl.creates-creates)
	}
	if got, want := rel.reported[name], wantCapacity(1000, "4", "8Gi"); !proto.Equal(got, want) {
		t.Errorf("replaced Worker reported %v, want %v", got, want)
	}
}

// An adopted Worker whose capacity ate-api-server never accepted is reported
// after the restart, as ateom keeps retrying its one report.
func TestAdoptReportsAWorkerNeverAccepted(t *testing.T) {
	c, ctl, rel, cl := newTestController(pool("bench", 1, nil))
	first := fakeworker.Name(testRun, "benchmark-workloads", "bench", 0)
	rel.reject[first] = codes.Unavailable
	if err := reconcile(t, c); err == nil {
		t.Fatal("reconcile succeeded with a rejected report")
	}
	delete(rel.reject, first)

	restarted := newController(ctl, rel, cl, testRun, 4, testDrainGrace)
	if err := restarted.adopt(context.Background()); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if err := reconcile(t, restarted); err != nil {
		t.Fatalf("reconcile after restart: %v", err)
	}
	if got, want := rel.reported[first], wantCapacity(1000, "86", "160Gi"); !proto.Equal(got, want) {
		t.Errorf("adopted Worker reported %v, want %v", got, want)
	}
	if got := cl.statuses["bench"]; got.Replicas != 1 || got.ReadyReplicas != 1 {
		t.Errorf("status = %+v, want 1 replica, 1 ready", got)
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
	capacity := func(wp *atev1alpha1.WorkerPool) *ateapipb.WorkerResources {
		t.Helper()
		tmpl, err := templateOf(wp)
		if err != nil {
			t.Fatal(err)
		}
		return tmpl.capacity(nil)
	}
	got := capacity(pool("p", 1, corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0.5"), corev1.ResourceMemory: resource.MustParse("1G")}))
	// ateom spells cpu as decimal milli-cores and memory as a binary-SI byte
	// count; 1G is not a power of two, so 1e9.
	if want := wantCapacity(1000, "500m", "1e9"); !proto.Equal(got, want) {
		t.Errorf("capacity = %v, want %v", got, want)
	}
	if none := capacity(pool("p", 1, nil)); none.GetResources() != nil || none.GetActors() != 1000 {
		t.Errorf("no limits and no allocatable: %v; want 1000 actors and no resources", none)
	}
}
