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
	"log/slog"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/cmd/benchmarking/isolate/internal/fakeworker"
	"github.com/agent-substrate/substrate/internal/hardware"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// workerClient is the part of ateapipb.ControlClient the controller uses.
type workerClient interface {
	CreateWorker(ctx context.Context, in *ateapipb.CreateWorkerRequest, opts ...grpc.CallOption) (*ateapipb.Worker, error)
	DeleteWorker(ctx context.Context, in *ateapipb.DeleteWorkerRequest, opts ...grpc.CallOption) (*ateapipb.Worker, error)
	DrainWorker(ctx context.Context, in *ateapipb.DrainWorkerRequest, opts ...grpc.CallOption) (*ateapipb.Worker, error)
	GetWorker(ctx context.Context, in *ateapipb.GetWorkerRequest, opts ...grpc.CallOption) (*ateapipb.Worker, error)
	UpdateWorker(ctx context.Context, in *ateapipb.UpdateWorkerRequest, opts ...grpc.CallOption) (*ateapipb.Worker, error)
	ListWorkers(ctx context.Context, in *ateapipb.ListWorkersRequest, opts ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error)
	ListWorkerActorAssignments(ctx context.Context, in *ateapipb.ListWorkerActorAssignmentsRequest, opts ...grpc.CallOption) (*ateapipb.ListWorkerActorAssignmentsResponse, error)
}

// capacityRelay sends a capacity report to the fake-atelet relay at addr.
type capacityRelay interface {
	Report(ctx context.Context, addr string, req *ateapipb.RegisterWorkerRequest) error
	// Prune lets go of every relay not in live, the relay addresses by node.
	Prune(live map[string]string)
}

// node is a benchmark node and what it can allocate.
type node struct {
	name        string
	allocatable corev1.ResourceList
}

// cluster is what the controller reads from and writes to Kubernetes.
type cluster interface {
	// Pools returns every WorkerPool.
	Pools() ([]*atev1alpha1.WorkerPool, error)
	// Nodes returns the benchmark nodes, sorted by name.
	Nodes(ctx context.Context) ([]node, error)
	// Relays returns the fake-atelet relay address on each node that has one.
	Relays(ctx context.Context) (map[string]string, error)
	// UpdateStatus writes a WorkerPool's status.
	UpdateStatus(ctx context.Context, wp *atev1alpha1.WorkerPool) error
}

// fakeWorker is a fake Worker the controller has registered.
type fakeWorker struct {
	// name is the Worker's name and pod UID, fresh for each Worker, as a real
	// Worker is named after its pod's UID. A replaced Worker's successor is a
	// new Worker, so nothing ate-api-server keyed by the old one carries over.
	name string
	// pod is the stand-in pod name, fakeworker.Name, shared by every Worker
	// that fills the same index of a pool. It is how adopt finds this run's
	// Workers.
	pod             string
	namespace, pool string
	index           int
	node            string
	// created is set once ate-api-server has the Worker. A Worker is tracked
	// from before its create, so a create the server commits but the caller
	// sees fail is still retired, or deleted at shutdown.
	created bool
	// labels and sandboxClass are what the Worker is registered with.
	labels       map[string]string
	sandboxClass string
	// tmpl is the pool template the Worker was made from; nil for a Worker
	// adopted after a restart until reconcile matches it to its pool.
	tmpl *template
	// capacity is fixed when the Worker is made, as ateom reads its limits
	// once at startup; reported is whether ate-api-server has accepted it.
	capacity *ateapipb.WorkerResources
	reported bool
	draining bool
	// drainedAt is when the controller drained the Worker, or adopted it
	// draining.
	drainedAt time.Time
}

// controller stands in for atecontroller's WorkerPool controller and worker
// syncer together. For every WorkerPool it keeps spec.replicas fake Workers,
// none backed by a pod, has each one's capacity reported through the
// fake-atelet on its node, and writes the pool's status from them.
//
// It keeps the registered Workers in memory and lists them from ate-api-server
// only at startup (adopt). A pass with nothing changed and nothing draining
// makes no ate-api-server calls, so a steady fleet adds no load to the system
// under test.
type controller struct {
	client      workerClient
	relay       capacityRelay
	cluster     cluster
	run         string
	concurrency int
	// drainGrace is how long a draining Worker waits for its Actors to leave
	// before it is deleted with them still assigned.
	drainGrace time.Duration
	now        func() time.Time
	newUID     func() string

	mu      sync.Mutex
	workers map[string]*fakeWorker
}

func newController(client workerClient, relay capacityRelay, cl cluster, run string, concurrency int, drainGrace time.Duration) *controller {
	return &controller{client: client, relay: relay, cluster: cl, run: run, concurrency: concurrency, drainGrace: drainGrace, now: time.Now, newUID: uuid.NewString, workers: map[string]*fakeWorker{}}
}

// adopt loads the fake Workers of this run that are already registered, as
// after a restart, so they are reconciled rather than created again.
func (c *controller) adopt(ctx context.Context) error {
	prefix := fakeworker.Prefix(c.run)
	var token string
	for {
		page, err := c.client.ListWorkers(ctx, &ateapipb.ListWorkersRequest{PageSize: 1000, PageToken: token})
		if err != nil {
			return fmt.Errorf("while listing Workers: %w", err)
		}
		for _, w := range page.GetWorkers() {
			pod := w.GetWorkerPod()
			if !strings.HasPrefix(pod, prefix) {
				continue
			}
			index, ok := fakeworker.Index(c.run, w.GetWorkerNamespace(), w.GetWorkerPool(), pod)
			if !ok {
				continue
			}
			name := w.GetMetadata().GetName()
			c.workers[name] = &fakeWorker{
				name:         name,
				pod:          pod,
				namespace:    w.GetWorkerNamespace(),
				pool:         w.GetWorkerPool(),
				index:        index,
				node:         w.GetNodeName(),
				labels:       w.GetLabels(),
				sandboxClass: w.GetSandboxClass(),
				capacity:     w.GetStatus().GetCapacity(),
				reported:     w.GetStatus().GetCapacity() != nil,
				created:      true,
				draining:     w.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_DRAINING,
			}
			if c.workers[name].draining {
				// When it was drained is not recorded, so its grace restarts.
				c.workers[name].drainedAt = c.now()
			}
		}
		if token = page.GetNextPageToken(); token == "" {
			break
		}
	}
	slog.InfoContext(ctx, "Adopted registered fake Workers", slog.Int("workers", len(c.workers)))
	return nil
}

// desiredWorker is a fake Worker some WorkerPool wants.
type desiredWorker struct {
	pool  *atev1alpha1.WorkerPool
	index int
}

// reconcile brings the fake Workers and the pools' status in line with the
// WorkerPools. Every step is idempotent, so a failed pass is retried by the
// next one.
func (c *controller) reconcile(ctx context.Context) error {
	pools, err := c.cluster.Pools()
	if err != nil {
		return fmt.Errorf("while listing WorkerPools: %w", err)
	}
	nodes, err := c.cluster.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("while listing nodes: %w", err)
	}
	relays, err := c.cluster.Relays(ctx)
	if err != nil {
		return fmt.Errorf("while listing fake-atelet pods: %w", err)
	}
	c.relay.Prune(relays)
	nodeNames := make([]string, len(nodes))
	allocatable := map[string]corev1.ResourceList{}
	for i, n := range nodes {
		nodeNames[i] = n.name
		allocatable[n.name] = n.allocatable
	}

	desired := map[string]desiredWorker{}
	// held names the pools whose spec cannot be read. Their Workers are left
	// as they are rather than retired, so an annotation typo reports an error
	// instead of tearing the pool down.
	held := map[string]bool{}
	var errs []error
	for _, wp := range pools {
		if _, err := maxActors(wp); err != nil {
			errs = append(errs, err)
			held[wp.Namespace+"/"+wp.Name] = true
			continue
		}
		for i := range int(wp.Spec.Replicas) {
			desired[fakeworker.Name(c.run, wp.Namespace, wp.Name, i)] = desiredWorker{pool: wp, index: i}
		}
	}

	var errMu sync.Mutex
	fail := func(err error) {
		errMu.Lock()
		defer errMu.Unlock()
		errs = append(errs, err)
	}

	// Each desired pod is served by one current Worker; every other Worker is
	// retired. A draining Worker is retired until it is gone, even if its pool
	// wants its pod again: it takes no new actors and cannot be undrained.
	// A real pool replaces every worker pod when its sandbox class, limits or
	// actor count change, and each new pod reports its capacity once, so its
	// fake Workers are replaced (see fits). So is a Worker whose node has left
	// the benchmark set, as a pod is when its node goes away; with no
	// benchmark node listed, Workers are left where they are, since nothing
	// could replace them. A replaced pod gets a new Worker
	// in the same pass, while the old one drains, as a Deployment creates a
	// replacement pod without waiting for the old one to terminate.
	onBenchmarkNode := func(w *fakeWorker) bool {
		_, ok := allocatable[w.node]
		return ok || len(nodes) == 0
	}
	c.mu.Lock()
	current := map[string]*fakeWorker{}
	var unwanted []string
	for _, name := range slices.Sorted(maps.Keys(c.workers)) {
		w := c.workers[name]
		if held[w.namespace+"/"+w.pool] && !w.draining {
			continue
		}
		d, ok := desired[w.pod]
		if ok && !w.draining && onBenchmarkNode(w) && fits(w, d.pool, allocatable) && current[w.pod] == nil {
			current[w.pod] = w
			continue
		}
		unwanted = append(unwanted, name)
	}
	c.mu.Unlock()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.concurrency)
	for _, name := range unwanted {
		g.Go(func() error {
			if err := c.retire(gctx, name); err != nil {
				fail(err)
			}
			return nil
		})
	}
	_ = g.Wait()

	g, gctx = errgroup.WithContext(ctx)
	g.SetLimit(c.concurrency)
	for _, pod := range slices.Sorted(maps.Keys(desired)) {
		d, w := desired[pod], current[pod]
		g.Go(func() error {
			if err := c.ensure(gctx, pod, d, w, nodeNames, allocatable, relays); err != nil {
				fail(err)
			}
			return nil
		})
	}
	_ = g.Wait()

	for _, wp := range pools {
		if err := c.syncStatus(ctx, wp, onBenchmarkNode, allocatable); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ensure registers a Worker for the desired pod if w, its current Worker, is
// nil, brings the Worker's labels in line with the pool's, then has its
// capacity reported until ate-api-server accepts it. The capacity is fixed
// when the Worker is made, as ateom reads its limits once at startup, and is
// reported once. The Worker is tracked before CreateWorker is called and
// marked created after, so a failed create is retried by the next pass under
// the same name, and one the server committed anyway is never lost: retire
// and deleteAll take a NotFound as done.
func (c *controller) ensure(ctx context.Context, pod string, d desiredWorker, w *fakeWorker, nodes []string, allocatable map[string]corev1.ResourceList, relays map[string]string) error {
	if w == nil {
		if len(nodes) == 0 {
			return errors.New("no benchmark node to place fake Workers on")
		}
		w = &fakeWorker{
			name:         c.newUID(),
			pod:          pod,
			namespace:    d.pool.Namespace,
			pool:         d.pool.Name,
			index:        d.index,
			node:         fakeworker.Node(d.index, nodes),
			labels:       maps.Clone(d.pool.GetLabels()),
			sandboxClass: string(d.pool.Spec.SandboxClass),
		}
		c.mu.Lock()
		c.workers[w.name] = w
		c.mu.Unlock()
	}
	name := w.name
	c.mu.Lock()
	created, tmpl, fixed, reported := w.created, w.tmpl, w.capacity, w.reported
	c.mu.Unlock()
	// A new Worker takes its template and capacity from the pool now. An
	// adopted Worker fits its pool, or it would not be current, so it takes
	// the pool's template and keeps the capacity it reported, if it did.
	if tmpl == nil || fixed == nil {
		t, err := templateOf(d.pool)
		if err != nil {
			return err
		}
		if fixed == nil {
			alloc, ok := allocatable[w.node]
			if !ok {
				return fmt.Errorf("node %s of Worker %s is not a benchmark node", w.node, name)
			}
			fixed = t.capacity(alloc)
		}
		c.mu.Lock()
		w.tmpl, w.capacity = &t, fixed
		c.mu.Unlock()
	}
	if !created {
		if err := c.create(ctx, w, d.pool); err != nil {
			return err
		}
		c.mu.Lock()
		w.created = true
		c.mu.Unlock()
	}
	if !maps.Equal(w.labels, d.pool.GetLabels()) {
		if err := c.updateLabels(ctx, name, w, d.pool.GetLabels()); err != nil {
			return err
		}
	}
	if reported {
		return nil
	}
	addr := relays[w.node]
	if addr == "" {
		return fmt.Errorf("no fake-atelet on node %s yet for Worker %s", w.node, name)
	}
	if err := c.relay.Report(ctx, addr, &ateapipb.RegisterWorkerRequest{
		Worker:   &ateapipb.ObjectRef{Name: name},
		Capacity: fixed,
		DefaultRuntime: &ateapipb.SandboxRuntime{
			SandboxClass:  string(d.pool.Spec.SandboxClass),
			CompatVersion: hardware.ProbeHost(),
		},
	}); err != nil {
		return fmt.Errorf("while reporting capacity for Worker %s: %w", name, err)
	}
	c.mu.Lock()
	w.reported = true
	c.mu.Unlock()
	return nil
}

// create registers the Worker with the fields the worker syncer would copy
// from the pool and its pod. Epoch stays 0: a rising epoch makes
// ate-api-server crash every Actor on the Worker.
func (c *controller) create(ctx context.Context, w *fakeWorker, wp *atev1alpha1.WorkerPool) error {
	name := w.name
	_, err := c.client.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: name},
		WorkerNamespace: wp.Namespace,
		WorkerPool:      wp.Name,
		WorkerPod:       w.pod,
		WorkerPodUid:    name,
		NodeName:        w.node,
		Ips:             []string{fakeworker.IP(w.index)},
		SandboxClass:    string(wp.Spec.SandboxClass),
		Labels:          maps.Clone(wp.GetLabels()),
	}})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("while creating Worker %s: %w", name, err)
	}
	return nil
}

// updateLabels writes the pool's labels onto a registered Worker, as the
// worker syncer does when a pool's labels change. UpdateWorker replaces the
// whole resource and takes the version it was read at as its precondition, so
// the Worker is read first; a conflicting write fails the call and the next
// pass retries it.
func (c *controller) updateLabels(ctx context.Context, name string, w *fakeWorker, want map[string]string) error {
	got, err := c.client.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: &ateapipb.ObjectRef{Name: name}})
	if err != nil {
		return fmt.Errorf("while reading Worker %s: %w", name, err)
	}
	if !maps.Equal(got.GetLabels(), want) {
		got.Labels = maps.Clone(want)
		if _, err := c.client.UpdateWorker(ctx, &ateapipb.UpdateWorkerRequest{Worker: got}); err != nil {
			return fmt.Errorf("while updating Worker %s labels: %w", name, err)
		}
	}
	c.mu.Lock()
	w.labels = maps.Clone(want)
	c.mu.Unlock()
	return nil
}

// retire removes a Worker no pool wants, the way a real one goes when its pod
// does: drained first so nothing new lands on it, deleted once no Actor is
// assigned to it. In production ateom stops the pod's Actors within its
// termination grace period, and the Worker is deleted when the pod is gone,
// which releases any Actor still bound. No ateom stops them here, so after
// drainGrace the Worker is deleted with its Actors still assigned, and
// DeleteWorker releases them the same way.
func (c *controller) retire(ctx context.Context, name string) error {
	c.mu.Lock()
	w := c.workers[name]
	c.mu.Unlock()
	ref := &ateapipb.ObjectRef{Name: name}
	if !w.draining {
		if _, err := c.client.DrainWorker(ctx, &ateapipb.DrainWorkerRequest{Worker: ref}); err != nil {
			if status.Code(err) == codes.NotFound {
				c.forget(name)
				return nil
			}
			return fmt.Errorf("while draining Worker %s: %w", name, err)
		}
		c.mu.Lock()
		w.draining = true
		w.drainedAt = c.now()
		c.mu.Unlock()
	}
	assigned, err := c.client.ListWorkerActorAssignments(ctx, &ateapipb.ListWorkerActorAssignmentsRequest{Worker: ref, PageSize: 1})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			c.forget(name)
			return nil
		}
		return fmt.Errorf("while listing Actors on Worker %s: %w", name, err)
	}
	if len(assigned.GetActorAssignments()) > 0 && c.now().Sub(w.drainedAt) < c.drainGrace {
		return nil
	}
	if _, err := c.client.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{Worker: ref}); err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("while deleting Worker %s: %w", name, err)
	}
	c.forget(name)
	return nil
}

func (c *controller) forget(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.workers, name)
}

// syncStatus writes the pool's status as the WorkerPool controller does from
// its Deployment: a replica is a registered Worker, ready once its capacity
// is reported, and the selector is the one worker pods would carry.
func (c *controller) syncStatus(ctx context.Context, wp *atev1alpha1.WorkerPool, onBenchmarkNode func(*fakeWorker) bool, allocatable map[string]corev1.ResourceList) error {
	want := atev1alpha1.WorkerPoolStatus{
		Selector: labels.SelectorFromSet(labels.Set{fakeworker.WorkerPoolLabel: wp.Name}).String(),
	}
	c.mu.Lock()
	for _, w := range c.workers {
		// A Worker its pool no longer wants, made from an older template, or
		// off the benchmark nodes is no replica even before its drain lands.
		if w.namespace != wp.Namespace || w.pool != wp.Name || !w.created || w.draining ||
			w.index >= int(wp.Spec.Replicas) || !onBenchmarkNode(w) || !fits(w, wp, allocatable) {
			continue
		}
		want.Replicas++
		if w.reported {
			want.ReadyReplicas++
		}
	}
	c.mu.Unlock()
	if wp.Status == want {
		return nil
	}
	updated := wp.DeepCopy()
	updated.Status = want
	if err := c.cluster.UpdateStatus(ctx, updated); err != nil {
		return fmt.Errorf("while updating WorkerPool %s/%s status: %w", wp.Namespace, wp.Name, err)
	}
	return nil
}

// deleteAll deletes every fake Worker this run registered, as at shutdown,
// after the benchmark has deleted its actors. It keeps going past a failure
// and reports every one.
func (c *controller) deleteAll(ctx context.Context) error {
	c.mu.Lock()
	names := slices.Sorted(maps.Keys(c.workers))
	c.mu.Unlock()
	var g errgroup.Group
	g.SetLimit(c.concurrency)
	errs := make([]error, len(names))
	for i, name := range names {
		g.Go(func() error {
			_, err := c.client.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{Worker: &ateapipb.ObjectRef{Name: name}})
			if err != nil && status.Code(err) != codes.NotFound {
				errs[i] = fmt.Errorf("while deleting Worker %s: %w", name, err)
				return nil
			}
			c.forget(name)
			return nil
		})
	}
	_ = g.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	slog.InfoContext(ctx, "Deleted fake Workers", slog.Int("workers", len(names)))
	return nil
}

// template is what a pool's spec fixes about each Worker it makes: the
// sandbox class, the cpu and memory limits, zero where unset, and the actor
// count. Changing any of them replaces a real pool's worker pods.
type template struct {
	sandboxClass          string
	cpuMilli, memoryBytes int64
	actors                int
}

// templateOf reads wp's template; it fails only on a bad max-actors
// annotation.
func templateOf(wp *atev1alpha1.WorkerPool) (template, error) {
	actors, err := maxActors(wp)
	if err != nil {
		return template{}, err
	}
	t := template{sandboxClass: string(wp.Spec.SandboxClass), actors: actors}
	if wp.Spec.Template != nil && wp.Spec.Template.Resources != nil {
		limits := wp.Spec.Template.Resources.Limits
		t.cpuMilli = limits.Cpu().MilliValue()
		t.memoryBytes = limits.Memory().Value()
	}
	return t, nil
}

// capacity is what a worker made from t reports on a node with allocatable:
// cpu and memory from its limits, or, with no limit set, the node's
// allocatable, which is what the downward API projects into ateom for an
// unset limit.
func (t template) capacity(allocatable corev1.ResourceList) *ateapipb.WorkerResources {
	cpu, memory := t.cpuMilli, t.memoryBytes
	if cpu == 0 {
		cpu = allocatable.Cpu().MilliValue()
	}
	if memory == 0 {
		memory = allocatable.Memory().Value()
	}
	return &ateapipb.WorkerResources{Actors: int32(t.actors), Resources: resources.CPUMemory(cpu, memory)}
}

// fits reports whether w was made from wp's current template, so a real pool
// would keep its pod. A Worker adopted after a restart has only its reported
// capacity to go by, so it fits when that is what wp would give it on its
// node now; one whose node's allocatable changed while fake-workersync was
// down is replaced too. A held pool's Workers fit, since they are left as
// they are.
func fits(w *fakeWorker, wp *atev1alpha1.WorkerPool, allocatable map[string]corev1.ResourceList) bool {
	if w.sandboxClass != string(wp.Spec.SandboxClass) {
		return false
	}
	t, err := templateOf(wp)
	if err != nil {
		return true
	}
	if w.tmpl != nil {
		return *w.tmpl == t
	}
	alloc, ok := allocatable[w.node]
	if w.capacity == nil || !ok {
		return true
	}
	return proto.Equal(w.capacity, t.capacity(alloc))
}

// maxActors is the pool's actor capacity per Worker: ateom's default, unless
// the pool's annotation overrides it.
func maxActors(wp *atev1alpha1.WorkerPool) (int, error) {
	v, ok := wp.Annotations[fakeworker.MaxActorsAnnotation]
	if !ok {
		return fakeworker.DefaultMaxActors, nil
	}
	// Bounded to 32 bits, the width of a capacity report's actor count.
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("WorkerPool %s/%s: %s must be an integer from 1 to %d, got %q", wp.Namespace, wp.Name, fakeworker.MaxActorsAnnotation, math.MaxInt32, v)
	}
	return int(n), nil
}
