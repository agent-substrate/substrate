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
	"slices"
	"strings"
	"sync"

	"github.com/agent-substrate/substrate/cmd/benchmarking/isolate/internal/fakeworker"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/labels"
)

// workerClient is the part of ateapipb.ControlClient the controller uses.
type workerClient interface {
	CreateWorker(ctx context.Context, in *ateapipb.CreateWorkerRequest, opts ...grpc.CallOption) (*ateapipb.Worker, error)
	DeleteWorker(ctx context.Context, in *ateapipb.DeleteWorkerRequest, opts ...grpc.CallOption) (*ateapipb.Worker, error)
	ListWorkers(ctx context.Context, in *ateapipb.ListWorkersRequest, opts ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error)
}

// node is a benchmark node.
type node struct {
	name string
}

// cluster is what the controller reads from and writes to Kubernetes.
type cluster interface {
	// Pools returns every WorkerPool.
	Pools() ([]*atev1alpha1.WorkerPool, error)
	// Nodes returns the benchmark nodes, sorted by name.
	Nodes(ctx context.Context) ([]node, error)
	// UpdateStatus writes a WorkerPool's status.
	UpdateStatus(ctx context.Context, wp *atev1alpha1.WorkerPool) error
}

// fakeWorker is a fake Worker the controller has registered.
type fakeWorker struct {
	namespace, pool string
	index           int
	node            string
	// created is set once ate-api-server has the Worker. A Worker is tracked
	// from before its create, so a create the server commits but the caller
	// sees fail is still retired, or deleted at shutdown.
	created bool
}

// controller stands in for atecontroller's WorkerPool controller and worker
// syncer together. For every WorkerPool it keeps spec.replicas fake Workers,
// none backed by a pod, and writes the pool's status from them.
//
// It keeps the registered Workers in memory and lists them from ate-api-server
// only at startup (adopt). A pass with nothing changed makes no
// ate-api-server calls, so a steady fleet adds no load to the system under
// test.
type controller struct {
	client      workerClient
	cluster     cluster
	run         string
	concurrency int

	mu      sync.Mutex
	workers map[string]*fakeWorker
}

func newController(client workerClient, cl cluster, run string, concurrency int) *controller {
	return &controller{client: client, cluster: cl, run: run, concurrency: concurrency, workers: map[string]*fakeWorker{}}
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
			name := w.GetMetadata().GetName()
			if !strings.HasPrefix(name, prefix) {
				continue
			}
			index, ok := fakeworker.Index(c.run, w.GetWorkerNamespace(), w.GetWorkerPool(), name)
			if !ok {
				continue
			}
			c.workers[name] = &fakeWorker{
				namespace: w.GetWorkerNamespace(),
				pool:      w.GetWorkerPool(),
				index:     index,
				node:      w.GetNodeName(),
				created:   true,
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
	nodeNames := make([]string, len(nodes))
	for i, n := range nodes {
		nodeNames[i] = n.name
	}

	desired := map[string]desiredWorker{}
	var errs []error
	for _, wp := range pools {
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

	c.mu.Lock()
	var unwanted []string
	for name := range c.workers {
		if _, ok := desired[name]; !ok {
			unwanted = append(unwanted, name)
		}
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
	for _, name := range slices.Sorted(maps.Keys(desired)) {
		d := desired[name]
		g.Go(func() error {
			if err := c.ensure(gctx, name, d, nodeNames); err != nil {
				fail(err)
			}
			return nil
		})
	}
	_ = g.Wait()

	for _, wp := range pools {
		if err := c.syncStatus(ctx, wp); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ensure registers the desired Worker if it is not yet. The Worker is
// tracked before CreateWorker is called and marked created after, so a failed
// create is retried by the next pass, and one the server committed anyway is
// never lost: retire and deleteAll take a NotFound as done.
func (c *controller) ensure(ctx context.Context, name string, d desiredWorker, nodes []string) error {
	c.mu.Lock()
	w := c.workers[name]
	created := w != nil && w.created
	c.mu.Unlock()
	if created {
		return nil
	}
	if w == nil {
		if len(nodes) == 0 {
			return errors.New("no benchmark node to place fake Workers on")
		}
		w = &fakeWorker{
			namespace: d.pool.Namespace,
			pool:      d.pool.Name,
			index:     d.index,
			node:      fakeworker.Node(d.index, nodes),
		}
		c.mu.Lock()
		c.workers[name] = w
		c.mu.Unlock()
	}
	if err := c.create(ctx, name, w, d.pool); err != nil {
		return err
	}
	c.mu.Lock()
	w.created = true
	c.mu.Unlock()
	return nil
}

// create registers the Worker with the fields the worker syncer would copy
// from the pool and its pod. Epoch stays 0: a rising epoch makes
// ate-api-server crash every Actor on the Worker.
func (c *controller) create(ctx context.Context, name string, w *fakeWorker, wp *atev1alpha1.WorkerPool) error {
	_, err := c.client.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: name},
		WorkerNamespace: wp.Namespace,
		WorkerPool:      wp.Name,
		WorkerPod:       name,
		WorkerPodUid:    fakeworker.PodUID(name),
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

// retire removes a Worker no pool wants.
func (c *controller) retire(ctx context.Context, name string) error {
	if _, err := c.client.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{Worker: &ateapipb.ObjectRef{Name: name}}); err != nil && status.Code(err) != codes.NotFound {
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
// its Deployment: a replica is a registered Worker, and the selector is the
// one worker pods would carry.
func (c *controller) syncStatus(ctx context.Context, wp *atev1alpha1.WorkerPool) error {
	want := atev1alpha1.WorkerPoolStatus{
		Selector: labels.SelectorFromSet(labels.Set{fakeworker.WorkerPoolLabel: wp.Name}).String(),
	}
	c.mu.Lock()
	for _, w := range c.workers {
		if w.namespace != wp.Namespace || w.pool != wp.Name || !w.created {
			continue
		}
		want.Replicas++
		want.ReadyReplicas++
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
