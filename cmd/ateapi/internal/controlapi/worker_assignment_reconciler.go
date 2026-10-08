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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

// workerAssignmentWorkerCount is the number of goroutines draining the work queue.
const workerAssignmentWorkerCount = 2

// errWorkerLeased reports a Worker another replica is reconciling.
var errWorkerLeased = errors.New("worker is being reconciled by another replica")

// workerWatcher delivers every Worker change to a handler. *workercache.Cache
// satisfies it.
type workerWatcher interface {
	AddHandler(handler func(*ateapipb.Worker))
}

// WorkerAssignmentReconciler keeps the Actors assigned to a Worker in line with
// its epoch. It watches for Workers whose epoch has risen past
// status.observed_epoch, and crashes the Actors a restarted worker took with it.
//
// TODO: Every ateapi replica runs this reconciler and queues every Worker, with
// only a per-Worker lease keeping them from releasing the same one at once.
// Have a single elected leader process Workers, possibly by moving controller
// loops like this one out of the API server into their own Deployment.
type WorkerAssignmentReconciler struct {
	persistence workerWorkflowStore
	workflow    *WorkerWorkflow
	workers     workerWatcher
	queue       workqueue.TypedRateLimitingInterface[string]
}

func NewWorkerAssignmentReconciler(persistence workerWorkflowStore, workers workerWatcher) *WorkerAssignmentReconciler {
	return &WorkerAssignmentReconciler{
		persistence: persistence,
		workflow:    NewWorkerWorkflow(persistence),
		workers:     workers,
		queue:       workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
	}
}

// Start launches the queue workers and subscribes to Worker changes.
func (r *WorkerAssignmentReconciler) Start(ctx context.Context) {
	for range workerAssignmentWorkerCount {
		go wait.UntilWithContext(ctx, r.runWorker, time.Second)
	}
	go func() {
		<-ctx.Done()
		r.queue.ShutDown()
	}()
	r.workers.AddHandler(r.enqueue)
}

// enqueue queues worker if its epoch has not been observed.
func (r *WorkerAssignmentReconciler) enqueue(worker *ateapipb.Worker) {
	if needsAssignmentReconcile(worker) {
		r.queue.Add(worker.GetMetadata().GetName())
	}
}

func (r *WorkerAssignmentReconciler) runWorker(ctx context.Context) {
	for r.processNextWorkItem(ctx) {
	}
}

func (r *WorkerAssignmentReconciler) processNextWorkItem(ctx context.Context) bool {
	name, quit := r.queue.Get()
	if quit {
		return false
	}
	defer r.queue.Done(name)

	if err := r.reconcileOne(ctx, name); err != nil {
		if errors.Is(err, errActorBusy) || errors.Is(err, errWorkerLeased) {
			slog.InfoContext(ctx, "Waiting on an operation in progress, requeueing",
				slog.String("worker", name),
				slog.Any("err", err))
			r.queue.AddRateLimited(name)
			return true
		}
		slog.ErrorContext(ctx, "Failed to reconcile the actors assigned to a worker, requeueing",
			slog.String("worker", name),
			slog.Any("err", err))
		r.queue.AddRateLimited(name)
		return true
	}
	r.queue.Forget(name)
	return true
}

// reconcileOne reconciles one Worker's assignments, holding the Worker's lease
// so concurrent replicas don't sweep it at once.
func (r *WorkerAssignmentReconciler) reconcileOne(ctx context.Context, name string) error {
	lease, err := r.persistence.AcquireLease(ctx, workerAssignmentLeaseKey(name))
	if err != nil {
		if errors.Is(err, store.ErrLeaseConflict) {
			// Retried in case that replica does not finish; once it has, the
			// retry finds the epoch observed.
			return errWorkerLeased
		}
		return fmt.Errorf("while acquiring lease: %w", err)
	}
	defer lease.Close()
	return r.workflow.ReconcileAssignments(lease.Context(), name)
}

func workerAssignmentLeaseKey(workerName string) string {
	return "lease:workerassignment:" + workerName
}
