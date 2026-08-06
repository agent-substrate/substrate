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
	"maps"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// syncerWorkerCount is the number of goroutines draining the work queue. The
// queue never hands the same key to two workers concurrently, so per-key
// ordering is preserved.
const syncerWorkerCount = 2

// workerKey identifies a worker row in the store. pool is captured at enqueue
// time because once the pod is gone from the informer cache, the
// ate.dev/worker-pool label cannot be recovered from a namespace/name key.
type workerKey struct {
	namespace string
	pool      string
	name      string
}

// WorkerPoolSyncer reconciles the state of worker pods from Kubernetes Informer
// into the store.
//
// Informer event handlers only enqueue keys; worker goroutines reconcile each
// key against the current informer cache state, requeuing with rate-limited
// backoff on transient failures such as store.ErrVersionConflict.
type WorkerPoolSyncer struct {
	persistence      store.Interface
	workerInformer   cache.SharedIndexInformer
	workerPoolLister listersv1alpha1.WorkerPoolLister
	queue            workqueue.TypedRateLimitingInterface[workerKey]
}

// NewWorkerPoolSyncer creates a new WorkerPoolSyncer.
func NewWorkerPoolSyncer(persistence store.Interface, workerInformer cache.SharedIndexInformer, workerPoolLister listersv1alpha1.WorkerPoolLister) *WorkerPoolSyncer {
	return &WorkerPoolSyncer{
		persistence:      persistence,
		workerInformer:   workerInformer,
		workerPoolLister: workerPoolLister,
		queue:            workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[workerKey]()),
	}
}

// Start registers the event handlers and starts the background workers. The
// informer's initial list synthesizes Add events for every existing pod, so no
// explicit startup re-list is needed as long as Start is called before the
// informer factory is started.
func (s *WorkerPoolSyncer) Start(ctx context.Context) {
	s.workerInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			s.enqueuePod(obj.(*corev1.Pod))
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod := oldObj.(*corev1.Pod)
			newPod := newObj.(*corev1.Pod)
			// If the pool label changed, enqueue the old key too so its
			// now-stale store row gets cleaned up.
			if oldPod.Labels[workerPodLabel] != newPod.Labels[workerPodLabel] {
				s.enqueuePod(oldPod)
			}
			s.enqueuePod(newPod)
		},
		DeleteFunc: func(obj interface{}) {
			var pod *corev1.Pod
			switch t := obj.(type) {
			case *corev1.Pod:
				pod = t
			case cache.DeletedFinalStateUnknown:
				var ok bool
				pod, ok = t.Obj.(*corev1.Pod)
				if !ok {
					slog.ErrorContext(ctx, "Failed to cast DeletedFinalStateUnknown object to Pod")
					return
				}
			default:
				slog.ErrorContext(ctx, "Unknown object type in delete handler", slog.Any("obj", obj))
				return
			}
			s.enqueuePod(pod)
		},
	})

	go func() {
		defer s.queue.ShutDown()
		if !cache.WaitForCacheSync(ctx.Done(), s.workerInformer.HasSynced) {
			slog.ErrorContext(ctx, "Syncer: failed to sync informer cache")
			return
		}
		for range syncerWorkerCount {
			go wait.UntilWithContext(ctx, s.runWorker, time.Second)
		}

		// Reconcile the other direction: enqueue every stored worker so records
		// whose pods no longer exist are cleaned up. This recovers delete events
		// missed while ate-api-server was down — neither the watch relist nor
		// the resync period can replay a delete across a process restart,
		// because the informer cache starts empty. Runs after the cache sync so
		// the indexer is an authoritative snapshot of live pods.
		s.enqueueStoredWorkers(ctx)

		<-ctx.Done()
	}()
}

func (s *WorkerPoolSyncer) enqueuePod(pod *corev1.Pod) {
	s.queue.Add(workerKey{namespace: pod.Namespace, pool: pod.Labels[workerPodLabel], name: pod.Name})
}

func (s *WorkerPoolSyncer) runWorker(ctx context.Context) {
	for s.processNextWorkItem(ctx) {
	}
}

func (s *WorkerPoolSyncer) processNextWorkItem(ctx context.Context) bool {
	key, quit := s.queue.Get()
	if quit {
		return false
	}
	defer s.queue.Done(key)

	if err := s.reconcile(ctx, key); err != nil {
		slog.ErrorContext(ctx, "Syncer: reconcile failed, requeueing",
			slog.String("worker", key.namespace+"/"+key.name),
			slog.String("pool", key.pool),
			slog.Any("err", err))
		s.queue.AddRateLimited(key)
		return true
	}
	s.queue.Forget(key)
	return true
}

// reconcile converges the store row for key with the current pod state in the
// informer cache. Returning an error requeues the key with backoff.
func (s *WorkerPoolSyncer) reconcile(ctx context.Context, key workerKey) error {
	obj, exists, err := s.workerInformer.GetIndexer().GetByKey(key.namespace + "/" + key.name)
	if err != nil {
		return err
	}
	if !exists {
		slog.InfoContext(ctx, "Syncer: removing worker from store (pod deleted)", slog.String("worker", key.namespace+"/"+key.name))
		return s.reconcileDeadWorker(ctx, key.namespace, key.pool, key.name)
	}
	pod := obj.(*corev1.Pod)
	if pod.Labels[workerPodLabel] != key.pool {
		// The pod moved to a different pool; this key's store row is stale.
		return s.reconcileDeadWorker(ctx, key.namespace, key.pool, key.name)
	}
	// Checked before eligibility: draining works off the stored record by name and
	// never reads the pod IP, while a Terminating pod can legitimately report no
	// IP once its sandbox is torn down. Gating on the IP first would drop the
	// transition and leave the worker schedulable for as long as the pod lingers.
	if pod.DeletionTimestamp != nil {
		// The pod has entered Terminating: mark the worker DRAINING so the
		// scheduler stops routing new actors to it. We deliberately do NOT touch
		// the bound actor here — inside the pod ateom has received SIGTERM and is
		// gracefully shutting the actor down. Actor cleanup happens on the Pod
		// Deleted event.
		return s.markWorkerDraining(ctx, key.namespace, key.pool, key.name)
	}
	if !isWorkerEligible(pod) {
		// No IP yet; a later update event re-enqueues the pod.
		return nil
	}
	return s.createOrUpdateWorker(ctx, key, pod)
}

func (s *WorkerPoolSyncer) createOrUpdateWorker(ctx context.Context, key workerKey, pod *corev1.Pod) error {
	pool, err := s.workerPoolLister.WorkerPools(key.namespace).Get(key.pool)
	if err != nil {
		return fmt.Errorf("getting WorkerPool %s/%s: %w", key.namespace, key.pool, err)
	}

	w, err := s.persistence.GetWorker(ctx, key.namespace, key.pool, key.name)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("getting worker from store: %w", err)
		}
		slog.InfoContext(ctx, "Syncer: creating worker in store", slog.String("worker", key.namespace+"/"+key.name))
		worker := &ateapipb.Worker{
			WorkerNamespace: pod.Namespace,
			WorkerPool:      key.pool,
			WorkerPod:       pod.Name,
			Ip:              pod.Status.PodIP,
			WorkerPodUid:    string(pod.UID),
			NodeName:        pod.Spec.NodeName,
			SandboxClass:    string(pool.Spec.SandboxClass),
			Labels:          pool.GetLabels(),
			State:           ateapipb.Worker_STATE_ACTIVE,
		}
		// TODO(thockin): for now this is the only place Workers are
		// created.  If/when this becomes a regular API, validation should
		// move there.
		if errs := resources.ValidateWorker(worker, nil); len(errs) > 0 {
			// Terminal: the inputs are deterministic, retrying cannot help. A
			// future pod event re-enqueues the key.
			slog.ErrorContext(ctx, "Invalid worker", slog.Any("err", errs.ToAggregate()))
			return nil
		}
		// ErrAlreadyExists means we lost a create race; requeue and converge
		// via the update path.
		return s.persistence.CreateWorker(ctx, worker)
	}

	if w.WorkerPodUid != string(pod.UID) {
		// The pod was deleted and recreated under the same name, and the queue
		// coalesced the two events. The store row belongs to the dead pod:
		// clean it up (releasing any actor bound to the old incarnation) and
		// requeue to create a fresh row.
		slog.InfoContext(ctx, "Syncer: worker in store belongs to a replaced pod, deleting", slog.String("worker", key.namespace+"/"+key.name))
		if err := s.reconcileDeadWorker(ctx, key.namespace, key.pool, key.name); err != nil {
			return err
		}
		return fmt.Errorf("worker %s/%s/%s belonged to replaced pod UID %s; requeueing to recreate", key.namespace, key.pool, key.name, w.WorkerPodUid)
	}

	changed := false
	if w.Ip != pod.Status.PodIP {
		// TODO: I don't think this is possible, but handling this case so we can log it just in case we can reproduce it.
		slog.InfoContext(ctx, "Syncer: updating worker in store (IP changed)", slog.String("worker", key.namespace+"/"+key.name))
		w.Ip = pod.Status.PodIP
		changed = true
	}
	if w.SandboxClass != string(pool.Spec.SandboxClass) {
		slog.InfoContext(ctx, "Syncer: updating worker in store (SandboxClass changed)", slog.String("worker", key.namespace+"/"+key.name))
		w.SandboxClass = string(pool.Spec.SandboxClass)
		changed = true
	}
	if !maps.Equal(w.Labels, pool.GetLabels()) {
		slog.InfoContext(ctx, "Syncer: updating worker in store (labels changed)", slog.String("worker", key.namespace+"/"+key.name))
		w.Labels = pool.GetLabels()
		changed = true
	}
	if !changed {
		return nil
	}

	// ErrVersionConflict requeues the key; the retry re-fetches the worker at
	// its new version.
	return s.persistence.UpdateWorker(ctx, w, w.Version)
}

func isWorkerEligible(pod *corev1.Pod) bool {
	return pod.Status.PodIP != ""
}

// markWorkerDraining transitions a worker to STATE_DRAINING so the scheduler
// stops routing new actors to it while its pod is Terminating. If the worker is
// already gone or already draining there is nothing more to do — the Pod
// Deleted event will clean up the record. A version conflict is returned so the
// caller requeues and retries against the updated record.
func (s *WorkerPoolSyncer) markWorkerDraining(ctx context.Context, namespace, pool, podName string) error {
	worker, err := s.persistence.GetWorker(ctx, namespace, pool, podName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if worker.GetState() == ateapipb.Worker_STATE_DRAINING {
		return nil
	}
	slog.InfoContext(ctx, "Syncer: marking worker draining (pod deleting)", slog.String("worker", namespace+"/"+podName))
	worker.State = ateapipb.Worker_STATE_DRAINING
	return s.persistence.UpdateWorker(ctx, worker, worker.GetVersion())
}

// reconcileDeadWorker cleans up a worker whose pod is gone. It releases the
// bound actor first and only deletes the worker record if that succeeds:
// deleting the record is what erases the actor->pod pointer, so on a release
// failure we intentionally leave the record in place (and return the error) so a
// later reconcile can retry. Returns nil once the actor is released and the
// worker record deleted.
func (s *WorkerPoolSyncer) reconcileDeadWorker(ctx context.Context, namespace, pool, podName string) error {
	if err := s.releaseActorOnDeadWorker(ctx, namespace, pool, podName); err != nil {
		return err
	}
	return s.persistence.DeleteWorker(ctx, namespace, pool, podName)
}

// enqueueStoredWorkers enqueues a key for every worker record in the store.
// Records whose pods are live and unchanged reconcile to a no-op; orphaned
// records (pod gone, or its name reused by a new pod UID) get cleaned up.
func (s *WorkerPoolSyncer) enqueueStoredWorkers(ctx context.Context) {
	var pageToken string
	for {
		workers, nextToken, err := s.persistence.ListWorkers(ctx, 1000, pageToken)
		if err != nil {
			slog.ErrorContext(ctx, "Syncer: failed to list workers for orphan reconcile", slog.Any("err", err))
			return
		}
		for _, w := range workers {
			s.queue.Add(workerKey{namespace: w.GetWorkerNamespace(), pool: w.GetWorkerPool(), name: w.GetWorkerPod()})
		}
		if nextToken == "" {
			return
		}
		pageToken = nextToken
	}
}

// releaseActorOnDeadWorker resets the actor bound to a vanishing worker pod. An
// actor that already reached STATUS_SUSPENDED (it saved its state cleanly during
// graceful termination) is left untouched and remains resumable. An actor that
// was still running when the pod disappeared is moved to STATUS_CRASHED and its
// pod pointers are cleared.
//
// UpdateActor uses optimistic version checking. A concurrent SuspendActor
// or ResumeActor wins; we fail this attempt so it can be retried with the
// updated state.
func (s *WorkerPoolSyncer) releaseActorOnDeadWorker(ctx context.Context, namespace, pool, podName string) error {
	worker, err := s.persistence.GetWorker(ctx, namespace, pool, podName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if worker.Assignment == nil {
		return nil
	}
	actor, err := s.persistence.GetActor(ctx, resources.ActorRefFromObjectRef(worker.Assignment.Actor))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	// Skip if a concurrent SuspendActor already cleared the pointer.
	assignment := actor.GetWorkerAssignment()
	if assignment.GetWorkerNamespace() != namespace || assignment.GetWorkerPod() != podName {
		return nil
	}
	// If the actor is suspended, it's already been released.
	if actor.Status == ateapipb.Actor_STATUS_SUSPENDED {
		return nil
	}
	opName := ateattr.OperationUnknown
	switch actor.GetStatus() {
	case ateapipb.Actor_STATUS_RESUMING:
		opName = ateattr.OperationResume
	case ateapipb.Actor_STATUS_SUSPENDING:
		opName = ateattr.OperationSuspend
	case ateapipb.Actor_STATUS_PAUSING:
		opName = ateattr.OperationPause
	}

	wasAlreadyCrashed := actor.GetStatus() == ateapipb.Actor_STATUS_CRASHED

	// Snapshot crash attributes before pod and pool pointers are cleared on actor.
	crashAttrs := ateattr.ActorMetricAttributes(actor, worker.GetSandboxClass(), opName, ateattr.ReasonWorkerPodGone)

	actor.Status = ateapipb.Actor_STATUS_CRASHED
	actor.WorkerAssignment = nil
	actor.InProgressSnapshot = ""

	_, err = s.persistence.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())

	if err == nil && !wasAlreadyCrashed {
		recordActorCrash(ctx, crashAttrs)
	}
	return err

}
