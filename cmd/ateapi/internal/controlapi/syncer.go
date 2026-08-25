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

// workerKey identifies the pod incarnation a queued event concerns. namespace
// and name locate the pod in the informer, which is indexed by namespace/name
// rather than by UID.
//
// uid belongs in the key because the workqueue dedupes by key equality. A pod
// and its same-named replacement are different Workers, and including the UID
// is what makes the queue treat them that way: without it, the delete of
// worker-1(uid-A) and the add of worker-1(uid-B) would collapse into a single
// item, and the reconcile — which reads current informer state — would see
// only uid-B and leave uid-A's row orphaned in the store.
type workerKey struct {
	namespace string
	name      string
	uid       string
}

// workerName is the resource name of the Worker this key identifies.
//
// The syncer mints Workers from Pods, so it is what gives them their names, and
// it names them after the pod UID. This method and createOrUpdateWorker are the
// only places that choice is expressed: everywhere else a Worker name is opaque
// and must be carried rather than rebuilt from pod identity.
func (k workerKey) workerName() string { return k.uid }

// logAttrs identifies the Worker in a log line, along with the pod it is
// derived from. The pod is the useful handle for an operator reaching for
// kubectl; the name is what identifies the row the syncer is acting on.
func (k workerKey) logAttrs() []any {
	return []any{
		slog.String("worker", k.workerName()),
		slog.String("pod", k.namespace+"/"+k.name),
	}
}

// WorkerPoolSyncer reconciles the state of worker pods from Kubernetes Informer
// into the store.
//
// Informer event handlers only enqueue keys; worker goroutines reconcile each
// key against the current informer cache state, requeuing with rate-limited
// backoff on transient failures such as store.ErrVersionConflict.
type WorkerPoolSyncer struct {
	persistence      workerPoolSyncerStore
	workerInformer   cache.SharedIndexInformer
	workerPoolLister listersv1alpha1.WorkerPoolLister
	queue            workqueue.TypedRateLimitingInterface[workerKey]
}

// workerPoolSyncerStore enumerates the exact storage methods needed by
// WorkerPoolSyncer and nothing more.
type workerPoolSyncerStore interface {
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.Actor) error) (*ateapipb.Actor, error)
	GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error)
	CreateWorker(ctx context.Context, worker *ateapipb.Worker) (*ateapipb.Worker, error)
	UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapipb.Worker) error) (*ateapipb.Worker, error)
	DeleteWorker(ctx context.Context, name string, pre store.DeletePreconditions) (*ateapipb.Worker, error)
	ListWorkers(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Worker], error)
}

// NewWorkerPoolSyncer creates a new WorkerPoolSyncer.
func NewWorkerPoolSyncer(persistence workerPoolSyncerStore, workerInformer cache.SharedIndexInformer, workerPoolLister listersv1alpha1.WorkerPoolLister) *WorkerPoolSyncer {
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
			// A pod's UID never changes, but a coalesced Delete+Create of the
			// same pod name surfaces here as an update. The two incarnations
			// have distinct keys, so enqueue the old one too to clean up its
			// now-orphaned store row.
			if oldPod.UID != newPod.UID {
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
	s.queue.Add(workerKey{namespace: pod.Namespace, name: pod.Name, uid: string(pod.UID)})
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
			append(key.logAttrs(), slog.Any("err", err))...)
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
		slog.InfoContext(ctx, "Syncer: removing worker from store (pod deleted)", key.logAttrs()...)
		return s.reconcileDeadWorker(ctx, key.workerName())
	}
	pod := obj.(*corev1.Pod)
	if string(pod.UID) != key.uid {
		// The pod was deleted and a new one took its name. This key names the
		// dead incarnation; the live pod was enqueued under its own key.
		slog.InfoContext(ctx, "Syncer: removing worker from store (pod replaced)", key.logAttrs()...)
		return s.reconcileDeadWorker(ctx, key.workerName())
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
		return s.markWorkerDraining(ctx, key)
	}
	if !isWorkerEligible(pod) {
		// No IP yet; a later update event re-enqueues the pod.
		return nil
	}
	return s.createOrUpdateWorker(ctx, key, pod)
}

func (s *WorkerPoolSyncer) createOrUpdateWorker(ctx context.Context, key workerKey, pod *corev1.Pod) error {
	poolName := pod.Labels[workerPodLabel]
	pool, err := s.workerPoolLister.WorkerPools(key.namespace).Get(poolName)
	if err != nil {
		return fmt.Errorf("getting WorkerPool %s/%s: %w", key.namespace, poolName, err)
	}

	w, err := s.persistence.GetWorker(ctx, key.workerName())
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("getting worker from store: %w", err)
		}
		slog.InfoContext(ctx, "Syncer: creating worker in store", key.logAttrs()...)
		worker := &ateapipb.Worker{
			// Workers are global-scoped, so the name carries no atespace. See
			// workerKey.workerName for where the name comes from.
			Metadata:        &ateapipb.ResourceMetadata{Name: key.workerName()},
			WorkerNamespace: pod.Namespace,
			WorkerPool:      poolName,
			WorkerPod:       pod.Name,
			Ip:              pod.Status.PodIP,
			WorkerPodUid:    string(pod.UID),
			NodeName:        pod.Spec.NodeName,
			SandboxClass:    string(pool.Spec.SandboxClass),
			Labels:          pool.GetLabels(),
			Capacity:        workerCapacity(pod),
			Status: &ateapipb.WorkerStatus{
				State: ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			},
		}
		// TODO: validateWorker now lives next to CreateWorker, which applies it
		// too. Once this path calls the RPC instead of the store, the check
		// here goes away and the errors below arrive as INVALID_ARGUMENT.
		if errs := validateWorker(worker, nil); len(errs) > 0 {
			// Terminal: the inputs are deterministic, retrying cannot help. A
			// future pod event re-enqueues the key.
			slog.ErrorContext(ctx, "Invalid worker", append(key.logAttrs(), slog.Any("err", errs.ToAggregate()))...)
			return nil
		}
		// ErrAlreadyExists means we lost a create race; requeue and converge
		// via the update path.
		_, err := s.persistence.CreateWorker(ctx, worker)
		return err
	}

	changed := false
	if w.Ip != pod.Status.PodIP {
		// TODO: I don't think this is possible, but handling this case so we can log it just in case we can reproduce it.
		slog.InfoContext(ctx, "Syncer: updating worker in store (IP changed)", key.logAttrs()...)
		w.Ip = pod.Status.PodIP
		changed = true
	}
	if w.SandboxClass != string(pool.Spec.SandboxClass) {
		slog.InfoContext(ctx, "Syncer: updating worker in store (SandboxClass changed)", key.logAttrs()...)
		w.SandboxClass = string(pool.Spec.SandboxClass)
		changed = true
	}
	if !maps.Equal(w.Labels, pool.GetLabels()) {
		slog.InfoContext(ctx, "Syncer: updating worker in store (labels changed)", key.logAttrs()...)
		w.Labels = pool.GetLabels()
		changed = true
	}
	if !changed {
		return nil
	}

	// ErrVersionConflict requeues the key; the retry re-fetches the worker at
	// its new version.
	_, err = s.persistence.UpdateWorker(ctx, key.workerName(), store.PreconditionFrom(w), func(toUpdate *ateapipb.Worker) error {
		toUpdate.Ip = w.GetIp()
		toUpdate.SandboxClass = w.GetSandboxClass()
		toUpdate.Labels = w.GetLabels()
		return nil
	})
	return err
}

func isWorkerEligible(pod *corev1.Pod) bool {
	return pod.Status.PodIP != ""
}

// ateomContainerName is the name of the container in a worker pod that hosts the
// actor's sandbox; its resource limits bound what an actor placed here can use.
const ateomContainerName = "ateom"

// workerCapacity returns the worker pod's capacity for hosting an actor — CPU
// in millicores and memory in bytes — taken from the ateom container's resource
// limits. A dimension the pod does not limit reports 0, which the scheduler
// treats as "unknown" (unconstrained); a pod that limits neither reports nil
// rather than an all-zero message that says the same thing. The actor sandbox
// runs nested in the ateom container's cgroup, so that container's limits — not
// the pod total — are the relevant envelope.
func workerCapacity(pod *corev1.Pod) *ateapipb.WorkerCapacity {
	var capacity ateapipb.WorkerCapacity
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.Name != ateomContainerName {
			continue
		}
		if v := c.Resources.Limits.Cpu(); v != nil {
			capacity.CpuMilli = v.MilliValue()
		}
		if v := c.Resources.Limits.Memory(); v != nil {
			capacity.MemoryBytes = v.Value()
		}
		break
	}
	if capacity.CpuMilli == 0 && capacity.MemoryBytes == 0 {
		return nil
	}
	return &capacity
}

// markWorkerDraining transitions a worker to STATE_DRAINING so the scheduler
// stops routing new actors to it while its pod is Terminating. If the worker is
// already gone or already draining there is nothing more to do — the Pod
// Deleted event will clean up the record. A version conflict is returned so the
// caller requeues and retries against the updated record.
func (s *WorkerPoolSyncer) markWorkerDraining(ctx context.Context, key workerKey) error {
	worker, err := s.persistence.GetWorker(ctx, key.workerName())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if worker.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_DRAINING {
		return nil
	}
	slog.InfoContext(ctx, "Syncer: marking worker draining (pod deleting)", key.logAttrs()...)
	_, err = s.persistence.UpdateWorker(ctx, key.workerName(), store.PreconditionFrom(worker), func(toUpdate *ateapipb.Worker) error {
		toUpdate.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
		return nil
	})
	return err
}

// reconcileDeadWorker cleans up a worker whose pod is gone. It releases the
// bound actor first and only deletes the worker record if that succeeds:
// deleting the record is what erases the actor->pod pointer, so on a release
// failure we intentionally leave the record in place (and return the error) so a
// later reconcile can retry. Returns nil once the actor is released and the
// worker record deleted.
func (s *WorkerPoolSyncer) reconcileDeadWorker(ctx context.Context, name string) error {
	if err := s.releaseActorOnDeadWorker(ctx, name); err != nil {
		return err
	}
	// The delete now reports absence rather than succeeding silently, but a
	// worker already gone is exactly the state this is driving towards.
	// Idempotency lives here, at the caller, so re-driving a reconcile is safe.
	_, err := s.persistence.DeleteWorker(ctx, name, store.DeletePreconditions{})
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	return err
}

// storedWorkerListBackoff and storedWorkerListCap are the exponential backoff
// schedule for retrying a failed page of the startup stored-worker scan. They
// are vars so tests can shrink them.
var (
	storedWorkerListBackoff = 500 * time.Millisecond
	storedWorkerListCap     = 30 * time.Second
)

// enqueueStoredWorkers enqueues a key for every worker record in the store.
// Records whose pods are live and unchanged reconcile to a no-op; orphaned
// records (pod gone, or its name reused by a new pod UID) get cleaned up.
//
// Each page's ListWorkers call is retried with capped backoff until it succeeds
// or ctx is cancelled, so a transient store error does not abandon the scan and
// leave ghost workers behind until the next restart (the per-key workqueue
// retries reconciles, but nothing retries this initial enqueue scan). Pages are
// enqueued as they are read, so the whole worker set is never held in memory at
// once and a late failure does not re-scan the pages already enqueued.
func (s *WorkerPoolSyncer) enqueueStoredWorkers(ctx context.Context) {
	var pageToken string
	for {
		page, err := s.listWorkersPageWithRetry(ctx, pageToken)
		if err != nil {
			// Only ctx cancellation (ate-api-server shutdown) ends the retry
			// loop. Pages read so far are already enqueued (partial progress);
			// the rest are recovered by the next startup scan.
			slog.ErrorContext(ctx, "Syncer: stopped enqueue of stored workers before completing the scan; remaining workers will be retried at the next startup", slog.Any("err", err))
			return
		}
		for _, w := range page.Items {
			// The key is a pod identity, so it is rebuilt from the stored pod
			// fields rather than from the Worker's name.
			s.queue.Add(workerKey{
				namespace: w.GetWorkerNamespace(),
				name:      w.GetWorkerPod(),
				uid:       w.GetWorkerPodUid(),
			})
		}
		if !page.HasNextPage() {
			return
		}
		pageToken = page.NextPageToken
	}
}

// listWorkersPageWithRetry reads one page of workers, retrying the store call
// with capped exponential backoff until it succeeds or ctx is cancelled. The
// page token is a stateless cursor, so retrying the failed call with the same
// token resumes from the same position. A fresh backoff per page means only
// consecutive failures of the same call accumulate delay; a page that succeeds
// resets it.
func (s *WorkerPoolSyncer) listWorkersPageWithRetry(ctx context.Context, pageToken string) (store.ListResponse[*ateapipb.Worker], error) {
	backoff := wait.Backoff{
		Duration: storedWorkerListBackoff,
		Factor:   2.0,
		Jitter:   0.1,
		// Steps must be large enough for the ramp (Duration*Factor^n) to reach
		// Cap, or Cap never triggers and the plateau sits at the last ramp step.
		// With Duration=500ms, Factor=2, the ramp hits Cap=30s at step 6
		// (0.5,1,2,4,8,16,30,30...).
		Steps: 6,
		Cap:   storedWorkerListCap,
	}
	for {
		page, err := s.persistence.ListWorkers(ctx, store.ListOptions{PageSize: 1000, PageToken: pageToken})
		if err == nil {
			return page, nil
		}
		slog.WarnContext(ctx, "Syncer: failed to list a page of stored workers for orphan cleanup, retrying", slog.Any("err", err))
		select {
		case <-ctx.Done():
			return store.ListResponse[*ateapipb.Worker]{}, fmt.Errorf("listing stored workers aborted: %w", ctx.Err())
		case <-time.After(backoff.Step()):
		}
	}
}

// releaseActorOnDeadWorker resets the actor bound to a vanishing worker pod. An
// actor that already reached ACTOR_STATE_SUSPENDED (it saved its state cleanly during
// graceful termination) is left untouched and remains resumable. An actor that
// was still running when the pod disappeared is moved to ACTOR_STATE_CRASHED and its
// pod pointers are cleared.
//
// UpdateActor uses optimistic version checking. A concurrent SuspendActor
// or ResumeActor wins; we fail this attempt so it can be retried with the
// updated state.
func (s *WorkerPoolSyncer) releaseActorOnDeadWorker(ctx context.Context, name string) error {
	worker, err := s.persistence.GetWorker(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if worker.GetStatus().GetAssignment().GetActor() == nil {
		return nil
	}
	actorRef := resources.ActorRefFromObjectRef(worker.GetStatus().GetAssignment().GetActor())
	actor, err := s.persistence.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if actor.GetMetadata().GetUid() != worker.GetStatus().GetAssignment().GetActorUid() {
		return nil
	}
	// Skip if a concurrent SuspendActor already cleared the pointer, or if the
	// actor has since been placed on a different worker.
	if actor.GetStatus().GetWorkerAssignment().GetWorker().GetName() != name {
		return nil
	}
	// If the actor is suspended, it's already been released.
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		return nil
	}
	opName := ateattr.OperationUnknown
	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_RESUMING:
		opName = ateattr.OperationResume
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		opName = ateattr.OperationSuspend
	case ateapipb.ActorState_ACTOR_STATE_PAUSING:
		opName = ateattr.OperationPause
	}

	wasAlreadyCrashed := actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED

	// Snapshot crash attributes before pod and pool pointers are cleared on actor.
	crashAttrs := ateattr.ActorMetricAttributes(actor, worker.GetSandboxClass(), opName, ateattr.ReasonWorkerPodGone)

	_, err = s.persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		toUpdate.Status.WorkerAssignment = nil
		// Both in-progress checkpoints die with the worker: the durable one was
		// never uploaded, the local one lived on the node that went away.
		toUpdate.Status.InProgressSnapshotName = ""
		toUpdate.Status.InProgressLocalSnapshotName = ""
		return nil
	})

	if err == nil && !wasAlreadyCrashed {
		recordActorCrash(ctx, crashAttrs)
	}
	return err

}
