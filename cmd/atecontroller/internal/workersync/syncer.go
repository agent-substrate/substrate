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

// Package workersync reconciles Kubernetes worker pods into the Worker registry
// behind the ateapi Control API.
package workersync

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"time"

	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// syncerWorkerCount is the number of goroutines draining the work queue. The
// queue never hands the same key to two workers concurrently, so per-key
// ordering is preserved.
const syncerWorkerCount = 2

// workerPodLabel names the WorkerPool a worker pod belongs to. Its presence is
// also what marks a pod as a worker pod at all, so it doubles as the selector
// the pod informer is narrowed by.
const workerPodLabel = "ate.dev/worker-pool"

// workerKey identifies the pod incarnation a queued event concerns. namespace
// and name locate the pod in the informer, which is indexed by namespace/name
// rather than by UID.
//
// uid belongs in the key because the workqueue dedupes by key equality. A pod
// and its same-named replacement are different Workers, and including the UID
// is what makes the queue treat them that way: without it, the delete of
// worker-1(uid-A) and the add of worker-1(uid-B) would collapse into a single
// item, and the reconcile — which reads current informer state — would see
// only uid-B and leave uid-A's record orphaned in the registry.
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

// workerRef is the Worker this key identifies, in the form the API's
// single-resource requests take. Workers are global-scoped, so no atespace.
func (k workerKey) workerRef() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Name: k.workerName()}
}

// logAttrs identifies the Worker in a log line, along with the pod it is
// derived from. The pod is the useful handle for an operator reaching for
// kubectl; the name is what identifies the record the syncer is acting on.
func (k workerKey) logAttrs() []any {
	return []any{
		slog.String("worker", k.workerName()),
		slog.String("pod", k.namespace+"/"+k.name),
	}
}

// WorkerPoolSyncer reconciles the state of worker pods from Kubernetes Informer
// into the Worker registry, over the ateapi Control API.
//
// Informer event handlers only enqueue keys; worker goroutines reconcile each
// key against the current informer cache state, requeuing with rate-limited
// backoff on transient failures such as a lost version precondition.
type WorkerPoolSyncer struct {
	client           ateapipb.ControlClient
	workerInformer   cache.SharedIndexInformer
	workerPoolLister listersv1alpha1.WorkerPoolLister
	queue            workqueue.TypedRateLimitingInterface[workerKey]
}

// NewWorkerPoolSyncer creates a new WorkerPoolSyncer.
func NewWorkerPoolSyncer(client ateapipb.ControlClient, workerInformer cache.SharedIndexInformer, workerPoolLister listersv1alpha1.WorkerPoolLister) *WorkerPoolSyncer {
	return &WorkerPoolSyncer{
		client:           client,
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
			// now-orphaned registry record.
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

		// Reconcile the other direction: enqueue every registered worker so
		// records whose pods no longer exist are cleaned up. This recovers
		// delete events missed while ate-controller was down — neither the watch
		// relist nor the resync period can replay a delete across a process
		// restart, because the informer cache starts empty. Runs after the cache
		// sync so the indexer is an authoritative snapshot of live pods.
		s.enqueueRegisteredWorkers(ctx)

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
		// The syncer builds its requests from pod and pool state, so a request
		// the API rejects as invalid would be resent verbatim on every retry.
		// INVALID_ARGUMENT is therefore terminal: the key is dropped and a
		// future pod event enqueues it again. Every other code — including the
		// UNAVAILABLE a transport failure surfaces as — requeues.
		if status.Code(err) == codes.InvalidArgument {
			slog.ErrorContext(ctx, "Syncer: reconcile rejected as invalid, dropping",
				append(key.logAttrs(), slog.Any("err", err))...)
			s.queue.Forget(key)
			return true
		}
		slog.ErrorContext(ctx, "Syncer: reconcile failed, requeueing",
			append(key.logAttrs(), slog.Any("err", err))...)
		s.queue.AddRateLimited(key)
		return true
	}
	s.queue.Forget(key)
	return true
}

// reconcile converges the registry record for key with the current pod state in
// the informer cache. Returning an error requeues the key with backoff.
func (s *WorkerPoolSyncer) reconcile(ctx context.Context, key workerKey) error {
	obj, exists, err := s.workerInformer.GetIndexer().GetByKey(key.namespace + "/" + key.name)
	if err != nil {
		return err
	}
	if !exists {
		slog.InfoContext(ctx, "Syncer: deregistering worker (pod deleted)", key.logAttrs()...)
		return s.reconcileDeadWorker(ctx, key)
	}
	pod := obj.(*corev1.Pod)
	if string(pod.UID) != key.uid {
		// The pod was deleted and a new one took its name. This key names the
		// dead incarnation; the live pod was enqueued under its own key.
		slog.InfoContext(ctx, "Syncer: deregistering worker (pod replaced)", key.logAttrs()...)
		return s.reconcileDeadWorker(ctx, key)
	}
	// Checked before eligibility: draining works off the registered record by name
	// and never reads the pod IP, while a Terminating pod can legitimately report
	// no IP once its sandbox is torn down. Gating on the IP first would drop the
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
		// The pod has no IP or is not Ready yet; a later update event re-enqueues it.
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

	w, err := s.client.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: key.workerRef()})
	if status.Code(err) == codes.NotFound {
		slog.InfoContext(ctx, "Syncer: registering worker", key.logAttrs()...)
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
		}
		// status is output-only: CreateWorker sets STATE_ACTIVE itself.
		//
		// ALREADY_EXISTS means we lost a create race; requeue and converge via
		// the update path. INVALID_ARGUMENT is terminal — see
		// processNextWorkItem.
		_, err := s.client.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: worker})
		return err
	}
	if err != nil {
		return fmt.Errorf("getting worker: %w", err)
	}

	// UpdateWorker replaces the whole resource, so the two mutable fields are
	// edited onto the Worker as it was read and the rest is sent back unchanged
	// — anything else altered here, including a field cleared by omission, is
	// rejected as INVALID_ARGUMENT. Everything else on a Worker is immutable
	// after create, so drift there cannot be repaired by an update; it takes a
	// new pod, which arrives under a new key.
	var changed bool
	if w.GetSandboxClass() != string(pool.Spec.SandboxClass) {
		slog.InfoContext(ctx, "Syncer: updating worker (SandboxClass changed)", key.logAttrs()...)
		w.SandboxClass = string(pool.Spec.SandboxClass)
		changed = true
	}
	if !maps.Equal(w.GetLabels(), pool.GetLabels()) {
		slog.InfoContext(ctx, "Syncer: updating worker (labels changed)", key.logAttrs()...)
		w.Labels = pool.GetLabels()
		changed = true
	}
	if w.GetIp() != pod.Status.PodIP {
		// TODO: I don't think this is possible, but handling this case so we can
		// log it just in case we can reproduce it. It is logged rather than
		// repaired because ip is immutable on a registered Worker: writing the
		// pod's value back would be rejected rather than applied.
		slog.WarnContext(ctx, "Syncer: registered worker IP disagrees with its pod",
			append(key.logAttrs(), slog.String("registered", w.GetIp()), slog.String("pod_ip", pod.Status.PodIP))...)
	}
	if !changed {
		return nil
	}

	// w carries the uid and version it was read at, which the API requires as the
	// update's precondition. ABORTED requeues the key; the retry re-fetches the
	// worker at its new version.
	_, err = s.client.UpdateWorker(ctx, &ateapipb.UpdateWorkerRequest{Worker: w})
	return err
}

func isWorkerEligible(pod *corev1.Pod) bool {
	if pod.Status.PodIP == "" {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
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
// stops routing new actors to it while its pod is Terminating. DrainWorker is
// idempotent, so a worker already draining costs nothing. If the worker is
// already gone there is nothing more to do — the Pod Deleted event will clean up
// the record. A version conflict comes back as ABORTED so the caller requeues
// and retries against the updated record.
func (s *WorkerPoolSyncer) markWorkerDraining(ctx context.Context, key workerKey) error {
	slog.InfoContext(ctx, "Syncer: marking worker draining (pod deleting)", key.logAttrs()...)
	_, err := s.client.DrainWorker(ctx, &ateapipb.DrainWorkerRequest{Worker: key.workerRef()})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

// reconcileDeadWorker cleans up a worker whose pod is gone. DeleteWorker
// releases the bound actor as part of the delete and fails the delete if that
// release fails, so a failure here leaves the record in place (and returns the
// error) for a later reconcile to retry.
//
// A worker already gone is exactly the state this is driving towards, so
// NOT_FOUND is success. Idempotency lives here, at the caller, so re-driving a
// reconcile is safe.
func (s *WorkerPoolSyncer) reconcileDeadWorker(ctx context.Context, key workerKey) error {
	_, err := s.client.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{Worker: key.workerRef()})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

// storedWorkerListBackoff and storedWorkerListCap are the exponential backoff
// schedule for retrying a failed page of the startup registered-worker scan.
// They are vars so tests can shrink them.
var (
	storedWorkerListBackoff = 500 * time.Millisecond
	storedWorkerListCap     = 30 * time.Second
)

// enqueueRegisteredWorkers enqueues a key for every worker record in the
// registry. Records whose pods are live and unchanged reconcile to a no-op;
// orphaned records (pod gone, or its name reused by a new pod UID) get cleaned
// up.
//
// Each page's ListWorkers call is retried with capped backoff until it succeeds
// or ctx is cancelled, so a transient failure does not abandon the scan and
// leave ghost workers behind until the next restart (the per-key workqueue
// retries reconciles, but nothing retries this initial enqueue scan). Pages are
// enqueued as they are read, so the whole worker set is never held in memory at
// once and a late failure does not re-scan the pages already enqueued.
func (s *WorkerPoolSyncer) enqueueRegisteredWorkers(ctx context.Context) {
	var pageToken string
	for {
		page, err := s.listWorkersPageWithRetry(ctx, pageToken)
		if err != nil {
			// Only ctx cancellation (ate-controller shutdown) ends the retry
			// loop. Pages read so far are already enqueued (partial progress);
			// the rest are recovered by the next startup scan.
			slog.ErrorContext(ctx, "Syncer: stopped enqueue of registered workers before completing the scan; remaining workers will be retried at the next startup", slog.Any("err", err))
			return
		}
		for _, w := range page.GetWorkers() {
			// The key is a pod identity, so it is rebuilt from the recorded pod
			// fields rather than from the Worker's name.
			s.queue.Add(workerKey{
				namespace: w.GetWorkerNamespace(),
				name:      w.GetWorkerPod(),
				uid:       w.GetWorkerPodUid(),
			})
		}
		if page.GetNextPageToken() == "" {
			return
		}
		pageToken = page.GetNextPageToken()
	}
}

// listWorkersPageWithRetry reads one page of workers, retrying the call with
// capped exponential backoff until it succeeds or ctx is cancelled. The page
// token is a stateless cursor, so retrying the failed call with the same token
// resumes from the same position. A fresh backoff per page means only
// consecutive failures of the same call accumulate delay; a page that succeeds
// resets it.
func (s *WorkerPoolSyncer) listWorkersPageWithRetry(ctx context.Context, pageToken string) (*ateapipb.ListWorkersResponse, error) {
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
		page, err := s.client.ListWorkers(ctx, &ateapipb.ListWorkersRequest{PageSize: 1000, PageToken: pageToken})
		if err == nil {
			return page, nil
		}
		slog.WarnContext(ctx, "Syncer: failed to list a page of registered workers for orphan cleanup, retrying", slog.Any("err", err))
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("listing registered workers aborted: %w", ctx.Err())
		case <-time.After(backoff.Step()):
		}
	}
}
