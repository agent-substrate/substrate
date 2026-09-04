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

	"github.com/agent-substrate/substrate/cmd/atecontroller/internal/workerpod"
	"github.com/agent-substrate/substrate/internal/ateattr"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
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
func (k workerKey) logAttrs() []slog.Attr {
	return ateattr.WorkerLogAttrs(k.workerName(), k.namespace+"/"+k.name)
}

// WorkerPoolSyncer reconciles the state of worker pods from Kubernetes Informer
// into the Worker registry, over the ateapi Control API.
//
// Informer event handlers only enqueue keys; worker goroutines reconcile each
// key against the current informer cache state, requeuing with rate-limited
// backoff on transient failures such as a lost version precondition.
type WorkerPoolSyncer struct {
	client           ateapipb.ControlClient
	pods             kubernetes.Interface
	workerInformer   cache.SharedIndexInformer
	workerPoolLister listersv1alpha1.WorkerPoolLister
	queue            workqueue.TypedRateLimitingInterface[workerKey]
	// tracer roots the spans the syncer opens, since an informer callback
	// carries none. A field rather than a reach for the global provider so a
	// test can record against a local one.
	tracer trace.Tracer
}

// NewWorkerPoolSyncer creates a new WorkerPoolSyncer.
func NewWorkerPoolSyncer(client ateapipb.ControlClient, pods kubernetes.Interface, workerInformer cache.SharedIndexInformer, workerPoolLister listersv1alpha1.WorkerPoolLister) *WorkerPoolSyncer {
	return &WorkerPoolSyncer{
		client:           client,
		pods:             pods,
		workerInformer:   workerInformer,
		workerPoolLister: workerPoolLister,
		queue:            workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[workerKey]()),
		tracer:           otel.Tracer("workersync"),
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
			slog.LogAttrs(ctx, slog.LevelError, "Syncer: reconcile rejected as invalid, dropping",
				append(key.logAttrs(), slog.Any("err", err))...)
			s.queue.Forget(key)
			return true
		}
		slog.LogAttrs(ctx, slog.LevelError, "Syncer: reconcile failed, requeueing",
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
		slog.LogAttrs(ctx, slog.LevelInfo, "Syncer: deregistering worker (pod deleted)", key.logAttrs()...)
		return s.reconcileDeadWorker(ctx, key)
	}
	pod := obj.(*corev1.Pod)
	if string(pod.UID) != key.uid {
		// The pod was deleted and a new one took its name. This key names the
		// dead incarnation; the live pod was enqueued under its own key.
		slog.LogAttrs(ctx, slog.LevelInfo, "Syncer: deregistering worker (pod replaced)", key.logAttrs()...)
		return s.reconcileDeadWorker(ctx, key)
	}
	// Read once and carried down: the sandbox check and createOrUpdateWorker both
	// need the record, and reading it twice would double the RPCs the syncer makes
	// on every pod event and every resync.
	worker, err := s.getWorker(ctx, key)
	if err != nil {
		return err
	}
	// Before draining and before eligibility: a restarted ateom leaves the pod,
	// its UID and its IP untouched, so nothing else here notices, and its actors
	// are stranded for as long as the check waits.
	if worker, err = s.recycleIfSandboxReplaced(ctx, key, pod, worker); err != nil {
		return err
	}
	// After the recycle, and on its own condition rather than as part of it: the
	// container id only changes once a replacement is Running, while a crash loop
	// is the kubelet refusing to start one, so the two are almost never true at
	// the same moment.
	quarantined, err := s.quarantineIfCrashLooping(ctx, key, pod, worker)
	if err != nil {
		return err
	}
	if quarantined {
		// The Worker is drained and its pod is on the way out; converging the
		// rest of the record against a pod that is being replaced is churn, and
		// the copy read above is stale after the drain anyway.
		return nil
	}
	// Checked before eligibility: draining works off the registered record by name
	// and never reads the pod IP, while a Terminating pod can legitimately report
	// no IP once its sandbox is torn down. Gating on the IP first would drop the
	// transition and leave the worker schedulable for as long as the pod lingers.
	if pod.DeletionTimestamp != nil {
		if worker == nil {
			// Never registered, so there is nothing to stop the scheduler placing on.
			return nil
		}
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
	return s.createOrUpdateWorker(ctx, key, pod, worker)
}

// createOrUpdateWorker converges the registry record with the pod. w is the
// record as reconcile read it, or nil when the pod has none yet.
func (s *WorkerPoolSyncer) createOrUpdateWorker(ctx context.Context, key workerKey, pod *corev1.Pod, w *ateapipb.Worker) error {
	poolName := pod.Labels[workerPodLabel]
	pool, err := s.workerPoolLister.WorkerPools(key.namespace).Get(poolName)
	if err != nil {
		return fmt.Errorf("getting WorkerPool %s/%s: %w", key.namespace, poolName, err)
	}

	if w == nil {
		slog.LogAttrs(ctx, slog.LevelInfo, "Syncer: registering worker", key.logAttrs()...)
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
			// The ateom container and the capacity are both status, reported
			// after this rather than claimed here: capacity is what the ateom
			// can actually supply, and until it reports nothing is placed on
			// this Worker at all.
		}
		// status is output-only: CreateWorker sets STATE_ACTIVE itself.
		//
		// ALREADY_EXISTS means we lost a create race; requeue and converge via
		// the update path. INVALID_ARGUMENT is terminal — see
		// processNextWorkItem.
		if _, err := s.client.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: worker}); err != nil {
			return err
		}
		// Reconcile again at once to record the ateom container. Until that
		// lands the Worker has none, and a restart in the gap reads as a Worker
		// that never had one — adopted rather than recycled, leaving anything
		// already placed on it bound to a sandbox that is gone.
		s.queue.Add(key)
		return nil
	}

	// UpdateWorker replaces the whole resource, so the two mutable fields are
	// edited onto the Worker as it was read and the rest is sent back unchanged
	// — anything else altered here, including a field cleared by omission, is
	// rejected as INVALID_ARGUMENT. Everything else on a Worker is immutable
	// after create, so drift there cannot be repaired by an update; it takes a
	// new pod, which arrives under a new key.
	var changed bool
	if w.GetSandboxClass() != string(pool.Spec.SandboxClass) {
		slog.LogAttrs(ctx, slog.LevelInfo, "Syncer: updating worker (SandboxClass changed)", key.logAttrs()...)
		w.SandboxClass = string(pool.Spec.SandboxClass)
		changed = true
	}
	if !maps.Equal(w.GetLabels(), pool.GetLabels()) {
		slog.LogAttrs(ctx, slog.LevelInfo, "Syncer: updating worker (labels changed)", key.logAttrs()...)
		w.Labels = pool.GetLabels()
		changed = true
	}
	if w.GetIp() != pod.Status.PodIP {
		// TODO: I don't think this is possible, but handling this case so we can
		// log it just in case we can reproduce it. It is logged rather than
		// repaired because ip is immutable on a registered Worker: writing the
		// pod's value back would be rejected rather than applied.
		slog.LogAttrs(ctx, slog.LevelWarn, "Syncer: registered worker IP disagrees with its pod",
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

// getWorker reads the registry record for key, reporting an unregistered pod as
// a nil Worker rather than an error: that is the state the create path drives
// from, not a failure.
func (s *WorkerPoolSyncer) getWorker(ctx context.Context, key workerKey) (*ateapipb.Worker, error) {
	w, err := s.client.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: key.workerRef()})
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting worker: %w", err)
	}
	return w, nil
}

// recycleIfSandboxReplaced recycles the Worker when the kubelet has swapped the
// ateom container under it, and returns the record as it now stands. The
// container id is the only evidence, and it lives on the record rather than in
// memory here: an ate-controller restart would lose it and strand every actor
// caught by the restart it missed.
//
// Nothing to compare yet is left for a later event. RecycleWorker decides what a
// match, a mismatch and an absent id each mean.
func (s *WorkerPoolSyncer) recycleIfSandboxReplaced(ctx context.Context, key workerKey, pod *corev1.Pod, w *ateapipb.Worker) (*ateapipb.Worker, error) {
	ateom := workerpod.AteomStatus(pod)
	if w == nil || ateom == nil || ateom.ContainerID == "" || w.GetStatus().GetAteomContainerId() == ateom.ContainerID {
		return w, nil
	}

	terminatedReason := workerpod.SandboxTerminatedReason(ateom)
	// An absent id is adopted rather than recycled: no sandbox was lost, so
	// there is nothing to report, count or correlate.
	if w.GetStatus().GetAteomContainerId() == "" {
		return s.recycleWorker(ctx, key, ateom.ContainerID, terminatedReason)
	}

	// One trace over the whole fault. Informer callbacks carry no span, so
	// without this the record below is detached from the RecycleWorker call and
	// from every Actor crashed record ateapi writes because of it. The trace id
	// reaches all of them whether or not the trace itself is sampled.
	ctx, span := s.tracer.Start(ctx, "ReconcileReplacedSandbox",
		trace.WithAttributes(s.sandboxLossAttributes(w, ateom, terminatedReason)...))
	defer span.End()

	attrs := key.logAttrs()
	attrs = append(attrs, ateattr.WorkerPoolLogAttrs(w.GetWorkerNamespace(), w.GetWorkerPool())...)
	attrs = append(attrs, ateattr.FailureLogAttrs(ateattr.ReasonWorkerSandboxGone)...)
	attrs = append(attrs,
		slog.String(string(ateattr.ContainerLastTerminatedReasonKey), terminatedReason),
		slog.Int(string(ateattr.ContainerRestartCountKey), int(ateom.RestartCount)))

	slog.LogAttrs(ctx, slog.LevelError, "Syncer: worker lost its sandbox, recycling", attrs...)

	recycled, err := s.recycleWorker(ctx, key, ateom.ContainerID, terminatedReason)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return nil, err
	}
	return recycled, nil
}

// quarantineIfCrashLooping takes a Worker out of the pool when the kubelet has
// given up restarting its ateom, and hands the pod back to its Deployment.
//
// Substrate keeps no restart threshold of its own: the kubelet already backs off
// exponentially and resets once a container stays up, and a second opinion here
// would only disagree with it. Draining is what stops the losses — every Actor
// placed on a sandbox that keeps dying is destroyed permanently, since
// ACTOR_STATE_CRASHED is terminal.
func (s *WorkerPoolSyncer) quarantineIfCrashLooping(ctx context.Context, key workerKey, pod *corev1.Pod, w *ateapipb.Worker) (bool, error) {
	if w == nil || pod.DeletionTimestamp != nil || !workerpod.InCrashLoop(workerpod.AteomStatus(pod)) {
		return false, nil
	}

	ctx, span := s.tracer.Start(ctx, "QuarantineCrashLoopingWorker",
		trace.WithAttributes(ateattr.WorkerAttributes(w.GetMetadata().GetName(), w.GetWorkerPod())...))
	defer span.End()

	attrs := key.logAttrs()
	attrs = append(attrs, ateattr.WorkerPoolLogAttrs(w.GetWorkerNamespace(), w.GetWorkerPool())...)
	attrs = append(attrs, slog.String(string(ateattr.ContainerStatusReasonKey), workerpod.CrashLoopBackOff))
	if ateom := workerpod.AteomStatus(pod); ateom != nil {
		attrs = append(attrs, slog.Int(string(ateattr.ContainerRestartCountKey), int(ateom.RestartCount)))
	}

	// Drain before deleting: the pod lingers through its grace period, and an
	// ACTIVE Worker takes placements for as long as it does.
	if err := s.markWorkerDraining(ctx, key); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return false, err
	}
	if err := s.replaceWorkerPod(ctx, key, attrs); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return false, err
	}
	return true, nil
}

// replaceWorkerPod hands a worker whose ateom will not stay up back to
// Kubernetes. Quarantining alone would stop the actor losses and leave the pool
// a worker short with nothing to repair it: the pod stays, and its Deployment
// has no reason to act. Deleting it does, and if the ateom is genuinely broken
// the replacement crash-loops as an ordinary pod, which every Kubernetes tool
// already surfaces and backs off.
//
// A pod already gone is the state this drives towards, so NotFound is success.
func (s *WorkerPoolSyncer) replaceWorkerPod(ctx context.Context, key workerKey, attrs []slog.Attr) error {
	slog.LogAttrs(ctx, slog.LevelError, "Syncer: ateom will not stay up, replacing the worker pod", attrs...)
	err := s.pods.CoreV1().Pods(key.namespace).Delete(ctx, key.name, metav1.DeleteOptions{
		// Guard against deleting a same-named pod that replaced this one
		// between the informer read and this call.
		Preconditions: &metav1.Preconditions{UID: (*types.UID)(&key.uid)},
	})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return err
}

// recycleWorker hands the observed container to the control plane, which
// decides whether that is an adoption or a lost sandbox. A Worker deleted in the
// meantime is the state this drives towards, so NOT_FOUND is success.
func (s *WorkerPoolSyncer) recycleWorker(ctx context.Context, key workerKey, ateomContainerID, terminatedReason string) (*ateapipb.Worker, error) {
	recycled, err := s.client.RecycleWorker(ctx, &ateapipb.RecycleWorkerRequest{
		Worker:                  key.workerRef(),
		AteomContainerId:        ateomContainerID,
		SandboxTerminatedReason: terminatedReason,
	})
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return recycled, nil
}

// sandboxLossAttributes describes the fault on the span. The pool and class come
// off the Worker record rather than the pod, which is where the control plane
// reads them from too.
func (s *WorkerPoolSyncer) sandboxLossAttributes(w *ateapipb.Worker, ateom *corev1.ContainerStatus, terminatedReason string) []attribute.KeyValue {
	attrs := ateattr.WorkerAttributes(w.GetMetadata().GetName(), w.GetWorkerPod())
	attrs = append(attrs, ateattr.FailureAttributes(ateattr.ReasonWorkerSandboxGone)...)
	attrs = append(attrs,
		ateattr.SandboxClassKey.String(ateattr.NormalizeSandboxClass(w.GetSandboxClass())),
		ateattr.ContainerLastTerminatedReasonKey.String(terminatedReason),
		ateattr.ContainerRestartCountKey.Int(int(ateom.RestartCount)))
	return append(attrs, ateattr.WorkerPoolAttributes(w.GetWorkerNamespace(), w.GetWorkerPool())...)
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

// markWorkerDraining transitions a worker to STATE_DRAINING so the scheduler
// stops routing new actors to it while its pod is Terminating. DrainWorker is
// idempotent, so a worker already draining costs nothing. If the worker is
// already gone there is nothing more to do — the Pod Deleted event will clean up
// the record. A version conflict comes back as ABORTED so the caller requeues
// and retries against the updated record.
func (s *WorkerPoolSyncer) markWorkerDraining(ctx context.Context, key workerKey) error {
	slog.LogAttrs(ctx, slog.LevelInfo, "Syncer: marking worker draining (pod deleting)", key.logAttrs()...)
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
