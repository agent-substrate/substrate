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
	"log/slog"
	"maps"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// WorkerPoolSyncer reconciles the state of worker pods from Kubernetes Informer
// into the store.
type WorkerPoolSyncer struct {
	persistence      store.Interface
	workerInformer   cache.SharedIndexInformer
	workerPoolLister listersv1alpha1.WorkerPoolLister
}

// NewWorkerPoolSyncer creates a new WorkerPoolSyncer.
func NewWorkerPoolSyncer(persistence store.Interface, workerInformer cache.SharedIndexInformer, workerPoolLister listersv1alpha1.WorkerPoolLister) *WorkerPoolSyncer {
	return &WorkerPoolSyncer{
		persistence:      persistence,
		workerInformer:   workerInformer,
		workerPoolLister: workerPoolLister,
	}
}

// Start starts the background reconciliation loop.
func (s *WorkerPoolSyncer) Start(ctx context.Context) {
	s.workerInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*corev1.Pod)
			s.syncWorkerToStore(ctx, pod)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			pod := newObj.(*corev1.Pod)
			s.syncWorkerToStore(ctx, pod)
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
			slog.InfoContext(ctx, "Syncer: removing worker from store (pod deleted)", slog.String("worker", pod.Namespace+"/"+pod.Name))
			// TODO: make this more robust. Informer event handlers cannot signal failure and
			// the informer never retries them, so a cleanup that fails here is not retried
			// until the next startup reconcile. The canonical fix is a rate-limited workqueue:
			// enqueue the worker key and run this from a worker goroutine that requeues with
			// backoff on error.
			if err := s.reconcileDeadWorker(ctx, pod.Namespace, pod.Labels[workerPodLabel], pod.Name); err != nil {
				slog.ErrorContext(ctx, "Failed to reconcile deleted worker", slog.String("worker", pod.Namespace+"/"+pod.Name), slog.Any("err", err))
			}
		},
	})

	go func() {
		if !cache.WaitForCacheSync(ctx.Done(), s.workerInformer.HasSynced) {
			slog.ErrorContext(ctx, "Syncer: failed to sync informer cache")
			return
		}

		slog.InfoContext(ctx, "Syncer: performing initial sync on startup")
		objs := s.workerInformer.GetIndexer().List()
		for _, obj := range objs {
			pod := obj.(*corev1.Pod)
			s.syncWorkerToStore(ctx, pod)
		}

		// Reconcile the other direction: clean up stored workers whose pods no
		// longer exist. This recovers delete events missed while ate-api-server
		// was down — neither the watch relist nor the resync period can replay a
		// delete across a process restart, because the informer cache starts empty.
		s.reconcileOrphanedWorkers(ctx)
	}()
}

func (s *WorkerPoolSyncer) syncWorkerToStore(ctx context.Context, pod *corev1.Pod) {
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
		if err := s.markWorkerDraining(ctx, pod.Namespace, pod.Labels[workerPodLabel], pod.Name); err != nil {
			slog.ErrorContext(ctx, "Failed to mark worker draining", slog.String("worker", pod.Namespace+"/"+pod.Name), slog.Any("err", err))
		}
		return
	}

	if !isWorkerEligible(pod) {
		return
	}

	poolName := pod.Labels[workerPodLabel]
	pool, err := s.workerPoolLister.WorkerPools(pod.Namespace).Get(poolName)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get WorkerPool for worker pod", slog.String("worker", pod.Namespace+"/"+pod.Name), slog.String("pool", poolName), slog.Any("err", err))
		return
	}

	w, err := s.persistence.GetWorker(ctx, pod.Namespace, poolName, pod.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			slog.InfoContext(ctx, "Syncer: creating worker in store", slog.String("worker", pod.Namespace+"/"+pod.Name))
			worker := &ateapipb.Worker{
				WorkerNamespace: pod.Namespace,
				WorkerPool:      poolName,
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
				err := status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
				slog.ErrorContext(ctx, "Invalid worker", slog.Any("err", err))
				return
			}
			err = s.persistence.CreateWorker(ctx, worker)
			if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
				slog.ErrorContext(ctx, "Failed to create worker in store", slog.Any("err", err))
			}
			return
		}
		slog.ErrorContext(ctx, "Failed to get worker from store", slog.Any("err", err))
		return
	}

	changed := false
	if w.Ip != pod.Status.PodIP {
		// TODO: I don't think this is possible, but handling this case so we can log it just in case we can reproduce it.
		slog.InfoContext(ctx, "Syncer: updating worker in store (IP changed)", slog.String("worker", pod.Namespace+"/"+pod.Name))
		w.Ip = pod.Status.PodIP
		changed = true
	}
	if w.SandboxClass != string(pool.Spec.SandboxClass) {
		slog.InfoContext(ctx, "Syncer: updating worker in store (SandboxClass changed)", slog.String("worker", pod.Namespace+"/"+pod.Name))
		w.SandboxClass = string(pool.Spec.SandboxClass)
		changed = true
	}
	if !maps.Equal(w.Labels, pool.GetLabels()) {
		slog.InfoContext(ctx, "Syncer: updating worker in store (labels changed)", slog.String("worker", pod.Namespace+"/"+pod.Name))
		w.Labels = pool.GetLabels()
		changed = true
	}

	if changed {
		if err = s.persistence.UpdateWorker(ctx, w, w.Version); err != nil {
			slog.ErrorContext(ctx, "Failed to update worker in store", slog.Any("err", err))
		}
	}
}

func isWorkerEligible(pod *corev1.Pod) bool {
	return pod.Status.PodIP != ""
}

// markWorkerDraining transitions a worker to STATE_DRAINING so the scheduler
// stops routing new actors to it while its pod is Terminating. Best-effort: if
// the worker is already gone or already draining, or a concurrent update wins,
// there is nothing more to do — the Pod Deleted event will clean up the record.
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

// reconcileOrphanedWorkers cleans up stored worker records whose pods no longer
// exist. It runs once after the informer cache has synced, when the indexer is a
// fresh, authoritative snapshot of the live worker pods, so a worker missing
// from the indexer (or present under a different pod UID, i.e. name reuse) is
// stale.
func (s *WorkerPoolSyncer) reconcileOrphanedWorkers(ctx context.Context) {
	var workers []*ateapipb.Worker
	var pageToken string
	for {
		wPage, nextToken, err := s.persistence.ListWorkers(ctx, 1000, pageToken)
		if err != nil {
			slog.ErrorContext(ctx, "Syncer: failed to list workers for orphan reconcile", slog.Any("err", err))
			return
		}
		workers = append(workers, wPage...)
		if nextToken == "" {
			break
		}
		pageToken = nextToken
	}
	indexer := s.workerInformer.GetIndexer()
	for _, w := range workers {
		key := w.GetWorkerNamespace() + "/" + w.GetWorkerPod()
		obj, exists, err := indexer.GetByKey(key)
		if err != nil {
			slog.ErrorContext(ctx, "Syncer: indexer lookup failed during orphan reconcile", slog.String("worker", key), slog.Any("err", err))
			continue
		}
		// The pod is still live only if it is present under the same UID the
		// worker recorded; a different UID means the name was reused by a new pod
		// and this record belongs to a dead incarnation.
		if exists {
			if pod, ok := obj.(*corev1.Pod); ok && string(pod.UID) == w.GetWorkerPodUid() {
				continue
			}
		}
		slog.InfoContext(ctx, "Syncer: reconciling orphaned worker (pod gone)", slog.String("worker", key))
		if err := s.reconcileDeadWorker(ctx, w.GetWorkerNamespace(), w.GetWorkerPool(), w.GetWorkerPod()); err != nil {
			slog.ErrorContext(ctx, "Syncer: failed to reconcile orphaned worker", slog.String("worker", key), slog.Any("err", err))
		}
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

	actor.Status = ateapipb.Actor_STATUS_CRASHED
	actor.WorkerAssignment = nil
	actor.InProgressSnapshot = ""

	_, err = s.persistence.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
	return err
}
