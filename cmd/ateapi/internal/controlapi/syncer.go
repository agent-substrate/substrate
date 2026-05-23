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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// WorkerPoolSyncer reconciles the state of worker pods from Kubernetes Informer
// into the store.
type WorkerPoolSyncer struct {
	persistence    store.Interface
	workerInformer cache.SharedIndexInformer
}

// NewWorkerPoolSyncer creates a new WorkerPoolSyncer.
func NewWorkerPoolSyncer(persistence store.Interface, workerInformer cache.SharedIndexInformer) *WorkerPoolSyncer {
	return &WorkerPoolSyncer{
		persistence:    persistence,
		workerInformer: workerInformer,
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
			slog.InfoContext(ctx, "Syncer: removing worker from store", slog.String("worker", pod.Namespace+"/"+pod.Name))
			if err := s.releaseActorOnDeadWorker(ctx, pod.Namespace, pod.Labels[workerPodLabel], pod.Name); err != nil {
				slog.ErrorContext(ctx, "Failed to release actor bound to deleted worker", slog.Any("err", err))
			}
			err := s.persistence.DeleteWorker(ctx, pod.Namespace, pod.Labels[workerPodLabel], pod.Name)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to delete worker from store during delete event", slog.Any("err", err))
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
	}()
}

func (s *WorkerPoolSyncer) syncWorkerToStore(ctx context.Context, pod *corev1.Pod) {
	if !isWorkerEligible(pod) {
		return
	}

	if pod.DeletionTimestamp != nil {
		slog.InfoContext(ctx, "Syncer: removing worker from store (pod deleting)", slog.String("worker", pod.Namespace+"/"+pod.Name))
		if err := s.releaseActorOnDeadWorker(ctx, pod.Namespace, pod.Labels[workerPodLabel], pod.Name); err != nil {
			slog.ErrorContext(ctx, "Failed to release actor bound to soft-deleting worker", slog.Any("err", err))
		}
		err := s.persistence.DeleteWorker(ctx, pod.Namespace, pod.Labels[workerPodLabel], pod.Name)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to delete worker from store during update event (deleting)", slog.Any("err", err))
		}
		return
	}

	w, err := s.persistence.GetWorker(ctx, pod.Namespace, pod.Labels[workerPodLabel], pod.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			slog.InfoContext(ctx, "Syncer: creating worker in store", slog.String("worker", pod.Namespace+"/"+pod.Name))
			err = s.persistence.CreateWorker(ctx, &ateapipb.Worker{
				WorkerNamespace: pod.Namespace,
				WorkerPool:      pod.Labels[workerPodLabel],
				WorkerPod:       pod.Name,
				Ip:              pod.Status.PodIP,
			})
			if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
				slog.ErrorContext(ctx, "Failed to create worker in store", slog.Any("err", err))
			}
			return
		}
		slog.ErrorContext(ctx, "Failed to get worker from store", slog.Any("err", err))
		return
	}

	if w.Ip != pod.Status.PodIP {
		// TODO: I don't think this is possible, but handling this case so we can log it just in case we can reproduce it.
		slog.InfoContext(ctx, "Syncer: updating worker in store (IP changed)", slog.String("worker", pod.Namespace+"/"+pod.Name))
		w.Ip = pod.Status.PodIP
		if err = s.persistence.UpdateWorker(ctx, w, w.Version); err != nil {
			slog.ErrorContext(ctx, "Failed to update worker in store", slog.Any("err", err))
		}
	}
}

func isWorkerEligible(pod *corev1.Pod) bool {
	return pod.Status.PodIP != ""
}

// releaseActorOnDeadWorker resets the actor bound to a vanishing worker
// pod back to STATUS_SUSPENDED so the next request reassigns it.
//
// UpdateActor uses optimistic version checking. A concurrent SuspendActor
// or ResumeActor wins; we drop this attempt silently.
//
// Best-effort only. The caller always proceeds to DeleteWorker after this
// returns, so any non-contention failure leaves the actor stranded
// (STATUS_RUNNING, pointer at a pod that no longer exists). Recovery
// then needs a manual SuspendActor.
//
// The long-term fix is a finalizer-based controller that holds the pod
// in Terminating state until the actor is gracefully suspended. Tracked
// in https://github.com/agent-substrate/substrate/issues/23.
func (s *WorkerPoolSyncer) releaseActorOnDeadWorker(ctx context.Context, namespace, pool, podName string) error {
	worker, err := s.persistence.GetWorker(ctx, namespace, pool, podName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if worker.GetActorId() == "" {
		return nil
	}
	actor, err := s.persistence.GetActor(ctx, worker.GetActorId())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	// Skip if a concurrent SuspendActor already cleared the pointer.
	if actor.GetAteomPodNamespace() != namespace || actor.GetAteomPodName() != podName {
		return nil
	}
	actor.Status = ateapipb.Actor_STATUS_SUSPENDED
	actor.AteomPodNamespace = ""
	actor.AteomPodName = ""
	actor.AteomPodIp = ""
	actor.InProgressSnapshot = ""
	if err := s.persistence.UpdateActor(ctx, actor, actor.GetVersion()); err != nil && !errors.Is(err, store.ErrPersistenceRetry) {
		return err
	}
	return nil
}
