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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

const (
	actorGCWorkerCount  = 2
	actorGCListPageSize = 100
)

type actorGCReconcilerStore interface {
	ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.Actor], error)
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapi.Actor, error)
}

type actorGCControl interface {
	DeleteActor(ctx context.Context, req *ateapi.DeleteActorRequest) (*ateapi.Actor, error)
}

// ActorGCReconciler retries deletion of actors stuck in ACTOR_STATE_DELETING.
type ActorGCReconciler struct {
	persistence    actorGCReconcilerStore
	control        actorGCControl
	queue          workqueue.TypedRateLimitingInterface[resources.ActorRef]
	resyncInterval time.Duration
}

// NewActorGCReconciler creates a new ActorGCReconciler.
func NewActorGCReconciler(persistence actorGCReconcilerStore, control actorGCControl, resyncInterval time.Duration) *ActorGCReconciler {
	return &ActorGCReconciler{
		persistence:    persistence,
		control:        control,
		resyncInterval: resyncInterval,
		queue:          workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[resources.ActorRef]()),
	}
}

// Enqueue adds an actor reference to the work queue for deletion retry.
func (r *ActorGCReconciler) Enqueue(ref resources.ActorRef) {
	r.queue.Add(ref)
}

// Start launches the queue workers and the periodic resync loop.
func (r *ActorGCReconciler) Start(ctx context.Context) {
	go func() {
		defer r.queue.ShutDown()
		for range actorGCWorkerCount {
			go wait.UntilWithContext(ctx, r.runWorker, time.Second)
		}
		wait.UntilWithContext(ctx, r.resync, r.resyncInterval)
	}()
}

// resync lists all actors across atespaces and enqueues those in ACTOR_STATE_DELETING.
func (r *ActorGCReconciler) resync(ctx context.Context) {
	pageToken := ""
	for {
		page, err := r.persistence.ListActors(ctx, "", store.ListOptions{PageSize: actorGCListPageSize, PageToken: pageToken})
		if err != nil {
			slog.ErrorContext(ctx, "Failed to list actors for GC resync", slog.Any("err", err))
			return
		}
		for _, actor := range page.Items {
			if actor.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_DELETING {
				ref := resources.ActorRefFromActor(actor)
				r.queue.Add(ref)
				slog.InfoContext(ctx, "Queued deleting actor for GC", slog.String("actor", ref.String()))
			}
		}
		if page.NextPageToken == "" {
			return
		}
		pageToken = page.NextPageToken
	}
}

func (r *ActorGCReconciler) runWorker(ctx context.Context) {
	for r.processNextWorkItem(ctx) {
	}
}

func (r *ActorGCReconciler) processNextWorkItem(ctx context.Context) bool {
	ref, quit := r.queue.Get()
	if quit {
		return false
	}
	defer r.queue.Done(ref)

	if err := r.reconcileOne(ctx, ref); err != nil {
		slog.ErrorContext(ctx, "Failed to GC deleting actor, requeueing",
			slog.Any("actor", ref),
			slog.Any("err", err))
		r.queue.AddRateLimited(ref)
		return true
	}
	r.queue.Forget(ref)
	return true
}

func (r *ActorGCReconciler) reconcileOne(ctx context.Context, ref resources.ActorRef) error {
	actor, err := r.persistence.GetActor(ctx, ref)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("while fetching actor %s for GC: %w", ref, err)
	}
	if actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_DELETING {
		return nil
	}

	req := &ateapi.DeleteActorRequest{
		Actor: ref.ToObjectRef(),
		Options: &ateapi.DeleteOptions{
			Uid: actor.GetMetadata().GetUid(),
		},
	}
	_, err = r.control.DeleteActor(ctx, req)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) || apierror.Code(err) == codes.NotFound {
			return nil
		}
		if errors.Is(err, store.ErrUIDConflict) || apierror.Code(err) == codes.Aborted {
			// Actor UID no longer matches; drop from queue
			return nil
		}
		return fmt.Errorf("while GC deleting actor %s: %w", ref, err)
	}
	slog.InfoContext(ctx, "Successfully completed GC delete for actor", slog.String("actor", ref.String()))
	return nil
}
