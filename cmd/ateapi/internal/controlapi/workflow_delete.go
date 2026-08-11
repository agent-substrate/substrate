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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DeleteActor executes the workflow to delete an actor. Idempotent.
func (w *ActorWorkflow) DeleteActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	ctx, lock, err := w.acquireActorLock(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lock.Close()

	actor, err := w.loadActorForDelete(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	if actor, err = w.ensureMarkedDeleting(ctx, actorRef, actor); err != nil {
		return nil, err
	}
	if err := w.ensureVolumesDeleted(ctx, actor); err != nil {
		return nil, err
	}
	return w.finalizeDeleted(ctx, actorRef)
}

// loadActorForDelete fetches the current actor record.
func (w *ActorWorkflow) loadActorForDelete(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "LoadActorForDelete")
	defer func() { err = done(err) }()

	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
		}
		return nil, fmt.Errorf("while fetching actor: %w", err)
	}
	return actor, nil
}

// ensureMarkedDeleting transitions the actor and its volumes to DELETING and
// persists the change, returning the stored copy. Skips when a previous
// attempt already marked the actor.
func (w *ActorWorkflow) ensureMarkedDeleting(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "MarkDeleting")
	defer func() { err = done(err) }()

	if actor.GetStatus() == ateapipb.Actor_STATUS_DELETING {
		markSkipped(ctx, "actor already DELETING")
		return actor, nil
	}
	if actor.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED &&
		actor.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		return nil, status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable status (status: %v)", actorRef, actor.GetStatus())
	}

	updated, err := w.store.UpdateActor(ctx, actorRef, func(dbActor *ateapipb.Actor) error {
		if err := store.CheckActorPrecondition(dbActor, actor.GetMetadata().GetUid(), actor.GetMetadata().GetVersion()); err != nil {
			return err
		}
		dbActor.Status = ateapipb.Actor_STATUS_DELETING
		for _, vol := range dbActor.GetActorVolumes() {
			vol.Status = ateapipb.ExternalVolume_STATUS_DELETING
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while setting actor status to DELETING: %w", err)
	}
	return updated, nil
}

// ensureVolumesDeleted removes the actor's external volumes. Volume deletion
// is idempotent, so a re-entered workflow can safely run it again.
func (w *ActorWorkflow) ensureVolumesDeleted(ctx context.Context, actor *ateapipb.Actor) (err error) {
	ctx, done := stepSpan(ctx, "DeleteVolumes")
	defer func() { err = done(err) }()

	if err := deleteActorVolumes(ctx, w.pluginRegistry, actor.GetMetadata().GetUid(), actor.GetActorVolumes()); err != nil {
		return status.Errorf(codes.Internal, "while deleting actor volumes: %v", err)
	}
	return nil
}

// finalizeDeleted removes the actor from the store and returns the deleted
// record. The store enforces that only a DELETING actor can be removed.
func (w *ActorWorkflow) finalizeDeleted(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "FinalizeDeleted")
	defer func() { err = done(err) }()

	deleted, err := w.store.DeleteActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			current, getErr := w.store.GetActor(ctx, actorRef)
			if getErr == nil {
				return nil, status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable status (status: %v)", actorRef, current.GetStatus())
			}
			return nil, status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable status", actorRef)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting actor from DB: %w", err)
	}
	return deleted, nil
}
