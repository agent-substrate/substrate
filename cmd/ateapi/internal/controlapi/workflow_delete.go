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
	"k8s.io/apimachinery/pkg/util/wait"
)

// DeleteInput holds the immutable parameters requested by the client.
type DeleteInput struct {
	ActorRef resources.ActorRef
}

// DeleteState holds the mutable state loaded and modified during execution.
type DeleteState struct {
	Actor        *ateapipb.Actor
	DeletedActor *ateapipb.Actor
}

type LoadActorForDeleteStep struct {
	store store.Interface
}

func (s *LoadActorForDeleteStep) Name() string { return "LoadActorForDelete" }
func (s *LoadActorForDeleteStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	// Always run to get the freshest state
	return false, nil
}
func (s *LoadActorForDeleteStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	return nil
}
func (s *LoadActorForDeleteStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	actor, err := s.store.GetActor(ctx, input.ActorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return status.Errorf(codes.NotFound, "Actor %s not found", input.ActorRef)
		}
		return fmt.Errorf("while fetching actor: %w", err)
	}
	state.Actor = actor
	return nil
}

func (s *LoadActorForDeleteStep) RetryBackoff() *wait.Backoff { return nil }

type MarkDeletingStep struct {
	store store.Interface
}

func (s *MarkDeletingStep) Name() string { return "MarkDeleting" }
func (s *MarkDeletingStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_DELETING, nil
}
func (s *MarkDeletingStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED &&
		state.Actor.GetStatus() != ateapipb.Actor_STATUS_CRASHED &&
		state.Actor.GetStatus() != ateapipb.Actor_STATUS_DELETING {
		return status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable status (status: %v)", input.ActorRef, state.Actor.GetStatus())
	}
	return nil
}
func (s *MarkDeletingStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	state.Actor.Status = ateapipb.Actor_STATUS_DELETING
	for _, vol := range state.Actor.GetActorVolumes() {
		vol.Status = ateapipb.ExternalVolume_STATUS_DELETING
	}
	updated, err := s.store.UpdateActor(ctx, state.Actor, state.Actor.GetMetadata().GetVersion())
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return fmt.Errorf("while setting actor status to DELETING: %w", err)
	}
	state.Actor = updated
	return nil
}

func (s *MarkDeletingStep) RetryBackoff() *wait.Backoff { return nil }

type DeleteVolumesStep struct {
	store store.Interface
}

func (s *DeleteVolumesStep) Name() string { return "DeleteVolumes" }
func (s *DeleteVolumesStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	return false, nil
}
func (s *DeleteVolumesStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_DELETING {
		return status.Errorf(codes.FailedPrecondition, "DeleteVolumesStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, state.Actor.GetStatus(), ateapipb.Actor_STATUS_DELETING)
	}
	return nil
}
func (s *DeleteVolumesStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	if err := deleteActorVolumes(ctx, state.Actor.GetMetadata().GetUid(), state.Actor.GetActorVolumes()); err != nil {
		return status.Errorf(codes.Internal, "while deleting actor volumes: %v", err)
	}
	return nil
}

func (s *DeleteVolumesStep) RetryBackoff() *wait.Backoff { return nil }

type FinalizeDeletedStep struct {
	store store.Interface
}

func (s *FinalizeDeletedStep) Name() string { return "FinalizeDeleted" }
func (s *FinalizeDeletedStep) IsComplete(ctx context.Context, input *DeleteInput, state *DeleteState) (bool, error) {
	return state.DeletedActor != nil, nil
}
func (s *FinalizeDeletedStep) CheckPrerequisite(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_DELETING {
		return status.Errorf(codes.FailedPrecondition, "FinalizeDeletedStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, state.Actor.GetStatus(), ateapipb.Actor_STATUS_DELETING)
	}
	return nil
}
func (s *FinalizeDeletedStep) Execute(ctx context.Context, input *DeleteInput, state *DeleteState) error {
	deleted, err := s.store.DeleteActor(ctx, input.ActorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return status.Errorf(codes.NotFound, "Actor %s not found", input.ActorRef)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			current, getErr := s.store.GetActor(ctx, input.ActorRef)
			if getErr == nil {
				return status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable status (status: %v)", input.ActorRef, current.GetStatus())
			}
			return status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable status", input.ActorRef)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return fmt.Errorf("while deleting actor from DB: %w", err)
	}
	state.DeletedActor = deleted
	return nil
}

func (s *FinalizeDeletedStep) RetryBackoff() *wait.Backoff { return nil }
