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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

// SuspendInput holds the immutable parameters requested by the client.
type SuspendInput struct {
	ActorRef resources.ActorRef
}

// SuspendState holds the mutable state loaded and modified during execution.
type SuspendState struct {
	Actor             *ateapipb.Actor
	ActorTemplate     *atev1alpha1.ActorTemplate
	SourceVersion     int64
	WireSnapshotScope string
}

type LoadActorForSuspendStep struct {
	store               store.Interface
	actorTemplateLister listersv1alpha1.ActorTemplateLister
}

func (s *LoadActorForSuspendStep) Name() string { return "LoadActorForSuspend" }
func (s *LoadActorForSuspendStep) IsComplete(ctx context.Context, input *SuspendInput, state *SuspendState) (bool, error) {
	// Always run to get the freshest state
	return false, nil
}
func (s *LoadActorForSuspendStep) CheckPrerequisite(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	return nil
}
func (s *LoadActorForSuspendStep) Execute(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	actor, err := s.store.GetActor(ctx, input.ActorRef)
	if err != nil {
		return err
	}
	state.Actor = actor
	state.SourceVersion = actor.GetMetadata().GetVersion()
	if actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDING {
		state.SourceVersion = actor.GetInProgressSnapshotSourceActorVersion()
	}

	actorTemplate, err := s.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil {
		return fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	state.ActorTemplate = actorTemplate

	return nil
}

func (s *LoadActorForSuspendStep) RetryBackoff() *wait.Backoff { return nil }

type MarkSuspendingStep struct {
	store store.Interface
}

func (s *MarkSuspendingStep) Name() string { return "MarkSuspending" }
func (s *MarkSuspendingStep) IsComplete(ctx context.Context, input *SuspendInput, state *SuspendState) (bool, error) {
	// Fast forward if we've already marked our intent or if we are further along.
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDING || state.Actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDED, nil
}
func (s *MarkSuspendingStep) CheckPrerequisite(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		return status.Errorf(codes.FailedPrecondition, "MarkSuspendingStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, state.Actor.GetStatus(), ateapipb.Actor_STATUS_RUNNING)
	}
	return nil
}

func (s *MarkSuspendingStep) Execute(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	state.Actor.Status = ateapipb.Actor_STATUS_SUSPENDING
	state.Actor.InProgressSnapshotSourceActorVersion = state.SourceVersion
	name := resources.NewSnapshotName()
	// Fail here rather than at checkpoint time if the template's location
	// cannot produce a usable URI: nothing has been written yet.
	if _, err := inProgressSnapshotURI(state, input.ActorRef.Atespace, name); err != nil {
		return err
	}
	state.Actor.InProgressSnapshotName = name
	updatedActor, err := s.store.UpdateActor(ctx, state.Actor, state.Actor.GetMetadata().GetVersion())
	if err != nil {
		return err
	}
	state.Actor = updatedActor
	return nil
}

func (s *MarkSuspendingStep) RetryBackoff() *wait.Backoff { return nil }

// commitSnapshotScope returns the scope a commit (suspend) snapshot is taken
// with. Golden actors always commit Full regardless of the template's
// onCommit: the golden snapshot is the base an OnGolden data resume is
// combined with at restore, so it must carry the guest memory and filesystem
// — a data-only golden would leave nothing to restore the guest from.
func commitSnapshotScope(atespace string, tmpl *atev1alpha1.ActorTemplate) atev1alpha1.SnapshotScope {
	if atespace == resources.GoldenActorAtespace {
		return atev1alpha1.SnapshotScopeFull
	}
	return tmpl.Spec.SnapshotsConfig.OnCommit
}

type CallAteletSuspendStep struct {
	store  store.Interface
	dialer *AteletDialer
}

func (s *CallAteletSuspendStep) Name() string { return "CallAteletSuspend" }
func (s *CallAteletSuspendStep) IsComplete(ctx context.Context, input *SuspendInput, state *SuspendState) (bool, error) {
	// If we are already SUSPENDED, we've already called Atelet
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDED, nil
}
func (s *CallAteletSuspendStep) CheckPrerequisite(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_SUSPENDING {
		return status.Errorf(codes.FailedPrecondition, "CallAteletSuspendStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, state.Actor.GetStatus(), ateapipb.Actor_STATUS_SUSPENDING)
	}
	if state.Actor.GetWorkerAssignment() == nil {
		// Missing active worker pod reference in SUSPENDING state indicates corrupted store state.
		if err := crashActor(ctx, s.store, input.ActorRef, ateattr.OperationSuspend, ateattr.ReasonCorruptedAssignment); err != nil {
			slog.ErrorContext(ctx, "Failed to crash actor", slog.String("err", err.Error()))
		}
		return fmt.Errorf("actor is CRASHED because it was in SUSPENDING state but has no active worker")
	}
	return nil
}
func (s *CallAteletSuspendStep) Execute(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	assignment := state.Actor.GetWorkerAssignment()
	ateletConn, err := s.dialer.DialForWorker(assignment.GetWorkerNamespace(), assignment.GetWorkerPod())
	if err != nil {
		if errors.Is(err, ErrWorkerPodNotFound) {
			slog.ErrorContext(ctx, "Worker pod gone before checkpoint, crashing actor", "namespace", assignment.GetWorkerNamespace(), "pod", assignment.GetWorkerPod(), "in_progress_snapshot_name", state.Actor.GetInProgressSnapshotName())
			if err := crashActor(ctx, s.store, input.ActorRef, ateattr.OperationSuspend, ateattr.ReasonWorkerPodGone); err != nil {
				slog.ErrorContext(ctx, "Failed to crash actor", slog.String("err", err.Error()))
			}
			return fmt.Errorf("actor is CRASHED because its worker pod is gone and no snapshot was written")
		}
		return fmt.Errorf("while getting atelet conn for worker pod: %w", err)
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplate(state.ActorTemplate, state.Actor)
	if err != nil {
		return err
	}

	snapshotURI, err := inProgressSnapshotURI(state, state.Actor.GetMetadata().GetAtespace(), state.Actor.GetInProgressSnapshotName())
	if err != nil {
		return err
	}

	// Checkpoint does not carry the sandbox config: atelet uses the version the
	// actor is currently running (recorded on-node at Run/Restore) and pins it
	// into the snapshot manifest.
	req := &ateletpb.CheckpointRequest{
		TargetAteomUid:         assignment.GetWorkerPodUid(),
		Atespace:               state.Actor.GetMetadata().GetAtespace(),
		ActorName:              state.Actor.GetMetadata().GetName(),
		ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
		ActorTemplateName:      state.Actor.GetActorTemplateName(),
		Spec:                   workloadSpec,
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.CheckpointRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
				SnapshotUri: snapshotURI.String(),
			},
		},
		Scope:    toAteletSnapshotScope(commitSnapshotScope(state.Actor.GetMetadata().GetAtespace(), state.ActorTemplate)),
		ActorUid: state.Actor.GetMetadata().Uid,
	}
	state.WireSnapshotScope = ateattr.SnapshotScopeValue(req.Scope)

	_, err = client.Checkpoint(ctx, req)
	return maybeCrashActor(ctx, s.store, input.ActorRef, err, "while checkpointing workload", ateattr.OperationSuspend)
}

func inProgressSnapshotURI(state *SuspendState, atespace, name string) (resources.SnapshotURI, error) {
	uri, err := resources.NewSnapshotURI(state.ActorTemplate.Spec.SnapshotsConfig.Location, atespace, name)
	if err != nil {
		return resources.SnapshotURI{}, fmt.Errorf("while building the snapshot URI for actor %s/%s: %w", atespace, state.Actor.GetMetadata().GetName(), err)
	}
	return uri, nil
}

func (s *CallAteletSuspendStep) RetryBackoff() *wait.Backoff { return nil }

type DetachVolumesStep struct {
	store          store.Interface
	pluginRegistry VolumePluginRegistry
}

func (s *DetachVolumesStep) Name() string { return "DetachVolumes" }

func (s *DetachVolumesStep) IsComplete(ctx context.Context, input *SuspendInput, state *SuspendState) (bool, error) {
	// TODO replace with a proper check on the volumes.
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDED && state.Actor.GetWorkerAssignment() == nil, nil
}

func (s *DetachVolumesStep) CheckPrerequisite(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	return nil
}

func (s *DetachVolumesStep) Execute(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	return detachActorVolumes(ctx, s.store, s.pluginRegistry, state.Actor, state.ActorTemplate, ateattr.OperationSuspend)
}

func (s *DetachVolumesStep) RetryBackoff() *wait.Backoff { return nil }

type FinalizeSuspendedStep struct {
	store store.Interface
}

func (s *FinalizeSuspendedStep) Name() string { return "FinalizeSuspended" }
func (s *FinalizeSuspendedStep) IsComplete(ctx context.Context, input *SuspendInput, state *SuspendState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_SUSPENDED, nil
}
func (s *FinalizeSuspendedStep) CheckPrerequisite(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	if state.Actor.GetStatus() != ateapipb.Actor_STATUS_SUSPENDING {
		return status.Errorf(codes.FailedPrecondition, "FinalizeSuspendedStep prerequisite not met for Actor: %s (got: %v, want %s)", input.ActorRef, state.Actor.GetStatus(), ateapipb.Actor_STATUS_SUSPENDING)
	}
	return nil
}
func (s *FinalizeSuspendedStep) Execute(ctx context.Context, input *SuspendInput, state *SuspendState) error {
	latestActor, err := s.store.GetActor(ctx, input.ActorRef)
	if err != nil {
		return err
	}

	// 1. Free the worker (if it hasn't been freed yet)
	if assignment := latestActor.GetWorkerAssignment(); assignment != nil {
		workerPod := assignment.GetWorkerPod()

		worker, err := s.store.GetWorker(ctx, assignment.GetWorkerNamespace(), assignment.GetWorkerPool(), workerPod)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("while getting worker for release: %w", err)
			}
			slog.WarnContext(ctx, "Worker already gone during finalize suspend, skipping release", "worker", workerPod)
		} else {
			// Only free it if it still belongs to us
			if wass := worker.Assignment; wass != nil {
				if wass.GetActorUid() == latestActor.GetMetadata().GetUid() {
					worker.Assignment = nil
					err = s.store.UpdateWorker(ctx, worker, worker.Version)
					if err != nil {
						return err
					}
				}
			}
		}

		// 2. Clear the actor's assignment, now that the worker is freed
		latestActor, err = s.store.GetActor(ctx, input.ActorRef)
		if err != nil {
			return err
		}
		latestActor.Status = ateapipb.Actor_STATUS_SUSPENDED
		if latestActor.InProgressSnapshotName != "" {
			snapshotName := latestActor.InProgressSnapshotName
			// The same inputs CallAteletSuspend used, so the recorded URI is
			// where the bytes were actually written.
			snapshotURI, err := inProgressSnapshotURI(state, input.ActorRef.Atespace, snapshotName)
			if err != nil {
				return err
			}
			snapshot := &ateapipb.ActorSnapshot{
				Metadata:               &ateapipb.ResourceMetadata{Atespace: input.ActorRef.Atespace, Name: snapshotName},
				SourceActor:            input.ActorRef.ToObjectRef(),
				SourceActorUid:         latestActor.GetMetadata().GetUid(),
				SourceActorVersion:     state.SourceVersion,
				ActorTemplateNamespace: latestActor.GetActorTemplateNamespace(),
				ActorTemplateName:      latestActor.GetActorTemplateName(),
				ActorTemplateUid:       string(state.ActorTemplate.GetUID()),
				ContentScope:           toActorSnapshotContentScope(commitSnapshotScope(input.ActorRef.Atespace, state.ActorTemplate)),
				SnapshotUri:            snapshotURI.String(),
			}
			if _, err := s.store.CreateActorSnapshot(ctx, snapshot); err != nil && !errors.Is(err, store.ErrAlreadyExists) {
				return err
			}
			latestActor.LatestSnapshot = &ateapipb.ObjectRef{Atespace: input.ActorRef.Atespace, Name: snapshotName}
			latestActor.InProgressSnapshotName = ""
			latestActor.InProgressSnapshotSourceActorVersion = 0
		}
		latestActor.WorkerAssignment = nil
		latestActor.LocalSnapshotInfo = nil
		updatedActor, err := s.store.UpdateActor(ctx, latestActor, latestActor.GetMetadata().GetVersion())
		if err != nil {
			return err
		}
		latestActor = updatedActor
	}

	state.Actor = latestActor
	return nil
}

func (s *FinalizeSuspendedStep) RetryBackoff() *wait.Backoff { return nil }
