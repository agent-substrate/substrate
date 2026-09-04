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
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DeleteActor executes the workflow to delete an actor. Idempotent.
func (w *ActorWorkflow) DeleteActor(ctx context.Context, actorRef resources.ActorRef, anyState bool) (*ateapipb.Actor, error) {
	ctx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	actor, err := w.loadActorForDelete(ctx, actorRef)
	if err != nil {
		return nil, err
	}

	if actor, err = w.ensureMarkedDeleting(ctx, actorRef, actor, anyState); err != nil {
		return nil, err
	}

	// DeleteActor will attempt best-effort cleanup across all steps, collecting any errors.
	// If any step fails, errors are returned so the caller can retry, and the actor record
	// is retained in the store in the DELETING state.
	// TODO: Ensure GC collects all the remaining resources if the cleanup fails.
	var errs []error
	// Cleanup stays best-effort: an unresolvable template is recorded and
	// the remaining steps run without it, like a missing one.
	actorTemplate, err := resolveActorTemplate(ctx, w.store, actor)
	if errors.Is(err, errActorTemplateNotFound) {
		actorTemplate, err = nil, nil
	}
	if err != nil {
		errs = append(errs, fmt.Errorf("while fetching actor template: %w", err))
	}

	var atletTerminatedErr, volumesDetachedErr error
	if err := w.ensureAteletTerminated(ctx, actorRef, actor, actorTemplate); err != nil {
		atletTerminatedErr = fmt.Errorf("while terminating atelet: %w", err)
		errs = append(errs, atletTerminatedErr)
	}
	if err := w.ensureVolumesDetachedForDelete(ctx, actor, actorTemplate); err != nil {
		volumesDetachedErr = fmt.Errorf("while detaching volumes: %w", err)
		errs = append(errs, volumesDetachedErr)
	}

	// Release worker if atelet termination and volume detachment did not fail.
	if atletTerminatedErr == nil && volumesDetachedErr == nil {
		if actor, err = w.ensureWorkerReleased(ctx, actorRef, actor); err != nil {
			errs = append(errs, fmt.Errorf("while releasing worker: %w", err))
		}
	} else {
		slog.WarnContext(ctx, "skipping releasing worker due to atelet termination or volume detachment failure",
			slog.Any("actor", actorRef),
			slog.Any("atletTerminatedErr", atletTerminatedErr),
			slog.Any("volumesDetachedErr", volumesDetachedErr))
	}

	if err := w.ensureVolumesDeleted(ctx, actor); err != nil {
		errs = append(errs, fmt.Errorf("while deleting volumes: %w", err))
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
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

// ensureAteletTerminated calls atelet to terminate the workload.
func (w *ActorWorkflow) ensureAteletTerminated(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate) (err error) {
	ctx, done := stepSpan(ctx, "CallAteletTerminate")
	defer func() { err = done(err) }()

	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		slog.InfoContext(ctx, "actor has no worker assignment, skipping atlet terminate request", slog.Any("actor", actorRef))
		return nil
	}

	if workerName := assignment.GetWorker().GetName(); workerName != "" {
		worker, err := w.store.GetWorker(ctx, workerName)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				slog.InfoContext(ctx, "worker not found in store, skipping atelet terminate request", slog.String("worker", workerName), slog.Any("actor", actorRef))
				return nil
			}
			return fmt.Errorf("while checking worker assignment: %w", err)
		}
		// Ask whether the worker still HOSTS this actor, not whether its one
		// assignment happens to be this actor: a worker hosting several is the
		// ordinary case, and the others are none of this delete's business.
		hosted, err := workerHostsActor(ctx, w.store, worker.GetMetadata().GetName(), actor.GetMetadata().GetUid())
		if err != nil {
			return err
		}
		if !hosted {
			slog.InfoContext(ctx, "worker is no longer assigned to this actor, skipping atelet terminate request",
				slog.String("worker", workerName),
				slog.Any("actor", actorRef))
			return nil
		}
	}

	workerPodNs := assignment.GetWorkerNamespace()
	workerPodName := assignment.GetWorkerPod()

	conn, err := w.dialer.DialForWorker(workerPodNs, workerPodName)
	if err != nil {
		if errors.Is(err, ErrWorkerPodNotFound) {
			slog.InfoContext(ctx, "worker pod not found, treating as terminated", slog.String("workerNamespace", workerPodNs), slog.String("workerPod", workerPodName))
			return nil
		}
		return fmt.Errorf("while connecting to worker pod %s/%s: %w", workerPodNs, workerPodName, err)
	}

	client := ateletpb.NewAteomHerderClient(conn)

	var workloadSpec *ateletpb.WorkloadSpec
	if actorTemplate != nil {
		spec, err := workloadSpecFromActorTemplate(actorTemplate, actor)
		if err != nil {
			return err
		}
		workloadSpec = spec
	} else {
		// When the template is missing/deleted, build a fallback workload spec with
		// all external volumes recorded on the actor so atelet can unmount them on the node.
		slog.WarnContext(ctx, "actor template not found, constructing fallback workload spec for atelet terminate",
			slog.String("actor", actorRef.Name),
			slog.String("templateAtespace", actor.GetActorTemplate().GetAtespace()),
			slog.String("templateName", actor.GetActorTemplate().GetName()))
		workloadSpec = &ateletpb.WorkloadSpec{}
		for _, vol := range actor.GetStatus().GetActorVolumes() {
			// StorageVolumeId is only populated once the volume is provisioned.
			// Skip volumes that were never created (e.g. failed during PENDING state).
			if vol.GetStorageVolumeId() != "" {
				workloadSpec.Volumes = append(workloadSpec.Volumes, &ateletpb.Volume{
					Name: vol.GetVolumeName(),
					Source: &ateletpb.Volume_External{
						External: &ateletpb.ExternalVolumeSource{
							StorageVolumeId: vol.GetStorageVolumeId(),
							VolumeType:      vol.GetVolumeType(),
							VolumeContext:   vol.GetVolumeContext(),
						},
					},
				})
			}
		}
	}

	req := &ateletpb.TerminateRequest{
		TargetAteomUid:        assignment.GetWorkerPodUid(),
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorUid:              actor.GetMetadata().GetUid(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		Spec:                  workloadSpec,
	}

	if _, err := client.Terminate(ctx, req); err != nil {
		if status.Code(err) == codes.NotFound {
			slog.InfoContext(ctx, "workload already terminated on atelet", slog.Any("actor", actorRef))
			return nil
		}
		return fmt.Errorf("while terminating actor on atelet: %w", err)
	}

	return nil
}

// ensureVolumesDetachedForDelete detaches external volumes.
func (w *ActorWorkflow) ensureVolumesDetachedForDelete(ctx context.Context, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate) (err error) {
	ctx, done := stepSpan(ctx, "DetachVolumesForDelete")
	defer func() { err = done(err) }()

	return detachActorVolumes(ctx, w.store, w.pluginRegistry, actor, actorTemplate, "delete")
}

// ensureWorkerReleased releases the worker assigned to the actor.
// releaseAssignmentWithoutBacklink releases an assignment the Actor does not
// reference, found by Actor UID. Absent is the ordinary case and not an error:
// most Actors reaching here really were released already.
func (w *ActorWorkflow) releaseAssignmentWithoutBacklink(ctx context.Context, actor *ateapipb.Actor) error {
	actorUID := actor.GetMetadata().GetUid()
	workerName, err := w.store.FindWorkerHostingActor(ctx, actorUID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			markSkipped(ctx, "worker already released")
			return nil
		}
		return fmt.Errorf("while looking for a worker still hosting actor %s: %w", actorUID, err)
	}

	// Read only to learn whether the Worker is still there; the release itself
	// is guarded by the assignment key.
	if _, err := w.store.GetWorker(ctx, workerName); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			markSkipped(ctx, "worker already released")
			return nil
		}
		return fmt.Errorf("while getting worker %s to release: %w", workerName, err)
	}

	slog.InfoContext(ctx, "Releasing an assignment the Actor does not reference",
		slog.String("worker", workerName), slog.String("actor_uid", actorUID))
	_, err = w.store.ReleaseActorFromWorker(ctx, workerName, actorUID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("while releasing worker %s: %w", workerName, err)
	}
	return nil
}

func (w *ActorWorkflow) ensureWorkerReleased(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor) (updated *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "ReleaseWorker")
	defer func() { err = done(err) }()

	if actor.GetStatus().GetWorkerAssignment() == nil {
		// An Actor with no backlink may still be bound. The assignment commits
		// before the Actor is updated to point at it, so a crash in between
		// leaves a row nothing on the Actor names. Releasing by Actor UID is
		// what recovers it; skipping would leave its share of the Worker's
		// capacity booked until the Worker itself went away. The resume path
		// recovers the same window through workerHoldingStaleClaim.
		return actor, w.releaseAssignmentWithoutBacklink(ctx, actor)
	}

	latestActor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, err
	}

	if latestActor.GetStatus().GetWorkerAssignment() != nil {
		_, _, err := releaseWorker(ctx, w.store, latestActor)
		if err != nil {
			return nil, err
		}

		latestActor, err = w.store.GetActor(ctx, actorRef)
		if err != nil {
			return nil, err
		}

		updatedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(latestActor), func(dbActor *ateapipb.Actor) error {
			if dbActor.Status != nil {
				dbActor.Status.LocalSnapshotInfo = nil
				dbActor.Status.WorkerAssignment = nil
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, store.ErrVersionConflict) {
				return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
			}
			return nil, err
		}
		latestActor = updatedActor
	}
	return latestActor, nil
}

// ensureMarkedDeleting transitions the actor and its volumes to DELETING and
// persists the change, returning the stored copy. Skips when a previous
// attempt already marked the actor.
func (w *ActorWorkflow) ensureMarkedDeleting(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, anyState bool) (updated *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "MarkDeleting")
	defer func() { err = done(err) }()

	st := actor.GetStatus().GetState()
	if st == ateapipb.ActorState_ACTOR_STATE_DELETING {
		markSkipped(ctx, "actor already DELETING")
		return actor, nil
	}
	shouldDelete := false
	switch st {
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		ateapipb.ActorState_ACTOR_STATE_CRASHED:
		shouldDelete = true
	default:
		// This allows deletion for any state
		shouldDelete = anyState
	}
	if !shouldDelete {
		return nil, status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable state (state: %v)", actorRef, st)
	}

	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_DELETING
		for _, vol := range toUpdate.GetStatus().GetActorVolumes() {
			vol.Status = ateapipb.ExternalVolume_STATUS_DELETING
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while setting actor state to DELETING: %w", err)
	}
	return storedActor, nil
}

// ensureVolumesDeleted removes the actor's external volumes. Volume deletion
// is idempotent, so a re-entered workflow can safely run it again. It assumes a no-op success if volume is not found
func (w *ActorWorkflow) ensureVolumesDeleted(ctx context.Context, actor *ateapipb.Actor) (err error) {
	ctx, done := stepSpan(ctx, "DeleteVolumes")
	defer func() { err = done(err) }()

	st := actor.GetStatus().GetState()
	if st != ateapipb.ActorState_ACTOR_STATE_DELETING {
		return status.Errorf(codes.FailedPrecondition, "DeleteVolumes prerequisite not met for Actor: %s (got: %v, want %s)", actor.GetMetadata().GetName(), st, ateapipb.ActorState_ACTOR_STATE_DELETING)
	}

	if err := deleteActorVolumes(ctx, w.pluginRegistry, actor.GetMetadata().GetUid(), actor.GetStatus().GetActorVolumes()); err != nil {
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
				return nil, status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable state (state: %v)", actorRef, current.GetStatus().GetState())
			}
			return nil, status.Errorf(codes.FailedPrecondition, "Actor %s is not in a deletable state", actorRef)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting actor from DB: %w", err)
	}
	return deleted, nil
}
