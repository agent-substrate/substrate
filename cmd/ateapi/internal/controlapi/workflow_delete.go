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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DeleteActor executes the workflow to delete an actor. Idempotent.
func (w *ActorWorkflow) DeleteActor(ctx context.Context, actorRef resources.ActorRef, precondition store.DeletePreconditions) (*ateapi.Actor, error) {
	ctx = preview.ContextWithOptIn(ctx)
	ctx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	actor, err := w.ensureMarkedDeleting(ctx, actorRef, precondition)
	if err != nil {
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
	if err := w.ensureAteletTerminated(ctx, actorRef, actor, actorTemplate, ateattr.OperationDelete); err != nil {
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

	if err := w.ensureExternalSnapshotsReleased(ctx, actor); err != nil {
		errs = append(errs, fmt.Errorf("while releasing external snapshots: %w", err))
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return w.finalizeDeleted(ctx, actor)
}

// ensureAteletTerminated calls atelet to terminate the workload for opName.
func (w *ActorWorkflow) ensureAteletTerminated(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, actorTemplate *ateapi.ActorTemplate, opName string) (err error) {
	ctx, done := stepSpan(ctx, "CallAteletTerminate")
	defer func() { err = done(err) }()

	nodeName := actor.GetStatus().GetAssignedNode()
	if nodeName == "" {
		slog.InfoContext(ctx, "actor has no assigned node, skipping atelet terminate request", slog.Any("actor", actorRef))
		return nil
	}

	workerPodUID := ""
	if assignment := actor.GetStatus().GetWorkerAssignment(); assignment != nil {
		workerPodUID = assignment.GetWorkerPodUid()
		if workerName := assignment.GetWorker().GetName(); workerName != "" {
			// Ask whether the worker still HOSTS this actor, not whether its one
			// assignment happens to be this actor: a worker hosting several is the
			// ordinary case, and the others are none of this delete's business.
			hosted, err := workerHostsActor(ctx, w.store, workerName, actor.GetMetadata().GetUid())
			if err != nil {
				return err
			}
			if !hosted {
				slog.InfoContext(ctx, "worker is no longer assigned to this actor, skipping worker workload termination",
					slog.String("worker", workerName),
					slog.Any("actor", actorRef))
				workerPodUID = ""
			}
		}
	}

	conn, err := w.dialer.DialForAteletOnNode(nodeName)
	if errors.Is(err, ErrNoAteletOnNode) {
		gone, nodeErr := w.dialer.NodeGone(ctx, nodeName)
		if nodeErr != nil {
			return fmt.Errorf("while connecting to atelet on node %q: %w (and while checking the node: %w)", nodeName, err, nodeErr)
		}
		if gone {
			// A deleted Node object counts as the node being gone for good. If the
			// machine still runs, whatever atelet holds for the actor stays there.
			slog.WarnContext(ctx, "assigned node no longer exists, skipping atelet terminate request",
				slog.String("node", nodeName), slog.Any("actor", actorRef))
			return nil
		}
	}
	if err != nil {
		return fmt.Errorf("while connecting to atelet on node %q: %w", nodeName, err)
	}

	client := ateletpb.NewAteletClient(conn)

	var workloadSpec *ateletpb.WorkloadSpec
	if actorTemplate != nil {
		spec, err := workloadSpecFromActorTemplate(actorTemplate, actor, nil)
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
		workloadSpec = fallbackWorkloadSpec(actor)
	}

	req := &ateletpb.TerminateRequest{
		WorkerPodUid:          workerPodUID,
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
		return handleAteletError(ctx, w.store, actorRef, opName, "Terminate", true, err)
	}

	return nil
}

// fallbackWorkloadSpec is the spec to terminate an actor with when its template
// cannot be resolved, whether deleted or never readable from the caller. Only
// the external volumes recorded on the actor are carried, because atelet still
// unmounts them from the node. A volume with no StorageVolumeId was never
// provisioned (e.g. the actor failed while PENDING), so there is nothing
// mounted to name.
func fallbackWorkloadSpec(actor *ateapi.Actor) *ateletpb.WorkloadSpec {
	// Given that external volumes has been in the codebase for a while and
	// its preview-ness is solely around the API, and that the impact of
	// disabling the gate while in use would be an orphaned volume
	// attachment, we are NOT checking the ExternalVolumes gate in the
	// delete/suspend path.
	spec := &ateletpb.WorkloadSpec{}
	if preview.IsEnabled(preview.GateExternalVolumes) {
		for _, vol := range actor.GetStatus().GetExternalVolumes().GetVolumes() {
			if vol.GetStorageVolumeId() == "" {
				continue
			}
			spec.Volumes = append(spec.Volumes, &ateletpb.Volume{
				Name: vol.GetName(),
				External: &ateletpb.ExternalVolumeSource{
					StorageVolumeId: vol.GetStorageVolumeId(),
					VolumeType:      vol.GetVolumeType(),
					VolumeContext:   vol.GetVolumeContext(),
				},
			})
		}
	}
	return spec
}

// ensureVolumesDetachedForDelete detaches external volumes.
func (w *ActorWorkflow) ensureVolumesDetachedForDelete(ctx context.Context, actor *ateapi.Actor, actorTemplate *ateapi.ActorTemplate) (err error) {
	ctx, done := stepSpan(ctx, "DetachVolumesForDelete")
	defer func() { err = done(err) }()

	// Given that external volumes has been in the codebase for a while and
	// its preview-ness is solely around the API, and that the impact of
	// disabling the gate while in use would be an orphaned volume
	// attachment, we are NOT checking the ExternalVolumes gate in the
	// delete/suspend path.
	return detachActorVolumes(ctx, w.pluginRegistry, actor, actorTemplate, "delete")
}

// ensureWorkerReleased releases the worker assigned to the actor.
// releaseAssignmentWithoutBacklink releases an assignment the Actor does not
// reference, found by Actor UID. Absent is the ordinary case and not an error:
// most Actors reaching here really were released already.
func (w *ActorWorkflow) releaseAssignmentWithoutBacklink(ctx context.Context, actor *ateapi.Actor) error {
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

func (w *ActorWorkflow) ensureWorkerReleased(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor) (updated *ateapi.Actor, err error) {
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
		_, err := releaseWorker(ctx, w.store, latestActor)
		if err != nil {
			return nil, err
		}

		latestActor, err = w.store.GetActor(ctx, actorRef)
		if err != nil {
			return nil, err
		}

		updatedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(latestActor), func(dbActor *ateapi.Actor) error {
			if dbActor.Status != nil {
				clearLocalSnapshots(dbActor.Status)
				dbActor.Status.AssignedNode = ""
				dbActor.Status.WorkerAssignment = nil
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, store.ErrVersionConflict) {
				return nil, apierror.Aborted("concurrent update conflict, please retry")
			}
			return nil, err
		}
		latestActor = updatedActor
	}
	return latestActor, nil
}

var errAlreadyMarked = errors.New("already marked for deletion")

// ensureMarkedDeleting sets the actor's delete_time, checking precondition,
// and moves it to DELETING, returning the stored copy. Skips when a previous
// attempt already marked the actor.
func (w *ActorWorkflow) ensureMarkedDeleting(ctx context.Context, actorRef resources.ActorRef, precondition store.DeletePreconditions) (updated *ateapi.Actor, err error) {
	ctx, done := stepSpan(ctx, "MarkDeleting")
	defer func() { err = done(err) }()

	var alreadyDeleting *ateapi.Actor
	storedActor, err := w.store.MarkActorForDeletion(ctx, actorRef, precondition, func(toUpdate *ateapi.Actor) error {
		if toUpdate.GetMetadata().GetDeleteTime() != nil {
			alreadyDeleting = toUpdate
			return errAlreadyMarked
		}
		toUpdate.Status.State = ateapi.ActorState_ACTOR_STATE_DELETING
		return nil
	})
	if errors.Is(err, errAlreadyMarked) {
		markSkipped(ctx, "actor already marked for deletion")
		return alreadyDeleting, nil
	}
	if err != nil {
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("Actor %s does not have uid %s", actorRef, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, fmt.Errorf("while marking actor for deletion: %w", err)
	}
	logActorStateChanged(ctx, storedActor, ateattr.OperationDelete)
	return storedActor, nil
}

// ensureVolumesDeleted removes the actor's external volumes. Volume deletion
// is idempotent, so a re-entered workflow can safely run it again. It assumes a no-op success if volume is not found
func (w *ActorWorkflow) ensureVolumesDeleted(ctx context.Context, actor *ateapi.Actor) (err error) {
	ctx, done := stepSpan(ctx, "DeleteVolumes")
	defer func() { err = done(err) }()

	st := actor.GetStatus().GetState()
	if st != ateapi.ActorState_ACTOR_STATE_DELETING {
		return apierror.FailedPrecondition("DeleteVolumes prerequisite not met for Actor: %s (got: %v, want %s)", actor.GetMetadata().GetName(), st, ateapi.ActorState_ACTOR_STATE_DELETING)
	}

	// Given that external volumes has been in the codebase for a while and
	// its preview-ness is solely around the API, and that the impact of
	// disabling the gate while in use would be an orphaned volume
	// attachment, we are NOT checking the ExternalVolumes gate in the
	// delete/suspend path.
	if err := deleteActorVolumes(ctx, w.pluginRegistry, actor.GetMetadata().GetUid(), actor.GetStatus().GetExternalVolumes().GetVolumes()); err != nil {
		return apierror.Internal("while deleting actor volumes: %v", err)
	}
	return nil
}

// ensureExternalSnapshotsReleased collects everything the actor wrote to
// object storage, by deleting its own prefix. It runs before the actor is
// removed, so a failed attempt is still rediscoverable on the retry.
//
// A snapshot borrowed from a tag lives under the tag's prefix, so it survives:
// the tag owns it and outlives the actor.
func (w *ActorWorkflow) ensureExternalSnapshotsReleased(ctx context.Context, actor *ateapi.Actor) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseExternalSnapshots")
	defer func() { err = done(err) }()

	if w.objectStorePlugin == nil {
		markSkipped(ctx, "no object store configured")
		return nil
	}

	prefix, err := actorSnapshotStoragePrefix(actor)
	if err != nil {
		return err
	}
	if prefix.IsZero() {
		markSkipped(ctx, "the actor owns no external snapshot")
		return nil
	}
	return w.cleanupSnapshot(ctx, prefix)
}

// actorSnapshotStoragePrefix returns the prefix holding every object the actor wrote:
// the snapshot it last took, the one a suspend was in the middle of taking, and
// anything a crashed suspend stranded. A zero prefix means the actor never
// wrote anything.
func actorSnapshotStoragePrefix(actor *ateapi.Actor) (resources.StoragePrefix, error) {
	actorOwner := actorSnapshotOwner(actor)
	if snap := findLatestDurableSnapshot(actor.GetStatus(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); snap != nil {
		snapshotURI := snap.GetDurableSnapshot().GetObject().GetSnapshotUri()
		uri, err := resources.ParseSnapshotURI(snapshotURI)
		if err != nil {
			return resources.StoragePrefix{}, fmt.Errorf("while parsing the external snapshot %q: %w", snapshotURI, err)
		}
		// A URI the actor does not own is a tag's snapshot, borrowed until the actor's
		// first suspend completes, which means it has written nothing of its
		// own yet.
		if uri.OwnedBy(actorOwner) {
			return uri.OwnerPrefix(), nil
		}
	}
	// Nothing of the actor's own is recorded. Unless a suspend died partway,
	// nothing was ever written under its prefix: the in-progress URI is
	// recorded before atelet uploads the first object.
	inProgressSnap := findLatestDurableSnapshot(actor.GetStatus(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	inProgress := inProgressSnap.GetDurableSnapshot().GetObject().GetSnapshotUri()
	if inProgress == "" {
		return resources.StoragePrefix{}, nil
	}
	uri, err := resources.ParseSnapshotURI(inProgress)
	if err != nil {
		return resources.StoragePrefix{}, fmt.Errorf("while parsing the in-progress snapshot %q: %w", inProgress, err)
	}
	if !uri.OwnedBy(actorOwner) {
		// Corrupted record. This should never happen. An in-progress snapshot should
		// always be owned by the actor that holds it.
		return resources.StoragePrefix{}, fmt.Errorf("the in-progress snapshot %q is not owned by actor %s", inProgress, actorOwner)
	}
	return uri.OwnerPrefix(), nil
}

// finalizeDeleted removes the actor from the store and returns the deleted
// record.
func (w *ActorWorkflow) finalizeDeleted(ctx context.Context, actor *ateapi.Actor) (_ *ateapi.Actor, err error) {
	ctx, done := stepSpan(ctx, "FinalizeDeleted")
	defer func() { err = done(err) }()

	actorRef := resources.ActorRefFromActor(actor)
	// Only the uid guards the delete. The mark already committed this
	// incarnation to deletion, and a later write such as an UpdateActor RPC,
	// which takes no actor lease, must not block removing the row.
	precondition := store.DeletePreconditions{UID: actor.GetMetadata().GetUid()}
	deleted, err := w.store.DeleteActor(ctx, actorRef, precondition)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("Actor %s does not have uid %s", actorRef, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting actor from DB: %w", err)
	}
	logActorDeleted(ctx, deleted, ateattr.OperationDelete)
	return deleted, nil
}
