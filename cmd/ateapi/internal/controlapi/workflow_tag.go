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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TagActorSnapshot tags the external snapshot held by the suspended actor the
// tag's source_actor names.
// The tag is given its own copy of that snapshot, so suspending the actor again
// or deleting the actor does not garbage collect the tag's snapshot.
//
// The tag is built in 4 phases:
//  1. Reserve the tag and record its storage location.
//  2. Copy the snapshot under the reserved tag's UID.
//  3. Snapshot the actor's external volumes, if requested.
//  4. Finalize: write the completed snapshot object to the tag.
//
// The tag captures whichever snapshot the actor holds when the workflow runs.
// An actor keeps no snapshot history, so a suspend that lands first moves what
// gets tagged; that race is inherent to naming an actor rather than a snapshot.
//
// All-or-nothing: if any phase after 1 fails, the create releases the copied
// objects and volume snapshots and drops the row before returning the error,
// so a failed create leaves no tag behind and the name is free to retry.
//
// Only a create whose process dies, or whose rollback itself fails, leaves a
// pending tag. Its name stays taken, so every later create under it is
// AlreadyExists until the tag is deleted, which collects whatever it stranded.
func (w *ActorWorkflow) TagActorSnapshot(ctx context.Context, tag *ateapipb.Tag, includeExternalVolumes bool) (_ *ateapipb.Tag, err error) {
	actorRef := resources.ActorRefFromObjectRef(tag.GetSourceActor())

	// Serializes against a suspend of the same actor, which would otherwise
	// collect the snapshot out from under the copy.
	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	// Serializes against a delete of the tag this creates, which would
	// otherwise collect the copy while it is being written.
	tagRef := resources.TagRef{Atespace: actorRef.Atespace, Name: tag.GetMetadata().GetName()}
	leaseCtx, tagLease, err := acquireTagLease(leaseCtx, w.store, tagRef)
	if err != nil {
		return nil, err
	}
	defer tagLease.Close()

	actor, actorTemplate, err := w.loadActorForTag(leaseCtx, actorRef)
	if err != nil {
		return nil, err
	}
	snapshot := actor.GetStatus().GetExternalSnapshot()

	reserved, err := w.ensureTagReserved(leaseCtx, tagRef, actor, actorTemplate, tag)
	if err != nil {
		return nil, err
	}
	// From here on the row exists, so any failure rolls the create back. The
	// deferred call runs before the leases are released.
	defer func() {
		if err == nil {
			return
		}
		cleanupCtx, cancel := cleanupContext(leaseCtx)
		defer cancel()
		if rbErr := w.rollbackTagCreate(cleanupCtx, reserved); rbErr != nil {
			slog.ErrorContext(cleanupCtx, "failed to roll back a failed tag create; delete the tag to collect what it left", slog.String("tag", tagRef.String()), slog.Any("error", rbErr))
		}
	}()

	dst, err := resources.NewTagSnapshotURI(reserved.GetStatus().GetStorageLocation(), tagRef.Atespace, reserved.GetMetadata().GetUid())
	if err != nil {
		return nil, fmt.Errorf("while building the snapshot URI for tag %s: %w", tagRef, err)
	}
	if err := w.ensureTagSnapshotCopied(leaseCtx, reserved, snapshot, dst); err != nil {
		return nil, err
	}
	snapshotted, err := w.ensureTagVolumesSnapshotted(leaseCtx, reserved, actor, includeExternalVolumes)
	if err != nil {
		return nil, err
	}
	return w.ensureTagFinalized(leaseCtx, snapshotted, snapshot, dst)
}

// DeleteTag releases the external snapshot the tag owns and then removes the
// row, in that order: the row is the only handle on that snapshot, so dropping
// it first would leak.
//
// The workflow is built in 3 phases:
//  1. Load the tag (which names the snapshot to collect).
//  2. Release that snapshot, tolerating a previous attempt partly collected.
//  3. Finalize: drop the row.
//
// Idempotent: a failure at any phase leaves the row in place, so the same
// delete run again rediscovers the work from it and resumes over whatever is
// left.
//
// The tag stays resolvable while its snapshot is being collected, so a
// CreateActor racing this delete can seed an Actor from content that is going
// away. That race is accepted for now.
//
// Note that this destroys the external snapshot: an Actor created from the tag
// and never suspended is still borrowing it and becomes unrecoverable. Do not
// delete a tag while clones of it exist.
func (w *ActorWorkflow) DeleteTag(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (*ateapipb.Tag, error) {
	// Serializes against a create of the same tag, whose copy would otherwise
	// keep writing into the prefix this is collecting.
	ctx, lease, err := acquireTagLease(ctx, w.store, tagRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	tag, err := w.loadTagForDelete(ctx, tagRef)
	if err != nil {
		return nil, err
	}
	// Checked before the snapshot is collected: a stale caller must not
	// reach that step.
	if err := precondition.Check(tag.GetMetadata()); err != nil {
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "Tag %s does not have uid %s", tagRef, precondition.UID)
		}
		return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
	}
	if err := w.ensureTagSnapshotReleased(ctx, tag); err != nil {
		return nil, err
	}
	if err := w.ensureTagVolumeSnapshotsReleased(ctx, tag); err != nil {
		return nil, err
	}
	return w.finalizeTagDeleted(ctx, tagRef, precondition)
}

// loadTagForDelete fetches the row the delete works from. The row records where
// the snapshot lives, so the work is rediscovered from it rather than rebuilt
// from the source actor, which may be long gone.
func (w *ActorWorkflow) loadTagForDelete(ctx context.Context, tagRef resources.TagRef) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "LoadTagForDelete")
	defer func() { err = done(err) }()

	tag, err := w.store.GetTag(ctx, tagRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Tag %s not found", tagRef)
		}
		return nil, fmt.Errorf("while getting tag %s: %w", tagRef, err)
	}
	return tag, nil
}

// ensureTagSnapshotReleased deletes the objects the tag's external snapshot is
// made of. It tolerates a partly-collected snapshot, so a retry finishes
// cleanly. It collects the in-progress snapshot of a pending tag too.
func (w *ActorWorkflow) ensureTagSnapshotReleased(ctx context.Context, tag *ateapipb.Tag) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseTagSnapshot")
	defer func() { err = done(err) }()

	if w.objectStore == nil {
		markSkipped(ctx, "no object store configured")
		return nil
	}
	tagRef := resources.TagRefFromTag(tag)
	uri, err := resources.NewTagSnapshotURI(tag.GetStatus().GetStorageLocation(), tagRef.Atespace, tag.GetMetadata().GetUid())
	if err != nil {
		return fmt.Errorf("while resolving the external snapshot of tag %s: %w", tagRef, err)
	}
	if err := objectstore.DeletePrefix(ctx, w.objectStore, uri.Prefix()); err != nil {
		return fmt.Errorf("while releasing the external snapshot %q of tag %s: %w", uri, tagRef, err)
	}
	return nil
}

// ensureTagVolumeSnapshotsReleased deletes the volume snapshots the tag owns.
//
// status.snapshot.volume_snapshots names them whether the tag is finalized or
// was left pending by a failed create, so a failed create's snapshots are
// collected too. Entries without a handle took no snapshot and are skipped. The
// row is dropped only after this succeeds, so a partial failure leaves every
// handle reachable for a retry.
func (w *ActorWorkflow) ensureTagVolumeSnapshotsReleased(ctx context.Context, tag *ateapipb.Tag) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseTagVolumeSnapshots")
	defer func() { err = done(err) }()

	snapshots := tag.GetStatus().GetSnapshot().GetVolumeSnapshots()
	if len(snapshots) == 0 {
		markSkipped(ctx, "tag holds no volume snapshots")
		return nil
	}
	if err := w.releaseVolumeSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("while releasing the volume snapshots of tag %s: %w", resources.TagRefFromTag(tag), err)
	}
	return nil
}

// finalizeTagDeleted drops the row, once nothing it names is left behind.
func (w *ActorWorkflow) finalizeTagDeleted(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTagDeleted")
	defer func() { err = done(err) }()

	tag, err := w.store.DeleteTag(ctx, tagRef, precondition)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Tag %s not found", tagRef)
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "Tag %s does not have uid %s", tagRef, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting tag %s: %w", tagRef, err)
	}
	return tag, nil
}

// tagCleanupTimeout bounds the cleanup a failed tag create runs after its
// caller's context is gone.
const tagCleanupTimeout = 2 * time.Minute

// cleanupContext returns a context for undoing a failed operation. It keeps
// ctx's values but not its cancellation, so a create that failed because its
// RPC was canceled or its lease was lost still cleans up after itself.
func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), tagCleanupTimeout)
}

// rollbackTagCreate undoes a create that failed after reserving its tag: it
// releases the objects copied under the tag's UID and the volume snapshots the
// row records, then drops the row. It runs the same steps as DeleteTag, so
// anything it fails to release stays reachable from the row for a later delete.
//
// The row is read again rather than taken from the caller, since it records
// the volume snapshot handles taken after it was reserved. A row that is gone,
// or that carries another UID, belongs to no part of this create and is left
// alone.
func (w *ActorWorkflow) rollbackTagCreate(ctx context.Context, reserved *ateapipb.Tag) (err error) {
	ctx, done := stepSpan(ctx, "RollbackTagCreate")
	defer func() { err = done(err) }()

	tagRef := resources.TagRefFromTag(reserved)
	tag, err := w.store.GetTag(ctx, tagRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("while getting tag %s: %w", tagRef, err)
	}
	if tag.GetMetadata().GetUid() != reserved.GetMetadata().GetUid() {
		return nil
	}
	if err := w.ensureTagSnapshotReleased(ctx, tag); err != nil {
		return err
	}
	if err := w.ensureTagVolumeSnapshotsReleased(ctx, tag); err != nil {
		return err
	}
	precondition := store.DeletePreconditions{UID: tag.GetMetadata().GetUid(), Version: tag.GetMetadata().GetVersion()}
	if _, err := w.store.DeleteTag(ctx, tagRef, precondition); err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("while deleting tag %s: %w", tagRef, err)
	}
	return nil
}

// loadActorForTag fetches the actor to tag and its template, and checks that
// the actor holds an external snapshot a tag can be made from.
func (w *ActorWorkflow) loadActorForTag(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, _ *ateapipb.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "LoadActorForTag")
	defer func() { err = done(err) }()

	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, nil, err
	}
	// Only a suspended actor's snapshot is complete. A running or
	// suspending actor's is either stale or still being written.
	if got := actor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "Actor %s must be %s to be tagged (got: %v)", actorRef, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, got)
	}
	snapshotURI := actor.GetStatus().GetExternalSnapshot().GetSnapshotUri()
	if snapshotURI == "" {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "Actor %s holds no external snapshot to tag", actorRef)
	}
	// Every way an Actor comes to hold guest state records the template that
	// state was built under: a boot through finalizeRunning, a create from a
	// tag through the tag's own UID. A snapshot without one is a broken row,
	// and tagging it would mint a tag that names no template.
	if actor.GetStatus().GetCurrentActorTemplateUid() == "" {
		return nil, nil, status.Errorf(codes.Internal, "Actor %s holds an external snapshot but records no template it was built under", actorRef)
	}
	actorTemplate, err := resolveActorTemplate(ctx, w.store, actor)
	if err != nil {
		return nil, nil, err
	}
	return actor, actorTemplate, nil
}

// ensureTagReserved takes the tag's name and records the storage location.
//
// A name already taken is AlreadyExists, whether the tag holding it is finished
// or was left pending by a create that died or could not roll back. Resuming a
// pending row would mean deciding whether the objects under it still belong to
// the snapshot being tagged, and the row may not even be this actor's; deleting
// the tag collects them and frees the name, so a retry is a delete followed by
// a create.
func (w *ActorWorkflow) ensureTagReserved(ctx context.Context, tagRef resources.TagRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, tag *ateapipb.Tag) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "ReserveTag")
	defer func() { err = done(err) }()

	location := actorTemplate.GetSnapshotConfig().GetStorageLocation()
	if err := resources.ValidateSnapshotLocation(location); err != nil {
		return nil, fmt.Errorf("invalid storage location for tag %s: %w", tagRef, err)
	}
	tagToCreate := &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: tagRef.Atespace, Name: tagRef.Name},
		Scope:       tag.GetScope(),
		SourceActor: resources.ActorRefFromActor(actor).ToObjectRef(),
		Status: &ateapipb.TagStatus{
			// The tag records the template the snapshot's guest state was built under, not
			// the one the actor currently points at. A suspended actor can be repointed,
			// and a tag that claimed the new template would hand clones the old template's
			// memory under the new one's identity, past the data-only downgrade a resume of
			// the actor itself would take.
			ActorTemplateUid: actor.GetStatus().GetCurrentActorTemplateUid(),
			StorageLocation:  location,
		},
	}

	stored, err := w.store.CreateTag(ctx, tagToCreate)
	switch {
	case err == nil:
		return stored, nil
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", tagRef.Atespace)
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, status.Errorf(codes.AlreadyExists, "Tag %s already exists; delete it and create it again to retry", tagRef)
	}
	return nil, fmt.Errorf("while reserving tag %s: %w", tagRef, err)
}

// ensureTagSnapshotCopied copies the actor's external snapshot to the tag's own
// prefix, derived from the reserved row's freshly minted UID. The prefix is
// empty by construction, so the copy never blends with another attempt's objects.
func (w *ActorWorkflow) ensureTagSnapshotCopied(ctx context.Context, tag *ateapipb.Tag, snapshot *ateapipb.ExternalSnapshot, dst resources.SnapshotURI) (err error) {
	ctx, done := stepSpan(ctx, "CopyTagSnapshot")
	defer func() { err = done(err) }()

	if w.objectStore == nil {
		markSkipped(ctx, "no object store configured")
		return nil
	}
	tagRef := resources.TagRefFromTag(tag)
	src, err := resources.ParseSnapshotURI(snapshot.GetSnapshotUri())
	if err != nil {
		return fmt.Errorf("while parsing the external snapshot %q of the source actor: %w", snapshot.GetSnapshotUri(), err)
	}
	if err := objectstore.CopyPrefix(ctx, w.objectStore, src.Prefix(), dst.Prefix()); err != nil {
		return fmt.Errorf("while copying the external snapshot for tag %s: %w", tagRef, err)
	}
	return nil
}

// ensureTagVolumesSnapshotted captures the source actor's external volumes and
// records them on the pending tag's status.snapshot.volume_snapshots.
//
// Every volume is checked before any is recorded or snapshotted, so a volume
// that cannot be captured fails the call without touching the storage system.
// The full list of volumes to capture is then recorded, each entry with an
// empty storage_snapshot_id, and each entry's handle is filled in as its
// snapshot is taken. An entry still without a handle therefore names a volume
// whose snapshot creation did not finish.
//
// Capture is all-or-nothing: a clone that silently came up with one empty disk
// would be worse than a failed create. Any failure fails the create, and the
// caller's rollback releases every snapshot the tag records. A handle that was
// taken but could not be recorded is released here, since the tag cannot name
// it.
//
// It does not wait for the storage system to finish copying. A driver may
// return a handle with ready_to_use false and finish in the background, and
// blocking here would put an unbounded storage operation inside a synchronous
// RPC. Readiness is checked at CreateActor instead, which is the first point
// the data is actually needed.
//
// No quiesce step is required. A SUSPENDED actor has already had its volumes
// unmounted by atelet and detached by the control plane, so the filesystem is
// cleanly unmounted with no dirty page cache, and the actor lease held by the
// caller keeps a concurrent resume from re-attaching them mid-capture.
func (w *ActorWorkflow) ensureTagVolumesSnapshotted(ctx context.Context, tag *ateapipb.Tag, actor *ateapipb.Actor, includeExternalVolumes bool) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "SnapshotVolumes")
	defer func() { err = done(err) }()

	if !includeExternalVolumes {
		markSkipped(ctx, "external volumes not requested")
		return tag, nil
	}
	// status.actor_volumes rather than the mounted subset: a template may
	// declare a volume no container mounts, and leaving it out would produce a
	// tag missing a volume.
	volumes := actor.GetStatus().GetActorVolumes()
	if len(volumes) == 0 {
		markSkipped(ctx, "actor has no external volumes")
		return tag, nil
	}

	plugins, err := w.tagVolumeSnapshotPlugins(ctx, actor, volumes)
	if err != nil {
		return nil, err
	}

	tagRef := resources.TagRefFromTag(tag)
	tag, err = w.recordTagVolumeSnapshots(ctx, tag, volumes)
	if err != nil {
		return nil, err
	}

	for i, vol := range volumes {
		volName := vol.GetVolumeName()
		// CSI CreateSnapshot is idempotent on (name, source volume), so a retry
		// within one call returns the snapshot the previous attempt made. The
		// tag UID keeps two attempts under the same tag name apart.
		snap, snapErr := plugins[i].CreateSnapshot(ctx, volume.CreateSnapshotRequest{
			Name:           tagVolumeSnapshotID(tag.GetMetadata().GetUid(), volName),
			SourceVolumeID: vol.GetStorageVolumeId(),
		})
		if snapErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to snapshot volume %q: %v", volName, snapErr)
		}
		if snap.SnapshotID == "" {
			return nil, status.Errorf(codes.Internal, "driver %q returned no snapshot handle for volume %q", vol.GetVolumeType(), volName)
		}

		// Record the handle before taking the next one. A create that dies here
		// leaves every snapshot it made named by the pending tag, so deleting
		// the tag collects them.
		updated, updErr := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
			entries := toUpdate.GetStatus().GetSnapshot().GetVolumeSnapshots()
			if i >= len(entries) || entries[i].GetVolumeName() != volName {
				return fmt.Errorf("tag %s does not record volume %q at index %d", tagRef, volName, i)
			}
			entry := entries[i]
			entry.StorageSnapshotId = snap.SnapshotID
			entry.ReadyToUse = snap.ReadyToUse
			entry.SizeBytes = snap.SizeBytes
			if !snap.CreationTime.IsZero() {
				entry.CreationTime = timestamppb.New(snap.CreationTime)
			}
			return nil
		})
		if updErr != nil {
			// The tag does not name this snapshot, so the caller's rollback
			// cannot find it; release it here.
			unrecorded := []*ateapipb.ExternalVolumeSnapshot{{VolumeName: volName, StorageSnapshotId: snap.SnapshotID, VolumeType: vol.GetVolumeType()}}
			cleanupCtx, cancel := cleanupContext(ctx)
			if rbErr := w.releaseVolumeSnapshots(cleanupCtx, unrecorded); rbErr != nil {
				slog.ErrorContext(cleanupCtx, "failed to release a volume snapshot the tag does not record; it is leaked", slog.String("tag", tagRef.String()), slog.String("snapshot", snap.SnapshotID), slog.Any("error", rbErr))
			}
			cancel()
			return nil, fmt.Errorf("while recording the volume snapshot of %q on tag %s: %w", volName, tagRef, updErr)
		}
		tag = updated
	}
	return tag, nil
}

// tagVolumeSnapshotPlugins checks that every volume can be snapshotted and
// returns the plugin to snapshot each one with, indexed like volumes.
func (w *ActorWorkflow) tagVolumeSnapshotPlugins(ctx context.Context, actor *ateapipb.Actor, volumes []*ateapipb.ExternalVolume) ([]volume.VolumePluginControlPlane, error) {
	plugins := make([]volume.VolumePluginControlPlane, 0, len(volumes))
	for _, vol := range volumes {
		volName := vol.GetVolumeName()
		if vol.GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED || vol.GetStorageVolumeId() == "" {
			return nil, status.Errorf(codes.FailedPrecondition, "cannot snapshot volume %q of Actor %s: it has not been provisioned", volName, resources.ActorRefFromActor(actor))
		}
		caps, err := w.pluginRegistry.GetCapabilities(ctx, vol.GetVolumeType())
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "failed to read capabilities of driver %q for volume %q: %v", vol.GetVolumeType(), volName, err)
		}
		if !caps.CreateDeleteSnapshot {
			return nil, status.Errorf(codes.FailedPrecondition, "volume %q uses driver %q, which does not support snapshots", volName, vol.GetVolumeType())
		}
		plugin, err := w.pluginRegistry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "failed to get volume plugin for driver %q: %v", vol.GetVolumeType(), err)
		}
		plugins = append(plugins, plugin)
	}
	return plugins, nil
}

// recordTagVolumeSnapshots records on the pending tag the volumes it is about
// to capture, each without a handle yet. Recording them before any snapshot is
// taken is what lets a reader tell "not requested" (no entry) from "requested
// and not finished" (an entry without a handle).
func (w *ActorWorkflow) recordTagVolumeSnapshots(ctx context.Context, tag *ateapipb.Tag, volumes []*ateapipb.ExternalVolume) (*ateapipb.Tag, error) {
	tagRef := resources.TagRefFromTag(tag)
	entries := make([]*ateapipb.ExternalVolumeSnapshot, 0, len(volumes))
	for _, vol := range volumes {
		entries = append(entries, &ateapipb.ExternalVolumeSnapshot{
			VolumeName: vol.GetVolumeName(),
			VolumeType: vol.GetVolumeType(),
		})
	}
	updated, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
		if toUpdate.GetStatus().GetSnapshot() != nil {
			return fmt.Errorf("tag %s already records a snapshot", tagRef)
		}
		toUpdate.Status.Snapshot = &ateapipb.ExternalSnapshot{VolumeSnapshots: entries}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("while recording the volumes to snapshot on tag %s: %w", tagRef, err)
	}
	return updated, nil
}

// releaseVolumeSnapshots deletes a set of volume snapshots, tolerating ones
// already gone and joining the failures so one bad handle does not strand the
// rest. Entries without a handle took no snapshot and are skipped.
func (w *ActorWorkflow) releaseVolumeSnapshots(ctx context.Context, snapshots []*ateapipb.ExternalVolumeSnapshot) error {
	var errs []error
	for _, snap := range snapshots {
		if snap.GetStorageSnapshotId() == "" {
			continue
		}
		plugin, err := w.pluginRegistry.GetPlugin(ctx, snap.GetVolumeType())
		if err != nil {
			errs = append(errs, fmt.Errorf("while getting volume plugin for driver %q: %w", snap.GetVolumeType(), err))
			continue
		}
		if err := plugin.DeleteSnapshot(ctx, snap.GetStorageSnapshotId()); err != nil {
			errs = append(errs, fmt.Errorf("while deleting volume snapshot %q: %w", snap.GetStorageSnapshotId(), err))
		}
	}
	return errors.Join(errs...)
}

// ensureTagFinalized publishes the copy by setting status.snapshot.snapshot_uri.
// Until this lands the tag is pending and unusable; deleting it collects any
// partial copy.
//
// The volume snapshots recorded by ensureTagVolumesSnapshotted are kept as is.
// status.snapshot is immutable once snapshot_uri is set, so a tag can never
// come to name a different set of volumes than the one it published.
func (w *ActorWorkflow) ensureTagFinalized(ctx context.Context, tag *ateapipb.Tag, snapshot *ateapipb.ExternalSnapshot, dst resources.SnapshotURI) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTag")
	defer func() { err = done(err) }()

	tagRef := resources.TagRefFromTag(tag)
	stored, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
		if toUpdate.Status.Snapshot == nil {
			toUpdate.Status.Snapshot = &ateapipb.ExternalSnapshot{}
		}
		toUpdate.Status.Snapshot.SnapshotUri = dst.String()
		// The copy is byte-identical to the source, so it carries the same
		// content.
		toUpdate.Status.Snapshot.ContentScope = snapshot.GetContentScope()
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "Tag %s was deleted while it was being created, please retry", tagRef)
		}
		return nil, fmt.Errorf("while finalizing tag %s: %w", tagRef, err)
	}
	return stored, nil
}
