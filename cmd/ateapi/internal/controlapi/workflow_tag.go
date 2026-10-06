// Copyright 2026 The Agent Substrate Authors
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
	"slices"
	"sync"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TagActorSnapshot tags the external snapshot held by the suspended actor the
// tag's source_actor names.
// The tag is given its own copy of that snapshot, so suspending the actor again
// or deleting the actor does not garbage collect the tag's snapshot.
//
// The tag is built in 7 phases:
//  1. Check that the name is free and the storage location is usable.
//  2. Make the actor's latest snapshot durable, if it is not uploaded yet: an
//     asynchronously suspended actor's snapshot may still be uploading.
//  3. Reserve the tag's name; the store assigns its UID.
//  4. Record the snapshot's URI, under the tag's UID, before anything is
//     copied there.
//  5. Copy the snapshot to that URI.
//  6. Snapshot the requested external volumes: none, the named ones, or all.
//  7. Finalize: mark the tag's snapshot COMPLETED.
//
// The tag captures whichever snapshot the actor holds when the workflow runs.
// An actor keeps no snapshot history, so a suspend that lands first moves what
// gets tagged; that race is inherent to naming an actor rather than a snapshot.
//
// Not idempotent: the name is taken as soon as phase 3 lands, so a create that
// dies after it leaves a pending tag and every later create under that name is
// AlreadyExists. To retry, delete the tag, which collects whatever the failed
// attempt stranded, and create it again.
func (w *ActorWorkflow) TagActorSnapshot(ctx context.Context, tag *ateapi.Tag, externalVolumes *ateapi.ExternalVolumeSelection) (*ateapi.Tag, error) {
	ctx = preview.ContextWithOptIn(ctx)
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
	// Resolved before anything else, so a volume the actor does not have or
	// cannot snapshot fails the call before anything is uploaded or copied.
	volumes, err := selectTagVolumes(actor, externalVolumes)
	if err != nil {
		return nil, err
	}
	plugins, err := w.tagVolumeSnapshotPlugins(leaseCtx, actor, volumes)
	if err != nil {
		return nil, err
	}
	// Runs before the upload, so a create that cannot succeed fails fast.
	if err := w.checkTagCreatable(leaseCtx, tagRef, actorTemplate); err != nil {
		return nil, err
	}
	// Runs before the name is taken, so a failed upload leaves no pending tag.
	actor, err = w.ensureTagSourceSnapshotDurable(leaseCtx, actorRef, actor)
	if err != nil {
		return nil, err
	}
	snapshot, err := sourceSnapshotForTag(actorRef, actor)
	if err != nil {
		return nil, err
	}

	reserved, err := w.ensureTagReserved(leaseCtx, tagRef, actor, snapshot, tag)
	if err != nil {
		return nil, err
	}
	recorded, dst, err := w.ensureTagSnapshotRecorded(leaseCtx, reserved, actorTemplate, snapshot)
	if err != nil {
		return nil, err
	}
	if err := w.ensureTagSnapshotCopied(leaseCtx, recorded, snapshot, dst); err != nil {
		return nil, err
	}
	snapshotted, err := w.ensureTagVolumesSnapshotted(leaseCtx, recorded, volumes, plugins)
	if err != nil {
		return nil, err
	}
	return w.ensureTagFinalized(leaseCtx, snapshotted, snapshot, dst)
}

// selectTagVolumes returns the source actor's external volumes a tag captures:
// none when sel is unset, then the ones it names, or all of them when it names
// none. The result keeps the actor's volume order.
//
// All volumes means status.external_volumes rather than the mounted subset: a
// template may declare a volume no container mounts, and leaving it out would
// produce a tag missing a volume.
func selectTagVolumes(actor *ateapi.Actor, sel *ateapi.ExternalVolumeSelection) ([]*ateapi.ExternalVolumeStatus, error) {
	if sel == nil {
		return nil, nil
	}
	names := sel.GetNames()
	volumes := actor.GetStatus().GetExternalVolumes().GetVolumes()
	if len(names) == 0 {
		return volumes, nil
	}
	requested := make(map[string]bool, len(names))
	for _, name := range names {
		requested[name] = true
	}
	var selected []*ateapi.ExternalVolumeStatus
	for _, vol := range volumes {
		if requested[vol.GetName()] {
			selected = append(selected, vol)
			delete(requested, vol.GetName())
		}
	}
	if len(requested) > 0 {
		var unknown []string
		for _, name := range names {
			if requested[name] {
				unknown = append(unknown, name)
			}
		}
		return nil, apierror.InvalidArgument("Actor %s has no external volumes named %q", resources.ActorRefFromActor(actor), unknown)
	}
	return selected, nil
}

// DeleteTag releases the external snapshot and volume snapshots the tag owns
// and then removes the row, in that order: the row is the only handle on them,
// so dropping it first would leak.
//
// The workflow is built in 4 phases:
//  1. Load the tag (which names what to collect).
//  2. Release that snapshot, tolerating a previous attempt partly collected.
//  3. Release the volume snapshots, recovering handles a failed create never
//     recorded.
//  4. Finalize: drop the row.
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
func (w *ActorWorkflow) DeleteTag(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (*ateapi.Tag, error) {
	ctx = preview.ContextWithOptIn(ctx)
	// Serializes against a create of the same tag, whose copy would otherwise
	// keep writing into the prefix this is collecting.
	ctx, lease, err := acquireTagLease(ctx, w.store, tagRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	tag, err := w.ensureTagMarkedForDeletion(ctx, tagRef, precondition)
	if err != nil {
		return nil, err
	}
	if err := w.ensureTagSnapshotReleased(ctx, tag); err != nil {
		return nil, err
	}
	if err := w.ensureTagVolumeSnapshotsReleased(ctx, tag); err != nil {
		return nil, err
	}
	return w.finalizeTagDeleted(ctx, tag)
}

// ensureTagMarkedForDeletion sets the tag's delete_time, checking precondition,
// and returns the stored copy. Skips when a previous attempt already marked the
// tag.
func (w *ActorWorkflow) ensureTagMarkedForDeletion(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (_ *ateapi.Tag, err error) {
	ctx, done := stepSpan(ctx, "MarkTagForDeletion")
	defer func() { err = done(err) }()

	var alreadyDeleting *ateapi.Tag
	marked, err := w.store.MarkTagForDeletion(ctx, tagRef, precondition, func(toUpdate *ateapi.Tag) error {
		if toUpdate.GetMetadata().GetDeleteTime() != nil {
			alreadyDeleting = toUpdate
			return errAlreadyMarked
		}
		return nil
	})
	if errors.Is(err, errAlreadyMarked) {
		markSkipped(ctx, "tag already marked for deletion")
		return alreadyDeleting, nil
	}
	if err != nil {
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("Tag %s does not have uid %s", tagRef, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Tag %s not found", tagRef)
		}
		return nil, fmt.Errorf("while marking tag %s for deletion: %w", tagRef, err)
	}
	return marked, nil
}

// ensureTagSnapshotReleased deletes the objects the tag's external snapshot is
// made of. It tolerates a partly-collected snapshot, so a retry finishes
// cleanly. It collects the in-progress snapshot of a pending tag too, and
// skips a tag whose create died before recording its snapshot: nothing is
// copied until the URI is recorded, so such a tag stranded nothing.
func (w *ActorWorkflow) ensureTagSnapshotReleased(ctx context.Context, tag *ateapi.Tag) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseTagSnapshot")
	defer func() { err = done(err) }()

	if w.objectStorePlugin == nil {
		markSkipped(ctx, "no object store configured")
		return nil
	}
	tagRef := resources.TagRefFromTag(tag)
	snapshotURI := tagDurableSnapshotURI(tag, ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_UNSPECIFIED)
	if snapshotURI == "" {
		markSkipped(ctx, "the tag records no snapshot")
		return nil
	}
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return fmt.Errorf("while resolving the external snapshot of tag %s: %w", tagRef, err)
	}
	if err := w.cleanupSnapshot(ctx, uri.Prefix()); err != nil {
		return fmt.Errorf("while releasing the external snapshot %q of tag %s: %w", uri, tagRef, err)
	}
	return nil
}

// ensureTagVolumeSnapshotsReleased deletes the volume snapshots the tag owns.
//
// status.snapshot.volume_snapshots names them whether the tag is finalized or
// was left behind by a failed create, so a failed create's snapshots are
// collected too. An entry without a handle may still name a snapshot that was
// cut but never recorded; its handle is recovered first (see
// recoverVolumeSnapshotHandle). The row is dropped only after this succeeds, so
// a partial failure leaves every handle reachable for a retry.
func (w *ActorWorkflow) ensureTagVolumeSnapshotsReleased(ctx context.Context, tag *ateapi.Tag) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseTagVolumeSnapshots")
	defer func() { err = done(err) }()

	snapshots := tag.GetStatus().GetSnapshot().GetVolumeSnapshots()
	if len(snapshots) == 0 {
		markSkipped(ctx, "tag holds no volume snapshots")
		return nil
	}
	tagRef := resources.TagRefFromTag(tag)
	var errs []error
	toRelease := make([]*ateapi.ExternalVolumeSnapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		if snap.GetStorageSnapshotId() != "" {
			toRelease = append(toRelease, snap)
			continue
		}
		recovered, err := w.recoverVolumeSnapshotHandle(ctx, tag.GetMetadata().GetUid(), snap)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if recovered != nil {
			toRelease = append(toRelease, recovered)
		}
	}
	if err := w.releaseVolumeSnapshots(ctx, toRelease); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("while releasing the volume snapshots of tag %s: %w", tagRef, err)
	}
	return nil
}

// recoverVolumeSnapshotHandle finds the handle of a volume snapshot the tag
// records without one. Such an entry names either a snapshot that failed or one
// the storage system cut whose handle a failed or crashed create never wrote
// back. CreateSnapshot is idempotent on the snapshot name and source volume, so
// issuing the create's request again returns the handle of a snapshot that
// exists. When none does, this cuts one that the caller then deletes.
//
// The storage system cannot look a snapshot up by name alone, so once the
// source volume is gone a snapshot cut from it can no longer be found. That
// snapshot is leaked: it is logged and skipped, returning nil, so the tag can
// still be deleted.
func (w *ActorWorkflow) recoverVolumeSnapshotHandle(ctx context.Context, tagUID string, snap *ateapi.ExternalVolumeSnapshot) (*ateapi.ExternalVolumeSnapshot, error) {
	volName := snap.GetSourceVolumeName()
	plugin, err := w.pluginRegistry.GetPlugin(ctx, snap.GetVolumeType())
	if err != nil {
		return nil, fmt.Errorf("while getting volume plugin for driver %q: %w", snap.GetVolumeType(), err)
	}
	name := tagVolumeSnapshotID(tagUID, volName)
	got, err := plugin.CreateSnapshot(ctx, volume.CreateSnapshotRequest{
		Name:           name,
		SourceVolumeID: snap.GetSourceVolumeId(),
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			slog.WarnContext(ctx, "source volume is gone, so a volume snapshot the tag never recorded cannot be found; it may be leaked",
				slog.String("snapshot_name", name), slog.String("source_volume_id", snap.GetSourceVolumeId()), slog.Any("error", err))
			return nil, nil
		}
		return nil, fmt.Errorf("while recovering the snapshot of volume %q: %w", volName, err)
	}
	if got.SnapshotID == "" {
		return nil, fmt.Errorf("driver %q returned no snapshot handle while recovering the snapshot of volume %q", snap.GetVolumeType(), volName)
	}
	return &ateapi.ExternalVolumeSnapshot{
		SourceVolumeName:  volName,
		SourceVolumeId:    snap.GetSourceVolumeId(),
		StorageSnapshotId: got.SnapshotID,
		VolumeType:        snap.GetVolumeType(),
	}, nil
}

// finalizeTagDeleted drops the row, once nothing it names is left behind.
func (w *ActorWorkflow) finalizeTagDeleted(ctx context.Context, tag *ateapi.Tag) (_ *ateapi.Tag, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTagDeleted")
	defer func() { err = done(err) }()

	tagRef := resources.TagRefFromTag(tag)
	precondition := store.DeletePreconditions{UID: tag.GetMetadata().GetUid()}
	tag, err = w.store.DeleteTag(ctx, tagRef, precondition)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Tag %s not found", tagRef)
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("Tag %s does not have uid %s", tagRef, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting tag %s: %w", tagRef, err)
	}
	return tag, nil
}

// loadActorForTag fetches the actor to tag and its template, and checks that
// the actor is suspended.
func (w *ActorWorkflow) loadActorForTag(ctx context.Context, actorRef resources.ActorRef) (_ *ateapi.Actor, _ *ateapi.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "LoadActorForTag")
	defer func() { err = done(err) }()

	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, nil, err
	}
	// Only a suspended actor's snapshot is complete. A running or
	// suspending actor's is either stale or still being written.
	if got := actor.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		return nil, nil, apierror.FailedPrecondition("Actor %s must be %s to be tagged (got: %v)", actorRef, ateapi.ActorState_ACTOR_STATE_SUSPENDED, got)
	}
	actorTemplate, err := resolveActorTemplate(ctx, w.store, actor)
	if err != nil {
		return nil, nil, err
	}
	return actor, actorTemplate, nil
}

// checkTagCreatable fails a create that cannot succeed before the snapshot is
// uploaded: a name already taken, or a storage location the copy cannot use.
// The tag lease is held, so no other create or delete of the name races the
// lookup.
func (w *ActorWorkflow) checkTagCreatable(ctx context.Context, tagRef resources.TagRef, actorTemplate *ateapi.ActorTemplate) (err error) {
	ctx, done := stepSpan(ctx, "CheckTagCreatable")
	defer func() { err = done(err) }()

	_, err = w.store.GetTag(ctx, tagRef)
	switch {
	case err == nil:
		return apierror.AlreadyExists("Tag %s already exists; delete it and create it again to retry", tagRef)
	case !errors.Is(err, store.ErrNotFound):
		return fmt.Errorf("while getting tag %s: %w", tagRef, err)
	}
	location, err := w.snapshotLocation(actorTemplate)
	if err != nil {
		return err
	}
	if err := resources.ValidateSnapshotLocation(location); err != nil {
		return apierror.FailedPrecondition("invalid storage location for tag %s: %w", tagRef, err)
	}
	return nil
}

// ensureTagSourceSnapshotDurable makes the actor's latest snapshot durable when
// it is not uploaded yet, the shape an asynchronous suspend leaves until
// atelet's background upload is recorded. It asks the atelet on the snapshot's
// node to upload it with PromoteSnapshot, which keeps the local copy so the
// actor still resumes on that node, and records the upload on the actor. It
// returns the actor as stored afterwards.
//
// A failed upload does not crash the actor: the tag fails and the actor is
// left as it was.
func (w *ActorWorkflow) ensureTagSourceSnapshotDurable(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor) (_ *ateapi.Actor, err error) {
	ctx, done := stepSpan(ctx, "PromoteTagSourceSnapshot")
	defer func() { err = done(err) }()

	snap := latestSnapshot(actor.GetStatus(), func(*ateapi.Snapshot) bool { return true })
	if snap.GetDurableSnapshot().GetState() == ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED {
		markSkipped(ctx, "the actor's latest snapshot is already durable")
		return actor, nil
	}
	// For example a PENDING upload whose node was lost. The tag falls back to
	// the latest durable snapshot, the one the actor would resume from.
	if snap.GetLocality() == "" {
		markSkipped(ctx, "the actor's latest snapshot is on no node to upload from")
		return actor, nil
	}
	snapshotURI := snap.GetDurableSnapshot().GetObject().GetSnapshotUri()
	if snapshotURI == "" {
		return nil, apierror.Internal("Actor %s holds a node-local snapshot at generation %d but records no URI to upload it to", actorRef, snap.GetGeneration())
	}
	// The uploaded snapshot is the one the tag copies, so a snapshot that
	// records no template fails before the upload rather than after it.
	if snap.GetActorTemplateUid() == "" {
		return nil, apierror.Internal("Actor %s holds a snapshot at generation %d but records no template it was built under", actorRef, snap.GetGeneration())
	}

	ateletConn, err := w.dialer.DialForAteletOnNode(snap.GetLocality())
	if err != nil {
		return nil, apierror.Unavailable("while getting atelet conn for node %q: %w", snap.GetLocality(), err)
	}
	// The fidelity is the one recorded in the snapshot, not the current
	// template's: a suspended actor can be repointed, and atelet requires the
	// fidelity its manifest records.
	resp, err := ateletpb.NewAteletClient(ateletConn).PromoteSnapshot(ctx, &ateletpb.PromoteSnapshotRequest{
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorUid:              actor.GetMetadata().GetUid(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: snap.GetUuid(),
			Generation:   snap.GetGeneration(),
			Fidelity:     fidelityToAtelet(snap.GetFidelity()),
			Object:       &ateletpb.ObjectStorage{SnapshotUri: snapshotURI},
		},
	})
	if err != nil {
		return nil, promoteSnapshotError(ctx, err)
	}

	// The reported durable snapshot must be the one requested.
	reported := resp.GetState().GetLatestDurable().GetSnapshot()
	if reported.GetSnapshotUuid() != snap.GetUuid() || reported.GetGeneration() != snap.GetGeneration() {
		return nil, apierror.Internal("atelet on node %q was asked to promote Actor %s's snapshot %s at generation %d, but reported %q at generation %d durable",
			snap.GetLocality(), actorRef, snap.GetUuid(), snap.GetGeneration(), reported.GetSnapshotUuid(), reported.GetGeneration())
	}
	// The actor lease is held for the whole tag, so the actor cannot be resumed
	// or suspended meanwhile and snap is still its newest snapshot here.
	applied := proto.CloneOf(actor.GetStatus())
	// The request was valid, so a state that conflicts with the record is
	// atelet breaking its contract.
	if err := applyAteletResponseState(ctx, applied, resp.GetState()); err != nil {
		return nil, apierror.Internal("atelet on node %q reported a state for Actor %s that conflicts with its record: %w", snap.GetLocality(), actorRef, err)
	}
	stored, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapi.Actor) error {
		// The precondition makes toUpdate the version applied was built from.
		toUpdate.Status.Snapshots = applied.GetSnapshots()
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while recording the upload of Actor %s's latest snapshot: %w", actorRef, err)
	}
	return stored, nil
}

// promoteSnapshotError maps an error from atelet's PromoteSnapshot to the
// error the tag fails with.
func promoteSnapshotError(ctx context.Context, err error) error {
	// The caller went away or the lease was lost: report that, not atelet's error.
	if ctx.Err() != nil {
		return fmt.Errorf("while calling atelet PromoteSnapshot: %w: %w", err, ctx.Err())
	}
	switch status.Code(err) {
	case codes.Aborted:
		return apierror.Aborted("while calling atelet PromoteSnapshot: %w", err)
	case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
		return apierror.Unavailable("while calling atelet PromoteSnapshot: %w", err)
	// NotFound: the local snapshot is gone and no uploaded copy exists.
	case codes.FailedPrecondition, codes.NotFound:
		return apierror.FailedPrecondition("while calling atelet PromoteSnapshot: %w", err)
	}
	return apierror.Internal("while calling atelet PromoteSnapshot: %w", err)
}

// sourceSnapshotForTag returns the actor's latest COMPLETED external snapshot,
// the one a tag copies.
func sourceSnapshotForTag(actorRef resources.ActorRef, actor *ateapi.Actor) (*ateapi.Snapshot, error) {
	snap := findLatestDurableSnapshot(actor.GetStatus(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED)
	if snap == nil {
		return nil, apierror.FailedPrecondition("Actor %s holds no external snapshot to tag", actorRef)
	}
	// Every way an Actor comes to hold an external snapshot records the
	// template its guest state was built under: a suspend through
	// ensureSuspendedFinalized, a create from a tag through the tag's own UID.
	// A snapshot without one is a broken row, and tagging it would mint a tag
	// that names no template.
	if snap.GetActorTemplateUid() == "" {
		return nil, apierror.Internal("Actor %s holds an external snapshot but records no template it was built under", actorRef)
	}
	return snap, nil
}

// ensureTagReserved takes the tag's name. The store assigns the tag's UID on
// create, and the UID keys the prefix the snapshot is copied to, so the
// snapshot is recorded by the next step.
//
// A name already taken is AlreadyExists, whether the tag holding it is finished
// or was left pending by a create that died. Resuming a pending row would mean
// deciding whether the objects under it still belong to the snapshot being
// tagged, and the row may not even be this actor's; deleting the tag collects
// them and frees the name, so a retry is a delete followed by a create.
func (w *ActorWorkflow) ensureTagReserved(ctx context.Context, tagRef resources.TagRef, actor *ateapi.Actor, snapshot *ateapi.Snapshot, tag *ateapi.Tag) (_ *ateapi.Tag, err error) {
	ctx, done := stepSpan(ctx, "ReserveTag")
	defer func() { err = done(err) }()

	tagToCreate := &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: tagRef.Atespace, Name: tagRef.Name},
		Scope:       tag.GetScope(),
		SourceActor: resources.ActorRefFromActor(actor).ToObjectRef(),
		Status: &ateapi.TagStatus{
			// The tag records the template the snapshot's guest state was built under, not
			// the one the actor currently points at. A suspended actor can be repointed,
			// and a tag that claimed the new template would hand clones the old template's
			// memory under the new one's identity, past the data-only downgrade a resume of
			// the actor itself would take.
			ActorTemplateUid: snapshot.GetActorTemplateUid(),
		},
	}

	stored, err := w.store.CreateTag(ctx, tagToCreate)
	switch {
	case err == nil:
		return stored, nil
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, apierror.FailedPrecondition("Atespace %s not found", tagRef.Atespace)
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, apierror.AlreadyExists("Tag %s already exists; delete it and create it again to retry", tagRef)
	}
	return nil, fmt.Errorf("while reserving tag %s: %w", tagRef, err)
}

// ensureTagSnapshotRecorded records the snapshot the tag will hold as PENDING:
// a copy of snapshot under the tag's own prefix, keyed on the UID the store
// assigned when the tag was reserved. It lands before anything is copied, so a
// tag whose create fails later still names the objects it stranded.
func (w *ActorWorkflow) ensureTagSnapshotRecorded(ctx context.Context, tag *ateapi.Tag, actorTemplate *ateapi.ActorTemplate, snapshot *ateapi.Snapshot) (_ *ateapi.Tag, _ resources.SnapshotURI, err error) {
	ctx, done := stepSpan(ctx, "RecordTagSnapshot")
	defer func() { err = done(err) }()

	tagRef := resources.TagRefFromTag(tag)
	location, err := w.snapshotLocation(actorTemplate)
	if err != nil {
		return nil, resources.SnapshotURI{}, err
	}
	dst, err := resources.NewTagSnapshotURI(location, tagRef.Atespace, tag.GetMetadata().GetUid())
	if err != nil {
		return nil, resources.SnapshotURI{}, fmt.Errorf("while building the snapshot URI for tag %s: %w", tagRef, err)
	}
	pending := newDurableSnapshot(
		1,
		ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG,
		snapshot.GetFidelity(),
		"",
		dst.Name(),
		dst.String(),
		ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING,
	)
	pending.SandboxRuntime = proto.CloneOf(snapshot.GetSandboxRuntime())
	stored, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapi.Tag) error {
		toUpdate.Status.Snapshot = pending
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, resources.SnapshotURI{}, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict) {
			return nil, resources.SnapshotURI{}, apierror.Aborted("Tag %s was deleted while it was being created, please retry", tagRef)
		}
		return nil, resources.SnapshotURI{}, fmt.Errorf("while recording the snapshot of tag %s: %w", tagRef, err)
	}
	return stored, dst, nil
}

// ensureTagSnapshotCopied copies the actor's external snapshot to the tag's own
// prefix. The prefix is empty by construction, so the copy never blends with
// another attempt's objects.
func (w *ActorWorkflow) ensureTagSnapshotCopied(ctx context.Context, tag *ateapi.Tag, snapshot *ateapi.Snapshot, dst resources.SnapshotURI) (err error) {
	ctx, done := stepSpan(ctx, "CopyTagSnapshot")
	defer func() { err = done(err) }()

	if w.objectStorePlugin == nil {
		markSkipped(ctx, "no object store configured")
		return nil
	}
	tagRef := resources.TagRefFromTag(tag)
	srcURI := snapshot.GetDurableSnapshot().GetObject().GetSnapshotUri()
	src, err := resources.ParseSnapshotURI(srcURI)
	if err != nil {
		return fmt.Errorf("while parsing the external snapshot %q of the source actor: %w", srcURI, err)
	}
	if err := w.copySnapshot(ctx, src.Prefix(), dst.Prefix()); err != nil {
		return fmt.Errorf("while copying the external snapshot for tag %s: %w", tagRef, err)
	}
	return nil
}

// ensureTagVolumesSnapshotted captures the given external volumes of the
// source actor, as picked by selectTagVolumes, with the plugins
// tagVolumeSnapshotPlugins returned, and records them on the pending tag's
// status.snapshot.volume_snapshots. Volumes left out are not recorded.
//
// The full list of volumes to capture is recorded first, each entry with its
// source volume ID and an empty storage_snapshot_id. The snapshots are taken
// concurrently, and every handle obtained is filled in with a single write
// once all of them return, so the store sees two writes however many volumes
// there are.
//
// An entry left without a handle names a snapshot that failed, or one that was
// cut but whose handle was never recorded because the create died or the write
// failed. Deleting the tag recovers the latter from the entry's source volume
// ID, so nothing is released here.
//
// Capture is all-or-nothing: a clone that silently came up with one empty disk
// would be worse than a failed create. Any failure fails the create and leaves
// the pending tag for DeleteTag. The handles of volumes that did succeed are
// recorded before the failure is returned.
//
// It does not wait for the storage system to finish copying. A driver may
// return a handle with ready_to_use false and finish in the background, and
// blocking here would put an unbounded storage operation inside a synchronous
// RPC. Readiness is checked instead when an Actor seeded from the tag
// provisions its volumes, which is the first point the data is actually needed.
//
// No quiesce step is required. A SUSPENDED actor has already had its volumes
// unmounted by atelet and detached by the control plane, so the filesystem is
// cleanly unmounted with no dirty page cache, and the actor lease held by the
// caller keeps a concurrent resume from re-attaching them mid-capture.
func (w *ActorWorkflow) ensureTagVolumesSnapshotted(ctx context.Context, tag *ateapi.Tag, volumes []*ateapi.ExternalVolumeStatus, plugins []volume.VolumePluginControlPlane) (_ *ateapi.Tag, err error) {
	ctx, done := stepSpan(ctx, "SnapshotVolumes")
	defer func() { err = done(err) }()

	if len(volumes) == 0 {
		markSkipped(ctx, "no external volumes to capture")
		return tag, nil
	}

	tagRef := resources.TagRefFromTag(tag)
	tag, err = w.recordTagVolumeSnapshots(ctx, tag, volumes)
	if err != nil {
		return nil, err
	}

	// Siblings are not canceled when one fails: a canceled call may still
	// have cut a snapshot whose handle would never come back, so each runs to
	// completion and reports its result.
	snaps := make([]volume.Snapshot, len(volumes))
	snapErrs := make([]error, len(volumes))
	var wg sync.WaitGroup
	for i, vol := range volumes {
		wg.Go(func() {
			snaps[i], snapErrs[i] = createTagVolumeSnapshot(ctx, plugins[i], tag.GetMetadata().GetUid(), vol)
		})
	}
	wg.Wait()

	if slices.ContainsFunc(snapErrs, func(err error) bool { return err == nil }) {
		updated, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapi.Tag) error {
			entries := toUpdate.GetStatus().GetSnapshot().GetVolumeSnapshots()
			for i, vol := range volumes {
				if snapErrs[i] != nil {
					continue
				}
				volName := vol.GetName()
				if i >= len(entries) || entries[i].GetSourceVolumeName() != volName {
					return fmt.Errorf("tag %s does not record volume %q at index %d", tagRef, volName, i)
				}
				snap := snaps[i]
				entry := entries[i]
				entry.StorageSnapshotId = snap.SnapshotID
				entry.ReadyToUse = snap.ReadyToUse
				entry.SizeBytes = snap.SizeBytes
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("while recording the volume snapshots on tag %s: %w", tagRef, err)
		}
		tag = updated
	}
	if err := errors.Join(snapErrs...); err != nil {
		return nil, apierror.Internal("%v", err)
	}
	return tag, nil
}

// createTagVolumeSnapshot snapshots one volume under the name the tag derives
// for it, failing if the driver returns no handle.
func createTagVolumeSnapshot(ctx context.Context, plugin volume.VolumePluginControlPlane, tagUID string, vol *ateapi.ExternalVolumeStatus) (volume.Snapshot, error) {
	volName := vol.GetName()
	// CSI CreateSnapshot is idempotent on (name, source volume), so a retry
	// returns the snapshot the previous attempt made; DeleteTag relies on this
	// to recover handles that were never recorded. The tag UID keeps two
	// attempts under the same tag name apart.
	snap, err := plugin.CreateSnapshot(ctx, volume.CreateSnapshotRequest{
		Name:           tagVolumeSnapshotID(tagUID, volName),
		SourceVolumeID: vol.GetStorageVolumeId(),
	})
	if err != nil {
		return volume.Snapshot{}, fmt.Errorf("failed to snapshot volume %q: %w", volName, err)
	}
	if snap.SnapshotID == "" {
		return volume.Snapshot{}, fmt.Errorf("driver %q returned no snapshot handle for volume %q", vol.GetVolumeType(), volName)
	}
	return snap, nil
}

// tagVolumeSnapshotPlugins checks that every volume can be snapshotted and
// returns the plugin to snapshot each one with, indexed like volumes. It runs
// before the tag is reserved, so a volume that cannot be captured fails the
// call without leaving a pending tag or touching the storage system.
func (w *ActorWorkflow) tagVolumeSnapshotPlugins(ctx context.Context, actor *ateapi.Actor, volumes []*ateapi.ExternalVolumeStatus) ([]volume.VolumePluginControlPlane, error) {
	plugins := make([]volume.VolumePluginControlPlane, 0, len(volumes))
	for _, vol := range volumes {
		volName := vol.GetName()
		if vol.GetStorageVolumeId() == "" {
			return nil, apierror.FailedPrecondition("cannot snapshot volume %q of Actor %s: it has not been provisioned", volName, resources.ActorRefFromActor(actor))
		}
		plugin, err := w.pluginRegistry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			return nil, apierror.FailedPrecondition("failed to get volume plugin for driver %q: %v", vol.GetVolumeType(), err)
		}
		caps, err := plugin.ControllerCapabilities(ctx)
		if err != nil {
			return nil, apierror.FailedPrecondition("failed to read capabilities of driver %q for volume %q: %v", vol.GetVolumeType(), volName, err)
		}
		if !caps.CreateDeleteSnapshot {
			return nil, apierror.FailedPrecondition("volume %q uses driver %q, which does not support snapshots", volName, vol.GetVolumeType())
		}
		plugins = append(plugins, plugin)
	}
	return plugins, nil
}

// recordTagVolumeSnapshots records on the pending tag the volumes it is about
// to capture, each with its source volume ID and without a handle yet.
// Recording them before any snapshot is taken is what lets a reader tell "not
// requested" (no entry) from "requested and not finished" (an entry without a
// handle), and what lets DeleteTag recover a handle that never got recorded.
func (w *ActorWorkflow) recordTagVolumeSnapshots(ctx context.Context, tag *ateapi.Tag, volumes []*ateapi.ExternalVolumeStatus) (*ateapi.Tag, error) {
	tagRef := resources.TagRefFromTag(tag)
	entries := make([]*ateapi.ExternalVolumeSnapshot, 0, len(volumes))
	for _, vol := range volumes {
		entries = append(entries, &ateapi.ExternalVolumeSnapshot{
			SourceVolumeName: vol.GetName(),
			SourceVolumeId:   vol.GetStorageVolumeId(),
			VolumeType:       vol.GetVolumeType(),
		})
	}
	updated, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapi.Tag) error {
		snap := toUpdate.GetStatus().GetSnapshot()
		if snap.GetDurableSnapshot().GetState() != ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING {
			return fmt.Errorf("tag %s records no pending snapshot", tagRef)
		}
		if len(snap.GetVolumeSnapshots()) > 0 {
			return fmt.Errorf("tag %s already records volume snapshots", tagRef)
		}
		snap.VolumeSnapshots = entries
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("while recording the volumes to snapshot on tag %s: %w", tagRef, err)
	}
	return updated, nil
}

// releaseVolumeSnapshots deletes a set of volume snapshots, tolerating ones
// already gone and joining the failures so one bad handle does not strand the
// rest. Entries without a handle are skipped.
func (w *ActorWorkflow) releaseVolumeSnapshots(ctx context.Context, snapshots []*ateapi.ExternalVolumeSnapshot) error {
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

// ensureTagFinalized publishes the copy by marking status.snapshot COMPLETED.
// Until this lands the tag is pending and unusable; deleting it collects any
// partial copy and volume snapshots.
//
// The volume snapshots recorded by ensureTagVolumesSnapshotted are kept as is.
// status.snapshot is immutable once COMPLETED, so a tag can never come to name
// a different set of volumes than the one it published.
func (w *ActorWorkflow) ensureTagFinalized(ctx context.Context, tag *ateapi.Tag, snapshot *ateapi.Snapshot, dst resources.SnapshotURI) (_ *ateapi.Tag, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTag")
	defer func() { err = done(err) }()

	tagRef := resources.TagRefFromTag(tag)
	// The copy is byte-identical to the source, so it carries the same content.
	finalSnapshot := newDurableSnapshot(
		1,
		ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG,
		snapshot.GetFidelity(),
		"",
		dst.Name(),
		dst.String(),
		ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
	)
	finalSnapshot.SandboxRuntime = proto.CloneOf(snapshot.GetSandboxRuntime())
	stored, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapi.Tag) error {
		finalSnapshot.VolumeSnapshots = toUpdate.GetStatus().GetSnapshot().GetVolumeSnapshots()
		toUpdate.Status.Snapshot = finalSnapshot
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("Tag %s was deleted while it was being created, please retry", tagRef)
		}
		return nil, fmt.Errorf("while finalizing tag %s: %w", tagRef, err)
	}
	return stored, nil
}
