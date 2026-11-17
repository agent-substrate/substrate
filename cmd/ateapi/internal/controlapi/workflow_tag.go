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
	"strings"
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
	"k8s.io/apimachinery/pkg/util/sets"
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
	// Volumes hold what the actor would resume with, which matches its latest
	// snapshot. When that snapshot is not durable, the tag would fall back to
	// an older one, so a tag that captures volumes is refused instead.
	if len(volumes) > 0 && snapshot.GetGeneration() != latestSnapshot(actor.GetStatus(), func(*ateapi.Snapshot) bool { return true }).GetGeneration() {
		return nil, apierror.FailedPrecondition("Actor %s's latest snapshot is not durable, so its external volumes cannot be tagged with an older one", actorRef)
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
	if err := w.ensureTagVolumesSnapshotted(leaseCtx, recorded, volumes, plugins); err != nil {
		return nil, err
	}
	return w.ensureTagFinalized(leaseCtx, recorded, snapshot, dst)
}

// selectTagVolumes returns the source actor's external volumes a tag captures:
// none when sel is unset, then the ones it names, or all of them when it names
// none. The result keeps the actor's volume order.
//
// All volumes means every entry in status.external_volumes, including volumes
// no container mounts. Leaving those out would produce a tag missing a volume.
func selectTagVolumes(actor *ateapi.Actor, sel *ateapi.ExternalVolumeSelection) ([]*ateapi.ExternalVolumeStatus, error) {
	if sel == nil {
		return nil, nil
	}
	names := sel.GetNames()
	actorRef := resources.ActorRefFromActor(actor)
	requested := sets.New(names...)
	var selected []*ateapi.ExternalVolumeStatus
	for _, vol := range actor.GetStatus().GetExternalVolumes().GetVolumes() {
		if len(names) > 0 && !requested.Has(vol.GetName()) {
			continue
		}
		selected = append(selected, vol)
		requested.Delete(vol.GetName())
	}
	if requested.Len() > 0 {
		return nil, apierror.InvalidArgument("Actor %s has no external volumes named %s", actorRef, strings.Join(sets.List(requested), ", "))
	}
	if len(selected) == 0 {
		return nil, apierror.FailedPrecondition("Actor %s has no external volumes to snapshot; create the tag without a volume selection", actorRef)
	}
	return selected, nil
}

// DeleteTag releases the ExternalVolumeSnapshots and the external snapshot the
// tag owns and then removes the row, in that order: the row is the only handle
// on the external snapshot, so dropping it first would leak.
//
// The workflow is built in 4 phases:
//  1. Load the tag (which names the snapshot to collect) and mark it for
//     deletion, so no new ExternalVolumeSnapshot is recorded for it.
//  2. Release its ExternalVolumeSnapshots: delete each storage snapshot, then
//     its row.
//  3. Release the external snapshot, tolerating a previous attempt partly
//     collected.
//  4. Finalize: drop the row.
//
// Idempotent: a failure at any phase leaves the tag's row in place, so the
// same delete run again rediscovers the work from it and resumes over whatever
// is left. Marking the tag bumps its version, so a retry with the version
// precondition of the first attempt fails with Aborted. Retry with no version
// precondition, or with the version of the marked tag.
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
	if err := w.ensureTagVolumeSnapshotsReleased(ctx, tag); err != nil {
		return nil, err
	}
	if err := w.ensureTagSnapshotReleased(ctx, tag); err != nil {
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

// ensureTagVolumeSnapshotsReleased deletes the ExternalVolumeSnapshots the tag
// owns, each storage snapshot first and then its row, so a partial failure
// leaves every handle reachable for a retry.
//
// The rows are recorded before the storage snapshots are taken, so a failed
// create's snapshots are collected too. The tag is marked for deletion, so no
// new row is recorded for it, and the list is complete. Each row is
// independent, and driver calls can take seconds, so they are released
// concurrently.
func (w *ActorWorkflow) ensureTagVolumeSnapshotsReleased(ctx context.Context, tag *ateapi.Tag) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseTagVolumeSnapshots")
	defer func() { err = done(err) }()

	tagRef := resources.TagRefFromTag(tag)
	snapshots, err := w.store.ListTagExternalVolumeSnapshots(ctx, tagRef, tag.GetMetadata().GetUid())
	if err != nil {
		return fmt.Errorf("while listing the external volume snapshots of tag %s: %w", tagRef, err)
	}
	if len(snapshots) == 0 {
		markSkipped(ctx, "tag owns no external volume snapshots")
		return nil
	}
	errs := make([]error, len(snapshots))
	var wg sync.WaitGroup
	for i, snap := range snapshots {
		wg.Go(func() {
			errs[i] = w.releaseTagVolumeSnapshot(ctx, snap)
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("while releasing the external volume snapshots of tag %s: %w", tagRef, err)
	}
	return nil
}

// releaseTagVolumeSnapshot deletes one storage snapshot and then its row. A
// row without a handle may still name a snapshot that was cut but never
// recorded; its handle is recovered first (see recoverVolumeSnapshotHandle).
// A failed recovery is written to the row's error_message, so readers see why
// the delete is stuck.
func (w *ActorWorkflow) releaseTagVolumeSnapshot(ctx context.Context, snap *ateapi.ExternalVolumeSnapshot) error {
	ref := resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(snap)
	handle := snap.GetStatus().GetSnapshotId()
	if handle == "" {
		recovered, err := w.recoverVolumeSnapshotHandle(ctx, snap)
		if err != nil {
			w.recordVolumeSnapshotStatus(ctx, snap, tagVolumeSnapshotFailed(err))
			return err
		}
		if recovered.SnapshotID != "" {
			w.recordVolumeSnapshotStatus(ctx, snap, tagVolumeSnapshotTaken(recovered))
			handle = recovered.SnapshotID
		}
	}
	if handle != "" {
		plugin, err := w.pluginRegistry.GetPlugin(ctx, snap.GetDriverName())
		if err != nil {
			return fmt.Errorf("while getting volume plugin for driver %q: %w", snap.GetDriverName(), err)
		}
		if err := plugin.DeleteSnapshot(ctx, handle); err != nil {
			return fmt.Errorf("while deleting volume snapshot %q: %w", handle, err)
		}
	}
	_, err := w.store.DeleteExternalVolumeSnapshot(ctx, ref, store.DeletePreconditions{UID: snap.GetMetadata().GetUid()})
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("while deleting ExternalVolumeSnapshot %s: %w", ref, err)
	}
	return nil
}

// recordVolumeSnapshotStatus writes the outcome of a recovery to its row: the
// recovered handle, so a retry deletes the snapshot by handle instead of
// recovering it again, or the error that kept the delete from finishing. A
// failed write is logged and otherwise ignored: the caller goes on with what
// it holds, and a retry recovers the handle again if it could not.
func (w *ActorWorkflow) recordVolumeSnapshotStatus(ctx context.Context, snap *ateapi.ExternalVolumeSnapshot, newStatus *ateapi.ExternalVolumeSnapshotStatus) {
	ref := resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(snap)
	_, err := w.store.UpdateExternalVolumeSnapshot(ctx, ref, store.PreconditionFrom(snap), func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
		toUpdate.Status = newStatus
		return nil
	})
	if err != nil {
		slog.WarnContext(ctx, "cannot record the result of recovering a volume snapshot handle",
			slog.String("external_volume_snapshot", ref.String()), slog.Any("error", err))
	}
}

// recoverVolumeSnapshotHandle finds the handle of a snapshot whose row records
// none. Such a row names either a snapshot that failed or one the storage
// system cut whose handle a failed or crashed create never wrote back.
// CreateSnapshot is idempotent on the snapshot name and source volume, so
// issuing the create's request again returns the handle of a snapshot that
// exists. When none does, this cuts one that the caller then deletes. That
// snapshot is of the volume as it is now, which may be attached if the source
// Actor has resumed, and it counts against the storage system's quota until
// it is deleted.
//
// It returns no handle to skip the row when there is nothing to recover:
//   - The driver lacks CREATE_DELETE_SNAPSHOT, so it never cut a snapshot.
//   - CreateSnapshot fails with a terminal code, so retrying cannot recover
//     the handle. NotFound is one such code: the storage system cannot look a
//     snapshot up by name alone, so once the source volume is gone a snapshot
//     cut from it can no longer be found.
//
// A snapshot skipped on a terminal error may be leaked. It is logged, and the
// tag can still be deleted. Only a transient error keeps the row for a retry.
func (w *ActorWorkflow) recoverVolumeSnapshotHandle(ctx context.Context, snap *ateapi.ExternalVolumeSnapshot) (volume.Snapshot, error) {
	driver := snap.GetDriverName()
	origin := snap.GetOrigin().GetTagCapture()
	volName := origin.GetSourceVolumeName()
	plugin, err := w.pluginRegistry.GetPlugin(ctx, driver)
	if err != nil {
		return volume.Snapshot{}, fmt.Errorf("while getting volume plugin for driver %q: %w", driver, err)
	}
	caps, err := plugin.ControllerCapabilities(ctx)
	if err != nil {
		return volume.Snapshot{}, fmt.Errorf("while reading capabilities of driver %q: %w", driver, err)
	}
	if !caps.CreateDeleteSnapshot {
		slog.InfoContext(ctx, "driver does not support snapshots, so it holds no volume snapshot to recover",
			slog.String("driver", driver), slog.String("source_volume_name", volName))
		return volume.Snapshot{}, nil
	}
	got, err := createTagVolumeSnapshot(ctx, plugin, snap.GetMetadata().GetName(), volName, origin.GetSourceVolumeId(), driver)
	if err != nil {
		if !transientSnapshotError(err) {
			slog.WarnContext(ctx, "cannot recover a volume snapshot the tag never recorded; it may be leaked",
				slog.String("snapshot_name", snap.GetMetadata().GetName()), slog.String("source_volume_id", origin.GetSourceVolumeId()),
				slog.String("code", status.Code(err).String()), slog.Any("error", err))
			return volume.Snapshot{}, nil
		}
		return volume.Snapshot{}, fmt.Errorf("while recovering the snapshot of volume %q: %w", volName, err)
	}
	return got, nil
}

// transientSnapshotError reports whether a failed CreateSnapshot may succeed on
// a retry. An error without a gRPC status has code Unknown and counts as
// transient. So does ResourceExhausted, which drivers return for rate limits
// and quota that clear over time.
func transientSnapshotError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.Canceled, codes.Unknown, codes.Internal, codes.ResourceExhausted:
		return true
	}
	return false
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
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, apierror.FailedPrecondition("Tag %s still owns ExternalVolumeSnapshots", tagRef)
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
// tagVolumeSnapshotPlugins returned. Volumes left out are not recorded.
//
// One ExternalVolumeSnapshot per volume is recorded first, with its source
// volume ID and an empty status. The snapshots are taken concurrently, and
// every result is recorded in one transaction once all of them return: the
// handle with whether it is ready to use, or the error.
//
// A row with an empty status names a snapshot that may have been cut but
// whose handle was never recorded because the create died or the write
// failed. Deleting the tag recovers it from the row's source volume ID, so
// nothing is released here.
//
// Capture is all-or-nothing: a clone that silently came up with one empty disk
// would be worse than a failed create. Any failure fails the create and leaves
// the pending tag for DeleteTag. The handles of volumes that did succeed are
// recorded before the failure is returned.
//
// It does not wait for the storage system to finish copying. A driver may
// return a handle that is not ready to use and finish in the background, and
// blocking here would put an unbounded storage operation inside a synchronous
// RPC.
//
// No quiesce step runs. A SUSPENDED actor's volumes are detached, and the
// actor lease held by the caller keeps a concurrent resume from re-attaching
// them mid-capture. atelet usually unmounts them cleanly first, but a revert
// that detaches without atelet leaves them only crash-consistent.
func (w *ActorWorkflow) ensureTagVolumesSnapshotted(ctx context.Context, tag *ateapi.Tag, volumes []*ateapi.ExternalVolumeStatus, plugins []volume.VolumePluginControlPlane) (err error) {
	ctx, done := stepSpan(ctx, "SnapshotVolumes")
	defer func() { err = done(err) }()

	if len(volumes) == 0 {
		markSkipped(ctx, "no external volumes to capture")
		return nil
	}

	tagRef := resources.TagRefFromTag(tag)
	tagUID := tag.GetMetadata().GetUid()
	for _, vol := range volumes {
		if _, err := w.store.CreateExternalVolumeSnapshot(ctx, newTagVolumeSnapshot(tag, vol)); err != nil {
			return tagCreateUpdateError(tagRef, err, "recording the volumes to snapshot of")
		}
	}

	// Siblings are not canceled when one fails: a canceled call may still
	// have cut a snapshot whose handle would never come back, so each runs to
	// completion and reports its result. Losing a lease does cancel ctx. A
	// driver may still cut the snapshot after that, and if DeleteTag has
	// dropped the row by then, the snapshot is leaked.
	snaps := make([]volume.Snapshot, len(volumes))
	snapErrs := make([]error, len(volumes))
	var wg sync.WaitGroup
	for i, vol := range volumes {
		wg.Go(func() {
			snaps[i], snapErrs[i] = createTagVolumeSnapshot(ctx, plugins[i], tagVolumeSnapshotID(tagUID, vol.GetName()), vol.GetName(), vol.GetStorageVolumeId(), vol.GetVolumeType())
		})
	}
	wg.Wait()

	index := make(map[string]int, len(volumes))
	for i, vol := range volumes {
		index[vol.GetName()] = i
	}
	if _, err := w.store.UpdateTagExternalVolumeSnapshots(ctx, tagRef, tagUID, func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
		volName := toUpdate.GetOrigin().GetTagCapture().GetSourceVolumeName()
		i, ok := index[volName]
		if !ok {
			return fmt.Errorf("tag %s owns a snapshot of volume %q, which this create did not take", tagRef, volName)
		}
		if snapErrs[i] != nil {
			toUpdate.Status = tagVolumeSnapshotFailed(snapErrs[i])
		} else {
			toUpdate.Status = tagVolumeSnapshotTaken(snaps[i])
		}
		return nil
	}); err != nil {
		return tagCreateUpdateError(tagRef, err, "recording the volume snapshots of")
	}
	if err := errors.Join(snapErrs...); err != nil {
		return volumeSnapshotError(tagRef, err)
	}
	return nil
}

// newTagVolumeSnapshot returns the ExternalVolumeSnapshot of vol, with an
// empty status, that tag records before it takes the snapshot.
func newTagVolumeSnapshot(tag *ateapi.Tag, vol *ateapi.ExternalVolumeStatus) *ateapi.ExternalVolumeSnapshot {
	tagRef := resources.TagRefFromTag(tag)
	return &ateapi.ExternalVolumeSnapshot{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: tagRef.Atespace,
			Name:     tagVolumeSnapshotID(tag.GetMetadata().GetUid(), vol.GetName()),
		},
		DriverName: vol.GetVolumeType(),
		Origin: &ateapi.ExternalVolumeSnapshotOrigin{TagCapture: &ateapi.TagVolumeSnapshotOrigin{
			Tag:              tagRef.ToObjectRef(),
			TagUid:           tag.GetMetadata().GetUid(),
			SourceVolumeName: vol.GetName(),
			SourceVolumeId:   vol.GetStorageVolumeId(),
		}},
		Status: &ateapi.ExternalVolumeSnapshotStatus{},
	}
}

// tagVolumeSnapshotTaken returns the status recording a snapshot the driver
// returned a handle for. It has no error_message, so writing it clears the
// error of an earlier failed attempt.
func tagVolumeSnapshotTaken(snap volume.Snapshot) *ateapi.ExternalVolumeSnapshotStatus {
	return &ateapi.ExternalVolumeSnapshotStatus{
		SnapshotId:       snap.SnapshotID,
		ReadyToUse:       snap.ReadyToUse,
		RestoreSizeBytes: snap.SizeBytes,
	}
}

// maxVolumeSnapshotErrorMessageLen is ExternalVolumeSnapshotStatus.error_message's
// maxLength.
const maxVolumeSnapshotErrorMessageLen = 4096

// tagVolumeSnapshotFailed returns the status recording a failed snapshot,
// with the error cut to fit error_message.
func tagVolumeSnapshotFailed(err error) *ateapi.ExternalVolumeSnapshotStatus {
	return &ateapi.ExternalVolumeSnapshotStatus{
		ErrorMessage: truncateUTF8(strings.ToValidUTF8(err.Error(), "�"), maxVolumeSnapshotErrorMessageLen),
	}
}

// volumeSnapshotError maps a failed volume snapshot to the error the create
// fails with, based on the code of the first failure that carries one.
//
// The failure leaves the tag pending, so a plain retry of the create fails
// with AlreadyExists. Transient driver codes therefore become Aborted rather
// than Unavailable, which client retry policies commonly retry on their own,
// and every message says to delete the tag first.
func volumeSnapshotError(tagRef resources.TagRef, err error) error {
	const format = "%v; delete Tag %s and create it again"
	switch status.Code(err) {
	case codes.ResourceExhausted:
		return apierror.ResourceExhausted(format, err, tagRef)
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return apierror.Aborted(format, err, tagRef)
	case codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound:
		return apierror.FailedPrecondition(format, err, tagRef)
	}
	return apierror.Internal(format, err, tagRef)
}

// tagCreateUpdateError maps a store error from a step of the create that
// writes after the snapshot is recorded. A version conflict leaves the tag
// pending, so the caller must delete it before creating it again. A tag that
// is gone, or is being deleted, can be created again once the delete ends.
func tagCreateUpdateError(tagRef resources.TagRef, err error, action string) error {
	if errors.Is(err, store.ErrVersionConflict) {
		return apierror.Aborted("concurrent update conflict; delete Tag %s and create it again", tagRef)
	}
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict) || errors.Is(err, store.ErrFailedPrecondition) {
		return apierror.Aborted("Tag %s was deleted while it was being created, please retry", tagRef)
	}
	return fmt.Errorf("while %s tag %s: %w", action, tagRef, err)
}

// createTagVolumeSnapshot snapshots one volume under name, failing if the
// driver returns no handle.
func createTagVolumeSnapshot(ctx context.Context, plugin volume.VolumePluginControlPlane, name, volName, sourceVolumeID, driver string) (volume.Snapshot, error) {
	// CSI CreateSnapshot is idempotent on (name, source volume), so a retry
	// returns the snapshot the previous attempt made; DeleteTag relies on this
	// to recover handles that were never recorded.
	snap, err := plugin.CreateSnapshot(ctx, volume.CreateSnapshotRequest{
		Name:           name,
		SourceVolumeID: sourceVolumeID,
	})
	if err != nil {
		return volume.Snapshot{}, fmt.Errorf("failed to snapshot volume %q: %w", volName, err)
	}
	if snap.SnapshotID == "" {
		return volume.Snapshot{}, fmt.Errorf("driver %q returned no snapshot handle for volume %q", driver, volName)
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

// ensureTagFinalized publishes the copy by marking status.snapshot COMPLETED.
// Until this lands the tag is pending and unusable; deleting it collects any
// partial copy and volume snapshots.
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
		toUpdate.Status.Snapshot = finalSnapshot
		return nil
	})
	if err != nil {
		return nil, tagCreateUpdateError(tagRef, err, "finalizing")
	}
	return stored, nil
}
