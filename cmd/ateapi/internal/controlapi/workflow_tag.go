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

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TagActorSnapshot tags the external snapshot held by the suspended actor the
// tag's source_actor names.
// The tag is given its own copy of that snapshot, so suspending the actor again
// or deleting the actor does not garbage collect the tag's snapshot.
//
// The tag is built in 6 phases:
//  1. Check that the name is free and the storage location is usable.
//  2. Make the actor's latest snapshot durable, if it is not uploaded yet: an
//     asynchronously suspended actor's snapshot may still be uploading.
//  3. Reserve the tag's name; the store assigns its UID.
//  4. Record the snapshot's URI, under the tag's UID, before anything is
//     copied there.
//  5. Copy the snapshot to that URI.
//  6. Finalize: mark the tag's snapshot COMPLETED.
//
// The tag captures whichever snapshot the actor holds when the workflow runs.
// An actor keeps no snapshot history, so a suspend that lands first moves what
// gets tagged; that race is inherent to naming an actor rather than a snapshot.
//
// Not idempotent: the name is taken as soon as phase 3 lands, so a create that
// dies after it leaves a pending tag and every later create under that name is
// AlreadyExists. To retry, delete the tag, which collects whatever the failed
// attempt stranded, and create it again.
func (w *ActorWorkflow) TagActorSnapshot(ctx context.Context, tag *ateapipb.Tag) (*ateapipb.Tag, error) {
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
	return w.ensureTagFinalized(leaseCtx, recorded, snapshot, dst)
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
	return w.finalizeTagDeleted(ctx, tag)
}

// ensureTagMarkedForDeletion sets the tag's delete_time, checking precondition,
// and returns the stored copy. Skips when a previous attempt already marked the
// tag.
func (w *ActorWorkflow) ensureTagMarkedForDeletion(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "MarkTagForDeletion")
	defer func() { err = done(err) }()

	var alreadyDeleting *ateapipb.Tag
	marked, err := w.store.MarkTagForDeletion(ctx, tagRef, precondition, func(toUpdate *ateapipb.Tag) error {
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
func (w *ActorWorkflow) ensureTagSnapshotReleased(ctx context.Context, tag *ateapipb.Tag) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseTagSnapshot")
	defer func() { err = done(err) }()

	if w.objectStorePlugin == nil {
		markSkipped(ctx, "no object store configured")
		return nil
	}
	tagRef := resources.TagRefFromTag(tag)
	snapshotURI := tagDurableSnapshotURI(tag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED)
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
func (w *ActorWorkflow) finalizeTagDeleted(ctx context.Context, tag *ateapipb.Tag) (_ *ateapipb.Tag, err error) {
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
		return nil, nil, apierror.FailedPrecondition("Actor %s must be %s to be tagged (got: %v)", actorRef, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, got)
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
func (w *ActorWorkflow) checkTagCreatable(ctx context.Context, tagRef resources.TagRef, actorTemplate *ateapipb.ActorTemplate) (err error) {
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
		return fmt.Errorf("invalid storage location for tag %s: %w", tagRef, err)
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
func (w *ActorWorkflow) ensureTagSourceSnapshotDurable(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "PromoteTagSourceSnapshot")
	defer func() { err = done(err) }()

	snap := latestSnapshot(actor.GetStatus(), func(*ateapipb.Snapshot) bool { return true })
	if snap.GetDurableSnapshot().GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
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
	stored, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
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
func sourceSnapshotForTag(actorRef resources.ActorRef, actor *ateapipb.Actor) (*ateapipb.Snapshot, error) {
	snap := findLatestDurableSnapshot(actor.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
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
func (w *ActorWorkflow) ensureTagReserved(ctx context.Context, tagRef resources.TagRef, actor *ateapipb.Actor, snapshot *ateapipb.Snapshot, tag *ateapipb.Tag) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "ReserveTag")
	defer func() { err = done(err) }()

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
func (w *ActorWorkflow) ensureTagSnapshotRecorded(ctx context.Context, tag *ateapipb.Tag, actorTemplate *ateapipb.ActorTemplate, snapshot *ateapipb.Snapshot) (_ *ateapipb.Tag, _ resources.SnapshotURI, err error) {
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
		ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG,
		snapshot.GetFidelity(),
		"",
		dst.Name(),
		dst.String(),
		ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING,
	)
	pending.SandboxRuntime = proto.CloneOf(snapshot.GetSandboxRuntime())
	stored, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
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
func (w *ActorWorkflow) ensureTagSnapshotCopied(ctx context.Context, tag *ateapipb.Tag, snapshot *ateapipb.Snapshot, dst resources.SnapshotURI) (err error) {
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

// ensureTagFinalized publishes the copy by marking status.snapshot COMPLETED.
// Until this lands the tag is pending and unusable; deleting it collects any
// partial copy.
func (w *ActorWorkflow) ensureTagFinalized(ctx context.Context, tag *ateapipb.Tag, snapshot *ateapipb.Snapshot, dst resources.SnapshotURI) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTag")
	defer func() { err = done(err) }()

	tagRef := resources.TagRefFromTag(tag)
	// The copy is byte-identical to the source, so it carries the same content.
	finalSnapshot := newDurableSnapshot(
		1,
		ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG,
		snapshot.GetFidelity(),
		"",
		dst.Name(),
		dst.String(),
		ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
	)
	finalSnapshot.SandboxRuntime = proto.CloneOf(snapshot.GetSandboxRuntime())
	stored, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
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
