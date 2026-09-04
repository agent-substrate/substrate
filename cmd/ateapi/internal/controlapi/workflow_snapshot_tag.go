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
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TagActorSnapshot tags the external snapshot held by the suspended actor the
// tag's source_actor names.
// The tag is given its own copy of that snapshot, so suspending the actor again
// or deleting the actor does not garbage collect the tag's snapshot.
//
// The tag is built in 3 phases:
//  1. Record intent: in status.in_progress_snapshot_uri
//  2. Write snapshot to the remote storage indicated in status.in_progress_snapshot_uri
//  3. Finalize: clear status.in_progress_snapshot_uri and write the final
//     snapshot object to the tag.
//
// The tag captures whichever snapshot the actor holds when the workflow runs.
// An actor keeps no snapshot history, so a suspend that lands first moves what
// gets tagged; that race is inherent to naming an actor rather than a snapshot.
//
// Not idempotent: the name is taken as soon as phase 1 lands, so a create that
// dies after it leaves a pending tag and every later create under that name is
// AlreadyExists. To retry, delete the tag, which collects whatever the failed
// attempt stranded, and create it again.
func (w *ActorWorkflow) TagActorSnapshot(ctx context.Context, tag *ateapipb.ActorSnapshotTag) (*ateapipb.ActorSnapshotTag, error) {
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
	tagRef := resources.ActorSnapshotTagRef{Atespace: actorRef.Atespace, Name: tag.GetMetadata().GetName()}
	leaseCtx, tagLease, err := acquireActorSnapshotTagLease(leaseCtx, w.store, tagRef)
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
	if err := w.ensureTagSnapshotCopied(leaseCtx, reserved, snapshot); err != nil {
		return nil, err
	}
	return w.ensureTagFinalized(leaseCtx, reserved, snapshot)
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

// ensureTagReserved takes the tag's name and sets status.in_progress_snapshot_uri
// to express the intent to create a new tag.
//
// A name already taken is AlreadyExists, whether the tag holding it is finished
// or was left pending by a create that died. Resuming a pending row would mean
// deciding whether the objects under it still belong to the snapshot being
// tagged, and the row may not even be this actor's; deleting the tag collects
// them and frees the name, so a retry is a delete followed by a create.
func (w *ActorWorkflow) ensureTagReserved(ctx context.Context, tagRef resources.ActorSnapshotTagRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, tag *ateapipb.ActorSnapshotTag) (_ *ateapipb.ActorSnapshotTag, err error) {
	ctx, done := stepSpan(ctx, "ReserveSnapshotTag")
	defer func() { err = done(err) }()

	dst, err := resources.NewTagSnapshotURI(actorTemplate.GetSnapshotsConfig().GetStorageLocation(), tagRef.Atespace, resources.NewSnapshotName())
	if err != nil {
		return nil, fmt.Errorf("while building the snapshot URI for tag %s: %w", tagRef, err)
	}
	tagToCreate := &ateapipb.ActorSnapshotTag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: tagRef.Atespace, Name: tagRef.Name},
		Scope:       tag.GetScope(),
		SourceActor: resources.ActorRefFromActor(actor).ToObjectRef(),
		Status: &ateapipb.ActorSnapshotTagStatus{
			// The tag records the template the snapshot's guest state was built under, not
			// the one the actor currently points at. A suspended actor can be repointed,
			// and a tag that claimed the new template would hand clones the old template's
			// memory under the new one's identity, past the data-only downgrade a resume of
			// the actor itself would take.
			ActorTemplateUid:      actor.GetStatus().GetCurrentActorTemplateUid(),
			InProgressSnapshotUri: dst.String(),
			SourceActorUid:        actor.GetMetadata().GetUid(),
		},
	}

	stored, err := w.store.CreateActorSnapshotTag(ctx, tagToCreate)
	switch {
	case err == nil:
		return stored, nil
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", tagRef.Atespace)
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, status.Errorf(codes.AlreadyExists, "ActorSnapshotTag %s already exists; delete it and create it again to retry", tagRef)
	}
	return nil, fmt.Errorf("while reserving actor snapshot tag %s: %w", tagRef, err)
}

// ensureTagSnapshotCopied copies the actor's external snapshot to the tag's own
// prefix, the one the reserved row names. That prefix is freshly minted, so it
// is empty by construction and the copy never blends with objects some other
// attempt left.
func (w *ActorWorkflow) ensureTagSnapshotCopied(ctx context.Context, tag *ateapipb.ActorSnapshotTag, snapshot *ateapipb.ExternalSnapshot) (err error) {
	ctx, done := stepSpan(ctx, "CopyTagSnapshot")
	defer func() { err = done(err) }()

	if w.objectStore == nil {
		markSkipped(ctx, "no object store configured")
		return nil
	}
	tagRef := resources.ActorSnapshotTagRefFromActorSnapshotTag(tag)
	src, err := resources.ParseSnapshotURI(snapshot.GetSnapshotUri())
	if err != nil {
		return fmt.Errorf("while parsing the external snapshot %q of the source actor: %w", snapshot.GetSnapshotUri(), err)
	}
	dst, err := resources.ParseSnapshotURI(tag.GetStatus().GetInProgressSnapshotUri())
	if err != nil {
		return fmt.Errorf("while parsing the in-progress snapshot %q of tag %s: %w", tag.GetStatus().GetInProgressSnapshotUri(), tagRef, err)
	}
	if err := objectstore.CopyPrefix(ctx, w.objectStore, src.Prefix(), dst.Prefix()); err != nil {
		return fmt.Errorf("while copying the external snapshot for tag %s: %w", tagRef, err)
	}
	return nil
}

// ensureTagFinalized publishes the copy: it sets status.snapshot to the URI the
// row already names and clears status.in_progress_snapshot_uri. Until this
// lands the tag is pending — visible, unusable, and naming exactly the objects
// an unfinished create stranded, so deleting it collects them.
func (w *ActorWorkflow) ensureTagFinalized(ctx context.Context, tag *ateapipb.ActorSnapshotTag, snapshot *ateapipb.ExternalSnapshot) (_ *ateapipb.ActorSnapshotTag, err error) {
	ctx, done := stepSpan(ctx, "FinalizeSnapshotTag")
	defer func() { err = done(err) }()

	tagRef := resources.ActorSnapshotTagRefFromActorSnapshotTag(tag)
	// The copy is byte-identical to the source, so it carries the same content.
	finalSnapshot := &ateapipb.ExternalSnapshot{
		SnapshotUri:  tag.GetStatus().GetInProgressSnapshotUri(),
		ContentScope: snapshot.GetContentScope(),
	}
	stored, err := w.store.UpdateActorSnapshotTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		toUpdate.Status.Snapshot = finalSnapshot
		toUpdate.Status.InProgressSnapshotUri = ""
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "ActorSnapshotTag %s was deleted while it was being created, please retry", tagRef)
		}
		return nil, fmt.Errorf("while finalizing actor snapshot tag %s: %w", tagRef, err)
	}
	return stored, nil
}
