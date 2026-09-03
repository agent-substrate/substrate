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

// TagActorSnapshot tags the external snapshot a suspended actor holds.
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
// Idempotent: a re-entered workflow adopts the pending row its previous attempt
// left, re-copies to the URI that row already names, and finalizes.
func (w *ActorWorkflow) TagActorSnapshot(ctx context.Context, actorRef resources.ActorRef, tag *ateapipb.ActorSnapshotTag) (*ateapipb.ActorSnapshotTag, error) {
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

	reserved, adoptedSnapshotURI, err := w.ensureTagReserved(leaseCtx, tagRef, actor, actorTemplate, tag)
	if err != nil {
		return nil, err
	}
	if err := w.ensureTagSnapshotCopied(leaseCtx, reserved, snapshot, adoptedSnapshotURI); err != nil {
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
	actorTemplate, err := resolveActorTemplate(ctx, w.store, actor)
	if err != nil {
		return nil, nil, err
	}
	return actor, actorTemplate, nil
}

// ensureTagReserved sets status.in_progress_snapshot_uri to express the intent
// to create a new tag.
//
// When it adopts a row a previous attempt left behind, the destination is that
// row's rather than a freshly minted one, and it returns that row's in-progress
// URI; a fresh reservation returns "". A name held by a finished tag, or by
// another actor's unfinished one, is AlreadyExists: adopting another actor's
// pending create would let two sources interleave objects into one prefix.
func (w *ActorWorkflow) ensureTagReserved(ctx context.Context, tagRef resources.ActorSnapshotTagRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, tag *ateapipb.ActorSnapshotTag) (_ *ateapipb.ActorSnapshotTag, adoptedSnapshotURI string, err error) {
	ctx, done := stepSpan(ctx, "ReserveSnapshotTag")
	defer func() { err = done(err) }()

	dst, err := resources.NewTagSnapshotURI(actorTemplate.GetSnapshotsConfig().GetStorageLocation(), tagRef.Atespace, resources.NewSnapshotName())
	if err != nil {
		return nil, "", fmt.Errorf("while building the snapshot URI for tag %s: %w", tagRef, err)
	}
	tagToCreate := &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: tagRef.Atespace, Name: tagRef.Name},
		Scope:    tag.GetScope(),
		Status: &ateapipb.ActorSnapshotTagStatus{
			ActorTemplateUid:      actorTemplate.GetMetadata().GetUid(),
			InProgressSnapshotUri: dst.String(),
			SourceActorUid:        actor.GetMetadata().GetUid(),
		},
	}

	stored, err := w.store.CreateActorSnapshotTag(ctx, tagToCreate)
	switch {
	case err == nil:
		return stored, "", nil
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, "", status.Errorf(codes.FailedPrecondition, "Atespace %s not found", tagRef.Atespace)
	case !errors.Is(err, store.ErrAlreadyExists):
		return nil, "", fmt.Errorf("while reserving actor snapshot tag %s: %w", tagRef, err)
	}

	// The tag already exists. This means one of the two are true:
	//   * We tried to write this tag before for this actor & snapshot,
	//      but failed and this is a retry
	//   * This tag exists for another actor/snapshot. We should fail right away,
	//     because a tag is immutable.
	// We read the tag back to figure out which is our situation.
	existing, err := w.store.GetActorSnapshotTag(ctx, tagRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Deleted between the insert and this read; the client retries.
			return nil, "", status.Errorf(codes.Aborted, "ActorSnapshotTag %s was deleted concurrently, please retry", tagRef)
		}
		return nil, "", fmt.Errorf("while getting actor snapshot tag %s: %w", tagRef, err)
	}
	switch {
	case existing.GetStatus().GetSnapshot().GetSnapshotUri() != "":
		// This tag already exists and is final.
		return nil, "", status.Errorf(codes.AlreadyExists, "ActorSnapshotTag %s already exists", tagRef)
	// This tag already exsits
	case existing.GetStatus().GetSourceActorUid() != actor.GetMetadata().GetUid():
		// This tag already exists and is final.
		return nil, "", status.Errorf(codes.AlreadyExists, "ActorSnapshotTag %s already exists", tagRef)
	case existing.GetStatus().GetInProgressSnapshotUri() == "":
		// Neither finished nor in progress: the row points to no objects at all, so
		// there is nothing this workflow can safely resume or collect. This should never happen.
		return nil, "", status.Errorf(codes.Internal, "ActorSnapshotTag %s has neither a snapshot nor one in progress", tagRef)
	}

	// In-progress tag.
	markSkipped(ctx, "resuming this actor's unfinished tag")
	return existing, existing.GetStatus().GetInProgressSnapshotUri(), nil
}

// ensureTagSnapshotCopied copies the actor's external snapshot to the tag's own
// prefix, the one the reserved row names. adoptedSnapshotURI is that same
// prefix when the row came from a previous attempt, and "" when it was freshly
// reserved.
func (w *ActorWorkflow) ensureTagSnapshotCopied(ctx context.Context, tag *ateapipb.ActorSnapshotTag, snapshot *ateapipb.ExternalSnapshot, adoptedSnapshotURI string) (err error) {
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
	if adoptedSnapshotURI != "" {
		// A previous attempt may have left a partial copy, and if the actor was
		// resuspended since, the new source has a different object set that
		// would otherwise union with the old one. A freshly minted destination
		// is empty by construction, so only an adopted one needs clearing.
		if err := objectstore.DeletePrefix(ctx, w.objectStore, dst.Prefix()); err != nil {
			return fmt.Errorf("while clearing the in-progress snapshot of tag %s: %w", tagRef, err)
		}
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
	if tag.GetStatus().GetSnapshot().GetSnapshotUri() != "" {
		markSkipped(ctx, "tag already names its snapshot")
		return tag, nil
	}
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
