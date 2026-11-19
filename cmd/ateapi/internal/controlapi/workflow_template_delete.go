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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
)

// DeleteActorTemplate executes the workflow to delete an ActorTemplate. The
// golden actor and tag go first and the row last, so a failed attempt leaves
// the row for a retry to rediscover them by.
func (w *ActorWorkflow) DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions) (*ateapi.ActorTemplate, error) {
	ctx = preview.ContextWithOptIn(ctx)
	// Serializes against the reconciler, which creates the golden actor and
	// tag under the same lease.
	ctx, lease, err := acquireLease(ctx, w.store, "lease:actortemplate:"+templateRef.Atespace+":"+templateRef.Name, "ActorTemplate "+templateRef.String())
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	tmpl, err := w.ensureTemplateMarkedForDeletion(ctx, templateRef, precondition)
	if err != nil {
		return nil, err
	}

	// Disabled templates never create golden resources.
	if tmpl.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() == ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
		return w.finalizeTemplateDeleted(ctx, tmpl)
	}

	// Both are named after the template's UID, in the template's atespace.
	goldenName := tmpl.GetMetadata().GetUid()
	if err := w.ensureGoldenActorDeleted(ctx, resources.ActorRef{Atespace: templateRef.Atespace, Name: goldenName}, tmpl); err != nil {
		return nil, err
	}
	if err := w.ensureGoldenTagDeleted(ctx, resources.TagRef{Atespace: templateRef.Atespace, Name: goldenName}, tmpl); err != nil {
		return nil, err
	}
	return w.finalizeTemplateDeleted(ctx, tmpl)
}

// ensureTemplateMarkedForDeletion sets the template's delete_time, checking
// precondition, and returns the stored copy. Skips when a previous attempt
// already marked the template.
func (w *ActorWorkflow) ensureTemplateMarkedForDeletion(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions) (_ *ateapi.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "MarkTemplateForDeletion")
	defer func() { err = done(err) }()

	var alreadyDeleting *ateapi.ActorTemplate
	marked, err := w.store.MarkActorTemplateForDeletion(ctx, templateRef, precondition, func(toUpdate *ateapi.ActorTemplate) error {
		if toUpdate.GetMetadata().GetDeleteTime() != nil {
			alreadyDeleting = toUpdate
			return errAlreadyMarked
		}
		return nil
	})
	if errors.Is(err, errAlreadyMarked) {
		markSkipped(ctx, "template already marked for deletion")
		return alreadyDeleting, nil
	}
	if err != nil {
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("ActorTemplate %s does not have uid %s", templateRef, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("ActorTemplate %s not found", templateRef)
		}
		return nil, fmt.Errorf("while marking actor template %s for deletion: %w", templateRef, err)
	}
	return marked, nil
}

// ensureGoldenActorDeleted removes the template's golden actor, whatever state
// it is in. Absent is the ordinary case for a template whose golden snapshot
// never finished.
func (w *ActorWorkflow) ensureGoldenActorDeleted(ctx context.Context, goldenRef resources.ActorRef, tmpl *ateapi.ActorTemplate) (err error) {
	ctx, done := stepSpan(ctx, "DeleteGoldenActor")
	defer func() { err = done(err) }()

	// Recording the golden tag follows deletion of the temporary actor.
	if tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() != nil {
		markSkipped(ctx, "golden actor already deleted")
		return nil
	}

	actor, err := w.store.GetActor(ctx, goldenRef)
	if errors.Is(err, store.ErrNotFound) {
		markSkipped(ctx, "golden actor already deleted")
		return nil
	}
	if err != nil {
		return fmt.Errorf("while getting golden actor: %w", err)
	}
	if resources.ActorTemplateRefFromObjectRef(actor.GetActorTemplate()) != resources.ActorTemplateRefFromActorTemplate(tmpl) {
		markSkipped(ctx, "actor belongs to another template")
		return nil
	}
	// The actor may be replaced or repointed before its lease is acquired.
	precondition := store.DeletePreconditions(store.PreconditionFrom(actor))
	if _, err := w.DeleteActor(ctx, goldenRef, precondition); err != nil {
		if apierror.Code(err) == codes.NotFound {
			markSkipped(ctx, "golden actor already deleted")
			return nil
		}
		return fmt.Errorf("while deleting golden actor: %w", err)
	}
	return nil
}

// ensureGoldenTagDeleted removes the template's golden tag and the snapshot it
// holds.
func (w *ActorWorkflow) ensureGoldenTagDeleted(ctx context.Context, goldenTagRef resources.TagRef, tmpl *ateapi.ActorTemplate) (err error) {
	ctx, done := stepSpan(ctx, "DeleteGoldenTag")
	defer func() { err = done(err) }()

	tag, err := w.store.GetTag(ctx, goldenTagRef)
	if errors.Is(err, store.ErrNotFound) {
		markSkipped(ctx, "golden tag already deleted")
		return nil
	}
	if err != nil {
		return fmt.Errorf("while getting golden tag: %w", err)
	}
	goldenActorRef := resources.ActorRef(goldenTagRef)
	if tag.GetStatus().GetActorTemplateUid() != tmpl.GetMetadata().GetUid() || resources.ActorRefFromObjectRef(tag.GetSourceActor()) != goldenActorRef {
		markSkipped(ctx, "tag belongs to another actor or template")
		return nil
	}
	precondition := store.DeletePreconditions(store.PreconditionFrom(tag))
	if _, err := w.DeleteTag(ctx, goldenTagRef, precondition); err != nil {
		if apierror.Code(err) == codes.NotFound {
			markSkipped(ctx, "golden tag already deleted")
			return nil
		}
		return fmt.Errorf("while deleting golden tag: %w", err)
	}
	return nil
}

// finalizeTemplateDeleted removes the template from the store and returns the
// deleted record.
func (w *ActorWorkflow) finalizeTemplateDeleted(ctx context.Context, tmpl *ateapi.ActorTemplate) (_ *ateapi.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTemplateDeleted")
	defer func() { err = done(err) }()

	templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl)
	precondition := store.DeletePreconditions{UID: tmpl.GetMetadata().GetUid()}
	deleted, err := w.store.DeleteActorTemplate(ctx, templateRef, precondition)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("ActorTemplate %s not found", templateRef)
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("ActorTemplate %s does not have uid %s", templateRef, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting actor template from DB: %w", err)
	}
	return deleted, nil
}
