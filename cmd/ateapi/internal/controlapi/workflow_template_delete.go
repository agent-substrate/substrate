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
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DeleteActorTemplate executes the workflow to delete an ActorTemplate. The
// golden actor and tag go first and the row last, so a failed attempt leaves
// the row for a retry to rediscover them by.
func (w *ActorWorkflow) DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions) (*ateapipb.ActorTemplate, error) {
	// Serializes against the reconciler, which creates the golden actor and
	// tag under the same lease.
	ctx, lease, err := acquireLease(ctx, w.store, "lease:actortemplate:"+templateRef.Atespace+":"+templateRef.Name, "ActorTemplate "+templateRef.String())
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	tmpl, err := w.loadTemplateForDelete(ctx, templateRef)
	if err != nil {
		return nil, err
	}
	// Checked before the golden actor and tag go: a stale caller must not
	// destroy them.
	if err := precondition.Check(tmpl.GetMetadata()); err != nil {
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "ActorTemplate %s does not have uid %s", templateRef, precondition.UID)
		}
		return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
	}

	// Both are named after the template's uid, in the reserved atespace.
	goldenName := tmpl.GetMetadata().GetUid()
	if err := w.ensureGoldenActorDeleted(ctx, resources.ActorRef{Atespace: resources.GoldenActorAtespace, Name: goldenName}); err != nil {
		return nil, err
	}
	if err := w.ensureGoldenTagDeleted(ctx, resources.TagRef{Atespace: resources.GoldenActorAtespace, Name: goldenName}); err != nil {
		return nil, err
	}
	return w.finalizeTemplateDeleted(ctx, templateRef, precondition)
}

// loadTemplateForDelete fetches the current template record.
func (w *ActorWorkflow) loadTemplateForDelete(ctx context.Context, templateRef resources.ActorTemplateRef) (_ *ateapipb.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "LoadTemplateForDelete")
	defer func() { err = done(err) }()

	tmpl, err := w.store.GetActorTemplate(ctx, templateRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorTemplate %s not found", templateRef)
		}
		return nil, fmt.Errorf("while getting actor template %s: %w", templateRef, err)
	}
	return tmpl, nil
}

// ensureGoldenActorDeleted removes the template's golden actor, whatever state
// it is in. Absent is the ordinary case for a template whose golden snapshot
// never finished.
func (w *ActorWorkflow) ensureGoldenActorDeleted(ctx context.Context, goldenRef resources.ActorRef) (err error) {
	ctx, done := stepSpan(ctx, "DeleteGoldenActor")
	defer func() { err = done(err) }()

	if _, err := w.DeleteActor(ctx, goldenRef, true, store.DeletePreconditions{}); err != nil {
		if status.Code(err) == codes.NotFound {
			markSkipped(ctx, "golden actor already deleted")
			return nil
		}
		return fmt.Errorf("while deleting golden actor: %w", err)
	}
	return nil
}

// ensureGoldenTagDeleted removes the template's golden tag and the snapshot it
// holds.
func (w *ActorWorkflow) ensureGoldenTagDeleted(ctx context.Context, goldenTagRef resources.TagRef) (err error) {
	ctx, done := stepSpan(ctx, "DeleteGoldenTag")
	defer func() { err = done(err) }()

	if _, err := w.DeleteTag(ctx, goldenTagRef, store.DeletePreconditions{}); err != nil {
		if status.Code(err) == codes.NotFound {
			markSkipped(ctx, "golden tag already deleted")
			return nil
		}
		return fmt.Errorf("while deleting golden tag: %w", err)
	}
	return nil
}

// finalizeTemplateDeleted removes the template from the store and returns the
// deleted record.
func (w *ActorWorkflow) finalizeTemplateDeleted(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions) (_ *ateapipb.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTemplateDeleted")
	defer func() { err = done(err) }()

	deleted, err := w.store.DeleteActorTemplate(ctx, templateRef, precondition)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorTemplate %s not found", templateRef)
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "ActorTemplate %s does not have uid %s", templateRef, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting actor template from DB: %w", err)
	}
	return deleted, nil
}
