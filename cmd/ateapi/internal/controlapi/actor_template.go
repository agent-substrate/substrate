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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/admission"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func (s *RPCService) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	// First scrub any fields that users are not allowed to set, then fill the
	// defaults so validation sees the final resource state.
	in := req.GetActorTemplate()
	if in != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(in.Metadata)
		in.Status = nil
		defaults.Apply(in)
	}

	// Validate the request, including the object within it.
	if errs := apivalidation.ValidateCreateActorTemplateRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	templateRef := resources.ActorTemplateRefFromActorTemplate(in)

	stored, err := s.admission.CreateActorTemplate(ctx, in)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		switch {
		case errors.Is(err, store.ErrAlreadyExists):
			return nil, apierror.AlreadyExists("ActorTemplate %s already exists", templateRef)
		case errors.Is(err, store.ErrFailedPrecondition):
			return nil, apierror.FailedPrecondition("%v", err)
		case errors.Is(err, admission.ErrInvalid):
			return nil, apierror.InvalidArgument("%w", err)
		case errors.Is(err, admission.ErrFailedPrecondition):
			return nil, apierror.FailedPrecondition("%w", err)
		default:
			return nil, fmt.Errorf("while recording actor template: %w", err)
		}
	}

	return stored, nil
}

func (s *RPCService) GetActorTemplate(ctx context.Context, req *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := apivalidation.ValidateGetActorTemplateRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	templateRef := resources.ActorTemplateRefFromObjectRef(req.GetActorTemplate())
	template, err := s.admission.GetActorTemplate(ctx, templateRef)
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("ActorTemplate %s not found", templateRef)
	} else if err != nil {
		return nil, fmt.Errorf("while getting actor template from DB: %w", err)
	}

	return template, nil
}

func (s *RPCService) ListActorTemplates(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	if errs := apivalidation.ValidateListActorTemplatesRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	page, err := s.admission.ListActorTemplates(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing actor templates in db: %w", err))
	}
	return &ateapipb.ListActorTemplatesResponse{
		ActorTemplates: page.Items,
		NextPageToken:  page.NextPageToken,
	}, nil
}

func (s *RPCService) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := apivalidation.ValidateDeleteActorTemplateRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.actorWorkflow.DeleteActorTemplate(ctx, resources.ActorTemplateRefFromObjectRef(req.GetActorTemplate()), toDeletePreconditions(req.GetOptions()))
}

// errActorTemplateNotFound matches (via errors.Is) resolution failures where
// the actor names a template that does not exist. Most callers return the
// error as is — it already carries FailedPrecondition — while delete
// tolerates it and cleans up without the template.
var errActorTemplateNotFound = apierror.FailedPrecondition("actor template not found")

// resolveActorTemplate resolves the substrate ActorTemplate the actor's
// actor_template ref names. A missing template surfaces as
// errActorTemplateNotFound.
func resolveActorTemplate(ctx context.Context, admission *admission.Admission, actor *ateapipb.Actor) (*ateapipb.ActorTemplate, error) {
	templateRef := resources.ActorTemplateRefFromObjectRef(actor.GetActorTemplate())
	template, err := admission.GetActorTemplate(ctx, templateRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("%w; ObjectRef: %s ", errActorTemplateNotFound, templateRef)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	return template, nil
}

// actorTemplateObjectRef returns a fresh copy of the actor's template
// reference — fresh so records built from it never alias the actor message.
func actorTemplateObjectRef(actor *ateapipb.Actor) *ateapipb.ObjectRef {
	ref := actor.GetActorTemplate()
	if ref == nil {
		return nil
	}
	return &ateapipb.ObjectRef{Atespace: ref.GetAtespace(), Name: ref.GetName()}
}
