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
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/validation"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	// First scrub any fields that users are not allowed to set.
	in := req.GetActorTemplate()
	if in != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(in.Metadata)
		in.Status = nil
	}

	// Validate the request, including the object within it.
	if errs := validation.ValidateCreateActorTemplateRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	templateRef := resources.ActorTemplateRefFromActorTemplate(in)

	stored, err := s.impl.CreateActorTemplate(ctx, in)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "ActorTemplate %s already exists", templateRef)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, fmt.Errorf("while recording actor template: %w", err)
	}

	return stored, nil
}

func (s *ServiceImpl) CreateActorTemplate(ctx context.Context, inTemplate *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error) {
	// Build the stored object: status is server-owned and starts empty.
	// TODO: check that sandbox_config.config_name matches sandbox_class.
	outTemplate := proto.Clone(inTemplate).(*ateapipb.ActorTemplate)
	outTemplate.Status = &ateapipb.ActorTemplateStatus{}

	// Validate the final value before storing it.
	if errs := validation.ValidateActorTemplateUpdate(ctx, field.NewPath("actor_template"), outTemplate, inTemplate); len(errs) > 0 {
		return nil, toGRPCInternalError(errs)
	}

	return s.store.CreateActorTemplate(ctx, outTemplate)
}

func (s *RPCService) GetActorTemplate(ctx context.Context, req *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := validation.ValidateGetActorTemplateRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	templateRef := resources.ActorTemplateRefFromObjectRef(req.GetActorTemplate())
	template, err := s.impl.GetActorTemplate(ctx, templateRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "ActorTemplate %s not found", templateRef)
	} else if err != nil {
		return nil, fmt.Errorf("while getting actor template from DB: %w", err)
	}

	return template, nil
}

func (s *ServiceImpl) GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	// TODO: implement this
	return s.store.GetActorTemplate(ctx, templateRef)
}

func (s *RPCService) ListActorTemplates(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	if errs := validation.ValidateListActorTemplatesRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	page, err := s.impl.ListActorTemplates(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, fmt.Errorf("while listing actor templates in db: %w", err)
	}
	return &ateapipb.ListActorTemplatesResponse{
		ActorTemplates: page.Items,
		NextPageToken:  page.NextPageToken,
	}, nil
}

func (s *ServiceImpl) ListActorTemplates(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorTemplate], error) {
	// TODO: implement this
	return s.store.ListActorTemplates(ctx, atespace, opts)
}

func (s *RPCService) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if errs := validation.ValidateDeleteActorTemplateRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	templateRef := resources.ActorTemplateRefFromObjectRef(req.GetActorTemplate())
	deleted, err := s.impl.DeleteActorTemplate(ctx, templateRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorTemplate %s not found", templateRef)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, fmt.Errorf("while deleting actor template from DB: %w", err)
	}

	return deleted, nil
}

func (s *ServiceImpl) DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	// TODO: implement this
	return s.store.DeleteActorTemplate(ctx, templateRef)
}

func (s *ServiceImpl) UpdateActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.Precondition, mutate func(dbTemplate *ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error) {
	// ActorTemplates are immutable to clients: there is no update RPC, and
	// the only writer is the template reconciler, which updates status
	// against the store directly. The store enforces metadata immutability,
	// so this layer has nothing to add.
	return s.store.UpdateActorTemplate(ctx, templateRef, precondition, mutate)
}

// actorTemplateGetter is the storage subset template resolution needs.
type actorTemplateGetter interface {
	GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error)
}

// errActorTemplateNotFound matches (via errors.Is) resolution failures where
// the actor names a template that does not exist. Most callers return the
// error as is — it already carries FailedPrecondition — while delete
// tolerates it and cleans up without the template.
var errActorTemplateNotFound = status.New(codes.FailedPrecondition, "actor template not found").Err()

// resolveActorTemplate resolves the substrate ActorTemplate the actor's
// actor_template ref names. A missing template surfaces as
// errActorTemplateNotFound.
func resolveActorTemplate(ctx context.Context, st actorTemplateGetter, actor *ateapipb.Actor) (*ateapipb.ActorTemplate, error) {
	templateRef := resources.ActorTemplateRefFromObjectRef(actor.GetActorTemplate())
	template, err := st.GetActorTemplate(ctx, templateRef)
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
