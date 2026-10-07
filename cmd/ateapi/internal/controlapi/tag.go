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

// CreateTag tags the external snapshot a suspended Actor holds, giving the
// tag its own copy of that snapshot so the Actor being suspended
// again or deleted cannot collect it. The work is a workflow because it spans
// two transactions around an object copy; see TagActorSnapshot.
func (s *RPCService) CreateTag(ctx context.Context, req *ateapipb.CreateTagRequest) (*ateapipb.Tag, error) {
	// First scrub any fields that users are not allowed to set, then fill the
	// defaults so validation sees the final resource state.
	inTag := req.Tag
	if inTag != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inTag.Metadata)
		inTag.Status = nil
		defaults.Apply(inTag)
	}

	if errs := apivalidation.ValidateCreateTagRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetTag().GetSourceActor())
	setSpanActorRefAttributes(ctx, actorRef)

	tag, err := s.actorWorkflow.TagActorSnapshot(ctx, req.GetTag())
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.FailedPrecondition("Actor %s not found", actorRef)
		}
		return nil, err
	}
	return tag, nil
}

func (s *RPCService) GetTag(ctx context.Context, req *ateapipb.GetTagRequest) (*ateapipb.Tag, error) {
	if errs := apivalidation.ValidateGetTagRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	tagRef := resources.TagRefFromObjectRef(req.GetTag())
	tag, err := s.admission.GetTag(ctx, tagRef)
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("Tag %s not found", tagRef)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting tag: %w", err)
	}
	return tag, nil
}

func (s *RPCService) ListTags(ctx context.Context, req *ateapipb.ListTagsRequest) (*ateapipb.ListTagsResponse, error) {
	if errs := apivalidation.ValidateListTagsRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	page, err := s.admission.ListTags(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing tags: %w", err))
	}
	return &ateapipb.ListTagsResponse{Tags: page.Items, NextPageToken: page.NextPageToken}, nil
}

func (s *RPCService) UpdateTag(ctx context.Context, req *ateapipb.UpdateTagRequest) (*ateapipb.Tag, error) {
	// First scrub any fields that users are not allowed to set.
	inTag := req.Tag
	if inTag != nil { // otherwise validation will flag it
		scrubResourceMetadataForUpdate(inTag.Metadata)
		inTag.Status = nil
	}

	if errs := apivalidation.ValidateUpdateTagRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	in := req.GetTag()
	tagRef := resources.TagRefFromTag(in)

	storedTag, err := s.admission.UpdateTagSpec(ctx, in)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		switch {
		case errors.Is(err, admission.ErrFailedPrecondition):
			return nil, apierror.FailedPrecondition("%w", err)
		case errors.Is(err, store.ErrVersionConflict):
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		case errors.Is(err, store.ErrUIDConflict):
			return nil, apierror.Aborted("Tag %s/%s not found with uid %s", tagRef.Atespace, tagRef.Name, in.GetMetadata().GetUid())
		case errors.Is(err, store.ErrNotFound):
			return nil, apierror.NotFound("Tag %s/%s not found", tagRef.Atespace, tagRef.Name)
		case errors.Is(err, store.ErrPreconditionRequired):
			return nil, apierror.InvalidArgument("while updating tag %s/%s: %v", tagRef.Atespace, tagRef.Name, err)
		case errors.Is(err, admission.ErrInvalid):
			return nil, apierror.InvalidArgument("%w", err)
		default:
			return nil, fmt.Errorf("while updating tag: %w", err)
		}
	}
	return storedTag, nil
}

// DeleteTag removes the tag and collects the external snapshot it owns.
func (s *RPCService) DeleteTag(ctx context.Context, req *ateapipb.DeleteTagRequest) (*ateapipb.Tag, error) {
	if errs := apivalidation.ValidateDeleteTagRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.actorWorkflow.DeleteTag(ctx, resources.TagRefFromObjectRef(req.GetTag()), toDeletePreconditions(req.GetOptions()))
}
