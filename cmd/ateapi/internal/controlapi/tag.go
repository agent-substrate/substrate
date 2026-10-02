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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
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
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, err
	}
	return tag, nil
}

func (s *ServiceImpl) CreateTag(ctx context.Context, tag *ateapipb.Tag) (*ateapipb.Tag, error) {
	// TODO: implement this
	return s.store.CreateTag(ctx, tag)
}

func (s *RPCService) GetTag(ctx context.Context, req *ateapipb.GetTagRequest) (*ateapipb.Tag, error) {
	if errs := apivalidation.ValidateGetTagRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	tagRef := resources.TagRefFromObjectRef(req.GetTag())
	tag, err := s.impl.GetTag(ctx, tagRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("Tag %s not found", tagRef)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting tag: %w", err)
	}
	return tag, nil
}

func (s *ServiceImpl) GetTag(ctx context.Context, tagRef resources.TagRef) (*ateapipb.Tag, error) {
	// TODO: implement this
	return s.store.GetTag(ctx, tagRef)
}

func (s *RPCService) ListTags(ctx context.Context, req *ateapipb.ListTagsRequest) (*ateapipb.ListTagsResponse, error) {
	if errs := apivalidation.ValidateListTagsRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	page, err := s.impl.ListTags(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing tags: %w", err))
	}
	return &ateapipb.ListTagsResponse{Tags: page.Items, NextPageToken: page.NextPageToken}, nil
}

func (s *ServiceImpl) ListTags(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.Tag], error) {
	// TODO: implement this
	return s.store.ListTags(ctx, atespace, opts)
}

// errTagPending is what the update's mutate closure returns when the stored
// tag's create never finished, so the caller can tell it apart from a store
// failure and answer FAILED_PRECONDITION.
var errTagPending = errors.New("tag is still being created")

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

	storedTag, err := s.impl.UpdateTag(ctx, tagRef, store.PreconditionFrom(in), func(toUpdate *ateapipb.Tag) error {
		// A tag whose create never finished names a partial copy. Publishing it
		// — or changing its scope at all — would hand out content that is still
		// being written, or may never be.
		if toUpdate.GetStatus().GetSnapshot().GetSnapshotUri() == "" {
			return errTagPending
		}
		// Metadata and status are server-owned fields.
		metadata, tagStatus := toUpdate.GetMetadata(), toUpdate.GetStatus()
		// Whole-object replace: clear first, so a field the client left unset is
		// cleared rather than kept from the stored tag. Merge cannot smuggle in
		// unknown fields because validation already rejected them, and a source
		// the client did not echo back is caught by the immutability check the
		// impl runs on the merged tag: a tag never moves between snapshots, so
		// it never moves between sources either.
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, in)
		// Restore the server-owned fields, discarding whatever the request
		// carried in them.
		toUpdate.Metadata, toUpdate.Status = metadata, tagStatus
		defaults.Apply(toUpdate)
		return nil
	})
	if err != nil {
		if errors.Is(err, errTagPending) {
			return nil, apierror.FailedPrecondition("Tag %s/%s is still being created", tagRef.Atespace, tagRef.Name)
		}
		if errors.Is(err, store.ErrImmutableField) {
			return nil, apierror.InvalidArgument("while updating tag %s/%s: %v", tagRef.Atespace, tagRef.Name, err)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("Tag %s/%s not found with uid %s", tagRef.Atespace, tagRef.Name, in.GetMetadata().GetUid())
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Tag %s/%s not found", tagRef.Atespace, tagRef.Name)
		}
		if errors.Is(err, store.ErrPreconditionRequired) {
			return nil, apierror.InvalidArgument("while updating tag %s/%s: %v", tagRef.Atespace, tagRef.Name, err)
		}
		return nil, fmt.Errorf("while updating tag: %w", err)
	}
	return storedTag, nil
}

func (s *ServiceImpl) UpdateTag(ctx context.Context, tagRef resources.TagRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.Tag) error) (*ateapipb.Tag, error) {
	return s.store.UpdateTag(ctx, tagRef, precondition, func(toUpdate *ateapipb.Tag) error {
		// Apply the mutation function to the stored value.
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate); err != nil {
			return err
		}

		// Validate the merged tag against the one it replaces. This is where
		// the rules the request could not be checked against land: scope, and
		// the immutability of metadata and source_actor.
		if errs := apivalidation.ValidateTagUpdate(ctx, field.NewPath("tag"), toUpdate, oldVal); len(errs) > 0 {
			return resources.ToAPIError(errs)
		}
		return nil
	})
}

// DeleteTag removes the tag and collects the external snapshot it owns.
func (s *RPCService) DeleteTag(ctx context.Context, req *ateapipb.DeleteTagRequest) (*ateapipb.Tag, error) {
	if errs := apivalidation.ValidateDeleteTagRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.actorWorkflow.DeleteTag(ctx, resources.TagRefFromObjectRef(req.GetTag()), toDeletePreconditions(req.GetOptions()))
}

func (s *ServiceImpl) DeleteTag(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (*ateapipb.Tag, error) {
	// TODO: implement this
	return s.store.DeleteTag(ctx, tagRef, precondition)
}
