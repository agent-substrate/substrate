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
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// CreateActorSnapshotTag tags the external snapshot a suspended Actor holds,
// giving the tag its own copy of that snapshot so the Actor being suspended
// again or deleted cannot collect it. The work is a workflow because it spans
// two transactions around an object copy; see TagActorSnapshot.
func (s *RPCService) CreateActorSnapshotTag(ctx context.Context, req *ateapipb.CreateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	// First scrub any fields that users are not allowed to set.
	inTag := req.ActorSnapshotTag
	if inTag != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inTag.Metadata)
		inTag.Status = nil
	}

	if errs := validateCreateActorSnapshotTagRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActorSnapshotTag().GetSourceActor())
	setSpanActorRefAttributes(ctx, actorRef)

	tag, err := s.actorWorkflow.TagActorSnapshot(ctx, req.GetActorSnapshotTag())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
		}
		return nil, err
	}
	return tag, nil
}

func (s *ServiceImpl) CreateActorSnapshotTag(ctx context.Context, tag *ateapipb.ActorSnapshotTag) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: implement this
	return s.store.CreateActorSnapshotTag(ctx, tag)
}

func validateCreateActorSnapshotTagRequest(ctx context.Context, req *ateapipb.CreateActorSnapshotTagRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateActorSnapshotTagRequest(ctx, op, nil, req, nil)
}

func ValidateCustom_CreateActorSnapshotTagRequest(_ context.Context, _ operation.Operation, p *field.Path, req, _ *ateapipb.CreateActorSnapshotTagRequest) field.ErrorList {
	tag := req.GetActorSnapshotTag()
	sourceActorAtespace := tag.GetSourceActor().GetAtespace()
	tagAtespace := tag.GetMetadata().GetAtespace()
	if sourceActorAtespace == "" || tagAtespace == "" {
		return nil // regular DV will handle it
	}
	if tagAtespace != sourceActorAtespace {
		return field.ErrorList{
			field.Invalid(p.Child("actor_snapshot_tag", "metadata", "atespace"), tagAtespace, "must match source_actor.atespace"),
		}
	}
	return nil
}

func (s *RPCService) GetActorSnapshotTag(ctx context.Context, req *ateapipb.GetActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validateGetActorSnapshotTagRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	tag, err := s.impl.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRefFromObjectRef(req.GetActorSnapshotTag()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshotTag not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting actor snapshot tag: %w", err)
	}
	return tag, nil
}

func (s *ServiceImpl) GetActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: implement this
	return s.store.GetActorSnapshotTag(ctx, tagRef)
}

func validateGetActorSnapshotTagRequest(ctx context.Context, req *ateapipb.GetActorSnapshotTagRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_GetActorSnapshotTagRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) ListActorSnapshotTags(ctx context.Context, req *ateapipb.ListActorSnapshotTagsRequest) (*ateapipb.ListActorSnapshotTagsResponse, error) {
	if errs := validateListActorSnapshotTagsRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	page, err := s.impl.ListActorSnapshotTags(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing actor snapshot tags: %w", err))
	}
	return &ateapipb.ListActorSnapshotTagsResponse{ActorSnapshotTags: page.Items, NextPageToken: page.NextPageToken}, nil
}

func (s *ServiceImpl) ListActorSnapshotTags(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorSnapshotTag], error) {
	// TODO: implement this
	return s.store.ListActorSnapshotTags(ctx, atespace, opts)
}

func validateListActorSnapshotTagsRequest(ctx context.Context, req *ateapipb.ListActorSnapshotTagsRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_ListActorSnapshotTagsRequest(ctx, op, nil, req, nil)
}

// errTagPending is what the update's mutate closure returns when the stored
// tag's create never finished, so the caller can tell it apart from a store
// failure and answer FAILED_PRECONDITION.
var errTagPending = errors.New("ActorSnapshotTag is still being created")

func (s *RPCService) UpdateActorSnapshotTag(ctx context.Context, req *ateapipb.UpdateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	// First scrub any fields that users are not allowed to set.
	inTag := req.ActorSnapshotTag
	if inTag != nil { // otherwise validation will flag it
		scrubResourceMetadataForUpdate(inTag.Metadata)
		inTag.Status = nil
	}

	if errs := validateUpdateActorSnapshotTagRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetActorSnapshotTag()
	tagRef := resources.ActorSnapshotTagRefFromActorSnapshotTag(in)

	storedTag, err := s.impl.UpdateActorSnapshotTag(ctx, tagRef, store.PreconditionFrom(in), func(toUpdate *ateapipb.ActorSnapshotTag) error {
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
		return nil
	})
	if err != nil {
		if errors.Is(err, errTagPending) {
			return nil, status.Errorf(codes.FailedPrecondition, "ActorSnapshotTag %s/%s is still being created", tagRef.Atespace, tagRef.Name)
		}
		if errors.Is(err, store.ErrImmutableField) {
			return nil, status.Errorf(codes.InvalidArgument, "while updating actor snapshot tag %s/%s: %v", tagRef.Atespace, tagRef.Name, err)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "ActorSnapshotTag %s/%s not found with uid %s", tagRef.Atespace, tagRef.Name, in.GetMetadata().GetUid())
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorSnapshotTag %s/%s not found", tagRef.Atespace, tagRef.Name)
		}
		if errors.Is(err, store.ErrPreconditionRequired) {
			return nil, status.Errorf(codes.InvalidArgument, "while updating actor snapshot tag %s/%s: %v", tagRef.Atespace, tagRef.Name, err)
		}
		return nil, fmt.Errorf("while updating actor snapshot tag: %w", err)
	}
	return storedTag, nil
}

func (s *ServiceImpl) UpdateActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.ActorSnapshotTag) error) (*ateapipb.ActorSnapshotTag, error) {
	return s.store.UpdateActorSnapshotTag(ctx, tagRef, precondition, func(toUpdate *ateapipb.ActorSnapshotTag) error {
		// Apply the mutation function to the stored value.
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate); err != nil {
			return err
		}

		// Validate the merged tag against the one it replaces. This is where
		// the rules the request could not be checked against land: scope, and
		// the immutability of metadata and source_actor.
		if errs := validateActorSnapshotTagUpdate(ctx, field.NewPath("actor_snapshot_tag"), toUpdate, oldVal); len(errs) > 0 {
			return toGRPCStatusError(errs)
		}
		return nil
	})
}

func validateUpdateActorSnapshotTagRequest(ctx context.Context, req *ateapipb.UpdateActorSnapshotTagRequest) field.ErrorList {
	// We model this as a create rather than an update because updates assume
	// the existence of a "current" value, which we do not have yet.  This is
	// validating the request itself. The result will be validated later, after
	// we have a current value to compare against.
	op := operation.Operation{Type: operation.Create}
	return Validate_UpdateActorSnapshotTagRequest(ctx, op, nil, req, nil)
}

func validateActorSnapshotTagUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapipb.ActorSnapshotTag) field.ErrorList {
	op := operation.Operation{Type: operation.Update}
	return Validate_ActorSnapshotTag(ctx, op, fldPath, newVal, oldVal)
}

// This exists only because nested subfield tags are not supported yet.
func ValidateCustom_UpdateActorSnapshotTagRequest_ActorSnapshotTag(ctx context.Context, op operation.Operation, fldPath *field.Path, tag, _ *ateapipb.ActorSnapshotTag) field.ErrorList {
	if tag == nil || tag.Metadata == nil {
		return nil // handled by DV
	}

	// Updates are validated in 2 steps: first the update request and then the
	// resource itself. DV for the request doesn't descend into the resource
	// metadata.  Once DV supports nested subfield tags, this can be changed to
	// something like:
	//   +k8s:subfield(metadata)=+k8s:subfield(atespace)=+k8s:required
	errs := Validate_ResourceMetadata(ctx, op, fldPath.Child("metadata"), tag.Metadata, nil)
	errs = append(errs, validate.RequiredValue(ctx, op, fldPath.Child("metadata", "atespace"), &tag.Metadata.Atespace, nil)...)
	return errs
}

// DeleteActorSnapshotTag releases the external snapshot the tag owns and then
// removes the row, in that order: the row is the only handle on that snapshot,
// so dropping it first would leak. A failure at any point fails the whole RPC;
// the client retries the same delete, which rediscovers the work from the row
// and resumes over whatever is left.
//
// The tag stays resolvable while its snapshot is being collected, so a
// CreateActor racing this delete can seed an Actor from content that is going
// away. That race is accepted for now.
//
// Note that this destroys the external snapshot: an Actor created from the tag
// and never suspended is still borrowing it and becomes unrecoverable. Do not
// delete a tag while clones of it exist.
func (s *RPCService) DeleteActorSnapshotTag(ctx context.Context, req *ateapipb.DeleteActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: mode delete orchestration to a workflow.
	if errs := validateDeleteActorSnapshotTagRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	tagRef := resources.ActorSnapshotTagRefFromObjectRef(req.GetActorSnapshotTag())

	// Serializes against a create of the same tag, whose copy would otherwise
	// keep writing into the prefix this is collecting.
	ctx, lease, err := acquireActorSnapshotTagLease(ctx, s.impl, tagRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	stored, err := s.impl.GetActorSnapshotTag(ctx, tagRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorSnapshotTag %s/%s not found", tagRef.Atespace, tagRef.Name)
		}
		return nil, fmt.Errorf("while getting actor snapshot tag: %w", err)
	}
	if err := s.releaseTagSnapshot(ctx, stored); err != nil {
		return nil, err
	}

	tag, err := s.impl.DeleteActorSnapshotTag(ctx, tagRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "ActorSnapshotTag %s/%s not found", tagRef.Atespace, tagRef.Name)
	}
	if err != nil {
		return nil, fmt.Errorf("while deleting actor snapshot tag: %w", err)
	}
	return tag, nil
}

// releaseTagSnapshot deletes the objects the tag's external snapshot is made
// of. It tolerates a partly-collected snapshot, so a retry finishes cleanly.
// It collects the in-progress snapshot too.
func (s *RPCService) releaseTagSnapshot(ctx context.Context, tag *ateapipb.ActorSnapshotTag) error {
	if s.objectStore == nil {
		return nil
	}
	atespace, name := tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName()
	for _, snapshotURI := range []string{
		tag.GetStatus().GetSnapshot().GetSnapshotUri(),
		tag.GetStatus().GetInProgressSnapshotUri(),
	} {
		if snapshotURI == "" {
			continue
		}
		uri, err := resources.ParseSnapshotURI(snapshotURI)
		if err != nil {
			return fmt.Errorf("while parsing the external snapshot %q of tag %s/%s: %w", snapshotURI, atespace, name, err)
		}
		if err := objectstore.DeletePrefix(ctx, s.objectStore, uri.Prefix()); err != nil {
			return fmt.Errorf("while releasing the external snapshot %q of tag %s/%s: %w", snapshotURI, atespace, name, err)
		}
	}
	return nil
}

func (s *ServiceImpl) DeleteActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: implement this
	return s.store.DeleteActorSnapshotTag(ctx, tagRef)
}

func validateDeleteActorSnapshotTagRequest(ctx context.Context, req *ateapipb.DeleteActorSnapshotTagRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteActorSnapshotTagRequest(ctx, op, nil, req, nil)
}
