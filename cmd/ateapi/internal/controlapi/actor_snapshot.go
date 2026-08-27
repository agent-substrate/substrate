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
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

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

func (s *ServiceImpl) CreateActorSnapshot(ctx context.Context, snapshot *ateapipb.ActorSnapshot) (*ateapipb.ActorSnapshot, error) {
	// TODO: implement this
	return s.store.CreateActorSnapshot(ctx, snapshot)
}

func (s *RPCService) GetActorSnapshot(ctx context.Context, req *ateapipb.GetActorSnapshotRequest) (*ateapipb.ActorSnapshot, error) {
	if errs := validateGetActorSnapshotRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	snapshot, err := s.impl.GetActorSnapshot(ctx, resources.ActorSnapshotRefFromObjectRef(req.GetActorSnapshot()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshot not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting actor snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *ServiceImpl) GetActorSnapshot(ctx context.Context, snapshotRef resources.ActorSnapshotRef) (*ateapipb.ActorSnapshot, error) {
	// TODO: implement this
	return s.store.GetActorSnapshot(ctx, snapshotRef)
}

func validateGetActorSnapshotRequest(ctx context.Context, req *ateapipb.GetActorSnapshotRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_GetActorSnapshotRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) GetActorSnapshotTag(ctx context.Context, req *ateapipb.GetActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validateGetActorSnapshotTagRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	tag, err := s.impl.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRefFromObjectRef(req.GetActorSnapshotTag()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshot tag not found")
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
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_GetActorSnapshotTagRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) ListActorSnapshots(ctx context.Context, req *ateapipb.ListActorSnapshotsRequest) (*ateapipb.ListActorSnapshotsResponse, error) {
	if errs := validateListActorSnapshotsRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	page, err := s.impl.ListActorSnapshots(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing actor snapshots: %w", err))
	}
	return &ateapipb.ListActorSnapshotsResponse{ActorSnapshots: page.Items, NextPageToken: page.NextPageToken}, nil
}

func (s *ServiceImpl) ListActorSnapshots(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorSnapshot], error) {
	// TODO: implement this
	return s.store.ListActorSnapshots(ctx, atespace, opts)
}

func validateListActorSnapshotsRequest(ctx context.Context, req *ateapipb.ListActorSnapshotsRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_ListActorSnapshotsRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) CreateActorSnapshotTag(ctx context.Context, req *ateapipb.CreateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	// First scrub any fields that users are not allowed to set.
	inTag := req.ActorSnapshotTag
	if inTag != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inTag.Metadata)
	}

	// Validate the request, including the object within it.
	if errs := validateCreateActorSnapshotTagRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	// Handle the creation, including validation of the final stored object.
	return s.impl.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRefFromObjectRef(inTag.GetSnapshot()), inTag)
}

func (s *ServiceImpl) CreateActorSnapshotTag(ctx context.Context, snapshotRef resources.ActorSnapshotRef, tag *ateapipb.ActorSnapshotTag) (*ateapipb.ActorSnapshotTag, error) {
	// A tag pins its snapshot against garbage collection through the owning
	// Atespace, so the two must live in the same one. This is a cross-field
	// rule declarative validation cannot express.
	atespace, name := tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName()
	if atespace != snapshotRef.Atespace {
		return nil, status.Error(codes.FailedPrecondition, "ActorSnapshot tags must belong to the snapshot's Atespace")
	}

	// Save the data in the storage layer.
	stored, err := s.store.CreateActorSnapshotTag(ctx, snapshotRef, tag)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "ActorSnapshot not found")
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", atespace)
		}
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "ActorSnapshot tag %s/%s already exists", atespace, name)
		}
		return nil, fmt.Errorf("while tagging actor snapshot: %w", err)
	}
	return stored, nil
}

func validateCreateActorSnapshotTagRequest(ctx context.Context, req *ateapipb.CreateActorSnapshotTagRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateActorSnapshotTagRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) UpdateActorSnapshotTag(ctx context.Context, req *ateapipb.UpdateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	// First scrub any fields that users are not allowed to set.
	inTag := req.ActorSnapshotTag
	if inTag != nil { // otherwise validation will flag it
		scrubResourceMetadataForUpdate(inTag.Metadata)
	}

	// Validate the request.
	if errs := validateUpdateActorSnapshotTagRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	atespace, name := inTag.GetMetadata().GetAtespace(), inTag.GetMetadata().GetName()
	tagRef := resources.ActorSnapshotTagRef{Atespace: atespace, Name: name}

	storedTag, err := s.impl.UpdateActorSnapshotTag(ctx, tagRef, store.PreconditionFrom(inTag), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		// Metadata is a server-owned field.
		metadata := toUpdate.GetMetadata()
		// Whole-object replace: clear first, so a field the client left unset is
		// cleared rather than kept from the stored tag. Merge cannot smuggle in
		// unknown fields because validation already rejected them.
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, inTag)
		// Restore metadata from the server.
		toUpdate.Metadata = metadata
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "ActorSnapshot tag %s/%s not found with uid %s", atespace, name, inTag.GetMetadata().GetUid())
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorSnapshot tag %s/%s not found", atespace, name)
		}
		if errors.Is(err, store.ErrPreconditionRequired) {
			return nil, status.Errorf(codes.InvalidArgument, "while updating actor snapshot tag %s/%s: %v", atespace, name, err)
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
		newVal := toUpdate

		// Validate the mutated value before doing any further work. This is
		// what enforces the immutable fields, since only the stored tag gives
		// declarative validation an old value to compare against.
		if errs := validateActorSnapshotTagUpdate(ctx, field.NewPath("actor_snapshot_tag"), newVal, oldVal); len(errs) > 0 {
			return toGRPCStatusError(errs)
		}

		return nil
	})
}

func validateUpdateActorSnapshotTagRequest(ctx context.Context, req *ateapipb.UpdateActorSnapshotTagRequest) field.ErrorList {
	// Call the generated validation.
	// We model this as a create rather than an update because updates assume
	// the existence of a "current" value, which we do not have yet.  This is
	// validating the request itself. The result will be validated later, after
	// we have a current value to compare against.
	op := operation.Operation{Type: operation.Create}
	return Validate_UpdateActorSnapshotTagRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) DeleteActorSnapshotTag(ctx context.Context, req *ateapipb.DeleteActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validateDeleteActorSnapshotTagRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	tag, err := s.impl.DeleteActorSnapshotTag(ctx, resources.ActorSnapshotTagRefFromObjectRef(req.GetActorSnapshotTag()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "ActorSnapshot tag %s/%s not found", req.GetActorSnapshotTag().GetAtespace(), req.GetActorSnapshotTag().GetName())
	}
	if err != nil {
		return nil, fmt.Errorf("while deleting actor snapshot tag: %w", err)
	}
	return tag, nil
}

func (s *ServiceImpl) DeleteActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: implement this
	return s.store.DeleteActorSnapshotTag(ctx, tagRef)
}

func validateDeleteActorSnapshotTagRequest(ctx context.Context, req *ateapipb.DeleteActorSnapshotTagRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteActorSnapshotTagRequest(ctx, op, nil, req, nil)
}

// validateActorSnapshotTagUpdate validates an ActorSnapshotTag against the
// previous stored value. It is what enforces the immutable fields, which need
// an old value to compare against.
func validateActorSnapshotTagUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapipb.ActorSnapshotTag) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Update}
	return Validate_ActorSnapshotTag(ctx, op, fldPath, newVal, oldVal)
}
