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
	"slices"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// actorSnapshotTagScopes lists the scopes a client may set on an ActorSnapshotTag.
// ACTOR_SNAPSHOT_TAG_SCOPE_UNSPECIFIED is deliberately absent: scope is required
// on the wire, not defaulted. See validateActorSnapshotTagScope.
var actorSnapshotTagScopes = []ateapipb.ActorSnapshotTagScope{
	ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
}

// actorSnapshotTagScopeNames names actorSnapshotTagScopes for error messages.
var actorSnapshotTagScopeNames = func() []string {
	names := make([]string, len(actorSnapshotTagScopes))
	for i, scope := range actorSnapshotTagScopes {
		names[i] = scope.String()
	}
	return names
}()

func (s *RPCService) GetActorSnapshot(ctx context.Context, req *ateapipb.GetActorSnapshotRequest) (*ateapipb.ActorSnapshot, error) {
	if errs := validateGetActorSnapshotRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	snapshot, err := s.persistence.GetActorSnapshot(ctx, req.GetActorSnapshot().GetAtespace(), req.GetActorSnapshot().GetName())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshot not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting actor snapshot: %w", err)
	}
	return snapshot, nil
}

func validateGetActorSnapshotRequest(req *ateapipb.GetActorSnapshotRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.ActorSnapshot, fldPath.Child("actor_snapshot"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}

func (s *RPCService) GetActorSnapshotTag(ctx context.Context, req *ateapipb.GetActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validateGetActorSnapshotTagRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	tag, err := s.persistence.GetActorSnapshotTag(ctx, req.GetActorSnapshotTag().GetAtespace(), req.GetActorSnapshotTag().GetName())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshot tag not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting actor snapshot tag: %w", err)
	}
	return tag, nil
}

func validateGetActorSnapshotTagRequest(req *ateapipb.GetActorSnapshotTagRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.ActorSnapshotTag, fldPath.Child("actor_snapshot_tag"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}

func (s *RPCService) ListActorSnapshots(ctx context.Context, req *ateapipb.ListActorSnapshotsRequest) (*ateapipb.ListActorSnapshotsResponse, error) {
	if errs := validateListActorSnapshotsRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	page, err := s.persistence.ListActorSnapshots(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing actor snapshots: %w", err))
	}
	return &ateapipb.ListActorSnapshotsResponse{ActorSnapshots: page.Items, NextPageToken: page.NextPageToken}, nil
}

func validateListActorSnapshotsRequest(req *ateapipb.ListActorSnapshotsRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	// An empty atespace is allowed here and means "all atespaces".
	if val, fldPath := req.Atespace, fldPath.Child("atespace"); val != "" {
		errs = append(errs, resources.ValidateResourceName(val, fldPath)...)
	}

	if val, fldPath := req.PageSize, fldPath.Child("page_size"); val < 0 {
		errs = append(errs, field.Invalid(fldPath, val, "must be greater than or equal to 0"))
	}

	return errs
}

func (s *RPCService) CreateActorSnapshotTag(ctx context.Context, req *ateapipb.CreateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validateCreateActorSnapshotTagRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	ref := req.GetActorSnapshotTag().GetSnapshot()
	if req.GetActorSnapshotTag().GetMetadata().GetAtespace() != ref.GetAtespace() {
		return nil, status.Error(codes.FailedPrecondition, "ActorSnapshot tags must belong to the snapshot's Atespace")
	}
	tag, err := s.persistence.CreateActorSnapshotTag(ctx, ref.GetAtespace(), ref.GetName(), req.GetActorSnapshotTag())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshot not found")
	}
	if errors.Is(err, store.ErrFailedPrecondition) {
		return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", req.GetActorSnapshotTag().GetMetadata().GetAtespace())
	}
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, status.Errorf(codes.AlreadyExists, "ActorSnapshot tag %s/%s already exists", req.GetActorSnapshotTag().GetMetadata().GetAtespace(), req.GetActorSnapshotTag().GetMetadata().GetName())
	}
	if err != nil {
		return nil, fmt.Errorf("while tagging actor snapshot: %w", err)
	}
	return tag, nil
}

func validateCreateActorSnapshotTagRequest(req *ateapipb.CreateActorSnapshotTagRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	tag := req.ActorSnapshotTag
	tagPath := fldPath.Child("actor_snapshot_tag")
	if tag == nil {
		errs = append(errs, field.Required(tagPath, ""))
		return errs
	}

	errs = append(errs, resources.ValidateObjectRef(&ateapipb.ObjectRef{Atespace: tag.GetMetadata().GetAtespace(), Name: tag.GetMetadata().GetName()}, tagPath.Child("metadata"))...)

	if val, p := tag.Snapshot, tagPath.Child("snapshot"); val == nil {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, p)...)
	}

	errs = append(errs, validateActorSnapshotTagScope(tag.GetScope(), tagPath.Child("scope"))...)

	return errs
}

func (s *RPCService) UpdateActorSnapshotTag(ctx context.Context, req *ateapipb.UpdateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validateUpdateActorSnapshotTagRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetActorSnapshotTag()
	atespace, name := in.GetMetadata().GetAtespace(), in.GetMetadata().GetName()

	storedTag, err := s.persistence.UpdateActorSnapshotTag(ctx, atespace, name, store.PreconditionFrom(in), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		// Metadata is a server-owned field.
		metadata := toUpdate.GetMetadata()
		// Whole-object replace: clear first, so a field the client left unset is
		// cleared rather than kept from the stored tag. Merge cannot smuggle in
		// unknown fields because validation already rejected them.
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, in)
		// Restore metadata from the server.
		toUpdate.Metadata = metadata
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrImmutableField) {
			return nil, status.Errorf(codes.InvalidArgument, "while updating actor snapshot tag %s/%s: %v", atespace, name, err)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "ActorSnapshot tag %s/%s not found with uid %s", atespace, name, in.GetMetadata().GetUid())
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

func validateUpdateActorSnapshotTagRequest(req *ateapipb.UpdateActorSnapshotTagRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	tag := req.GetActorSnapshotTag()
	tagPath := fldPath.Child("actor_snapshot_tag")
	if tag == nil {
		return field.ErrorList{field.Required(tagPath, "")}
	}

	errs = append(errs, validateNoUnknownFields(tag, tagPath)...)

	errs = append(errs, resources.ValidateUpdateMetadataRef(tag.GetMetadata(), tagPath.Child("metadata"))...)

	errs = append(errs, validateActorSnapshotTagScope(tag.GetScope(), tagPath.Child("scope"))...)

	return errs
}

func (s *RPCService) DeleteActorSnapshotTag(ctx context.Context, req *ateapipb.DeleteActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validateDeleteActorSnapshotTagRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	tag, err := s.persistence.DeleteActorSnapshotTag(ctx, req.GetActorSnapshotTag().GetAtespace(), req.GetActorSnapshotTag().GetName())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "ActorSnapshot tag %s/%s not found", req.GetActorSnapshotTag().GetAtespace(), req.GetActorSnapshotTag().GetName())
	}
	if err != nil {
		return nil, fmt.Errorf("while deleting actor snapshot tag: %w", err)
	}
	return tag, nil
}

func validateDeleteActorSnapshotTagRequest(req *ateapipb.DeleteActorSnapshotTagRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.ActorSnapshotTag, fldPath.Child("actor_snapshot_tag"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}

// validateActorSnapshotTagScope checks that scope is one a client may set.
func validateActorSnapshotTagScope(scope ateapipb.ActorSnapshotTagScope, p *field.Path) field.ErrorList {
	switch {
	case scope == ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_UNSPECIFIED:
		return field.ErrorList{field.Required(p, "must be one of: "+strings.Join(actorSnapshotTagScopeNames, ", "))}
	case !slices.Contains(actorSnapshotTagScopes, scope):
		return field.ErrorList{field.NotSupported(p, scope.String(), actorSnapshotTagScopeNames)}
	}
	return nil
}
