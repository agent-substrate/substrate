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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/fieldmask"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// actorSnapshotTagScopes lists the scopes a client may set on an ActorSnapshotTag.
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

func (s *Service) GetActorSnapshot(ctx context.Context, req *ateapipb.GetActorSnapshotRequest) (*ateapipb.ActorSnapshot, error) {
	// TODO: extract into validateGetActorSnapshotRequest(req) field.ErrorList,
	// as done for other RPC methods, and return via toGRPCStatusError.
	if errs := resources.ValidateObjectRef(req.GetSnapshot(), field.NewPath("snapshot")); len(errs) > 0 {
		return nil, status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	snapshot, err := s.persistence.GetActorSnapshot(ctx, req.GetSnapshot().GetAtespace(), req.GetSnapshot().GetName())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshot not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting actor snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *Service) GetActorSnapshotTag(ctx context.Context, req *ateapipb.GetActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: extract into validateGetActorSnapshotTagRequest(req) field.ErrorList,
	// as done for other RPC methods, and return via toGRPCStatusError.
	if errs := resources.ValidateObjectRef(req.GetTag(), field.NewPath("tag")); len(errs) > 0 {
		return nil, status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	_, tag, err := s.persistence.GetActorSnapshotByTag(ctx, req.GetTag().GetAtespace(), req.GetTag().GetName())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "ActorSnapshot tag not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting actor snapshot tag: %w", err)
	}
	return tag, nil
}

func (s *Service) ListActorSnapshots(ctx context.Context, req *ateapipb.ListActorSnapshotsRequest) (*ateapipb.ListActorSnapshotsResponse, error) {
	// TODO: extract into validateListActorSnapshotsRequest(req) field.ErrorList,
	// as done for other RPC methods, and return via toGRPCStatusError.
	var fldPath *field.Path
	var errs field.ErrorList
	if req.GetAtespace() != "" {
		errs = append(errs, resources.ValidateResourceName(req.GetAtespace(), fldPath.Child("atespace"))...)
	}
	if req.GetPageSize() < 0 {
		errs = append(errs, field.Invalid(fldPath.Child("page_size"), req.GetPageSize(), "must be greater than or equal to 0"))
	}
	if len(errs) > 0 {
		return nil, status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	snapshots, nextToken, err := s.persistence.ListActorSnapshots(ctx, req.GetAtespace(), effectivePageSize(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, fmt.Errorf("while listing actor snapshots: %w", err)
	}
	return &ateapipb.ListActorSnapshotsResponse{Snapshots: snapshots, NextPageToken: nextToken}, nil
}

func (s *Service) CreateActorSnapshotTag(ctx context.Context, req *ateapipb.CreateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: extract into validateCreateActorSnapshotTagRequest(req) field.ErrorList,
	// folding in validateActorSnapshotTag below, as done for other RPC methods,
	// and return via toGRPCStatusError.
	if errs := resources.ValidateObjectRef(req.GetSnapshot(), field.NewPath("snapshot")); len(errs) > 0 {
		return nil, status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	if err := validateActorSnapshotTag(req.GetTag(), "tag"); err != nil {
		return nil, err
	}
	lock, _, ref, _, err := s.lockActorSnapshot(ctx, req.GetSnapshot(), nil)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if req.GetTag().GetMetadata().GetAtespace() != ref.GetAtespace() {
		return nil, status.Error(codes.FailedPrecondition, "ActorSnapshot tags must belong to the snapshot's Atespace")
	}
	atespaceLock, err := s.persistence.AcquireLock(lock.Context(), "lock:atespace:"+req.GetTag().GetMetadata().GetAtespace())
	if errors.Is(err, store.ErrLockConflict) {
		return nil, status.Error(codes.Aborted, "another operation is using this Atespace")
	}
	if err != nil {
		return nil, fmt.Errorf("while locking tag Atespace: %w", err)
	}
	defer atespaceLock.Close()
	exists, err := s.persistence.AtespaceExists(atespaceLock.Context(), req.GetTag().GetMetadata().GetAtespace())
	if err != nil {
		return nil, fmt.Errorf("while checking tag Atespace: %w", err)
	}
	if !exists {
		return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", req.GetTag().GetMetadata().GetAtespace())
	}
	tag, err := s.persistence.TagActorSnapshot(atespaceLock.Context(), ref.GetAtespace(), ref.GetName(), req.GetTag())
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, status.Errorf(codes.AlreadyExists, "ActorSnapshot tag %s/%s already exists", req.GetTag().GetMetadata().GetAtespace(), req.GetTag().GetMetadata().GetName())
	}
	if err != nil {
		return nil, fmt.Errorf("while tagging actor snapshot: %w", err)
	}
	return tag, nil
}

// actorSnapshotTagMutableFields lists the ActorSnapshotTag field paths a client
// may name in an UpdateActorSnapshotTag update_mask.
var actorSnapshotTagMutableFields = fieldmask.NewMutableFields("scope")

func (s *Service) UpdateActorSnapshotTag(ctx context.Context, req *ateapipb.UpdateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validateUpdateActorSnapshotTagRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetTag()
	atespace, name := in.GetMetadata().GetAtespace(), in.GetMetadata().GetName()

	storedTag, err := s.persistence.UpdateActorSnapshotTag(ctx, atespace, name, store.WithPrecondition(in, func(toUpdate *ateapipb.ActorSnapshotTag) error {
		fieldmask.Apply(toUpdate, in, req.GetUpdateMask())
		return nil
	}))
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "ActorSnapshot tag %s/%s not found with uid %s", atespace, name, in.GetMetadata().GetUid())
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ActorSnapshot tag %s/%s not found", atespace, name)
		}
		return nil, fmt.Errorf("while updating actor snapshot tag: %w", err)
	}
	return storedTag, nil
}

func validateUpdateActorSnapshotTagRequest(req *ateapipb.UpdateActorSnapshotTagRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	tag := req.GetTag()
	tagPath := fldPath.Child("tag")
	if tag == nil {
		return field.ErrorList{field.Required(tagPath, "")}
	}

	errs = append(errs, resources.ValidateResourceMetadataRef(tag.GetMetadata(), tagPath.Child("metadata"))...)

	errs = append(errs, fieldmask.Validate(req.GetUpdateMask(), actorSnapshotTagMutableFields, field.NewPath("update_mask"))...)

	if scope, p := tag.GetScope(), tagPath.Child("scope"); validateActorSnapshotTagScope(scope) != nil {
		errs = append(errs, field.NotSupported(p, scope.String(), actorSnapshotTagScopeNames))
	}

	return errs
}

func (s *Service) DeleteActorSnapshotTag(ctx context.Context, req *ateapipb.DeleteActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: extract into validateDeleteActorSnapshotTagRequest(req) field.ErrorList,
	// as done for other RPC methods, and return via toGRPCStatusError.
	if errs := resources.ValidateObjectRef(req.GetTag(), field.NewPath("tag")); len(errs) > 0 {
		return nil, status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	lock, _, _, _, err := s.lockActorSnapshot(ctx, nil, req.GetTag())
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	tag, err := s.persistence.DeleteActorSnapshotTag(lock.Context(), req.GetTag().GetAtespace(), req.GetTag().GetName())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "ActorSnapshot tag %s/%s not found", req.GetTag().GetAtespace(), req.GetTag().GetName())
	}
	if err != nil {
		return nil, fmt.Errorf("while deleting actor snapshot tag: %w", err)
	}
	return tag, nil
}

func (s *Service) getActorSnapshot(ctx context.Context, canonical, tag *ateapipb.ObjectRef) (*ateapipb.ActorSnapshot, *ateapipb.ObjectRef, *ateapipb.ActorSnapshotTag, error) {
	if canonical != nil && tag != nil {
		return nil, nil, nil, fmt.Errorf("getActorSnapshot: exactly one of canonical or tag must be set, got both")
	}
	if canonical != nil {
		snapshot, err := s.persistence.GetActorSnapshot(ctx, canonical.GetAtespace(), canonical.GetName())
		if err != nil {
			return nil, nil, nil, err
		}
		return snapshot, canonical, nil, nil
	}
	snapshot, resolvedTag, err := s.persistence.GetActorSnapshotByTag(ctx, tag.GetAtespace(), tag.GetName())
	if err != nil {
		return nil, nil, nil, err
	}
	resolvedCanonical := &ateapipb.ObjectRef{Atespace: snapshot.GetMetadata().GetAtespace(), Name: snapshot.GetMetadata().GetName()}
	return snapshot, resolvedCanonical, resolvedTag, nil
}

// lockActorSnapshot resolves and locks a snapshot by its canonical identity
// or by tag. Exactly one of canonical or tag must be set.
func (s *Service) lockActorSnapshot(ctx context.Context, canonical, tag *ateapipb.ObjectRef) (*store.Lock, *ateapipb.ActorSnapshot, *ateapipb.ObjectRef, *ateapipb.ActorSnapshotTag, error) {
	_, resolvedCanonical, _, err := s.getActorSnapshot(ctx, canonical, tag)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil, nil, nil, status.Error(codes.NotFound, "ActorSnapshot not found")
	}
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("while getting actor snapshot: %w", err)
	}
	lock, err := s.persistence.AcquireLock(ctx, "lock:actor-snapshot:"+resolvedCanonical.GetAtespace()+":"+resolvedCanonical.GetName())
	if errors.Is(err, store.ErrLockConflict) {
		return nil, nil, nil, nil, status.Error(codes.Aborted, "another operation is using this ActorSnapshot")
	}
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("while locking actor snapshot: %w", err)
	}
	snapshot, lockedCanonical, lockedTag, err := s.getActorSnapshot(lock.Context(), canonical, tag)
	if err != nil || resolvedCanonical.GetAtespace() != lockedCanonical.GetAtespace() || resolvedCanonical.GetName() != lockedCanonical.GetName() {
		lock.Close()
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, nil, nil, status.Error(codes.NotFound, "ActorSnapshot not found")
		}
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("while getting actor snapshot: %w", err)
		}
		return nil, nil, nil, nil, status.Error(codes.Aborted, "ActorSnapshot reference changed, please retry")
	}
	return lock, snapshot, lockedCanonical, lockedTag, nil
}

func validateActorSnapshotTag(tag *ateapipb.ActorSnapshotTag, name string) error {
	var fldPath *field.Path
	p := fldPath.Child(name)
	if tag == nil {
		return status.Error(codes.InvalidArgument, field.ErrorList{field.Required(p, "")}.ToAggregate().Error())
	}
	if errs := resources.ValidateObjectRef(&ateapipb.ObjectRef{Atespace: tag.GetMetadata().GetAtespace(), Name: tag.GetMetadata().GetName()}, p.Child("metadata")); len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return validateActorSnapshotTagScope(tag.GetScope())
}

func validateActorSnapshotTagScope(scope ateapipb.ActorSnapshotTagScope) error {
	if slices.Contains(actorSnapshotTagScopes, scope) {
		return nil
	}
	return status.Error(codes.InvalidArgument, "invalid ActorSnapshot tag scope")
}
