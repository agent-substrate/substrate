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
)

func (s *ServiceImpl) CreateActorSnapshot(ctx context.Context, snapshot *ateapipb.ActorSnapshot) (*ateapipb.ActorSnapshot, error) {
	// TODO: implement this
	return s.store.CreateActorSnapshot(ctx, snapshot)
}

func (s *RPCService) GetActorSnapshot(ctx context.Context, req *ateapipb.GetActorSnapshotRequest) (*ateapipb.ActorSnapshot, error) {
	if errs := validation.ValidateGetActorSnapshotRequest(req); len(errs) > 0 {
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

func (s *RPCService) GetActorSnapshotTag(ctx context.Context, req *ateapipb.GetActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validation.ValidateGetActorSnapshotTagRequest(req); len(errs) > 0 {
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

func (s *RPCService) ListActorSnapshots(ctx context.Context, req *ateapipb.ListActorSnapshotsRequest) (*ateapipb.ListActorSnapshotsResponse, error) {
	if errs := validation.ValidateListActorSnapshotsRequest(req); len(errs) > 0 {
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

func (s *RPCService) CreateActorSnapshotTag(ctx context.Context, req *ateapipb.CreateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validation.ValidateCreateActorSnapshotTagRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	ref := req.GetActorSnapshotTag().GetSnapshot()
	if req.GetActorSnapshotTag().GetMetadata().GetAtespace() != ref.GetAtespace() {
		return nil, status.Error(codes.FailedPrecondition, "ActorSnapshot tags must belong to the snapshot's Atespace")
	}
	tag, err := s.impl.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRefFromObjectRef(ref), req.GetActorSnapshotTag())
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

func (s *ServiceImpl) CreateActorSnapshotTag(ctx context.Context, snapshotRef resources.ActorSnapshotRef, tag *ateapipb.ActorSnapshotTag) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: implement this
	return s.store.CreateActorSnapshotTag(ctx, snapshotRef, tag)
}

func (s *RPCService) UpdateActorSnapshotTag(ctx context.Context, req *ateapipb.UpdateActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validation.ValidateUpdateActorSnapshotTagRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetActorSnapshotTag()
	atespace, name := in.GetMetadata().GetAtespace(), in.GetMetadata().GetName()
	tagRef := resources.ActorSnapshotTagRef{Atespace: atespace, Name: name}

	storedTag, err := s.impl.UpdateActorSnapshotTag(ctx, tagRef, store.PreconditionFrom(in), func(toUpdate *ateapipb.ActorSnapshotTag) error {
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

func (s *ServiceImpl) UpdateActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.ActorSnapshotTag) error) (*ateapipb.ActorSnapshotTag, error) {
	// TODO: implement this
	return s.store.UpdateActorSnapshotTag(ctx, tagRef, precondition, mutate)
}

func (s *RPCService) DeleteActorSnapshotTag(ctx context.Context, req *ateapipb.DeleteActorSnapshotTagRequest) (*ateapipb.ActorSnapshotTag, error) {
	if errs := validation.ValidateDeleteActorSnapshotTagRequest(req); len(errs) > 0 {
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
