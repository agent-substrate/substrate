// Copyright 2026 The Agent Substrate Authors
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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) GetExternalVolumeSnapshot(ctx context.Context, req *ateapi.GetExternalVolumeSnapshotRequest) (*ateapi.ExternalVolumeSnapshot, error) {
	if errs := apivalidation.ValidateGetExternalVolumeSnapshotRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	ref := resources.ExternalVolumeSnapshotRefFromObjectRef(req.GetExternalVolumeSnapshot())
	snapshot, err := s.impl.GetExternalVolumeSnapshot(ctx, ref)
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("ExternalVolumeSnapshot %s not found", ref)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting external volume snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *RPCService) ListExternalVolumeSnapshots(ctx context.Context, req *ateapi.ListExternalVolumeSnapshotsRequest) (*ateapi.ListExternalVolumeSnapshotsResponse, error) {
	if errs := apivalidation.ValidateListExternalVolumeSnapshotsRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	page, err := s.impl.ListExternalVolumeSnapshots(ctx, req.GetAtespace(), toStoreListOptions(req.GetOptions()))
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing external volume snapshots: %w", err))
	}
	return &ateapi.ListExternalVolumeSnapshotsResponse{ExternalVolumeSnapshots: page.Items, NextPageToken: page.NextPageToken}, nil
}

// CreateExternalVolumeSnapshot validates and stores a snapshot the server
// recorded.
func (s *ServiceImpl) CreateExternalVolumeSnapshot(ctx context.Context, snapshot *ateapi.ExternalVolumeSnapshot) (*ateapi.ExternalVolumeSnapshot, error) {
	if errs := apivalidation.ValidateExternalVolumeSnapshot(ctx, snapshot); len(errs) > 0 {
		return nil, resources.ToInternalError(errs)
	}
	return s.store.CreateExternalVolumeSnapshot(ctx, snapshot)
}

// Pass-through.
func (s *ServiceImpl) GetExternalVolumeSnapshot(ctx context.Context, ref resources.ExternalVolumeSnapshotRef) (*ateapi.ExternalVolumeSnapshot, error) {
	return s.store.GetExternalVolumeSnapshot(ctx, ref)
}

// Pass-through.
func (s *ServiceImpl) ListExternalVolumeSnapshots(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.ExternalVolumeSnapshot], error) {
	return s.store.ListExternalVolumeSnapshots(ctx, atespace, opts)
}

// UpdateExternalVolumeSnapshot applies mutate and validates the result as an
// update of the stored snapshot. Changing a field marked `+k8s:immutable`
// returns an error matching store.ErrImmutableField, as the store does. Other
// validation failures return an Internal error.
func (s *ServiceImpl) UpdateExternalVolumeSnapshot(ctx context.Context, ref resources.ExternalVolumeSnapshotRef, precondition store.Precondition, mutate func(*ateapi.ExternalVolumeSnapshot) error) (*ateapi.ExternalVolumeSnapshot, error) {
	return s.store.UpdateExternalVolumeSnapshot(ctx, ref, precondition, validatedExternalVolumeSnapshotMutation(ctx, mutate))
}

// Pass-through.
func (s *ServiceImpl) ListTagExternalVolumeSnapshots(ctx context.Context, tagRef resources.TagRef, tagUID string) ([]*ateapi.ExternalVolumeSnapshot, error) {
	return s.store.ListTagExternalVolumeSnapshots(ctx, tagRef, tagUID)
}

// UpdateTagExternalVolumeSnapshots applies mutate to each snapshot the tag
// owns and validates each result as an update of the stored snapshot.
func (s *ServiceImpl) UpdateTagExternalVolumeSnapshots(ctx context.Context, tagRef resources.TagRef, tagUID string, mutate func(*ateapi.ExternalVolumeSnapshot) error) ([]*ateapi.ExternalVolumeSnapshot, error) {
	return s.store.UpdateTagExternalVolumeSnapshots(ctx, tagRef, tagUID, validatedExternalVolumeSnapshotMutation(ctx, mutate))
}

// Pass-through.
func (s *ServiceImpl) DeleteExternalVolumeSnapshot(ctx context.Context, ref resources.ExternalVolumeSnapshotRef, precondition store.DeletePreconditions) (*ateapi.ExternalVolumeSnapshot, error) {
	return s.store.DeleteExternalVolumeSnapshot(ctx, ref, precondition)
}

// validatedExternalVolumeSnapshotMutation wraps mutate so that it fails when
// its result is not a valid update of the snapshot it was given.
func validatedExternalVolumeSnapshotMutation(ctx context.Context, mutate func(*ateapi.ExternalVolumeSnapshot) error) func(*ateapi.ExternalVolumeSnapshot) error {
	return func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		errs := apivalidation.ValidateExternalVolumeSnapshotUpdate(ctx, toUpdate, oldVal)
		if len(errs) == 0 {
			return nil
		}
		if slices.ContainsFunc(errs, func(e *field.Error) bool { return e.Origin == "immutable" }) {
			return fmt.Errorf("%w: %w", store.ErrImmutableField, resources.ToInternalError(errs))
		}
		return resources.ToInternalError(errs)
	}
}
