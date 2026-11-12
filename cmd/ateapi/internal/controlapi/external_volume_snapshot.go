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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// requireExternalVolumes refuses a request for ExternalVolumeSnapshots unless
// the ExternalVolumes gate is enabled for it.
func requireExternalVolumes(ctx context.Context, fldPath *field.Path) error {
	if preview.IsEnabledFor(ctx, preview.GateExternalVolumes) {
		return nil
	}
	return resources.ToAPIError(field.ErrorList{field.Forbidden(fldPath,
		"ExternalVolumeSnapshots require the ExternalVolumes preview gate to be enabled on the server and the client to opt in to preview features (kubectl ate --preview, or the "+preview.Header+" header)")})
}

func (s *RPCService) GetExternalVolumeSnapshot(ctx context.Context, req *ateapi.GetExternalVolumeSnapshotRequest) (*ateapi.ExternalVolumeSnapshot, error) {
	if err := requireExternalVolumes(ctx, field.NewPath("external_volume_snapshot")); err != nil {
		return nil, err
	}
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
	if err := requireExternalVolumes(ctx, field.NewPath("atespace")); err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateListExternalVolumeSnapshotsRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	page, err := s.impl.ListExternalVolumeSnapshots(ctx, req.GetAtespace(), toStoreListOptions(req.GetOptions()))
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing external volume snapshots: %w", err))
	}
	return &ateapi.ListExternalVolumeSnapshotsResponse{ExternalVolumeSnapshots: page.Items, NextPageToken: page.NextPageToken}, nil
}

// Pass-through.
func (s *ServiceImpl) CreateExternalVolumeSnapshot(ctx context.Context, snapshot *ateapi.ExternalVolumeSnapshot) (*ateapi.ExternalVolumeSnapshot, error) {
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

// Pass-through.
func (s *ServiceImpl) UpdateExternalVolumeSnapshot(ctx context.Context, ref resources.ExternalVolumeSnapshotRef, precondition store.Precondition, mutate func(*ateapi.ExternalVolumeSnapshot) error) (*ateapi.ExternalVolumeSnapshot, error) {
	return s.store.UpdateExternalVolumeSnapshot(ctx, ref, precondition, mutate)
}
