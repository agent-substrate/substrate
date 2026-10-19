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

	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/defaults"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateSharedVolume(ctx context.Context, req *ateapipb.CreateSharedVolumeRequest) (*ateapipb.SharedVolume, error) {
	inVolume := req.SharedVolume
	if inVolume != nil {
		scrubResourceMetadataForCreate(inVolume.Metadata)
		inVolume.Status = nil
		defaults.Apply(inVolume)
	}
	if errs := apivalidation.ValidateCreateSharedVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.sharedVolumeWorkflow.CreateSharedVolume(ctx, inVolume)
}

func (s *ServiceImpl) CreateSharedVolume(ctx context.Context, volume *ateapipb.SharedVolume) (*ateapipb.SharedVolume, error) {
	return s.store.CreateSharedVolume(ctx, volume)
}

func (s *RPCService) GetSharedVolume(ctx context.Context, req *ateapipb.GetSharedVolumeRequest) (*ateapipb.SharedVolume, error) {
	if errs := apivalidation.ValidateGetSharedVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	volumeRef := resources.SharedVolumeRefFromObjectRef(req.GetSharedVolume())
	volume, err := s.impl.GetSharedVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("SharedVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while getting shared volume: %w", err)
	}
	return volume, nil
}

func (s *ServiceImpl) GetSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef) (*ateapipb.SharedVolume, error) {
	return s.store.GetSharedVolume(ctx, volumeRef)
}

func (s *RPCService) ListSharedVolumes(ctx context.Context, req *ateapipb.ListSharedVolumesRequest) (*ateapipb.ListSharedVolumesResponse, error) {
	if errs := apivalidation.ValidateListSharedVolumesRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	page, err := s.impl.ListSharedVolumes(ctx, req.GetAtespace(), toStoreListOptions(req.GetOptions()))
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing shared volumes: %w", err))
	}
	return &ateapipb.ListSharedVolumesResponse{SharedVolumes: page.Items, NextPageToken: page.NextPageToken}, nil
}

func (s *ServiceImpl) ListSharedVolumes(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.SharedVolume], error) {
	return s.store.ListSharedVolumes(ctx, atespace, opts)
}

var errSharedVolumePending = errors.New("shared volume is still being created")

func (s *RPCService) UpdateSharedVolume(ctx context.Context, req *ateapipb.UpdateSharedVolumeRequest) (*ateapipb.SharedVolume, error) {
	inVolume := req.SharedVolume
	if inVolume != nil {
		scrubResourceMetadataForUpdate(inVolume.Metadata)
		inVolume.Status = nil
	}
	if errs := apivalidation.ValidateUpdateSharedVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	in := req.GetSharedVolume()
	volumeRef := resources.SharedVolumeRefFromSharedVolume(in)

	stored, err := s.impl.UpdateSharedVolume(ctx, volumeRef, store.PreconditionFrom(in), func(toUpdate *ateapipb.SharedVolume) error {
		if toUpdate.GetStatus().GetState() != ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY {
			return errSharedVolumePending
		}
		metadata, volumeStatus := toUpdate.GetMetadata(), toUpdate.GetStatus()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, in)
		toUpdate.Metadata, toUpdate.Status = metadata, volumeStatus
		defaults.Apply(toUpdate)
		return nil
	})
	if err != nil {
		if errors.Is(err, errSharedVolumePending) {
			return nil, apierror.FailedPrecondition("SharedVolume %s is still being created", volumeRef)
		}
		if errors.Is(err, store.ErrImmutableField) || errors.Is(err, store.ErrPreconditionRequired) {
			return nil, apierror.InvalidArgument("while updating shared volume %s: %v", volumeRef, err)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("SharedVolume %s not found with uid %s", volumeRef, in.GetMetadata().GetUid())
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("SharedVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while updating shared volume: %w", err)
	}
	return stored, nil
}

func (s *ServiceImpl) UpdateSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.SharedVolume) error) (*ateapipb.SharedVolume, error) {
	return s.store.UpdateSharedVolume(ctx, volumeRef, precondition, func(toUpdate *ateapipb.SharedVolume) error {
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		if errs := apivalidation.ValidateSharedVolumeUpdate(ctx, field.NewPath("shared_volume"), toUpdate, oldVal); len(errs) > 0 {
			return resources.ToAPIError(errs)
		}
		return nil
	})
}

func (s *RPCService) DeleteSharedVolume(ctx context.Context, req *ateapipb.DeleteSharedVolumeRequest) (*ateapipb.SharedVolume, error) {
	if errs := apivalidation.ValidateDeleteSharedVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.sharedVolumeWorkflow.DeleteSharedVolume(ctx, resources.SharedVolumeRefFromObjectRef(req.GetSharedVolume()), toDeletePreconditions(req.GetOptions()))
}

func (s *ServiceImpl) DeleteSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef, precondition store.DeletePreconditions) (*ateapipb.SharedVolume, error) {
	return s.store.DeleteSharedVolume(ctx, volumeRef, precondition)
}
