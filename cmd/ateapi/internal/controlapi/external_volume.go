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

func (s *RPCService) CreateExternalVolume(ctx context.Context, req *ateapipb.CreateExternalVolumeRequest) (*ateapipb.ExternalVolume, error) {
	inVolume := req.ExternalVolume
	if inVolume != nil {
		scrubResourceMetadataForCreate(inVolume.Metadata)
		inVolume.Refs = nil
		defaults.Apply(inVolume)
	}
	if errs := apivalidation.ValidateCreateExternalVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.externalVolumeWorkflow.CreateExternalVolume(ctx, inVolume)
}

func (s *ServiceImpl) CreateExternalVolume(ctx context.Context, volume *ateapipb.ExternalVolume) (*ateapipb.ExternalVolume, error) {
	return s.store.CreateExternalVolume(ctx, volume)
}

func (s *RPCService) GetExternalVolume(ctx context.Context, req *ateapipb.GetExternalVolumeRequest) (*ateapipb.ExternalVolume, error) {
	if errs := apivalidation.ValidateGetExternalVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	volumeRef := resources.ExternalVolumeRefFromObjectRef(req.GetExternalVolume())
	volume, err := s.impl.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while getting shared volume: %w", err)
	}
	return volume, nil
}

func (s *ServiceImpl) GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error) {
	return s.store.GetExternalVolume(ctx, volumeRef)
}

func (s *RPCService) ListExternalVolumes(ctx context.Context, req *ateapipb.ListExternalVolumesRequest) (*ateapipb.ListExternalVolumesResponse, error) {
	if errs := apivalidation.ValidateListExternalVolumesRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	page, err := s.impl.ListExternalVolumes(ctx, req.GetAtespace(), toStoreListOptions(req.GetOptions()))
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing shared volumes: %w", err))
	}
	return &ateapipb.ListExternalVolumesResponse{ExternalVolumes: page.Items, NextPageToken: page.NextPageToken}, nil
}

func (s *ServiceImpl) ListExternalVolumes(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ExternalVolume], error) {
	return s.store.ListExternalVolumes(ctx, atespace, opts)
}

func (s *RPCService) UpdateExternalVolume(ctx context.Context, req *ateapipb.UpdateExternalVolumeRequest) (*ateapipb.ExternalVolume, error) {
	inVolume := req.ExternalVolume
	if inVolume != nil {
		scrubResourceMetadataForUpdate(inVolume.Metadata)
		inVolume.Refs = nil
	}
	if errs := apivalidation.ValidateUpdateExternalVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	in := req.GetExternalVolume()
	volumeRef := resources.ExternalVolumeRefFromExternalVolume(in)

	stored, err := s.impl.UpdateExternalVolume(ctx, volumeRef, store.PreconditionFrom(in), func(toUpdate *ateapipb.ExternalVolume) error {
		metadata, refs := toUpdate.GetMetadata(), toUpdate.GetRefs()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, in)
		toUpdate.Metadata, toUpdate.Refs = metadata, refs
		defaults.Apply(toUpdate)
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrImmutableField) || errors.Is(err, store.ErrPreconditionRequired) {
			return nil, apierror.InvalidArgument("while updating shared volume %s: %v", volumeRef, err)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("ExternalVolume %s not found with uid %s", volumeRef, in.GetMetadata().GetUid())
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while updating shared volume: %w", err)
	}
	return stored, nil
}

func (s *ServiceImpl) UpdateExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.ExternalVolume) error) (*ateapipb.ExternalVolume, error) {
	return s.store.UpdateExternalVolume(ctx, volumeRef, precondition, func(toUpdate *ateapipb.ExternalVolume) error {
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		if errs := apivalidation.ValidateExternalVolumeUpdate(ctx, field.NewPath("external_volume"), toUpdate, oldVal); len(errs) > 0 {
			return resources.ToAPIError(errs)
		}
		return nil
	})
}

func (s *RPCService) DeleteExternalVolume(ctx context.Context, req *ateapipb.DeleteExternalVolumeRequest) (*ateapipb.ExternalVolume, error) {
	if errs := apivalidation.ValidateDeleteExternalVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.externalVolumeWorkflow.DeleteExternalVolume(ctx, resources.ExternalVolumeRefFromObjectRef(req.GetExternalVolume()), toDeletePreconditions(req.GetOptions()))
}

func (s *ServiceImpl) DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) (*ateapipb.ExternalVolume, error) {
	return s.store.DeleteExternalVolume(ctx, volumeRef, precondition)
}
