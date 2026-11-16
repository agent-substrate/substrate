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
	"go.ate.dev/substrate/cmd/ateapi/internal/defaults"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateExternalVolume(ctx context.Context, req *ateapi.CreateExternalVolumeRequest) (*ateapi.ExternalVolume, error) {
	// The resource is new and nothing mounts it yet, so the gate guards only
	// its creation. Reads and deletes work with the gate off, so turning it
	// off cannot strand a record.
	if !preview.IsEnabledFor(ctx, preview.GateExternalVolumes) {
		return nil, resources.ToAPIError(field.ErrorList{field.Forbidden(
			field.NewPath("external_volume"),
			"requires the ExternalVolumes preview gate to be enabled on the server and the client to opt in to preview features (kubectl ate --preview, or the "+preview.Header+" header)")})
	}

	// First scrub any fields that users are not allowed to set, then fill the
	// defaults so validation sees the final resource state.
	inVolume := req.GetExternalVolume()
	if inVolume != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inVolume.Metadata)
		defaults.Apply(inVolume)
	}

	// Validate the request, including the object within it.
	if errs := apivalidation.ValidateCreateExternalVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	volumeRef := resources.ExternalVolumeRefFromExternalVolume(inVolume)
	stored, err := s.impl.CreateExternalVolume(ctx, inVolume)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, apierror.AlreadyExists("ExternalVolume %s already exists", volumeRef)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, apierror.FailedPrecondition("Atespace %s not found", volumeRef.Atespace)
		}
		return nil, fmt.Errorf("while recording external volume: %w", err)
	}
	return stored, nil
}

func (s *ServiceImpl) CreateExternalVolume(ctx context.Context, volume *ateapi.ExternalVolume) (*ateapi.ExternalVolume, error) {
	return s.store.CreateExternalVolume(ctx, volume)
}

func (s *RPCService) GetExternalVolume(ctx context.Context, req *ateapi.GetExternalVolumeRequest) (*ateapi.ExternalVolume, error) {
	if errs := apivalidation.ValidateGetExternalVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	volumeRef := resources.ExternalVolumeRefFromObjectRef(req.GetExternalVolume())
	volume, err := s.impl.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while getting external volume from DB: %w", err)
	}
	return volume, nil
}

func (s *ServiceImpl) GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapi.ExternalVolume, error) {
	return s.store.GetExternalVolume(ctx, volumeRef)
}

func (s *RPCService) ListExternalVolumes(ctx context.Context, req *ateapi.ListExternalVolumesRequest) (*ateapi.ListExternalVolumesResponse, error) {
	if errs := apivalidation.ValidateListExternalVolumesRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	page, err := s.impl.ListExternalVolumes(ctx, req.GetAtespace(), toStoreListOptions(req.GetOptions()))
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing external volumes in db: %w", err))
	}
	return &ateapi.ListExternalVolumesResponse{
		ExternalVolumes: page.Items,
		NextPageToken:   page.NextPageToken,
	}, nil
}

func (s *ServiceImpl) ListExternalVolumes(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.ExternalVolume], error) {
	return s.store.ListExternalVolumes(ctx, atespace, opts)
}

// DeleteExternalVolume removes the record. The storage it names is left in
// place.
func (s *RPCService) DeleteExternalVolume(ctx context.Context, req *ateapi.DeleteExternalVolumeRequest) (*ateapi.ExternalVolume, error) {
	if errs := apivalidation.ValidateDeleteExternalVolumeRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	volumeRef := resources.ExternalVolumeRefFromObjectRef(req.GetExternalVolume())
	precondition := toDeletePreconditions(req.GetOptions())
	deleted, err := s.impl.DeleteExternalVolume(ctx, volumeRef, precondition)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("ExternalVolume %s not found", volumeRef)
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("ExternalVolume %s does not have uid %s", volumeRef, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting external volume from DB: %w", err)
	}
	return deleted, nil
}

func (s *ServiceImpl) DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) (*ateapi.ExternalVolume, error) {
	return s.store.DeleteExternalVolume(ctx, volumeRef, precondition)
}
