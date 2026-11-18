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
	"strings"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/defaults"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateCsiDriverConfig(ctx context.Context, req *ateapi.CreateCsiDriverConfigRequest) (*ateapi.CsiDriverConfig, error) {
	in := proto.CloneOf(req.GetCsiDriverConfig())
	if in != nil {
		scrubResourceMetadataForCreate(in.Metadata)
		defaults.Apply(in)
	}
	if errs := apivalidation.ValidateCreateCsiDriverConfigRequest(ctx, &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: in}); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.CreateCsiDriverConfig(ctx, in)
}

func (s *ServiceImpl) CreateCsiDriverConfig(ctx context.Context, config *ateapi.CsiDriverConfig) (*ateapi.CsiDriverConfig, error) {
	stored, err := s.store.CreateCsiDriverConfig(ctx, config)
	if errors.Is(err, store.ErrAlreadyExists) {
		if strings.Contains(err.Error(), "driver_name") {
			return nil, apierror.AlreadyExists("CsiDriverConfig for driver_name %q already exists", config.GetDriverName())
		}
		return nil, apierror.AlreadyExists("CsiDriverConfig %q already exists", config.GetMetadata().GetName())
	}
	if err != nil {
		return nil, fmt.Errorf("while recording csi driver config: %w", err)
	}
	return stored, nil
}

func (s *RPCService) GetCsiDriverConfig(ctx context.Context, req *ateapi.GetCsiDriverConfigRequest) (*ateapi.CsiDriverConfig, error) {
	if errs := apivalidation.ValidateGetCsiDriverConfigRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.GetCsiDriverConfig(ctx, req.GetCsiDriverConfig().GetName())
}

func (s *ServiceImpl) GetCsiDriverConfig(ctx context.Context, name string) (*ateapi.CsiDriverConfig, error) {
	config, err := s.store.GetCsiDriverConfig(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("CsiDriverConfig %s not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting csi driver config from DB: %w", err)
	}
	return config, nil
}

func (s *ServiceImpl) GetCsiDriverConfigByDriverName(ctx context.Context, driverName string) (*ateapi.CsiDriverConfig, error) {
	return s.store.GetCsiDriverConfigByDriverName(ctx, driverName)
}

func (s *RPCService) ListCsiDriverConfigs(ctx context.Context, req *ateapi.ListCsiDriverConfigsRequest) (*ateapi.ListCsiDriverConfigsResponse, error) {
	if errs := apivalidation.ValidateListCsiDriverConfigsRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	page, err := s.impl.ListCsiDriverConfigs(ctx, toStoreListOptions(req.GetOptions()))
	if err != nil {
		return nil, err
	}
	return &ateapi.ListCsiDriverConfigsResponse{
		CsiDriverConfigs: page.Items,
		NextPageToken:    page.NextPageToken,
	}, nil
}

func (s *ServiceImpl) ListCsiDriverConfigs(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.CsiDriverConfig], error) {
	page, err := s.store.ListCsiDriverConfigs(ctx, opts)
	if err != nil {
		return page, mapListError(fmt.Errorf("while listing csi driver configs in db: %w", err))
	}
	return page, nil
}

func (s *RPCService) UpdateCsiDriverConfig(ctx context.Context, req *ateapi.UpdateCsiDriverConfigRequest) (*ateapi.CsiDriverConfig, error) {
	in := proto.CloneOf(req.GetCsiDriverConfig())
	if in != nil {
		scrubResourceMetadataForUpdate(in.Metadata)
	}
	if errs := apivalidation.ValidateUpdateCsiDriverConfigRequest(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: in}); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.UpdateCsiDriverConfig(ctx, in.GetMetadata().GetName(), store.PreconditionFrom(in), func(toUpdate *ateapi.CsiDriverConfig) error {
		metadata := toUpdate.GetMetadata()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, in)
		toUpdate.Metadata = metadata
		defaults.Apply(toUpdate)
		return nil
	})
}

func (s *ServiceImpl) UpdateCsiDriverConfig(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapi.CsiDriverConfig) error) (*ateapi.CsiDriverConfig, error) {
	updated, err := s.store.UpdateCsiDriverConfig(ctx, name, precondition, func(toUpdate *ateapi.CsiDriverConfig) error {
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		if errs := apivalidation.ValidateCsiDriverConfigUpdate(ctx, field.NewPath("csi_driver_config"), toUpdate, oldVal); len(errs) > 0 {
			return resources.ToAPIError(errs)
		}
		return nil
	})
	switch {
	case err == nil:
		return updated, nil
	case errors.Is(err, store.ErrNotFound):
		return nil, apierror.NotFound("CsiDriverConfig %s not found", name)
	case errors.Is(err, store.ErrPreconditionRequired), errors.Is(err, store.ErrImmutableField):
		return nil, apierror.InvalidArgument("while updating csi driver config %s: %v", name, err)
	case errors.Is(err, store.ErrUIDConflict):
		return nil, apierror.Aborted("CsiDriverConfig %s does not have uid %s", name, precondition.UID)
	case errors.Is(err, store.ErrVersionConflict):
		return nil, apierror.Aborted("concurrent update conflict, please retry")
	default:
		return nil, fmt.Errorf("while updating csi driver config: %w", err)
	}
}

func (s *RPCService) DeleteCsiDriverConfig(ctx context.Context, req *ateapi.DeleteCsiDriverConfigRequest) (*ateapi.CsiDriverConfig, error) {
	if errs := apivalidation.ValidateDeleteCsiDriverConfigRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.DeleteCsiDriverConfig(ctx, req.GetCsiDriverConfig().GetName(), toDeletePreconditions(req.GetOptions()))
}

func (s *ServiceImpl) DeleteCsiDriverConfig(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.CsiDriverConfig, error) {
	deleted, err := s.store.DeleteCsiDriverConfig(ctx, name, precondition)
	switch {
	case err == nil:
		return deleted, nil
	case errors.Is(err, store.ErrNotFound):
		return nil, apierror.NotFound("CsiDriverConfig %s not found", name)
	case errors.Is(err, store.ErrUIDConflict):
		return nil, apierror.Aborted("CsiDriverConfig %s does not have uid %s", name, precondition.UID)
	case errors.Is(err, store.ErrVersionConflict):
		return nil, apierror.Aborted("concurrent update conflict, please retry")
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, apierror.FailedPrecondition("CsiDriverConfig %s cannot be deleted while referenced by an ExternalVolumeClass", name)
	default:
		return nil, fmt.Errorf("while deleting csi driver config from DB: %w", err)
	}
}
