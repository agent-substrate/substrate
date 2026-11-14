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
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateExternalVolumeClassAccessPolicy(ctx context.Context, req *ateapi.CreateExternalVolumeClassAccessPolicyRequest) (*ateapi.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForCreate(policy.Metadata)
		defaults.Apply(policy)
	}
	if errs := apivalidation.ValidateCreateExternalVolumeClassAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.CreateExternalVolumeClassAccessPolicy(ctx, req.GetExternalVolumeClass().GetName(), policy)
}

func (s *ServiceImpl) CreateExternalVolumeClassAccessPolicy(ctx context.Context, name string, policy *ateapi.AccessPolicy) (*ateapi.AccessPolicy, error) {
	created, err := s.store.CreateExternalVolumeClassAccessPolicy(ctx, name, policy)
	return mapAccessPolicyWrite(created, err, "parent ExternalVolumeClass or referenced member Atespace does not exist")
}

func (s *RPCService) GetExternalVolumeClassAccessPolicy(ctx context.Context, req *ateapi.GetExternalVolumeClassAccessPolicyRequest) (*ateapi.AccessPolicy, error) {
	if errs := apivalidation.ValidateGetExternalVolumeClassAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.GetExternalVolumeClassAccessPolicy(ctx, req.GetExternalVolumeClass().GetName())
}

func (s *ServiceImpl) GetExternalVolumeClassAccessPolicy(ctx context.Context, name string) (*ateapi.AccessPolicy, error) {
	policy, err := s.store.GetExternalVolumeClassAccessPolicy(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("AccessPolicy for external volume class %s not found", name)
		}
		return nil, fmt.Errorf("while getting ExternalVolumeClass access policy: %w", err)
	}
	return policy, nil
}

func (s *RPCService) UpdateExternalVolumeClassAccessPolicy(ctx context.Context, req *ateapi.UpdateExternalVolumeClassAccessPolicyRequest) (*ateapi.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForUpdate(policy.Metadata)
	}
	if errs := apivalidation.ValidateUpdateExternalVolumeClassAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.UpdateExternalVolumeClassAccessPolicy(ctx, req.GetExternalVolumeClass().GetName(), store.PreconditionFrom(policy), replaceAccessPolicy(policy))
}

func (s *ServiceImpl) UpdateExternalVolumeClassAccessPolicy(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapi.AccessPolicy) error) (*ateapi.AccessPolicy, error) {
	updated, err := s.store.UpdateExternalVolumeClassAccessPolicy(ctx, name, precondition, func(toUpdate *ateapi.AccessPolicy) error {
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		errs := apivalidation.ValidateExternalVolumeClassAccessPolicyUpdate(ctx, field.NewPath("access_policy"), toUpdate, oldVal)
		return resources.ToAPIError(errs)
	})
	return mapAccessPolicyWrite(updated, err, "referenced member Atespace does not exist")
}

func (s *RPCService) DeleteExternalVolumeClassAccessPolicy(ctx context.Context, req *ateapi.DeleteExternalVolumeClassAccessPolicyRequest) (*ateapi.AccessPolicy, error) {
	if errs := apivalidation.ValidateDeleteExternalVolumeClassAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.DeleteExternalVolumeClassAccessPolicy(ctx, req.GetExternalVolumeClass().GetName(), toDeletePreconditions(req.GetOptions()))
}

func (s *ServiceImpl) DeleteExternalVolumeClassAccessPolicy(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.AccessPolicy, error) {
	deleted, err := s.store.DeleteExternalVolumeClassAccessPolicy(ctx, name, precondition)
	return mapAccessPolicyWrite(deleted, err, "")
}
