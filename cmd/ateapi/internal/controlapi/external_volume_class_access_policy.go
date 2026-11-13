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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
)

// Pass-through.

func (s *ServiceImpl) CreateExternalVolumeClassAccessPolicy(ctx context.Context, name string, policy *ateapi.AccessPolicy) (*ateapi.AccessPolicy, error) {
	return s.store.CreateExternalVolumeClassAccessPolicy(ctx, name, policy)
}

func (s *ServiceImpl) GetExternalVolumeClassAccessPolicy(ctx context.Context, name string) (*ateapi.AccessPolicy, error) {
	return s.store.GetExternalVolumeClassAccessPolicy(ctx, name)
}

func (s *ServiceImpl) UpdateExternalVolumeClassAccessPolicy(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapi.AccessPolicy) error) (*ateapi.AccessPolicy, error) {
	return s.store.UpdateExternalVolumeClassAccessPolicy(ctx, name, precondition, mutate)
}

func (s *ServiceImpl) DeleteExternalVolumeClassAccessPolicy(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.AccessPolicy, error) {
	return s.store.DeleteExternalVolumeClassAccessPolicy(ctx, name, precondition)
}
