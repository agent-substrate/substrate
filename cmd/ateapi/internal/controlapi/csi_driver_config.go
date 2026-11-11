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

func (s *ServiceImpl) CreateCsiDriverConfig(ctx context.Context, config *ateapi.CsiDriverConfig) (*ateapi.CsiDriverConfig, error) {
	return s.store.CreateCsiDriverConfig(ctx, config)
}

func (s *ServiceImpl) GetCsiDriverConfig(ctx context.Context, name string) (*ateapi.CsiDriverConfig, error) {
	return s.store.GetCsiDriverConfig(ctx, name)
}

func (s *ServiceImpl) GetCsiDriverConfigByDriverName(ctx context.Context, driverName string) (*ateapi.CsiDriverConfig, error) {
	return s.store.GetCsiDriverConfigByDriverName(ctx, driverName)
}

func (s *ServiceImpl) ListCsiDriverConfigs(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.CsiDriverConfig], error) {
	return s.store.ListCsiDriverConfigs(ctx, opts)
}

func (s *ServiceImpl) UpdateCsiDriverConfig(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapi.CsiDriverConfig) error) (*ateapi.CsiDriverConfig, error) {
	return s.store.UpdateCsiDriverConfig(ctx, name, precondition, mutate)
}

func (s *ServiceImpl) DeleteCsiDriverConfig(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.CsiDriverConfig, error) {
	return s.store.DeleteCsiDriverConfig(ctx, name, precondition)
}
