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
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func (s *Service) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	if err := validateListWorkersRequest(req); err != nil {
		return nil, err
	}
	workers, err := s.persistence.ListWorkers(ctx)
	if err != nil {
		return nil, fmt.Errorf("while listing workers in db: %w", err)
	}
	return &ateapipb.ListWorkersResponse{
		Workers: workers,
	}, nil
}

func validateListWorkersRequest(_ *ateapipb.ListWorkersRequest) error {
	// No fields to validate for now.
	return nil
}
