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

package atepg

import (
	"context"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
)

func TestUpdateWorker_DiscardsMetadataEdits(t *testing.T) {
	testUpdateDiscardsMetadataEdits(t,
		func(t *testing.T, s *Persistence) *ateapi.Worker {
			created, err := s.CreateWorker(t.Context(), &ateapi.Worker{Metadata: &ateapi.ResourceMetadata{Name: "w"}})
			if err != nil {
				t.Fatalf("CreateWorker failed: %v", err)
			}
			return created
		},
		func(ctx context.Context, s *Persistence, pc store.Precondition, mutate func(*ateapi.Worker) error) (*ateapi.Worker, error) {
			return s.UpdateWorker(ctx, "w", pc, mutate)
		},
		func(ctx context.Context, s *Persistence) (*ateapi.Worker, error) {
			return s.GetWorker(ctx, "w")
		})
}
