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
	"go.ate.dev/substrate/internal/resources"
)

func TestUpdateEgressPolicy_DiscardsMetadataEdits(t *testing.T) {
	ref := resources.ActorRef{Atespace: "team-a", Name: "a"}
	testUpdateDiscardsMetadataEdits(t,
		func(t *testing.T, s *Persistence) *ateapi.EgressPolicy {
			if _, err := s.CreateActor(t.Context(), &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
				Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
			}); err != nil {
				t.Fatalf("CreateActor failed: %v", err)
			}
			created, err := s.CreateEgressPolicy(t.Context(), ref, &ateapi.EgressPolicy{
				Metadata: &ateapi.ResourceMetadata{Atespace: ref.Atespace, Name: "default"},
			})
			if err != nil {
				t.Fatalf("CreateEgressPolicy failed: %v", err)
			}
			return created
		},
		func(ctx context.Context, s *Persistence, pc store.Precondition, mutate func(*ateapi.EgressPolicy) error) (*ateapi.EgressPolicy, error) {
			return s.UpdateEgressPolicy(ctx, ref, pc, mutate)
		},
		func(ctx context.Context, s *Persistence) (*ateapi.EgressPolicy, error) {
			return s.GetEgressPolicy(ctx, ref)
		})
}
