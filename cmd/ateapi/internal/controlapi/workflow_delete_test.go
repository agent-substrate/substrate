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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeleteActorWorkflow_ExecutionPaths(t *testing.T) {
	tests := []struct {
		name       string
		seedStatus ateapipb.Actor_Status
		wantErr    bool
		wantCode   codes.Code
	}{
		{
			name:       "delete suspended actor succeeds",
			seedStatus: ateapipb.Actor_STATUS_SUSPENDED,
			wantErr:    false,
		},
		{
			name:       "delete crashed actor succeeds",
			seedStatus: ateapipb.Actor_STATUS_CRASHED,
			wantErr:    false,
		},
		{
			name:       "delete deleting actor succeeds",
			seedStatus: ateapipb.Actor_STATUS_DELETING,
			wantErr:    false,
		},
		{
			name:       "delete running actor rejected",
			seedStatus: ateapipb.Actor_STATUS_RUNNING,
			wantErr:    true,
			wantCode:   codes.FailedPrecondition,
		},
		{
			name:       "delete paused actor rejected",
			seedStatus: ateapipb.Actor_STATUS_PAUSED,
			wantErr:    true,
			wantCode:   codes.FailedPrecondition,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", tc.seedStatus)

			deleted, err := w.DeleteActor(ctx, actorRef)
			if tc.wantErr {
				if got := status.Code(err); got != tc.wantCode {
					t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tc.wantCode, err)
				}
			} else {
				if err != nil {
					t.Fatalf("DeleteActor failed: %v", err)
				}
				if deleted == nil {
					t.Fatalf("expected non-nil deleted actor")
				}
				if _, err := st.GetActor(ctx, actorRef); err == nil {
					t.Errorf("expected actor to be deleted from store, but it still exists")
				}
			}
		})
	}
}

func TestEnsureMarkedDeleting_StatusMatrix(t *testing.T) {
	allowed := map[ateapipb.Actor_Status]bool{
		ateapipb.Actor_STATUS_SUSPENDED: true,
		ateapipb.Actor_STATUS_CRASHED:   true,
		ateapipb.Actor_STATUS_DELETING:  true, // skipped, not re-marked
	}

	for _, seedStatus := range allActorStatuses {
		ctx := context.Background()
		st, cleanup := storetest.SetupTestStore(t)
		w := newTestActorWorkflow(t, st, "ns", "tmpl1")

		actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
		seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", seedStatus)
		actor, err := st.GetActor(ctx, actorRef)
		if err != nil {
			t.Fatalf("status %v: get seeded actor: %v", seedStatus, err)
		}

		updated, err := w.ensureMarkedDeleting(ctx, actorRef, actor)
		assertPrerequisiteResult(t, seedStatus, err, allowed[seedStatus])
		if err == nil && updated.GetStatus() != ateapipb.Actor_STATUS_DELETING {
			t.Errorf("status %v: ensureMarkedDeleting returned actor in %v, want DELETING", seedStatus, updated.GetStatus())
		}
		cleanup()
	}
}
