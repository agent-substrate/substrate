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
		name        string
		seedState   ateapipb.ActorState
		anyState    bool
		missingTmpl bool
		wantErr     bool
		wantCode    codes.Code
	}{
		{
			name:      "delete suspended actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:  false,
			wantErr:   false,
		},
		{
			name:      "delete crashed actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			anyState:  false,
			wantErr:   false,
		},
		{
			name:      "delete deleting actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_DELETING,
			anyState:  false,
			wantErr:   false,
		},
		{
			name:      "delete running actor rejected when not any_state",
			seedState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			anyState:  false,
			wantErr:   true,
			wantCode:  codes.FailedPrecondition,
		},
		{
			name:      "delete paused actor rejected when not any_state",
			seedState: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			anyState:  false,
			wantErr:   true,
			wantCode:  codes.FailedPrecondition,
		},
		{
			name:      "any_state delete suspended actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:      "any_state delete running actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:      "any_state delete paused actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:      "any_state delete crashed actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:        "delete suspended actor with missing template succeeds",
			seedState:   ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:    false,
			missingTmpl: true,
			wantErr:     false,
		},
		{
			name:        "any_state delete suspended actor with missing template succeeds",
			seedState:   ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:    true,
			missingTmpl: true,
			wantErr:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			tmplName := "tmpl1"
			if tc.missingTmpl {
				tmplName = "missing-tmpl"
			}
			seedWorkflowActor(t, ctx, st, actorRef, "ns", tmplName, tc.seedState)

			deleted, err := w.DeleteActor(ctx, actorRef, tc.anyState)
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

func TestEnsureMarkedDeleting_StateMatrix(t *testing.T) {
	tests := []struct {
		name     string
		anyState bool
		allowed  map[ateapipb.ActorState]bool
	}{
		{
			name:     "standard delete",
			anyState: false,
			allowed: map[ateapipb.ActorState]bool{
				ateapipb.ActorState_ACTOR_STATE_SUSPENDED: true,
				ateapipb.ActorState_ACTOR_STATE_CRASHED:   true,
				ateapipb.ActorState_ACTOR_STATE_DELETING:  true, // skipped
			},
		},
		{
			name:     "any_state delete",
			anyState: true,
			allowed: map[ateapipb.ActorState]bool{
				ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED: true,
				ateapipb.ActorState_ACTOR_STATE_RUNNING:     true,
				ateapipb.ActorState_ACTOR_STATE_RESUMING:    true,
				ateapipb.ActorState_ACTOR_STATE_SUSPENDING:  true,
				ateapipb.ActorState_ACTOR_STATE_PAUSING:     true,
				ateapipb.ActorState_ACTOR_STATE_PAUSED:      true,
				ateapipb.ActorState_ACTOR_STATE_SUSPENDED:   true,
				ateapipb.ActorState_ACTOR_STATE_CRASHED:     true,
				ateapipb.ActorState_ACTOR_STATE_DELETING:    true, // skipped
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, seedState := range allActorStates {
				ctx := context.Background()
				st, cleanup := storetest.SetupTestStore(t)
				w := newTestActorWorkflow(t, st, "ns", "tmpl1")

				actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
				seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", seedState)
				actor, err := st.GetActor(ctx, actorRef)
				if err != nil {
					t.Fatalf("state %v: get seeded actor: %v", seedState, err)
				}

				updated, err := w.ensureMarkedDeleting(ctx, actorRef, actor, tc.anyState)
				assertPrerequisiteResult(t, seedState, err, tc.allowed[seedState])
				if err == nil && seedState != ateapipb.ActorState_ACTOR_STATE_DELETING {
					if updated.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_DELETING {
						t.Errorf("state %v: ensureMarkedDeleting returned actor in %v, want DELETING", seedState, updated.GetStatus().GetState())
					}
				}
				cleanup()
			}
		})
	}
}
