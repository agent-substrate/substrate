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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestPauseActorWorkflow exercises the pause workflow end-to-end against
// seeded actor statuses, covering both the rejected and the idempotent-success
// paths. The atelet dialer is nil, so any step that unexpectedly reaches it
// panics.
func TestPauseActorWorkflow(t *testing.T) {
	tests := []struct {
		name       string
		seedStatus ateapipb.Actor_Status
		// wantErr true means PauseActor must fail with FailedPrecondition.
		wantErr bool
		// wantStatus is the stored status after the call.
		wantStatus ateapipb.Actor_Status
	}{
		{
			// Pausing a SUSPENDED actor is rejected by MarkPausingStep's
			// CheckPrerequisite and the actor's status is left untouched.
			name:       "not running rejected",
			seedStatus: ateapipb.Actor_STATUS_SUSPENDED,
			wantErr:    true,
			wantStatus: ateapipb.Actor_STATUS_SUSPENDED,
		},
		{
			// Pausing a PAUSED actor succeeds idempotently via IsComplete
			// fast-forward without calling atelet.
			name:       "already paused succeeds",
			seedStatus: ateapipb.Actor_STATUS_PAUSED,
			wantStatus: ateapipb.Actor_STATUS_PAUSED,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			seedWorkflowActor(t, ctx, st, "team-a", "id1", "ns", "tmpl1", tc.seedStatus)

			actor, err := w.PauseActor(ctx, "team-a", "id1")
			if tc.wantErr {
				if got := status.Code(err); got != codes.FailedPrecondition {
					t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
				}
			} else {
				if err != nil {
					t.Fatalf("PauseActor failed: %v", err)
				}
				if actor.GetStatus() != tc.wantStatus {
					t.Errorf("returned status = %v, want %v", actor.GetStatus(), tc.wantStatus)
				}
			}

			got, err := st.GetActor(ctx, "team-a", "id1")
			if err != nil {
				t.Fatalf("GetActor failed: %v", err)
			}
			if got.GetStatus() != tc.wantStatus {
				t.Errorf("stored status = %v, want %v", got.GetStatus(), tc.wantStatus)
			}
		})
	}
}

// TestPauseSteps_CheckPrerequisite verifies each pause step's CheckPrerequisite
// against every actor status: nil for the step's allowed statuses,
// FailedPrecondition for all others.
func TestPauseSteps_CheckPrerequisite(t *testing.T) {
	tests := []struct {
		name string
		step WorkflowStep[*PauseInput, *PauseState]
		// allowed lists the statuses CheckPrerequisite accepts; nil means
		// every status is accepted.
		allowed map[ateapipb.Actor_Status]bool
	}{
		{
			// Loading has no prerequisite: it is allowed from every status.
			name:    "LoadActorForPauseStep",
			step:    &LoadActorForPauseStep{},
			allowed: nil,
		},
		{
			// Pausing is allowed only from RUNNING.
			name: "MarkPausingStep",
			step: &MarkPausingStep{},
			allowed: map[ateapipb.Actor_Status]bool{
				ateapipb.Actor_STATUS_RUNNING: true,
			},
		},
		{
			// The checkpoint call is allowed only from PAUSING (PAUSED is
			// fast-forwarded by IsComplete).
			name: "CallAteletPauseStep",
			step: &CallAteletPauseStep{},
			allowed: map[ateapipb.Actor_Status]bool{
				ateapipb.Actor_STATUS_PAUSING: true,
			},
		},
		{
			// Finalizing is allowed only from PAUSING: a persisted PAUSED
			// actor always has its worker pod fields cleared and is
			// fast-forwarded by IsComplete.
			name: "FinalizePausedStep",
			step: &FinalizePausedStep{},
			allowed: map[ateapipb.Actor_Status]bool{
				ateapipb.Actor_STATUS_PAUSING: true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			for _, st := range allActorStatuses {
				err := tc.step.CheckPrerequisite(ctx, &PauseInput{ActorName: "id1"}, &PauseState{Actor: &ateapipb.Actor{Status: st}})
				assertPrerequisiteResult(t, st, err, tc.allowed == nil || tc.allowed[st])
			}
		})
	}
}
