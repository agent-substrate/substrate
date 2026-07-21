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

// TestFinalizePausedStep_WorkerGone reproduces the scenario where the worker pod
// disappears from the DB during pause finalization, so the node it ran on is
// unknown.
//
// Old behavior: NodeVmsWithLocalSnapshots = []string{""}, which made
// findFreeWorker search for a worker with node name "", never found, a
// permanent "no free workers available" on resume.
//
// Current behavior: NodeVmsWithLocalSnapshots is left nil, and the actor is
// crashed instead of left PAUSED, since a local snapshot with an unknown node
// can never be safely resumed.
func TestFinalizePausedStep_WorkerGone(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	const atespace, actorName = "team-a", "actor-1"

	actor := &ateapipb.Actor{
		Metadata:           &ateapipb.ResourceMetadata{Atespace: atespace, Name: actorName},
		Status:             ateapipb.Actor_STATUS_PAUSING,
		AteomPodNamespace:  "default",
		AteomPodName:       "worker-pod-1",
		WorkerPoolName:     "pool1",
		InProgressSnapshot: "snap-prefix",
	}
	if _, err := st.CreateActor(ctx, actor); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	// Intentionally NOT creating the worker in store, simulates worker already gone.

	step := &FinalizePausedStep{store: st}
	input := &PauseInput{Atespace: atespace, ActorName: actorName}
	state := &PauseState{}
	if err := step.Execute(ctx, input, state); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	got, err := st.GetActor(ctx, atespace, actorName)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}

	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("status = %v, want CRASHED (node name unknown, cannot resume safely)", got.GetStatus())
	}
	for _, n := range got.GetLatestSnapshotInfo().GetLocal().GetNodeVmsWithLocalSnapshots() {
		if n == "" {
			t.Errorf("BUG: empty string in NodeVmsWithLocalSnapshots, findFreeWorker would never match")
		}
	}

	state.Actor = got
	done, err := step.IsComplete(ctx, input, state)
	if err != nil {
		t.Fatalf("IsComplete: %v", err)
	}
	if !done {
		t.Error("IsComplete = false, want true once the actor is CRASHED and the worker is freed")
	}
}

// TestFindFreeWorker_EmptyNodeRestriction shows the root symptom the fix
// avoids: old code wrote []string{""} into NodeVmsWithLocalSnapshots when the
// node name was unknown, and findFreeWorker required worker.NodeName == "",
// which never matches a real worker.
func TestFindFreeWorker_EmptyNodeRestriction(t *testing.T) {
	workers := []*ateapipb.Worker{
		{WorkerNamespace: "default", WorkerPool: "pool1", WorkerPod: "w1", NodeName: "node1"},
		{WorkerNamespace: "default", WorkerPool: "pool1", WorkerPod: "w2", NodeName: "node2"},
	}

	s := &AssignWorkerStep{}

	// Old behavior: []string{""}, no worker has NodeName == "", returns nil.
	got, err := s.findFreeWorker(workers, "", nil, nil, []string{""})
	if err != nil {
		t.Fatalf("findFreeWorker: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil with old buggy input, got %v", got)
	}

	// Fixed behavior: nil restrictions, any free worker matches.
	got, err = s.findFreeWorker(workers, "", nil, nil, nil)
	if err != nil {
		t.Fatalf("findFreeWorker: %v", err)
	}
	if got == nil {
		t.Error("expected a worker with nil restrictions, got nil")
	}
}

// TestPauseActorWorkflow_RejectedAndIdempotentPaths covers the two
// short-circuit paths of the pause workflow: rejection by MarkPausingStep's
// CheckPrerequisite and the IsComplete idempotent fast-forward.
func TestPauseActorWorkflow_RejectedAndIdempotentPaths(t *testing.T) {
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
				// Worker pod fields are populated so CallAteletPauseStep's
				// missing-worker crash branch is not taken; this test only
				// verifies status gating.
				err := tc.step.CheckPrerequisite(ctx, &PauseInput{ActorName: "id1"}, &PauseState{Actor: &ateapipb.Actor{Status: st, AteomPodNamespace: "ns", AteomPodName: "worker-1"}})
				assertPrerequisiteResult(t, st, err, tc.allowed == nil || tc.allowed[st])
			}
		})
	}
}

// TestPauseActor_CrashesWhenPausingActorMissingWorkerPod verifies that a
// PAUSING actor with no worker pod recorded is moved to CRASHED by
// CallAteletPauseStep's prerequisite check and the pause fails with
// FailedPrecondition.
func TestPauseActor_CrashesWhenPausingActorMissingWorkerPod(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	seedWorkflowActor(t, ctx, st, "team-a", "id1", "ns", "tmpl1", ateapipb.Actor_STATUS_PAUSING)

	_, err := w.PauseActor(ctx, "team-a", "id1")
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
	}

	got, err := st.GetActor(ctx, "team-a", "id1")
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("stored status = %v, want %v", got.GetStatus(), ateapipb.Actor_STATUS_CRASHED)
	}
}
