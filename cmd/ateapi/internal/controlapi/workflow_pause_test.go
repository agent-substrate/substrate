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
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestEnsurePausedFinalized_WorkerGone reproduces the scenario where the worker
// pod disappears from the DB during pause finalization, so the node it ran on
// is unknown.
//
// Old behavior: NodeVmsWithLocalSnapshots = []string{""}, which made the
// scheduler's node restriction search for a worker with node name "", never
// found, a permanent "no free workers available" on resume.
//
// Current behavior: NodeVmsWithLocalSnapshots is left nil, and the actor is
// crashed instead of left PAUSED, since a local snapshot with an unknown node
// can never be safely resumed.
func TestEnsurePausedFinalized_WorkerGone(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	records := crashRecords(t)

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_PAUSING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				WorkerNamespace: "default",
				WorkerPool:      "pool1",
				WorkerPod:       "worker-pod-1",
			},
			InProgressLocalSnapshotName: "local-snap-1",
		},
	}
	storetest.MustCreateActor(t, ctx, st, actor)
	// Intentionally NOT creating the worker in store, simulates worker already gone.

	w := &ActorWorkflow{store: st}
	finalized, err := w.ensurePausedFinalized(ctx, actorRef, &ateapipb.ActorTemplate{})
	if err != nil {
		t.Fatalf("ensurePausedFinalized: %v", err)
	}

	got, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}

	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("state = %v, want CRASHED (node name unknown, cannot resume safely)", got.GetStatus().GetState())
	}
	for _, n := range got.GetStatus().GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots() {
		if n == "" {
			t.Errorf("BUG: empty string in NodeVmsWithLocalSnapshots, the scheduler's node restriction would never match a real worker")
		}
	}

	if finalized.GetStatus().GetWorkerAssignment() != nil {
		t.Error("returned actor still has a worker assignment, want it cleared")
	}
	if finalized.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("returned state = %v, want CRASHED", finalized.GetStatus().GetState())
	}

	// This site records the crash counter, so it must write the record too, or
	// a pause-finalize crash is the one kind nothing can attribute to an actor.
	if len(*records) != 1 {
		t.Fatalf("got %d crash records, want 1", len(*records))
	}
	if got := (*records)[0][string(ateattr.ActorUIDKey)]; got == "" {
		t.Error("crash record carries no ate.actor.uid")
	}
	if got := (*records)[0][string(ateattr.FailureDomainKey)]; got != ateattr.FailureDomainInfrastructure {
		t.Errorf("ate.failure.domain = %q, want %q", got, ateattr.FailureDomainInfrastructure)
	}
}

// TestEnsurePausedFinalized_RecordsContentScope verifies pause finalization
// records the scope the pause checkpoint captured (the template's onPause) in
// LocalSnapshotInfo, so a later suspend of the PAUSED actor knows what the
// local snapshot contains even if the template's onPause changes while the
// actor sits PAUSED.
func TestEnsurePausedFinalized_RecordsContentScope(t *testing.T) {
	tests := []struct {
		name    string
		onPause ateapipb.SnapshotContentScope
		want    ateapipb.SnapshotContentScope
	}{
		{"data", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA},
		{"full", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
		{"unset defaults to full", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			ctx := context.Background()
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

			created := storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_PAUSING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						WorkerNamespace: "default",
						WorkerPool:      "pool1",
						WorkerPod:       "worker-pod-1",
					},
					InProgressLocalSnapshotName: "snap-prefix",
				},
			})
			if _, err := st.CreateWorker(ctx, &ateapipb.Worker{
				WorkerNamespace: "default",
				WorkerPool:      "pool1",
				WorkerPod:       "worker-pod-1",
				NodeName:        "node1",
				Status: &ateapipb.WorkerStatus{
					Assignment: &ateapipb.ActorAssignment{
						Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
						ActorUid: created.GetMetadata().GetUid(),
					},
				},
			}); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}

			w := &ActorWorkflow{store: st}
			tmpl := &ateapipb.ActorTemplate{
				SnapshotsConfig: &ateapipb.SnapshotsConfig{OnPause: tc.onPause},
			}
			got, err := w.ensurePausedFinalized(ctx, actorRef, tmpl)
			if err != nil {
				t.Fatalf("ensurePausedFinalized: %v", err)
			}

			if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_PAUSED {
				t.Fatalf("state = %v, want PAUSED", got.GetStatus().GetState())
			}
			if scope := got.GetStatus().GetLocalSnapshotInfo().GetContentScope(); scope != tc.want {
				t.Errorf("LocalSnapshotInfo.ContentScope = %v, want %v", scope, tc.want)
			}
		})
	}
}

// TestPauseActorWorkflow_RejectedAndIdempotentPaths covers the two
// short-circuit paths of the pause workflow: rejection of the pause edge for
// a non-RUNNING actor and the idempotent fast-forward for a PAUSED one.
func TestPauseActorWorkflow_RejectedAndIdempotentPaths(t *testing.T) {
	tests := []struct {
		name      string
		seedState ateapipb.ActorState
		// wantErr true means PauseActor must fail with FailedPrecondition.
		wantErr bool
		// wantState is the stored state after the call.
		wantState ateapipb.ActorState
	}{
		{
			// Pausing a SUSPENDED actor is rejected by MarkPausingStep's
			// CheckPrerequisite and the actor's state is left untouched.
			name:      "not running rejected",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			wantErr:   true,
			wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
		{
			// Pausing a PAUSED actor succeeds idempotently via IsComplete
			// fast-forward without calling atelet.
			name:      "already paused succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			wantState: ateapipb.ActorState_ACTOR_STATE_PAUSED,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", tc.seedState)

			actor, err := w.PauseActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
			if tc.wantErr {
				if got := status.Code(err); got != codes.FailedPrecondition {
					t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
				}
			} else {
				if err != nil {
					t.Fatalf("PauseActor failed: %v", err)
				}
				if actor.GetStatus().GetState() != tc.wantState {
					t.Errorf("returned state = %v, want %v", actor.GetStatus().GetState(), tc.wantState)
				}
			}

			got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
			if err != nil {
				t.Fatalf("GetActor failed: %v", err)
			}
			if got.GetStatus().GetState() != tc.wantState {
				t.Errorf("stored state = %v, want %v", got.GetStatus().GetState(), tc.wantState)
			}
		})
	}
}

// TestEnsureMarkedPausing_StateMatrix verifies the pause edge's state gating
// against every actor state: RUNNING takes the edge, PAUSING skips (a
// previous attempt already marked the actor), everything else is rejected
// with FailedPrecondition. PAUSED is rejected here because the orchestrator
// early-returns before this step for a fully paused actor.
func TestEnsureMarkedPausing_StateMatrix(t *testing.T) {
	allowed := map[ateapipb.ActorState]bool{
		ateapipb.ActorState_ACTOR_STATE_RUNNING: true,
		ateapipb.ActorState_ACTOR_STATE_PAUSING: true, // skipped, not re-marked
	}

	for _, seedState := range allActorStates {
		ctx := context.Background()
		persistence := newTestPersistence(t)
		w := &ActorWorkflow{store: persistence}

		actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

		actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
			Status:   &ateapipb.ActorStatus{State: seedState},
		})

		marked, err := w.ensureMarkedPausing(ctx, actorRef, actor)
		assertPrerequisiteResult(t, seedState, err, allowed[seedState])
		if err == nil && marked.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_PAUSING {
			t.Errorf("state %v: ensureMarkedPausing returned actor in %v, want PAUSING", seedState, marked.GetStatus().GetState())
		}
	}
}

func TestEnsureAteletPaused_DanglingWorkerDoesNotRecordPhantomSnapshot(t *testing.T) {
	tests := []struct {
		name         string
		prevSnapshot *ateapipb.ObjectRef
	}{
		{
			name:         "keeps previous snapshot",
			prevSnapshot: &ateapipb.ObjectRef{Atespace: "team-a", Name: "prev"},
		},
		{
			name:         "stays nil without previous snapshot",
			prevSnapshot: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			actor := &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_PAUSING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						WorkerNamespace: "worker-ns",
						WorkerPool:      "pool",
						WorkerPod:       "pod-gone",
					},
					InProgressLocalSnapshotName: "actor-1-never-written",
					LatestSnapshot:              tt.prevSnapshot,
				},
			}
			created := storetest.MustCreateActor(t, ctx, persistence, actor)

			w := &ActorWorkflow{store: persistence, dialer: newDanglingDialer()}
			if _, err := w.ensureAteletPaused(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, created, &ateapipb.ActorTemplate{}); err == nil {
				t.Fatal("ensureAteletPaused: want error for dangling worker, got nil")
			}

			stored, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
				t.Errorf("state = %v, want CRASHED", stored.GetStatus().GetState())
			}
			if got := stored.GetStatus().GetInProgressLocalSnapshotName(); got != "actor-1-never-written" {
				t.Errorf("InProgressLocalSnapshotName = %q, want preserved for debugging", got)
			}
			if tt.prevSnapshot == nil {
				if stored.GetStatus().GetLatestSnapshot() != nil {
					t.Errorf("LatestSnapshot = %v, want nil", stored.GetStatus().GetLatestSnapshot())
				}
			} else if got, want := stored.GetStatus().GetLatestSnapshot().GetName(), tt.prevSnapshot.GetName(); got != want {
				t.Errorf("LatestSnapshot name = %q, want %q", got, want)
			}
		})
	}
}

// TestPauseActor_CrashesWhenPausingActorMissingWorkerPod verifies that a
// PAUSING actor with no worker pod recorded is moved to CRASHED by
// ensureAteletPaused's corrupted-assignment check and the pause fails with
// FailedPrecondition.
func TestPauseActor_CrashesWhenPausingActorMissingWorkerPod(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_PAUSING)

	_, err := w.PauseActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
	}

	got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("stored state = %v, want %v", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_CRASHED)
	}
}

// TestEnsureMarkedPausing_GoldenAtespaceRejected verifies golden actors
// cannot be paused: by design they can only be suspended (committed).
func TestEnsureMarkedPausing_GoldenAtespaceRejected(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	_, err := w.ensureMarkedPausing(context.Background(),
		resources.ActorRef{Atespace: resources.GoldenActorAtespace, Name: "golden-1"},
		&ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING}})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status.Code = %v (err %v), want FailedPrecondition", got, err)
	}
}
