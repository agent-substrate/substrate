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
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
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

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		Status:   ateapipb.Actor_STATUS_PAUSING,
		WorkerAssignment: &ateapipb.WorkerAssignment{
			WorkerNamespace: "default",
			WorkerPool:      "pool1",
			WorkerPod:       "worker-pod-1",
		},
		InProgressLocalSnapshotName: "local-snap-1",
	}
	if _, err := st.CreateActor(ctx, actor); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	// Intentionally NOT creating the worker in store, simulates worker already gone.

	w := &ActorWorkflow{store: st}
	finalized, err := w.ensurePausedFinalized(ctx, actorRef, &atev1alpha1.ActorTemplate{})
	if err != nil {
		t.Fatalf("ensurePausedFinalized: %v", err)
	}

	got, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}

	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("status = %v, want CRASHED (node name unknown, cannot resume safely)", got.GetStatus())
	}
	for _, n := range got.GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots() {
		if n == "" {
			t.Errorf("BUG: empty string in NodeVmsWithLocalSnapshots, the scheduler's node restriction would never match a real worker")
		}
	}

	if finalized.GetWorkerAssignment() != nil {
		t.Error("returned actor still has a worker assignment, want it cleared")
	}
	if finalized.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("returned status = %v, want CRASHED", finalized.GetStatus())
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
		onPause atev1alpha1.SnapshotScope
		want    ateapipb.SnapshotContentScope
	}{
		{"data", atev1alpha1.SnapshotScopeData, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA},
		{"full", atev1alpha1.SnapshotScopeFull, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
		{"unset defaults to full", "", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			ctx := context.Background()
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

			created, err := st.CreateActor(ctx, &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				Status:   ateapipb.Actor_STATUS_PAUSING,
				WorkerAssignment: &ateapipb.WorkerAssignment{
					WorkerNamespace: "default",
					WorkerPool:      "pool1",
					WorkerPod:       "worker-pod-1",
				},
				InProgressLocalSnapshotName: "snap-prefix",
			})
			if err != nil {
				t.Fatalf("CreateActor: %v", err)
			}
			if err := st.CreateWorker(ctx, &ateapipb.Worker{
				WorkerNamespace: "default",
				WorkerPool:      "pool1",
				WorkerPod:       "worker-pod-1",
				NodeName:        "node1",
				Assignment: &ateapipb.Assignment{
					Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
					ActorUid: created.GetMetadata().GetUid(),
				},
			}); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}

			w := &ActorWorkflow{store: st}
			tmpl := &atev1alpha1.ActorTemplate{
				Spec: atev1alpha1.ActorTemplateSpec{SnapshotsConfig: atev1alpha1.SnapshotsConfig{OnPause: tc.onPause}},
			}
			got, err := w.ensurePausedFinalized(ctx, actorRef, tmpl)
			if err != nil {
				t.Fatalf("ensurePausedFinalized: %v", err)
			}

			if got.GetStatus() != ateapipb.Actor_STATUS_PAUSED {
				t.Fatalf("status = %v, want PAUSED", got.GetStatus())
			}
			if scope := got.GetLocalSnapshotInfo().GetContentScope(); scope != tc.want {
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

			seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", tc.seedStatus)

			actor, err := w.PauseActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
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

			got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
			if err != nil {
				t.Fatalf("GetActor failed: %v", err)
			}
			if got.GetStatus() != tc.wantStatus {
				t.Errorf("stored status = %v, want %v", got.GetStatus(), tc.wantStatus)
			}
		})
	}
}

// TestEnsureMarkedPausing_StatusMatrix verifies the pause edge's status gating
// against every actor status: RUNNING takes the edge, PAUSING skips (a
// previous attempt already marked the actor), everything else is rejected
// with FailedPrecondition. PAUSED is rejected here because the orchestrator
// early-returns before this step for a fully paused actor.
func TestEnsureMarkedPausing_StatusMatrix(t *testing.T) {
	allowed := map[ateapipb.Actor_Status]bool{
		ateapipb.Actor_STATUS_RUNNING: true,
		ateapipb.Actor_STATUS_PAUSING: true, // skipped, not re-marked
	}

	for _, seedStatus := range allActorStatuses {
		ctx := context.Background()
		persistence := newTestPersistence(t)
		w := &ActorWorkflow{store: persistence}

		actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
		actor, err := persistence.CreateActor(ctx, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
			Status:   seedStatus,
		})
		if err != nil {
			t.Fatalf("status %v: CreateActor: %v", seedStatus, err)
		}

		marked, err := w.ensureMarkedPausing(ctx, actorRef, actor)
		assertPrerequisiteResult(t, seedStatus, err, allowed[seedStatus])
		if err == nil && marked.GetStatus() != ateapipb.Actor_STATUS_PAUSING {
			t.Errorf("status %v: ensureMarkedPausing returned actor in %v, want PAUSING", seedStatus, marked.GetStatus())
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
				Status:   ateapipb.Actor_STATUS_PAUSING,
				WorkerAssignment: &ateapipb.WorkerAssignment{
					WorkerNamespace: "worker-ns",
					WorkerPool:      "pool",
					WorkerPod:       "pod-gone",
				},
				InProgressLocalSnapshotName: "actor-1-never-written",
				LatestSnapshot:              tt.prevSnapshot,
			}
			created, err := persistence.CreateActor(ctx, actor)
			if err != nil {
				t.Fatalf("CreateActor: %v", err)
			}

			w := &ActorWorkflow{store: persistence, dialer: newDanglingDialer()}
			if _, err := w.ensureAteletPaused(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, created, &atev1alpha1.ActorTemplate{}); err == nil {
				t.Fatal("ensureAteletPaused: want error for dangling worker, got nil")
			}

			stored, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if stored.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
				t.Errorf("status = %v, want CRASHED", stored.GetStatus())
			}
			if got := stored.GetInProgressLocalSnapshotName(); got != "actor-1-never-written" {
				t.Errorf("InProgressLocalSnapshotName = %q, want preserved for debugging", got)
			}
			if tt.prevSnapshot == nil {
				if stored.GetLatestSnapshot() != nil {
					t.Errorf("LatestSnapshot = %v, want nil", stored.GetLatestSnapshot())
				}
			} else if got, want := stored.GetLatestSnapshot().GetName(), tt.prevSnapshot.GetName(); got != want {
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

	seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", ateapipb.Actor_STATUS_PAUSING)

	_, err := w.PauseActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
	}

	got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("stored status = %v, want %v", got.GetStatus(), ateapipb.Actor_STATUS_CRASHED)
	}
}
