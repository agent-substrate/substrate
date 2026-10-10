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
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

// TestEnsurePausedFinalized_WorkerGone reproduces the scenario where the worker
// pod disappears from the DB during pause finalization, so the node it ran on
// is unknown.
//
// Current behavior: AssignedNode is left empty, and the actor is crashed
// instead of left PAUSED, since a local snapshot with an unknown node can
// never be safely resumed.
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
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS),
			},
		},
	}
	storetest.MustCreateActor(t, ctx, st, actor)
	// Intentionally NOT creating the worker in store, simulates worker already gone.

	w := &ActorWorkflow{store: st}
	finalized, err := w.ensurePausedFinalized(ctx, actorRef)
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
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "pause failed: "+crashMessageLocalSnapshotNodeUnknown; msg != want {
		t.Errorf("crash message = %q, want %q", msg, want)
	}
	if got.GetStatus().GetAssignedNode() != "" {
		t.Errorf("AssignedNode = %q, want empty", got.GetStatus().GetAssignedNode())
	}
	if _, gotSt := findLatestSnapshotStorage(got.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS); gotSt.GetLocal().GetSnapshotName() != "local-snap-1" {
		t.Errorf("in-progress local snapshot name = %q, want %q preserved on crash", gotSt.GetLocal().GetSnapshotName(), "local-snap-1")
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
	if got := (*records)[0].attrs[string(ateattr.ActorUIDKey)]; got == "" {
		t.Error("crash record carries no ate.actor.uid")
	}
}

// TestEnsurePausedFinalized_AlreadyCrashed verifies that if the actor was
// already crashed out-of-band when ensurePausedFinalized runs with no
// AssignedNode, its existing Crash status and snapshots are preserved and no
// duplicate crash log record is emitted.
func TestEnsurePausedFinalized_AlreadyCrashed(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	records := crashRecords(t)

	originalCrash := newActorCrash(ateattr.OperationPause, "original crash reason")
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			Crash: originalCrash,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				WorkerNamespace: "default",
				WorkerPool:      "pool1",
				WorkerPod:       "worker-pod-1",
			},
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS),
			},
		},
	}
	storetest.MustCreateActor(t, ctx, st, actor)

	w := &ActorWorkflow{store: st}
	finalized, err := w.ensurePausedFinalized(ctx, actorRef)
	if err != nil {
		t.Fatalf("ensurePausedFinalized: %v", err)
	}
	if finalized.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("state = %v, want CRASHED", finalized.GetStatus().GetState())
	}
	if got, want := finalized.GetStatus().GetCrash().GetMessage(), originalCrash.GetMessage(); got != want {
		t.Errorf("crash message = %q, want original %q preserved", got, want)
	}
	if _, gotSt := findLatestSnapshotStorage(finalized.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS); gotSt.GetLocal().GetSnapshotName() != "local-snap-1" {
		t.Errorf("in-progress local snapshot name = %q, want %q preserved", gotSt.GetLocal().GetSnapshotName(), "local-snap-1")
	}
	if finalized.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("WorkerAssignment = %v, want nil", finalized.GetStatus().GetWorkerAssignment())
	}
	if len(*records) != 0 {
		t.Errorf("got %d crash records, want 0 for an actor that was already crashed", len(*records))
	}
}

// TestEnsurePausedFinalized_RecordsFidelity verifies pause marks the
// scope the pause checkpoint captures (the template's preferredFidelity) in
// SnapshotStorage and finalization promotes it to COMPLETED, so a later suspend
// or resume of the PAUSED actor knows what the local snapshot contains.
func TestEnsurePausedFinalized_RecordsFidelity(t *testing.T) {
	tests := []struct {
		name     string
		fidelity ateapipb.SnapshotFidelity
		want     ateapipb.SnapshotFidelity
	}{
		{"volumes", ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES},
		{"rootfs", ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS},
		{"memory", ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			ctx := context.Background()
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

			workerName := testWorkerUID("worker-pod-1")
			created := storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				Status: &ateapipb.ActorStatus{
					State:        ateapipb.ActorState_ACTOR_STATE_RUNNING,
					AssignedNode: "node1",
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker:          &ateapipb.ObjectRef{Name: workerName},
						WorkerNamespace: "default",
						WorkerPool:      "pool1",
						WorkerPod:       "worker-pod-1",
						WorkerPodUid:    workerName,
					},
				},
			})
			if _, err := st.CreateWorker(ctx, &ateapipb.Worker{
				Metadata:        &ateapipb.ResourceMetadata{Name: workerName},
				WorkerNamespace: "default",
				WorkerPool:      "pool1",
				WorkerPod:       "worker-pod-1",
				WorkerPodUid:    workerName,
				NodeName:        "node1",
				Status:          &ateapipb.WorkerStatus{},
			}); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}
			seedAssignment(t, st, workerName, &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorUid: created.GetMetadata().GetUid(),
			})

			w := &ActorWorkflow{store: st}
			tmpl := &ateapipb.ActorTemplate{
				SnapshotConfig: &ateapipb.SnapshotConfig{PreferredFidelity: tc.fidelity},
			}
			if _, err := w.ensureMarkedPausing(ctx, actorRef, created, tmpl); err != nil {
				t.Fatalf("ensureMarkedPausing: %v", err)
			}
			got, err := w.ensurePausedFinalized(ctx, actorRef)
			if err != nil {
				t.Fatalf("ensurePausedFinalized: %v", err)
			}

			if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_PAUSED {
				t.Fatalf("state = %v, want PAUSED", got.GetStatus().GetState())
			}
			_, localSt := findLatestSnapshotStorage(got.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if scope := localSt.GetFidelity(); scope != tc.want {
				t.Errorf("local SnapshotStorage.Fidelity = %v, want %v", scope, tc.want)
			}
			if got.GetStatus().GetAssignedNode() != "node1" {
				t.Errorf("AssignedNode = %q, want %q", got.GetStatus().GetAssignedNode(), "node1")
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
				if got := apierror.Code(err); got != codes.FailedPrecondition {
					t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
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

		marked, err := w.ensureMarkedPausing(ctx, actorRef, actor, &ateapipb.ActorTemplate{})
		assertPrerequisiteResult(t, seedState, err, allowed[seedState])
		if err == nil && marked.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_PAUSING {
			t.Errorf("state %v: ensureMarkedPausing returned actor in %v, want PAUSING", seedState, marked.GetStatus().GetState())
		}
	}
}

func TestEnsureAteletPaused_DialFailureLeavesActorRetryable(t *testing.T) {
	tests := []struct {
		name         string
		prevSnapshot string
	}{
		{
			name:         "keeps previous external snapshot",
			prevSnapshot: someActorSnapshotURI(t, testStorageLocation, "team-a", "prev"),
		},
		{
			name:         "stays empty without previous external snapshot",
			prevSnapshot: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			var snapshots []*ateapipb.Snapshot
			if tt.prevSnapshot != "" {
				snapshots = append(snapshots, newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", tt.prevSnapshot, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED))
			}
			snapshots = append(snapshots, newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "actor-1-never-written", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS))

			actor := &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_PAUSING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						WorkerNamespace: "worker-ns",
						WorkerPool:      "pool",
						WorkerPod:       "pod-gone",
						NodeName:        "node-gone",
					},
					LastAssignedGeneration: 2,
					Snapshots:              snapshots,
				},
			}
			created := storetest.MustCreateActor(t, ctx, persistence, actor)

			w := &ActorWorkflow{store: persistence, dialer: newDanglingDialer()}
			if _, err := w.ensureAteletPaused(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, created, &ateapipb.ActorTemplate{}); err == nil {
				t.Fatal("ensureAteletPaused: want error when atelet is unreachable, got nil")
			}

			stored, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_PAUSING {
				t.Errorf("state = %v, want unchanged PAUSING", stored.GetStatus().GetState())
			}
			if _, gotSt := findLatestSnapshotStorage(stored.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS); gotSt.GetLocal().GetSnapshotName() != "actor-1-never-written" {
				t.Errorf("in-progress local snapshot name = %q, want preserved for debugging", gotSt.GetLocal().GetSnapshotName())
			}
			_, gotSt := findLatestSnapshotStorage(stored.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if got := gotSt.GetObject().GetSnapshotUri(); got != tt.prevSnapshot {
				t.Errorf("SnapshotUri = %q, want %q", got, tt.prevSnapshot)
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
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
	}

	got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("stored state = %v, want %v", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_CRASHED)
	}
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "pause failed: "+crashMessageWorkerAssignmentMissing; msg != want {
		t.Errorf("crash message = %q, want %q", msg, want)
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
		&ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING}},
		&ateapipb.ActorTemplate{})
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("apierror.Code = %v (err %v), want FailedPrecondition", got, err)
	}
}
