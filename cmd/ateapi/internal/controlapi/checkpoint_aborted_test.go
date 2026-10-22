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
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/checkpointabort"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestCheckpointAborted_ActorState drives a sync and an async SuspendActor of
// a RUNNING actor against an atelet whose Checkpoint fails, and pins the actor
// state each failure leaves. The fake atelet sends each error through atelet's
// interceptor. Only an aborted checkpoint (checkpointabort.Errorf, which
// ateworker-microvm returns for a failed MSHV freeze and atelet passes through)
// puts the actor back to RUNNING on its worker with its snapshots as they were;
// every other error keeps its old outcome, including atelet's other Aborted, a
// stale snapshot request, which keeps the actor SUSPENDING for a retry.
func TestCheckpointAborted_ActorState(t *testing.T) {
	type op struct {
		name    string
		run     func(*ActorWorkflow, context.Context, resources.ActorRef) (*ateapipb.Actor, error)
		marking ateapipb.ActorState
	}
	suspendIn := func(mode suspendMode) func(*ActorWorkflow, context.Context, resources.ActorRef) (*ateapipb.Actor, error) {
		return func(w *ActorWorkflow, ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
			return w.SuspendActor(ctx, actorRef, mode)
		}
	}
	suspend := op{"sync suspend", suspendIn(suspendSync), ateapipb.ActorState_ACTOR_STATE_SUSPENDING}
	async := op{"async suspend", suspendIn(suspendAsync), ateapipb.ActorState_ACTOR_STATE_SUSPENDING}

	abort := checkpointabort.Errorf("while calling worker CheckpointWorkload: while freezing guest container %q: injected", "app")
	stale := apierror.Aborted("snapshot %q has generation 2, below the actor's newest snapshot %q at generation 3", "s2", "s3")
	tests := []struct {
		name      string
		op        op
		ateletErr error
		wantState ateapipb.ActorState
		wantCode  codes.Code
	}{
		{"sync abort", suspend, abort, ateapipb.ActorState_ACTOR_STATE_RUNNING, codes.FailedPrecondition},
		{"async abort", async, abort, ateapipb.ActorState_ACTOR_STATE_RUNNING, codes.FailedPrecondition},
		{"sync stale snapshot request stays marked for a retry", suspend, stale, ateapipb.ActorState_ACTOR_STATE_SUSPENDING, codes.Aborted},
		{"async stale snapshot request stays marked for a retry", async, stale, ateapipb.ActorState_ACTOR_STATE_SUSPENDING, codes.Aborted},
		{"sync internal crashes", suspend, errors.New("while pausing guest: injected"), ateapipb.ActorState_ACTOR_STATE_CRASHED, codes.Internal},
		{"async internal crashes", async, errors.New("while pausing guest: injected"), ateapipb.ActorState_ACTOR_STATE_CRASHED, codes.Internal},
		{"sync failed precondition crashes", suspend, apierror.FailedPrecondition("no durable-dir or CSI volumes found"), ateapipb.ActorState_ACTOR_STATE_CRASHED, codes.Internal},
		{"sync unavailable stays marked for a retry", suspend, apierror.Unavailable("atelet restarting"), ateapipb.ActorState_ACTOR_STATE_SUSPENDING, codes.Unavailable},
		{"async unavailable stays marked for a retry", async, apierror.Unavailable("atelet restarting"), ateapipb.ActorState_ACTOR_STATE_SUSPENDING, codes.Unavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			w, atelet := newWireCaptureWorkflow(t, persistence)
			createAbortTestTemplate(t, ctx, persistence)

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			prev := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "prev",
				someActorSnapshotURI(t, testStorageLocation, "team-a", "prev"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			seeded := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns", Name: "tmpl1"},
				Status: &ateapipb.ActorStatus{
					State:                  ateapipb.ActorState_ACTOR_STATE_RUNNING,
					AssignedNode:           "node-1",
					WorkerAssignment:       wireTestAssignment(),
					LastAssignedGeneration: 1,
					Snapshots:              []*ateapipb.Snapshot{prev},
				},
			})
			atelet.onCheckpoint = func(context.Context) error { return tc.ateletErr }

			_, err := tc.op.run(w, ctx, actorRef)
			if got := apierror.Code(err); got != tc.wantCode {
				t.Errorf("%s error code = %v, want %v (err: %v)", tc.op.name, got, tc.wantCode, err)
			}
			wantRolledBack := tc.wantState == ateapipb.ActorState_ACTOR_STATE_RUNNING
			if got := isCheckpointRolledBack(err); got != wantRolledBack {
				t.Errorf("isCheckpointRolledBack(%v) = %v, want %v", err, got, wantRolledBack)
			}
			if wantRolledBack && !strings.Contains(err.Error(), "suspend of actor ") {
				t.Errorf("%s error = %q, want it to name the RPC", tc.op.name, err)
			}
			if atelet.checkpointRequest() == nil {
				t.Fatal("atelet Checkpoint was not called")
			}
			stored, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if got := stored.GetStatus().GetState(); got != tc.wantState {
				t.Fatalf("state = %v, want %v (crash: %v)", got, tc.wantState, stored.GetStatus().GetCrash())
			}
			// The mark took a generation, which is never given back.
			if got := stored.GetStatus().GetLastAssignedGeneration(); got != 2 {
				t.Errorf("last_assigned_generation = %d, want 2", got)
			}
			if tc.wantState != ateapipb.ActorState_ACTOR_STATE_RUNNING {
				return
			}

			// Back as it was before the operation: same worker, same snapshots, no crash.
			if diff := cmp.Diff(seeded.GetStatus().GetWorkerAssignment(), stored.GetStatus().GetWorkerAssignment(), protocmp.Transform()); diff != "" {
				t.Errorf("worker assignment changed (-before +after):\n%s", diff)
			}
			if diff := cmp.Diff(seeded.GetStatus().GetSnapshots(), stored.GetStatus().GetSnapshots(), protocmp.Transform()); diff != "" {
				t.Errorf("snapshots changed (-before +after):\n%s", diff)
			}
			if stored.GetStatus().GetCrash() != nil {
				t.Errorf("crash = %v, want none", stored.GetStatus().GetCrash())
			}

			// A later request starts a new checkpoint at the next generation.
			atelet.onCheckpoint = nil
			if _, err := tc.op.run(w, ctx, actorRef); err != nil {
				t.Fatalf("%s after the abort: %v", tc.op.name, err)
			}
			again, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if got := again.GetStatus().GetLastAssignedGeneration(); got != 3 {
				t.Errorf("last_assigned_generation after the retry = %d, want 3", got)
			}
			if snapshotAtLatestGeneration(again.GetStatus()) == nil {
				t.Errorf("no snapshot at generation 3 after the retry: %v", again.GetStatus().GetSnapshots())
			}
		})
	}
}

// TestRollBackAbortedCheckpoint_LeavesOtherStates pins that a rollback does
// not overwrite a transition made while the checkpoint ran.
func TestRollBackAbortedCheckpoint_LeavesOtherStates(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	marked := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			WorkerAssignment:       wireTestAssignment(),
			LastAssignedGeneration: 2,
		},
	})
	// The syncer crashed the actor while the checkpoint ran.
	mustUpdateActorStatus(t, ctx, persistence, marked, func(s *ateapipb.ActorStatus) {
		s.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		s.WorkerAssignment = nil
	})

	w := &ActorWorkflow{store: persistence}
	err := w.rollBackAbortedCheckpoint(ctx, actorRef, marked, checkpointabort.Errorf("injected"))
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Errorf("error code = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
	}
	if isCheckpointRolledBack(err) {
		t.Errorf("isCheckpointRolledBack(%v) = true, want false: the rollback changed nothing", err)
	}
	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("state = %v, want CRASHED kept", got)
	}
}

func createAbortTestTemplate(t *testing.T, ctx context.Context, persistence store.Interface) {
	t.Helper()
	storetest.MustCreateAtespace(t, ctx, persistence, "ns")
	if _, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation:   testStorageLocation,
			PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
}
