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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
)

func namesWorker(worker string) func(*ateapi.Actor) {
	return func(a *ateapi.Actor) {
		a.Status.WorkerAssignment.Worker = workerRef(worker)
	}
}

func TestCleanStaleAssignments_Rows(t *testing.T) {
	namesNone := func(a *ateapi.Actor) { a.Status.WorkerAssignment = nil }

	for _, tc := range []struct {
		name string
		// actor is applied to the stored Actor. A nil actor stores none.
		actor func(*ateapi.Actor)
		// rowUID names the actor UID on the row. Empty uses the stored Actor's.
		rowUID       string
		dryRun       bool
		holdLease    bool
		wantReleased bool
		wantFixed    int
		wantCrashed  bool
		wantErr      error
	}{
		{name: "actor names the worker", actor: namesWorker(apiWorkerName)},
		{name: "actor names the worker while busy", actor: namesWorker(apiWorkerName), holdLease: true},
		{name: "actor is gone", rowUID: "gone-uid", wantReleased: true, wantFixed: 1},
		{name: "actor is another incarnation", actor: namesNone, rowUID: "old-uid", wantReleased: true, wantFixed: 1},
		{name: "actor names no worker", actor: namesNone, wantReleased: true, wantFixed: 1},
		// The Actor pass then crashes the Actor, whose worker is gone.
		{name: "actor names another worker", actor: namesWorker(apiOtherWorkerName), wantReleased: true, wantFixed: 2, wantCrashed: true},
		{name: "dry run", rowUID: "gone-uid", dryRun: true, wantFixed: 1},
		{name: "actor busy", rowUID: "gone-uid", holdLease: true, wantErr: errActorBusy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
			rowUID := tc.rowUID
			if tc.actor != nil {
				actor := seedAPIActor(t, ctx, persistence, ateapi.ActorState_ACTOR_STATE_RUNNING, tc.actor)
				if rowUID == "" {
					rowUID = actor.GetMetadata().GetUid()
				}
			}
			assignAPIWorker(t, ctx, persistence, apiWorkerName, rowUID)
			if tc.holdLease {
				lease, err := persistence.AcquireLease(ctx, actorLeaseKey(apiActorRef))
				if err != nil {
					t.Fatalf("AcquireLease() failed: %v", err)
				}
				t.Cleanup(lease.Close)
			}

			fixed, err := CleanStaleAssignments(ctx, persistence, tc.dryRun)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("CleanStaleAssignments() error = %v, want %v", err, tc.wantErr)
			}
			if fixed != tc.wantFixed {
				t.Errorf("CleanStaleAssignments() = %d, want %d", fixed, tc.wantFixed)
			}

			_, err = persistence.GetWorkerAssignment(ctx, apiWorkerName, rowUID)
			if gone := errors.Is(err, store.ErrNotFound); gone != tc.wantReleased {
				t.Errorf("GetWorkerAssignment() error = %v, want released %t", err, tc.wantReleased)
			}
			wantActors := int32(1)
			if tc.wantReleased {
				wantActors = 0
			}
			if got := mustGetWorker(t, ctx, persistence).GetStatus().GetAllocated().GetActors(); got != wantActors {
				t.Errorf("worker allocated actors = %d, want %d", got, wantActors)
			}
			if tc.wantCrashed {
				if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_CRASHED {
					t.Errorf("actor state = %v, want CRASHED", got)
				}
			}
		})
	}
}

// An Actor naming a Worker that holds no row for it is crashed if it is
// RESUMING or its sandbox is gone, and left for an operator otherwise.
func TestCleanStaleAssignments_ActorWithoutRow(t *testing.T) {
	ranUnder := func(epoch int64) func(*ateapi.Actor) {
		return func(a *ateapi.Actor) { a.Status.WorkerAssignment.WorkerEpoch = epoch }
	}

	const (
		resuming   = ateapi.ActorState_ACTOR_STATE_RESUMING
		running    = ateapi.ActorState_ACTOR_STATE_RUNNING
		suspending = ateapi.ActorState_ACTOR_STATE_SUSPENDING
		reverting  = ateapi.ActorState_ACTOR_STATE_REVERTING
		deleting   = ateapi.ActorState_ACTOR_STATE_DELETING
		crashed    = ateapi.ActorState_ACTOR_STATE_CRASHED
	)

	for _, tc := range []struct {
		name  string
		state ateapi.ActorState
		actor func(*ateapi.Actor)
		// row books the Actor on apiWorkerName.
		row       bool
		dryRun    bool
		holdLease bool
		// wantState is the Actor's state after the run. Zero means unchanged.
		wantState ateapi.ActorState
		// wantCleared is whether the run clears the Actor's worker assignment.
		wantCleared bool
		wantMessage string
		wantErr     error
	}{
		{name: "row present", state: running, actor: ranUnder(2), row: true},
		{name: "resuming", state: resuming, actor: ranUnder(2), wantState: crashed, wantCleared: true, wantMessage: crashMessageAssignmentMissing},
		// A resume records worker_epoch only once it is RUNNING.
		{name: "resuming on the current worker process", state: resuming, actor: ranUnder(0), wantState: crashed, wantCleared: true, wantMessage: crashMessageAssignmentMissing},
		{name: "resuming on a gone worker", state: resuming, actor: namesWorker(apiOtherWorkerName), wantState: crashed, wantCleared: true, wantMessage: crashMessageWorkerPodGone},
		{name: "running on a gone worker", state: running, actor: namesWorker(apiOtherWorkerName), wantState: crashed, wantCleared: true, wantMessage: crashMessageWorkerPodGone},
		{name: "running on a restarted worker", state: running, actor: ranUnder(1), wantState: crashed, wantCleared: true, wantMessage: crashMessageWorkerRestarted},
		{name: "running on the same worker process", state: running, actor: ranUnder(2), wantErr: errActorLeft},
		{name: "suspending on a restarted worker", state: suspending, actor: ranUnder(1), wantState: crashed, wantCleared: true, wantMessage: crashMessageWorkerRestarted},
		{name: "suspending on the same worker process", state: suspending, actor: ranUnder(2), wantErr: errActorLeft},
		{name: "reverting on a restarted worker", state: reverting, actor: ranUnder(1), wantState: crashed, wantCleared: true, wantMessage: crashMessageWorkerRestarted},
		{name: "reverting on the same worker process", state: reverting, actor: ranUnder(2), wantErr: errActorLeft},
		{name: "deleting on a gone worker", state: deleting, actor: namesWorker(apiOtherWorkerName), wantCleared: true},
		{name: "deleting on the same worker process", state: deleting, actor: ranUnder(2), wantErr: errActorLeft},
		{name: "dry run", state: resuming, actor: ranUnder(2), dryRun: true},
		{name: "busy", state: resuming, actor: ranUnder(2), holdLease: true, wantErr: errActorBusy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			seedEpochWorker(t, ctx, persistence, 2, 2)
			actor := seedAPIActor(t, ctx, persistence, tc.state, tc.actor)
			if tc.row {
				assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())
			}
			if tc.holdLease {
				lease, err := persistence.AcquireLease(ctx, actorLeaseKey(apiActorRef))
				if err != nil {
					t.Fatalf("AcquireLease() failed: %v", err)
				}
				t.Cleanup(lease.Close)
			}

			fixed, err := CleanStaleAssignments(ctx, persistence, tc.dryRun)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("CleanStaleAssignments() error = %v, want %v", err, tc.wantErr)
			}
			wantFixed := 0
			if tc.wantCleared || tc.dryRun {
				wantFixed = 1
			}
			if fixed != wantFixed {
				t.Errorf("CleanStaleAssignments() = %d, want %d", fixed, wantFixed)
			}

			got := mustGetActor(t, ctx, persistence)
			wantState := tc.wantState
			if wantState == 0 {
				wantState = tc.state
			}
			if got.GetStatus().GetState() != wantState {
				t.Errorf("actor state = %v, want %v", got.GetStatus().GetState(), wantState)
			}
			if named := got.GetStatus().GetWorkerAssignment() != nil; named == tc.wantCleared {
				t.Errorf("actor names a worker = %t, want %t", named, !tc.wantCleared)
			}
			if got, want := got.GetStatus().GetCrash().GetMessage(), tc.wantMessage; want != "" && !strings.HasSuffix(got, want) {
				t.Errorf("crash message = %q, want it to end with %q", got, want)
			}
		})
	}
}
