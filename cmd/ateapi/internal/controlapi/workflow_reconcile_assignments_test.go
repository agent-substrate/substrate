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
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// seedEpochWorker stores apiWorkerName with epoch active, and observed as the
// epoch its earlier Actors were last released for.
func seedEpochWorker(t *testing.T, ctx context.Context, persistence store.Interface, epoch, observed int64) {
	t.Helper()
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, func(w *ateapipb.Worker) {
		w.Status = &ateapipb.WorkerStatus{
			State:           ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			RegisteredEpoch: epoch,
			ObservedEpoch:   observed,
		}
	}))
}

// mustRaiseEpoch activates epoch, as WorkerService.RegisterWorker does when a
// new worker process registers with it.
func mustRaiseEpoch(t *testing.T, ctx context.Context, persistence store.Interface, epoch int64) {
	t.Helper()
	worker := mustGetWorker(t, ctx, persistence)
	if _, err := persistence.UpdateWorker(ctx, apiWorkerName, store.PreconditionFrom(worker), func(toUpdate *ateapipb.Worker) error {
		toUpdate.Status.RegisteredEpoch = epoch
		return nil
	}); err != nil {
		t.Fatalf("UpdateWorker() activating epoch %d failed: %v", epoch, err)
	}
}

func mustReconcileAssignments(t *testing.T, ctx context.Context, persistence store.Interface) {
	t.Helper()
	if err := NewWorkerWorkflow(persistence, nil).ReconcileAssignments(ctx, apiWorkerName); err != nil {
		t.Fatalf("ReconcileAssignments() failed: %v", err)
	}
}

func mustGetWorker(t *testing.T, ctx context.Context, persistence store.Interface) *ateapipb.Worker {
	t.Helper()
	got, err := persistence.GetWorker(ctx, apiWorkerName)
	if err != nil {
		t.Fatalf("GetWorker() failed: %v", err)
	}
	return got
}

func mustGetActor(t *testing.T, ctx context.Context, persistence store.Interface) *ateapipb.Actor {
	t.Helper()
	got, err := persistence.GetActor(ctx, apiActorRef)
	if err != nil {
		t.Fatalf("GetActor() failed: %v", err)
	}
	return got
}

// seedRunningActor stores a RUNNING Actor assigned to apiWorkerName.
func seedRunningActor(t *testing.T, ctx context.Context, persistence store.Interface) {
	t.Helper()
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())
}

func TestReconcileAssignments_CrashesEarlierActors(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING, func(a *ateapipb.Actor) {
		a.Status.LastAssignedGeneration = 1
		a.Status.Snapshots = []*ateapipb.Snapshot{
			newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "partial-local-snapshot", ""),
		}
	})
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())
	mustRaiseEpoch(t, ctx, persistence, 2)

	mustReconcileAssignments(t, ctx, persistence)

	worker := mustGetWorker(t, ctx, persistence)
	if worker.GetStatus().GetObservedEpoch() != 2 {
		t.Errorf("observed_epoch = %d, want 2", worker.GetStatus().GetObservedEpoch())
	}
	if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("worker state = %v, want it left ACTIVE", worker.GetStatus().GetState())
	}
	if worker.GetStatus().GetAllocated().GetActors() != 0 {
		t.Errorf("worker allocated = %v, want the crashed actor's share freed", worker.GetStatus().GetAllocated())
	}
	got := mustGetActor(t, ctx, persistence)
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want CRASHED: its sandbox died with the worker", got.GetStatus().GetState())
	}
	if msg := got.GetStatus().GetCrash().GetMessage(); msg != crashMessageWorkerRestarted {
		t.Errorf("crash message = %q, want %q", msg, crashMessageWorkerRestarted)
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("actor worker assignment = %v, want it cleared", got.GetStatus().GetWorkerAssignment())
	}
	if gotSnap := snapshotAtLatestGeneration(got.GetStatus()); gotSnap.GetUuid() != "partial-local-snapshot" {
		t.Errorf("in-progress local checkpoint = %q, want %q preserved", gotSnap.GetUuid(), "partial-local-snapshot")
	}
	if a := firstAssignment(t, persistence, apiWorkerName); a != nil {
		t.Errorf("worker still hosts %v, want the assignment released", a)
	}
}

// An Actor restored on the new worker process records that process's epoch
// when it becomes RUNNING, so it is kept.
func TestReconcileAssignments_KeepsActorRestoredOnNewProcess(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	mustRaiseEpoch(t, ctx, persistence, 2)
	mustUpdateActorStatus(t, ctx, persistence, mustGetActor(t, ctx, persistence), func(s *ateapipb.ActorStatus) {
		s.WorkerAssignment.WorkerEpoch = 2
	})

	mustReconcileAssignments(t, ctx, persistence)

	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING", got)
	}
	if firstAssignment(t, persistence, apiWorkerName) == nil {
		t.Error("assignment released, want it kept")
	}
	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 2 {
		t.Errorf("observed_epoch = %d, want 2", got)
	}
}

func TestReconcileAssignments_AlreadyObservedWritesNothing(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 2, 2)
	seedRunningActor(t, ctx, persistence)
	version := mustGetWorker(t, ctx, persistence).GetMetadata().GetVersion()

	mustReconcileAssignments(t, ctx, persistence)

	if got := mustGetWorker(t, ctx, persistence).GetMetadata().GetVersion(); got != version {
		t.Errorf("worker version = %d, want %d: nothing to write", got, version)
	}
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING", got)
	}
}

// Each assignment from an earlier epoch is released, but what happens to its
// Actor depends on where the Actor stands.
func TestReconcileAssignments_ReleasesByActorState(t *testing.T) {
	const (
		here = iota
		unassigned
		otherWorker
	)
	tests := []struct {
		name        string
		state       ateapipb.ActorState
		pointsAt    int
		wantState   ateapipb.ActorState
		wantRelease bool
	}{
		// Suspended cleanly before the restart: still resumable.
		{name: "suspended stays suspended", state: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, wantRelease: true},
		{name: "crashed is released", state: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantRelease: true},
		// Nothing it ran on the Worker: a claim a resume is still recording,
		// or one its next resume recovers. Left alone.
		{name: "crashed unassigned is left", state: ateapipb.ActorState_ACTOR_STATE_CRASHED, pointsAt: unassigned, wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED},
		{name: "suspended unassigned is left", state: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, pointsAt: unassigned, wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		{name: "running elsewhere is left", state: ateapipb.ActorState_ACTOR_STATE_RUNNING, pointsAt: otherWorker, wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING},
		{name: "suspended elsewhere is left", state: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, pointsAt: otherWorker, wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		// Nothing it ran is kept: its resume restores it on the new process.
		{name: "resuming is left to its resume", state: ateapipb.ActorState_ACTOR_STATE_RESUMING, wantState: ateapipb.ActorState_ACTOR_STATE_RESUMING},
		{name: "deleting stays deleting", state: ateapipb.ActorState_ACTOR_STATE_DELETING, wantState: ateapipb.ActorState_ACTOR_STATE_DELETING, wantRelease: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			_, persistence := newWorkerAPIService(t)
			seedEpochWorker(t, ctx, persistence, 1, 1)
			actor := seedAPIActor(t, ctx, persistence, tc.state, func(a *ateapipb.Actor) {
				switch tc.pointsAt {
				case unassigned:
					a.Status.WorkerAssignment = nil
				case otherWorker:
					a.Status.WorkerAssignment.Worker = workerRef("other-worker")
				}
			})
			assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())
			mustRaiseEpoch(t, ctx, persistence, 2)

			mustReconcileAssignments(t, ctx, persistence)

			if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != tc.wantState {
				t.Errorf("actor state = %v, want %v", got, tc.wantState)
			}
			released := firstAssignment(t, persistence, apiWorkerName) == nil
			if released != tc.wantRelease {
				t.Errorf("assignment released = %v, want %v", released, tc.wantRelease)
			}
		})
	}
}

// An assignment whose Actor is gone, or has been recreated, names nothing
// running, so it is released without touching the live Actor.
func TestReconcileAssignments_ReleasesOrphanedAssignments(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, "11111111-1111-1111-1111-111111111111")
	orphan := newAPIAssignment("22222222-2222-2222-2222-222222222222")
	orphan.Actor = &ateapipb.ObjectRef{Atespace: "team-a", Name: "gone"}
	if err := persistence.BindActorToWorker(ctx, apiWorkerName, orphan, nil); err != nil {
		t.Fatalf("BindActorToWorker() failed: %v", err)
	}
	mustRaiseEpoch(t, ctx, persistence, 2)

	mustReconcileAssignments(t, ctx, persistence)

	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING: the assignment named another incarnation of it", got)
	}
	if a := firstAssignment(t, persistence, apiWorkerName); a != nil {
		t.Errorf("worker still hosts %v, want every orphaned assignment released", a)
	}
}

// An assignment whose Actor has an operation in progress waits for it: the rest
// are released, but observed_epoch stays put until a pass gets the Actor too.
func TestReconcileAssignments_WaitsForBusyActor(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	orphan := newAPIAssignment("22222222-2222-2222-2222-222222222222")
	orphan.Actor = &ateapipb.ObjectRef{Atespace: "team-a", Name: "gone"}
	if err := persistence.BindActorToWorker(ctx, apiWorkerName, orphan, nil); err != nil {
		t.Fatalf("BindActorToWorker() failed: %v", err)
	}
	mustRaiseEpoch(t, ctx, persistence, 2)

	lease, err := persistence.AcquireLease(ctx, actorLeaseKey(apiActorRef))
	if err != nil {
		t.Fatalf("AcquireLease() failed: %v", err)
	}
	err = NewWorkerWorkflow(persistence, nil).ReconcileAssignments(ctx, apiWorkerName)
	if !errors.Is(err, errActorBusy) {
		t.Fatalf("ReconcileAssignments() = %v, want errActorBusy", err)
	}
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING while its lease is held", got)
	}
	if _, err := persistence.GetWorkerAssignment(ctx, apiWorkerName, orphan.GetActorUid()); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("orphaned assignment: GetWorkerAssignment() = %v, want it released", err)
	}
	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 1 {
		t.Errorf("observed_epoch = %d, want 1 until the busy actor is released", got)
	}

	lease.Close()
	mustReconcileAssignments(t, ctx, persistence)
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want CRASHED once the lease is free", got)
	}
	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 2 {
		t.Errorf("observed_epoch = %d, want 2", got)
	}
}

// hookedStore runs each hook once, just before the first call it names.
// beforeLease lands between the pass listing an assignment and acting on it;
// beforeUpdateWorker between the pass reading the Worker and recording
// observed_epoch on it.
type hookedStore struct {
	store.Interface
	beforeLease        func()
	beforeUpdateWorker func()
}

// runOnce calls *hook, if set, and clears it.
func runOnce(hook *func()) {
	if h := *hook; h != nil {
		*hook = nil
		h()
	}
}

func (s *hookedStore) AcquireLease(ctx context.Context, key string) (*store.Lease, error) {
	runOnce(&s.beforeLease)
	return s.Interface.AcquireLease(ctx, key)
}

func (s *hookedStore) UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapipb.Worker) error) (*ateapipb.Worker, error) {
	runOnce(&s.beforeUpdateWorker)
	return s.Interface.UpdateWorker(ctx, name, precondition, mutate)
}

// An Actor bound again in the current epoch after the pass listed its earlier
// assignment runs on the restarted worker, so it is kept.
func TestReconcileAssignments_KeepsActorReboundSinceList(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	mustRaiseEpoch(t, ctx, persistence, 2)

	actorUID := mustGetActor(t, ctx, persistence).GetMetadata().GetUid()
	rebinding := &hookedStore{Interface: persistence, beforeLease: func() {
		if _, err := persistence.ReleaseActorFromWorker(ctx, apiWorkerName, actorUID); err != nil {
			t.Fatalf("ReleaseActorFromWorker() failed: %v", err)
		}
		assignAPIWorker(t, ctx, persistence, apiWorkerName, actorUID)
		// As finalizeRunning does, the Actor records the epoch of the
		// process it was restored on.
		actor := mustGetActor(t, ctx, persistence)
		if _, err := persistence.UpdateActor(ctx, apiActorRef, store.PreconditionFrom(actor), func(a *ateapipb.Actor) error {
			a.Status.WorkerAssignment.WorkerEpoch = 2
			return nil
		}); err != nil {
			t.Fatalf("UpdateActor() failed: %v", err)
		}
	}}
	if err := NewWorkerWorkflow(rebinding, nil).ReconcileAssignments(ctx, apiWorkerName); err != nil {
		t.Fatalf("ReconcileAssignments() failed: %v", err)
	}

	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING", got)
	}
	if firstAssignment(t, persistence, apiWorkerName) == nil {
		t.Error("assignment released, want the new binding kept")
	}
	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 2 {
		t.Errorf("observed_epoch = %d, want 2", got)
	}
}

// A bind that moves the Worker on after the pass read it only makes recording
// observed_epoch read the Worker again: the bind came after the sweep, so it is
// kept.
func TestReconcileAssignments_RetriesRecordAfterConcurrentBind(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	mustRaiseEpoch(t, ctx, persistence, 2)

	const boundUID = "33333333-3333-3333-3333-333333333333"
	binding := &hookedStore{Interface: persistence, beforeUpdateWorker: func() {
		assignAPIWorker(t, ctx, persistence, apiWorkerName, boundUID)
	}}
	if err := NewWorkerWorkflow(binding, nil).ReconcileAssignments(ctx, apiWorkerName); err != nil {
		t.Fatalf("ReconcileAssignments() failed: %v", err)
	}

	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 2 {
		t.Errorf("observed_epoch = %d, want 2", got)
	}
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want CRASHED", got)
	}
	if _, err := persistence.GetWorkerAssignment(ctx, apiWorkerName, boundUID); err != nil {
		t.Fatalf("GetWorkerAssignment() = %v, want the concurrent bind kept", err)
	}
}

// A Worker replaced under the same name after the pass read it is a new
// incarnation, whose observed_epoch the pass must not raise: the new Worker's
// own restarts would then look already observed.
func TestReconcileAssignments_LeavesReplacedWorker(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	mustRaiseEpoch(t, ctx, persistence, 5)

	replacing := &hookedStore{Interface: persistence, beforeUpdateWorker: func() {
		if _, err := persistence.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
			t.Fatalf("DeleteWorker() failed: %v", err)
		}
		seedEpochWorker(t, ctx, persistence, 1, 1)
	}}
	if err := NewWorkerWorkflow(replacing, nil).ReconcileAssignments(ctx, apiWorkerName); err != nil {
		t.Fatalf("ReconcileAssignments() failed: %v", err)
	}

	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 1 {
		t.Errorf("replacement observed_epoch = %d, want 1", got)
	}
}

// A release that fails leaves observed_epoch where it was, so the next pass
// redoes it.
func TestReconcileAssignments_FailureKeepsObservedEpoch(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	mustRaiseEpoch(t, ctx, persistence, 2)

	failing := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: errors.New("crash failed")}, nil)
	if err := failing.ReconcileAssignments(ctx, apiWorkerName); err == nil {
		t.Fatal("ReconcileAssignments() = nil error, want the crash failure reported")
	}
	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 1 {
		t.Errorf("observed_epoch = %d, want 1 until the release succeeds", got)
	}
	if firstAssignment(t, persistence, apiWorkerName) == nil {
		t.Error("assignment released, want it kept for the retry")
	}

	mustReconcileAssignments(t, ctx, persistence)
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state after the retry = %v, want CRASHED", got)
	}
	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 2 {
		t.Errorf("observed_epoch after the retry = %d, want 2", got)
	}
}

func TestReconcileAssignments_WorkerGone(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerAPIService(t)
	mustReconcileAssignments(t, ctx, persistence)
}

// Placing an Actor records no epoch: only the worker process that restores it
// knows which epoch it runs under.
func TestAssignWorkerAttempt_LeavesWorkerEpochUnset(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	name := testWorkerUID("pod-1")
	_, err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: name},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		WorkerPodUid:    name,
		NodeName:        "node-1",
		Status: &ateapipb.WorkerStatus{
			State:           ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			RegisteredEpoch: 1,
			Capacity:        &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses:  workerSandboxClasses(testSandboxRuntime()),
		},
	})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc)}
	tmpl := &ateapipb.ActorTemplate{}
	stored, _, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, tmpl)
	if err != nil {
		t.Fatalf("assignWorkerAttempt: %v", err)
	}
	if got := stored.GetStatus().GetWorkerAssignment().GetWorkerEpoch(); got != 0 {
		t.Errorf("actor worker_epoch = %d, want 0 until the Actor is RUNNING", got)
	}
}
