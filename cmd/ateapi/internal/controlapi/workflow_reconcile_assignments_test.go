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
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// withEpoch returns a modifier func (see validWorker) which sets the worker's
// epoch.
func withEpoch(epoch int64) func(*ateapipb.Worker) {
	return func(w *ateapipb.Worker) { w.Epoch = epoch }
}

// seedEpochWorker stores apiWorkerName at epoch, with observed as the epoch
// its earlier Actors were last released for, and its ips observed.
func seedEpochWorker(t *testing.T, ctx context.Context, persistence store.Interface, epoch, observed int64) {
	t.Helper()
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, withEpoch(epoch), func(w *ateapipb.Worker) {
		w.Status = &ateapipb.WorkerStatus{
			State:                 ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			ObservedEpoch:         observed,
			IpsGeneration:         1,
			ObservedIpsGeneration: 1,
		}
	}))
}

// assertIPsObserved checks that the Worker's observed_ips_generation has
// caught up with its ips_generation, or, if !want, that it has not.
func assertIPsObserved(t *testing.T, ctx context.Context, persistence store.Interface, want bool) {
	t.Helper()
	status := mustGetWorker(t, ctx, persistence).GetStatus()
	if got := status.GetObservedIpsGeneration() >= status.GetIpsGeneration(); got != want {
		t.Errorf("observed_ips_generation = %d with ips_generation %d: observed = %t, want %t",
			status.GetObservedIpsGeneration(), status.GetIpsGeneration(), got, want)
	}
}

// changeIPs sends the UpdateWorker the syncer sends when the worker pod's IPs
// change.
func changeIPs(t *testing.T, ctx context.Context, svc *RPCService, persistence store.Interface, ips ...string) {
	t.Helper()
	_, err := svc.UpdateWorker(ctx, &ateapipb.UpdateWorkerRequest{
		Worker: updateFrom(mustGetWorker(t, ctx, persistence), func(w *ateapipb.Worker) { w.Ips = ips }),
	})
	if err != nil {
		t.Fatalf("UpdateWorker() changing ips to %v failed: %v", ips, err)
	}
}

// raiseEpoch sends the UpdateWorker the syncer sends when the worker pod's
// ateom restarts.
func raiseEpoch(t *testing.T, ctx context.Context, svc *RPCService, persistence store.Interface, epoch int64) (*ateapipb.Worker, error) {
	t.Helper()
	return svc.UpdateWorker(ctx, &ateapipb.UpdateWorkerRequest{
		Worker: updateFrom(mustGetWorker(t, ctx, persistence), func(w *ateapipb.Worker) { w.Epoch = epoch }),
	})
}

// mustRaiseEpoch is raiseEpoch for a raise the test expects to succeed.
func mustRaiseEpoch(t *testing.T, ctx context.Context, svc *RPCService, persistence store.Interface, epoch int64) {
	t.Helper()
	if _, err := raiseEpoch(t, ctx, svc, persistence, epoch); err != nil {
		t.Fatalf("UpdateWorker() raising epoch to %d failed: %v", epoch, err)
	}
}

func mustReconcileAssignments(t *testing.T, ctx context.Context, persistence store.Interface) {
	t.Helper()
	if err := NewWorkerWorkflow(persistence).ReconcileAssignments(ctx, apiWorkerName); err != nil {
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

func TestBindActorToWorker_StampsWorkerEpochAndIP(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 4, 4)

	assignment := newAPIAssignment("uid-1")
	assignment.WorkerEpoch = 99
	assignment.WorkerPodIps = []string{"10.9.9.9"}
	assignment.WorkerIpsGeneration = 99
	if err := persistence.BindActorToWorker(ctx, apiWorkerName, assignment, nil); err != nil {
		t.Fatalf("BindActorToWorker() failed: %v", err)
	}
	if got := assignment.GetWorkerEpoch(); got != 4 {
		t.Errorf("assignment worker_epoch read back = %d, want 4", got)
	}
	if got := assignment.GetWorkerPodIps(); !slices.Equal(got, []string{"10.1.2.3"}) {
		t.Errorf("assignment worker_pod_ips read back = %v, want [10.1.2.3]", got)
	}
	if got := assignment.GetWorkerIpsGeneration(); got != 1 {
		t.Errorf("assignment worker_ips_generation read back = %d, want 1", got)
	}
	stored := firstAssignment(t, persistence, apiWorkerName)
	if got := stored.GetWorkerEpoch(); got != 4 {
		t.Errorf("stored worker_epoch = %d, want 4", got)
	}
	if got := stored.GetWorkerPodIps(); !slices.Equal(got, []string{"10.1.2.3"}) {
		t.Errorf("stored worker_pod_ips = %v, want [10.1.2.3]", got)
	}
	if got := stored.GetWorkerIpsGeneration(); got != 1 {
		t.Errorf("stored worker_ips_generation = %d, want 1", got)
	}
}

// A new Worker hosts nothing, so its first epoch and ips are observed from the
// start.
func TestCreateWorker_ObservesInitialEpochAndIP(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWorkerAPIService(t)

	got, err := svc.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: validWorker(apiWorkerName, withEpoch(3))})
	if err != nil {
		t.Fatalf("CreateWorker() failed: %v", err)
	}
	if got.GetEpoch() != 3 || got.GetStatus().GetObservedEpoch() != 3 {
		t.Errorf("epoch = %d, observed_epoch = %d, want both 3", got.GetEpoch(), got.GetStatus().GetObservedEpoch())
	}
	if needsAssignmentReconcile(got) {
		t.Errorf("needsAssignmentReconcile(%v) = true, want a new Worker already observed", got.GetStatus())
	}
}

// Changing the ips only records them and raises ips_generation; the rewrite is
// the reconciler's.
func TestUpdateWorker_IPChangeLeavesActors(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)

	changeIPs(t, ctx, svc, persistence, "10.9.9.9")

	worker := mustGetWorker(t, ctx, persistence)
	if !slices.Equal(worker.GetIps(), []string{"10.9.9.9"}) {
		t.Errorf("ips = %v, want [10.9.9.9]", worker.GetIps())
	}
	if worker.GetStatus().GetIpsGeneration() != 2 || worker.GetStatus().GetObservedIpsGeneration() != 1 {
		t.Errorf("ips_generation = %d, observed_ips_generation = %d, want 2 and 1", worker.GetStatus().GetIpsGeneration(), worker.GetStatus().GetObservedIpsGeneration())
	}
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(got, []string{"10.1.2.3"}) {
		t.Errorf("actor worker_pod_ips = %v, want [10.1.2.3]", got)
	}
}

// Raising the epoch only records it; the release is the reconciler's.
func TestUpdateWorker_RaisedEpochLeavesActors(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)

	updated, err := raiseEpoch(t, ctx, svc, persistence, 2)
	if err != nil {
		t.Fatalf("UpdateWorker() failed: %v", err)
	}
	if updated.GetEpoch() != 2 || updated.GetStatus().GetObservedEpoch() != 1 {
		t.Errorf("epoch = %d, observed_epoch = %d, want 2 and 1", updated.GetEpoch(), updated.GetStatus().GetObservedEpoch())
	}
	if got := updated.GetStatus().GetIpsGeneration(); got != 1 {
		t.Errorf("ips_generation = %d, want 1: the ips did not change", got)
	}
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING", got)
	}
}

func TestUpdateWorker_EpochCannotDecrease(t *testing.T) {
	for _, tc := range []struct {
		name string
		to   int64
	}{
		{name: "lowered", to: 2},
		{name: "cleared", to: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			svc, persistence := newWorkerAPIService(t)
			seedEpochWorker(t, ctx, persistence, 3, 3)

			_, err := raiseEpoch(t, ctx, svc, persistence, tc.to)
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("UpdateWorker() = %v (err %v), want %v", got, err, codes.InvalidArgument)
			}
			if got := mustGetWorker(t, ctx, persistence).GetEpoch(); got != 3 {
				t.Errorf("worker epoch = %d, want 3", got)
			}
		})
	}
}

func TestReconcileAssignments_CrashesEarlierActors(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING, func(a *ateapipb.Actor) {
		a.Status.InProgressLocalSnapshotName = "partial-local-snapshot"
	})
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())
	mustRaiseEpoch(t, ctx, svc, persistence, 2)

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
		t.Errorf("actor state = %v, want CRASHED: its sandbox died with the ateom", got.GetStatus().GetState())
	}
	if msg := got.GetStatus().GetCrash().GetMessage(); msg != crashMessageAteomRestarted {
		t.Errorf("crash message = %q, want %q", msg, crashMessageAteomRestarted)
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("actor worker assignment = %v, want it cleared", got.GetStatus().GetWorkerAssignment())
	}
	if got.GetStatus().GetInProgressLocalSnapshotName() != "" {
		t.Errorf("in-progress local checkpoint not cleared: %v", got.GetStatus())
	}
	if a := firstAssignment(t, persistence, apiWorkerName); a != nil {
		t.Errorf("worker still hosts %v, want the assignment released", a)
	}
}

// An Actor placed after the raise was stamped with the new epoch, so it runs on
// the restarted ateom and is kept.
func TestReconcileAssignments_KeepsActorsPlacedAfterRaise(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	mustRaiseEpoch(t, ctx, svc, persistence, 2)
	seedRunningActor(t, ctx, persistence)

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

// A Worker registered before epochs were reported has observed_epoch 0. Its
// first epoch is recorded without crashing anything, and a later restart
// treats its unstamped assignments as older.
func TestReconcileAssignments_FirstEpochCrashesNothing(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	seedRunningActor(t, ctx, persistence)

	mustRaiseEpoch(t, ctx, svc, persistence, 3)
	mustReconcileAssignments(t, ctx, persistence)
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("actor state after the first epoch = %v, want RUNNING", got)
	}
	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 3 {
		t.Fatalf("observed_epoch = %d, want 3", got)
	}

	mustRaiseEpoch(t, ctx, svc, persistence, 4)
	mustReconcileAssignments(t, ctx, persistence)
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state after a restart = %v, want CRASHED", got)
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
		// Left behind by an operation that failed partway: released without
		// touching the Actor.
		{name: "crashed unassigned is released", state: ateapipb.ActorState_ACTOR_STATE_CRASHED, pointsAt: unassigned, wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantRelease: true},
		{name: "suspended unassigned is released", state: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, pointsAt: unassigned, wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, wantRelease: true},
		{name: "paused unassigned is released", state: ateapipb.ActorState_ACTOR_STATE_PAUSED, pointsAt: unassigned, wantState: ateapipb.ActorState_ACTOR_STATE_PAUSED, wantRelease: true},
		{name: "running elsewhere is released", state: ateapipb.ActorState_ACTOR_STATE_RUNNING, pointsAt: otherWorker, wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING, wantRelease: true},
		{name: "suspended elsewhere is released", state: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, pointsAt: otherWorker, wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, wantRelease: true},
		{name: "resuming crashes", state: ateapipb.ActorState_ACTOR_STATE_RESUMING, wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantRelease: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			svc, persistence := newWorkerAPIService(t)
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
			mustRaiseEpoch(t, ctx, svc, persistence, 2)

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
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, "11111111-1111-1111-1111-111111111111")
	orphan := newAPIAssignment("22222222-2222-2222-2222-222222222222")
	orphan.Actor = &ateapipb.ObjectRef{Atespace: "team-a", Name: "gone"}
	if err := persistence.BindActorToWorker(ctx, apiWorkerName, orphan, nil); err != nil {
		t.Fatalf("BindActorToWorker() failed: %v", err)
	}
	mustRaiseEpoch(t, ctx, svc, persistence, 2)

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
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	orphan := newAPIAssignment("22222222-2222-2222-2222-222222222222")
	orphan.Actor = &ateapipb.ObjectRef{Atespace: "team-a", Name: "gone"}
	if err := persistence.BindActorToWorker(ctx, apiWorkerName, orphan, nil); err != nil {
		t.Fatalf("BindActorToWorker() failed: %v", err)
	}
	mustRaiseEpoch(t, ctx, svc, persistence, 2)

	lease, err := persistence.AcquireLease(ctx, actorLeaseKey(apiActorRef))
	if err != nil {
		t.Fatalf("AcquireLease() failed: %v", err)
	}
	err = NewWorkerWorkflow(persistence).ReconcileAssignments(ctx, apiWorkerName)
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
// assignment runs on the restarted ateom, so it is kept.
func TestReconcileAssignments_KeepsActorReboundSinceList(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	mustRaiseEpoch(t, ctx, svc, persistence, 2)

	actorUID := mustGetActor(t, ctx, persistence).GetMetadata().GetUid()
	rebinding := &hookedStore{Interface: persistence, beforeLease: func() {
		if _, err := persistence.ReleaseActorFromWorker(ctx, apiWorkerName, actorUID); err != nil {
			t.Fatalf("ReleaseActorFromWorker() failed: %v", err)
		}
		assignAPIWorker(t, ctx, persistence, apiWorkerName, actorUID)
		// As assignWorkerAttempt does, the Actor records the epoch it was
		// bound in.
		actor := mustGetActor(t, ctx, persistence)
		if _, err := persistence.UpdateActor(ctx, apiActorRef, store.PreconditionFrom(actor), func(a *ateapipb.Actor) error {
			a.Status.WorkerAssignment.WorkerEpoch = 2
			return nil
		}); err != nil {
			t.Fatalf("UpdateActor() failed: %v", err)
		}
	}}
	if err := NewWorkerWorkflow(rebinding).ReconcileAssignments(ctx, apiWorkerName); err != nil {
		t.Fatalf("ReconcileAssignments() failed: %v", err)
	}

	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING", got)
	}
	if a := firstAssignment(t, persistence, apiWorkerName); a.GetWorkerEpoch() != 2 {
		t.Errorf("assignment = %v, want the epoch 2 binding kept", a)
	}
	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 2 {
		t.Errorf("observed_epoch = %d, want 2", got)
	}
}

// A bind that moves the Worker on after the pass read it only makes recording
// observed_epoch read the Worker again: the bind is stamped with the current
// epoch, so it is kept.
func TestReconcileAssignments_RetriesRecordAfterConcurrentBind(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	mustRaiseEpoch(t, ctx, svc, persistence, 2)

	const boundUID = "33333333-3333-3333-3333-333333333333"
	binding := &hookedStore{Interface: persistence, beforeUpdateWorker: func() {
		assignAPIWorker(t, ctx, persistence, apiWorkerName, boundUID)
	}}
	if err := NewWorkerWorkflow(binding).ReconcileAssignments(ctx, apiWorkerName); err != nil {
		t.Fatalf("ReconcileAssignments() failed: %v", err)
	}

	if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 2 {
		t.Errorf("observed_epoch = %d, want 2", got)
	}
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want CRASHED", got)
	}
	a, err := persistence.GetWorkerAssignment(ctx, apiWorkerName, boundUID)
	if err != nil {
		t.Fatalf("GetWorkerAssignment() = %v, want the concurrent bind kept", err)
	}
	if a.GetWorkerEpoch() != 2 {
		t.Errorf("concurrent bind epoch = %d, want 2", a.GetWorkerEpoch())
	}
}

// A Worker replaced under the same name after the pass read it is a new
// incarnation, whose observed_epoch the pass must not raise: the new Worker's
// own restarts would then look already observed.
func TestReconcileAssignments_LeavesReplacedWorker(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	mustRaiseEpoch(t, ctx, svc, persistence, 5)

	replacing := &hookedStore{Interface: persistence, beforeUpdateWorker: func() {
		if _, err := persistence.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
			t.Fatalf("DeleteWorker() failed: %v", err)
		}
		seedEpochWorker(t, ctx, persistence, 1, 1)
	}}
	if err := NewWorkerWorkflow(replacing).ReconcileAssignments(ctx, apiWorkerName); err != nil {
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
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	mustRaiseEpoch(t, ctx, svc, persistence, 2)

	failing := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: errors.New("crash failed")})
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

func TestReconcileAssignments_RewritesActorIP(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	changeIPs(t, ctx, svc, persistence, "10.9.9.9")

	mustReconcileAssignments(t, ctx, persistence)

	got := mustGetActor(t, ctx, persistence)
	if ips := got.GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(ips, []string{"10.9.9.9"}) {
		t.Errorf("actor worker_pod_ips = %v, want [10.9.9.9]", ips)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING: its sandbox survives an ip change", got.GetStatus().GetState())
	}
	if a := firstAssignment(t, persistence, apiWorkerName); a == nil {
		t.Error("assignment released, want it kept")
	} else if !slices.Equal(a.GetWorkerPodIps(), []string{"10.9.9.9"}) {
		t.Errorf("assignment worker_pod_ips = %v, want [10.9.9.9]", a.GetWorkerPodIps())
	}
	assertIPsObserved(t, ctx, persistence, true)
	worker := mustGetWorker(t, ctx, persistence)
	if worker.GetStatus().GetObservedEpoch() != 1 {
		t.Errorf("observed_epoch = %d, want 1 left alone", worker.GetStatus().GetObservedEpoch())
	}

	// Observed on the next pass: no further write.
	version := worker.GetMetadata().GetVersion()
	mustReconcileAssignments(t, ctx, persistence)
	if v := mustGetWorker(t, ctx, persistence).GetMetadata().GetVersion(); v != version {
		t.Errorf("worker version = %d after an observed pass, want %d", v, version)
	}
}

func TestReconcileAssignments_RewritesIPChangedBack(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)

	changeIPs(t, ctx, svc, persistence, "10.9.9.9")
	mustReconcileAssignments(t, ctx, persistence)
	changeIPs(t, ctx, svc, persistence, "10.1.2.3")
	mustReconcileAssignments(t, ctx, persistence)

	if ips := mustGetActor(t, ctx, persistence).GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(ips, []string{"10.1.2.3"}) {
		t.Errorf("actor worker_pod_ips = %v, want [10.1.2.3]", ips)
	}
	if ips := firstAssignment(t, persistence, apiWorkerName).GetWorkerPodIps(); !slices.Equal(ips, []string{"10.1.2.3"}) {
		t.Errorf("assignment worker_pod_ips = %v, want [10.1.2.3]", ips)
	}
}

// failingSetAssignmentIPsStore fails every rewrite of an assignment row's
// worker_pod_ips.
type failingSetAssignmentIPsStore struct {
	store.Interface
}

func (failingSetAssignmentIPsStore) SetAssignmentWorkerPodIPs(context.Context, string, string, []string, int64) error {
	return errors.New("row rewrite failed")
}

// A pass that rewrites the Actor but fails on its row is redone: the retry
// skips the Actor, which already holds the ips, and rewrites the row.
func TestReconcileAssignments_FailedRowRewriteRetried(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	changeIPs(t, ctx, svc, persistence, "10.9.9.9")

	failing := NewWorkerWorkflow(failingSetAssignmentIPsStore{Interface: persistence})
	if err := failing.ReconcileAssignments(ctx, apiWorkerName); err == nil {
		t.Fatal("ReconcileAssignments() = nil error, want the row rewrite failure reported")
	}
	actor := mustGetActor(t, ctx, persistence)
	if ips := actor.GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(ips, []string{"10.9.9.9"}) {
		t.Errorf("actor worker_pod_ips = %v, want [10.9.9.9] rewritten before the row", ips)
	}
	assertIPsObserved(t, ctx, persistence, false)

	mustReconcileAssignments(t, ctx, persistence)
	if ips := firstAssignment(t, persistence, apiWorkerName).GetWorkerPodIps(); !slices.Equal(ips, []string{"10.9.9.9"}) {
		t.Errorf("assignment worker_pod_ips = %v, want [10.9.9.9]", ips)
	}
	if got := mustGetActor(t, ctx, persistence); got.GetMetadata().GetVersion() != actor.GetMetadata().GetVersion() {
		t.Errorf("actor version = %d, want %d: it already held the ips", got.GetMetadata().GetVersion(), actor.GetMetadata().GetVersion())
	}
	assertIPsObserved(t, ctx, persistence, true)
}

// A pass that fails between the Actor and its row leaves the Actor at the new
// ips and the row at the old. If the ips then change back, the row matches them
// but the Actor does not, and it is still rewritten.
func TestReconcileAssignments_FailedRowRewriteThenIPChangedBack(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	changeIPs(t, ctx, svc, persistence, "10.9.9.9")

	failing := NewWorkerWorkflow(failingSetAssignmentIPsStore{Interface: persistence})
	if err := failing.ReconcileAssignments(ctx, apiWorkerName); err == nil {
		t.Fatal("ReconcileAssignments() = nil error, want the row rewrite failure reported")
	}
	changeIPs(t, ctx, svc, persistence, "10.1.2.3")
	mustReconcileAssignments(t, ctx, persistence)

	if ips := mustGetActor(t, ctx, persistence).GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(ips, []string{"10.1.2.3"}) {
		t.Errorf("actor worker_pod_ips = %v, want [10.1.2.3]", ips)
	}
	if ips := firstAssignment(t, persistence, apiWorkerName).GetWorkerPodIps(); !slices.Equal(ips, []string{"10.1.2.3"}) {
		t.Errorf("assignment worker_pod_ips = %v, want [10.1.2.3]", ips)
	}
}

// A dual-stack pod's second family is written to its Actors like any other
// change of ips.
func TestReconcileAssignments_RewritesFamilyAdded(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	changeIPs(t, ctx, svc, persistence, "10.1.2.3", "fd00::1")

	mustReconcileAssignments(t, ctx, persistence)

	want := []string{"10.1.2.3", "fd00::1"}
	if ips := mustGetActor(t, ctx, persistence).GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(ips, want) {
		t.Errorf("actor worker_pod_ips = %v, want %v", ips, want)
	}
	if ips := firstAssignment(t, persistence, apiWorkerName).GetWorkerPodIps(); !slices.Equal(ips, want) {
		t.Errorf("assignment worker_pod_ips = %v, want %v", ips, want)
	}
	assertIPsObserved(t, ctx, persistence, true)
}

// An Actor bound while the ips were briefly different holds those ips after
// they change back to the ones last observed. The raised ips_generation still
// has it rewritten.
func TestReconcileAssignments_RewritesActorBoundToIPsSinceChangedBack(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)

	changeIPs(t, ctx, svc, persistence, "10.9.9.9")
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING, func(a *ateapipb.Actor) {
		a.Status.WorkerAssignment.WorkerPodIps = []string{"10.9.9.9"}
		a.Status.WorkerAssignment.WorkerIpsGeneration = 2
	})
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())
	changeIPs(t, ctx, svc, persistence, "10.1.2.3")

	mustReconcileAssignments(t, ctx, persistence)

	if ips := mustGetActor(t, ctx, persistence).GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(ips, []string{"10.1.2.3"}) {
		t.Errorf("actor worker_pod_ips = %v, want [10.1.2.3]", ips)
	}
	if ips := firstAssignment(t, persistence, apiWorkerName).GetWorkerPodIps(); !slices.Equal(ips, []string{"10.1.2.3"}) {
		t.Errorf("assignment worker_pod_ips = %v, want [10.1.2.3]", ips)
	}
	assertIPsObserved(t, ctx, persistence, true)
}

// A raised epoch and a changed ip are reconciled in one pass: the Actor placed
// before the restart is crashed, and the one placed after is pointed at the
// new ip.
func TestReconcileAssignments_EpochRaiseAndIPChange(t *testing.T) {
	for _, tc := range []struct {
		name        string
		placedAfter bool
		wantState   ateapipb.ActorState
	}{
		{name: "placed before", placedAfter: false, wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED},
		{name: "placed after", placedAfter: true, wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			svc, persistence := newWorkerAPIService(t)
			seedEpochWorker(t, ctx, persistence, 1, 1)
			if !tc.placedAfter {
				seedRunningActor(t, ctx, persistence)
			}
			mustRaiseEpoch(t, ctx, svc, persistence, 2)
			changeIPs(t, ctx, svc, persistence, "10.9.9.9")
			if tc.placedAfter {
				// Seeded with the old ip, as if the placer read a stale cache.
				seedRunningActor(t, ctx, persistence)
			}

			mustReconcileAssignments(t, ctx, persistence)

			got := mustGetActor(t, ctx, persistence)
			if got.GetStatus().GetState() != tc.wantState {
				t.Errorf("actor state = %v, want %v", got.GetStatus().GetState(), tc.wantState)
			}
			if tc.placedAfter {
				if ips := got.GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(ips, []string{"10.9.9.9"}) {
					t.Errorf("actor worker_pod_ips = %v, want [10.9.9.9]", ips)
				}
				if ips := firstAssignment(t, persistence, apiWorkerName).GetWorkerPodIps(); !slices.Equal(ips, []string{"10.9.9.9"}) {
					t.Errorf("assignment worker_pod_ips = %v, want [10.9.9.9]", ips)
				}
			}
			if got := mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch(); got != 2 {
				t.Errorf("observed_epoch = %d, want 2", got)
			}
			assertIPsObserved(t, ctx, persistence, true)
		})
	}
}

// A row whose Actor points elsewhere is left over from an operation that failed
// partway, so the ip rewrite releases it without touching the Actor.
func TestReconcileAssignments_IPChangeReleasesOrphanedAssignment(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING, func(a *ateapipb.Actor) {
		a.Status.WorkerAssignment.Worker = workerRef("other-worker")
	})
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())
	changeIPs(t, ctx, svc, persistence, "10.9.9.9")

	mustReconcileAssignments(t, ctx, persistence)

	if a := firstAssignment(t, persistence, apiWorkerName); a != nil {
		t.Errorf("worker still hosts %v, want the orphaned assignment released", a)
	}
	if got := mustGetActor(t, ctx, persistence); got.GetMetadata().GetVersion() != actor.GetMetadata().GetVersion() {
		t.Errorf("actor version = %d, want %d: it runs elsewhere", got.GetMetadata().GetVersion(), actor.GetMetadata().GetVersion())
	}
}

func TestReconcileAssignments_IPRewriteWaitsForBusyActor(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	changeIPs(t, ctx, svc, persistence, "10.9.9.9")

	lease, err := persistence.AcquireLease(ctx, actorLeaseKey(apiActorRef))
	if err != nil {
		t.Fatalf("AcquireLease() failed: %v", err)
	}
	err = NewWorkerWorkflow(persistence).ReconcileAssignments(ctx, apiWorkerName)
	if !errors.Is(err, errActorBusy) {
		t.Fatalf("ReconcileAssignments() = %v, want errActorBusy", err)
	}
	assertIPsObserved(t, ctx, persistence, false)

	lease.Close()
	mustReconcileAssignments(t, ctx, persistence)
	if ips := mustGetActor(t, ctx, persistence).GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(ips, []string{"10.9.9.9"}) {
		t.Errorf("actor worker_pod_ips = %v, want [10.9.9.9] once the lease is free", ips)
	}
	assertIPsObserved(t, ctx, persistence, true)
}

func TestReconcileAssignments_FailureKeepsIPsUnobserved(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	changeIPs(t, ctx, svc, persistence, "10.9.9.9")

	failing := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: errors.New("rewrite failed")})
	if err := failing.ReconcileAssignments(ctx, apiWorkerName); err == nil {
		t.Fatal("ReconcileAssignments() = nil error, want the rewrite failure reported")
	}
	assertIPsObserved(t, ctx, persistence, false)
}

// ipChangingStore changes the Worker's ips to each of ips in turn before the
// first Actor update goes through, as the syncer would if the pod's IPs changed
// again mid-pass.
type ipChangingStore struct {
	store.Interface
	t       *testing.T
	ips     [][]string
	changed *bool
}

func (s ipChangingStore) UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	if !*s.changed {
		*s.changed = true
		svc := newServiceImpl(s.Interface, nil)
		for _, ips := range s.ips {
			worker := mustGetWorker(s.t, ctx, s.Interface)
			if _, err := svc.UpdateWorker(ctx, apiWorkerName, store.PreconditionFrom(worker), func(w *ateapipb.Worker) error {
				w.Ips = ips
				return nil
			}); err != nil {
				s.t.Fatalf("UpdateWorker() failed: %v", err)
			}
		}
	}
	return s.Interface.UpdateActor(ctx, actorRef, precondition, mutate)
}

// Ips that change again mid-pass are not recorded as observed: the Actors were
// rewritten to the ones before them.
func TestReconcileAssignments_IPChangedMidPass(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	changeIPs(t, ctx, svc, persistence, "10.9.9.9")

	var changed bool
	racing := NewWorkerWorkflow(ipChangingStore{Interface: persistence, t: t, ips: [][]string{{"10.8.8.8"}}, changed: &changed})
	if err := racing.ReconcileAssignments(ctx, apiWorkerName); err != nil {
		t.Fatalf("ReconcileAssignments() failed: %v", err)
	}
	assertIPsObserved(t, ctx, persistence, false)

	mustReconcileAssignments(t, ctx, persistence)
	if ips := mustGetActor(t, ctx, persistence).GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(ips, []string{"10.8.8.8"}) {
		t.Errorf("actor worker_pod_ips = %v, want [10.8.8.8]", ips)
	}
	assertIPsObserved(t, ctx, persistence, true)
}

// Ips that change and change back mid-pass are not recorded as observed
// either, though they match the ones the pass wrote: an Actor bound in
// between holds the ips in between.
func TestReconcileAssignments_IPChangedBackMidPass(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	changeIPs(t, ctx, svc, persistence, "10.9.9.9")

	var changed bool
	racing := NewWorkerWorkflow(ipChangingStore{Interface: persistence, t: t, ips: [][]string{{"10.8.8.8"}, {"10.9.9.9"}}, changed: &changed})
	if err := racing.ReconcileAssignments(ctx, apiWorkerName); err != nil {
		t.Fatalf("ReconcileAssignments() failed: %v", err)
	}
	assertIPsObserved(t, ctx, persistence, false)
}

// afterWorkerReadStore runs after once, the first time the Worker is read: as
// a pass starts, before it lists the Worker's assignments.
type afterWorkerReadStore struct {
	store.Interface
	after func()
	ran   *bool
}

func (s afterWorkerReadStore) GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error) {
	worker, err := s.Interface.GetWorker(ctx, name)
	if !*s.ran {
		*s.ran = true
		s.after()
	}
	return worker, err
}

// An Actor bound after the pass read the Worker, while the ips changed again,
// holds newer ips than the pass. The pass leaves it at them.
func TestReconcileAssignments_KeepsActorBoundToNewerIPs(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	changeIPs(t, ctx, svc, persistence, "10.9.9.9")

	var actor *ateapipb.Actor
	var ran bool
	racing := NewWorkerWorkflow(afterWorkerReadStore{Interface: persistence, ran: &ran, after: func() {
		changeIPs(t, ctx, svc, persistence, "10.8.8.8")
		// As assignWorkerAttempt stores it: with the ips and generation the
		// bind read under the Worker's row lock.
		actor = seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING, func(a *ateapipb.Actor) {
			a.Status.WorkerAssignment.WorkerPodIps = []string{"10.8.8.8"}
			a.Status.WorkerAssignment.WorkerIpsGeneration = 3
		})
		assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())
	}})
	if err := racing.ReconcileAssignments(ctx, apiWorkerName); err != nil {
		t.Fatalf("ReconcileAssignments() failed: %v", err)
	}

	got := mustGetActor(t, ctx, persistence)
	if got.GetMetadata().GetVersion() != actor.GetMetadata().GetVersion() {
		t.Errorf("actor version = %d, want %d: it already held newer ips", got.GetMetadata().GetVersion(), actor.GetMetadata().GetVersion())
	}
	if ips := got.GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(ips, []string{"10.8.8.8"}) {
		t.Errorf("actor worker_pod_ips = %v, want [10.8.8.8]", ips)
	}
	if ips := firstAssignment(t, persistence, apiWorkerName).GetWorkerPodIps(); !slices.Equal(ips, []string{"10.8.8.8"}) {
		t.Errorf("assignment worker_pod_ips = %v, want [10.8.8.8]", ips)
	}
	assertIPsObserved(t, ctx, persistence, false)

	mustReconcileAssignments(t, ctx, persistence)
	if got := mustGetActor(t, ctx, persistence).GetMetadata().GetVersion(); got != actor.GetMetadata().GetVersion() {
		t.Errorf("actor version = %d after the next pass, want %d", got, actor.GetMetadata().GetVersion())
	}
	assertIPsObserved(t, ctx, persistence, true)
}

// silentWorkerWatch lists Workers from the store but watches none of their
// changes.
type silentWorkerWatch struct {
	store.Interface
}

func (silentWorkerWatch) WatchWorkers(context.Context) (*store.WorkerWatch, error) {
	return store.NewWorkerWatch(make(chan store.WorkerEvent), func() {}), nil
}

// The Actor's assignment carries the epoch, ips and ips_generation the bind
// read under the Worker's row lock, not the ones in the scheduler's cache.
func TestAssignWorkerAttempt_StampsWorkerEpochAndIP(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	name := testWorkerUID("pod-1")
	created, err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: name},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		WorkerPodUid:    name,
		SandboxClass:    "gvisor",
		Ips:             []string{"10.0.0.1"},
		Epoch:           1,
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
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
	// The cache hears of no change, so it keeps the Worker as it was before
	// the update.
	wc := workercache.New(silentWorkerWatch{persistence}, time.Hour)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	precondition := store.Precondition{UID: created.GetMetadata().GetUid(), Version: created.GetMetadata().GetVersion()}
	if _, err := persistence.UpdateWorker(ctx, name, precondition, func(w *ateapipb.Worker) error {
		w.Epoch = 2
		w.Ips = []string{"10.0.0.2"}
		w.Status.IpsGeneration = 2
		return nil
	}); err != nil {
		t.Fatalf("UpdateWorker: %v", err)
	}

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc)}
	tmpl := &ateapipb.ActorTemplate{SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR}}
	stored, _, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, actor, tmpl)
	if err != nil {
		t.Fatalf("assignWorkerAttempt: %v", err)
	}
	if got := stored.GetStatus().GetWorkerAssignment().GetWorkerEpoch(); got != 2 {
		t.Errorf("actor worker_epoch = %d, want 2", got)
	}
	if got := stored.GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(got, []string{"10.0.0.2"}) {
		t.Errorf("actor worker_pod_ips = %v, want [10.0.0.2]", got)
	}
	if got := stored.GetStatus().GetWorkerAssignment().GetWorkerIpsGeneration(); got != 2 {
		t.Errorf("actor worker_ips_generation = %d, want 2", got)
	}
	assignment, err := persistence.GetWorkerAssignment(ctx, name, actor.GetMetadata().GetUid())
	if err != nil {
		t.Fatalf("GetWorkerAssignment: %v", err)
	}
	if got := assignment.GetWorkerEpoch(); got != 2 {
		t.Errorf("assignment worker_epoch = %d, want 2", got)
	}
	if got := assignment.GetWorkerIpsGeneration(); got != 2 {
		t.Errorf("assignment worker_ips_generation = %d, want 2", got)
	}
}
