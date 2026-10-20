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
	"strings"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// newWorkerDeleteWorkflow returns a workflow backed by a real store, which is
// what makes the release assertions below meaningful — a fake would decide the
// outcome the test is trying to observe.
func newWorkerDeleteWorkflow(t *testing.T) (*WorkerWorkflow, store.Interface) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	return NewWorkerWorkflow(persistence, newNodeAteletDialer(t, &capturingTerminator{})), persistence
}

// apiActorRef names the Actor seedAPIActor stores.
var apiActorRef = resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

// seedAPIActor stores an Actor bound to apiWorkerName in the given state — the
// shape the delete's release step acts on. Its coordinates line up with
// validWorker and newAPIAssignment, so the two seeds agree about who is bound
// to whom.
func seedAPIActor(t *testing.T, ctx context.Context, persistence store.Interface, state ateapi.ActorState, opts ...func(*ateapi.Actor)) *ateapi.Actor {
	t.Helper()
	actor := &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: apiActorRef.Atespace, Name: apiActorRef.Name},
		ActorTemplate: &ateapi.ObjectRef{Atespace: "ate-system", Name: "tmpl"},
		Status: &ateapi.ActorStatus{
			State: state,
			WorkerAssignment: &ateapi.WorkerAssignment{
				Worker:          workerRef(apiWorkerName),
				WorkerNamespace: "ate-system",
				WorkerPool:      "pool-1",
				WorkerPod:       "worker-pod-1",
				WorkerPodUid:    apiWorkerName,
				WorkerPodIps:    []string{"10.1.2.3"},
				SandboxRuntime:  &ateapi.ActorSandboxRuntime{SandboxClass: "gvisor"},
			},
		},
	}
	for _, opt := range opts {
		opt(actor)
	}
	// MustCreateActor rather than the store method: an Actor's parent atespace
	// has to exist first, and none of these tests are about that.
	return storetest.MustCreateActor(t, ctx, persistence, actor)
}

// A Worker is deleted because its pod is gone, which takes the Actor running on
// it with it. The release happens in this workflow rather than in the caller
// that noticed the pod had vanished, because an assignment write stays
// in-process: there is no bind/release RPC for that caller to reach for.
// A Worker left ACTIVE through the sweep can take a bind onto a page already
// passed, and the delete then cascades that assignment away while the Actor
// still points at it. Failing the release keeps the record around, so the
// state it held during the sweep can be read.
func TestDeleteWorkerWorkflow_DrainsBeforeSweeping(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapi.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	wf := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: errors.New("release failed")}, newNodeAteletDialer(t, &capturingTerminator{}))
	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err == nil {
		t.Fatal("DeleteWorker() = nil error, want the release failure reported")
	}

	got, err := persistence.GetWorker(ctx, apiWorkerName)
	if err != nil {
		t.Fatalf("GetWorker() failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapi.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("worker state during the sweep = %v, want DRAINING so nothing new binds",
			got.GetStatus().GetState())
	}
}

func TestDeleteWorkerWorkflow_ReleasesBoundActor(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapi.ActorState_ACTOR_STATE_RUNNING, func(a *ateapi.Actor) {
		a.Status.LastAssignedGeneration = 2
		a.Status.Snapshots = []*ateapi.Snapshot{
			newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "last", someActorSnapshotURI(t, testStorageLocation, apiActorRef.Atespace, "last"), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newInProgressSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "partial-snapshot"),
		}
	})
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}

	got, err := persistence.GetActor(ctx, apiActorRef)
	if err != nil {
		t.Fatalf("GetActor() failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want CRASHED: it never suspended cleanly", got.GetStatus().GetState())
	}
	if msg := got.GetStatus().GetCrash().GetMessage(); msg != crashMessageWorkerPodGone {
		t.Errorf("crash message = %q, want %q", msg, crashMessageWorkerPodGone)
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("actor worker assignment = %v, want it cleared", got.GetStatus().GetWorkerAssignment())
	}
	// The in-progress checkpoint is kept: it names the prefix whatever atelet
	// already uploaded lives under, which delete or revert needs to collect it.
	gotInProgressSnap := findLatestDurableSnapshot(got.GetStatus(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	if want := someActorSnapshotURI(t, testStorageLocation, apiActorRef.Atespace, "partial-snapshot"); gotInProgressSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != want {
		t.Errorf("in-progress external checkpoint not preserved: %v", got.GetStatus())
	}
	// The last completed snapshot is what makes the actor resumable, so it stays.
	gotCompletedSnap := findLatestDurableSnapshot(got.GetStatus(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if want := someActorSnapshotURI(t, testStorageLocation, apiActorRef.Atespace, "last"); gotCompletedSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != want {
		t.Errorf("external snapshot = %q, want it preserved as %q", gotCompletedSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(), want)
	}
}

// The Actor's state when its pod vanished decides what the release does: one
// that had already suspended saved its state cleanly and stays resumable, while
// anything still in flight crashes and is counted. An Actor being deleted stays
// DELETING so it can never be resumed. An Actor already counted as crashed is
// released again without being counted twice.
func TestDeleteWorkerWorkflow_ReleasedActorStateTransitions(t *testing.T) {
	tests := []struct {
		name       string
		start      ateapi.ActorState
		wantState  ateapi.ActorState
		wantOp     string
		wantMetric bool
	}{
		{name: "running becomes crashed", start: ateapi.ActorState_ACTOR_STATE_RUNNING, wantState: ateapi.ActorState_ACTOR_STATE_CRASHED, wantOp: ateattr.OperationUnknown, wantMetric: true},
		{name: "resuming becomes crashed", start: ateapi.ActorState_ACTOR_STATE_RESUMING, wantState: ateapi.ActorState_ACTOR_STATE_CRASHED, wantOp: ateattr.OperationResume, wantMetric: true},
		{name: "suspending becomes crashed", start: ateapi.ActorState_ACTOR_STATE_SUSPENDING, wantState: ateapi.ActorState_ACTOR_STATE_CRASHED, wantOp: ateattr.OperationSuspend, wantMetric: true},
		{name: "reverting becomes crashed", start: ateapi.ActorState_ACTOR_STATE_REVERTING, wantState: ateapi.ActorState_ACTOR_STATE_CRASHED, wantOp: ateattr.OperationRevert, wantMetric: true},
		{name: "deleting stays deleting", start: ateapi.ActorState_ACTOR_STATE_DELETING, wantState: ateapi.ActorState_ACTOR_STATE_DELETING, wantMetric: false},
		{name: "suspended stays suspended", start: ateapi.ActorState_ACTOR_STATE_SUSPENDED, wantState: ateapi.ActorState_ACTOR_STATE_SUSPENDED, wantMetric: false},
		{name: "crashed is not counted twice", start: ateapi.ActorState_ACTOR_STATE_CRASHED, wantState: ateapi.ActorState_ACTOR_STATE_CRASHED, wantMetric: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			if err := RegisterActorCrashes(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("ateapi")); err != nil {
				t.Fatalf("RegisterActorCrashes: %v", err)
			}

			ctx := context.Background()
			wf, persistence := newWorkerDeleteWorkflow(t)
			seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
			actor := seedAPIActor(t, ctx, persistence, tc.start)
			assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

			if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteWorker() failed: %v", err)
			}

			got, err := persistence.GetActor(ctx, apiActorRef)
			if err != nil {
				t.Fatalf("GetActor() failed: %v", err)
			}
			if got.GetStatus().GetState() != tc.wantState {
				t.Errorf("actor state = %v, want %v", got.GetStatus().GetState(), tc.wantState)
			}
			if tc.wantState == ateapi.ActorState_ACTOR_STATE_CRASHED && got.GetStatus().GetWorkerAssignment() != nil {
				t.Errorf("crashed actor worker assignment = %v, want it cleared", got.GetStatus().GetWorkerAssignment())
			}
			if tc.wantMetric {
				assertCrashMetricDatapoint(t, reader, tc.wantOp, "ate-system", "tmpl", "pool-1", "gvisor", 1)
			} else {
				assertNoCrashMetricDatapoint(t, reader)
			}
		})
	}
}

// The Worker's assignment names the Actor incarnation it was bound to. An Actor
// that has since been recreated under the same name is a different incarnation,
// so the dead Worker's assignment says nothing about it and must not crash it.
func TestDeleteWorkerWorkflow_IgnoresStaleIncarnationAssignment(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	seedAPIActor(t, ctx, persistence, ateapi.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, "old-incarnation-uid")

	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}

	got, err := persistence.GetActor(ctx, apiActorRef)
	if err != nil {
		t.Fatalf("GetActor() failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING: the dead worker was bound to a different incarnation", got.GetStatus().GetState())
	}
}

// An Actor that has since been placed on another Worker is no longer this
// Worker's to release, even though this Worker still points at it.
func TestDeleteWorkerWorkflow_IgnoresActorMovedElsewhere(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapi.ActorState_ACTOR_STATE_RUNNING, func(a *ateapi.Actor) {
		a.Status.WorkerAssignment.Worker = workerRef(apiOtherWorkerName)
	})
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}

	got, err := persistence.GetActor(ctx, apiActorRef)
	if err != nil {
		t.Fatalf("GetActor() failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING: it is running on another worker", got.GetStatus().GetState())
	}
	if got.GetStatus().GetWorkerAssignment().GetWorker().GetName() != apiOtherWorkerName {
		t.Errorf("actor worker assignment = %v, want it left pointing at the other worker", got.GetStatus().GetWorkerAssignment())
	}
}

// An assignment whose Actor no longer exists leaves nothing to release, so the
// delete goes through: the state it is driving towards — no Actor pointing at
// this Worker — already holds.
func TestDeleteWorkerWorkflow_AssignedToAbsentActorDeletesAnyway(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	assignAPIWorker(t, ctx, persistence, apiWorkerName, "actor-uid-1")

	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}
	// An assignment is its own record now, so what proves the delete went
	// through is that nothing is left pointing at the Worker.
	if got := firstAssignment(t, persistence, apiWorkerName); got != nil {
		t.Errorf("assignment after delete = %v, want none", got)
	}
}

// A release that fails must leave the Worker in place: the delete is what
// erases the Actor's pointer at it, so the record has to stay findable for the
// caller to rediscover and retry.
func TestDeleteWorkerWorkflow_FailedReleaseKeepsWorker(t *testing.T) {
	tests := []struct {
		name       string
		updateErr  error
		wantCode   codes.Code
		wantActor  ateapi.ActorState
		wantWorker bool
	}{
		{
			// A transient store failure says nothing about who owns what, so
			// both records stay as they were.
			name:       "store unavailable",
			updateErr:  errors.New("store is down"),
			wantCode:   codes.Internal,
			wantActor:  ateapi.ActorState_ACTOR_STATE_RUNNING,
			wantWorker: true,
		},
		{
			// A concurrent Suspend or Resume got there first. The caller
			// retries against the state that write left behind.
			name:       "lost the version guard",
			updateErr:  store.ErrVersionConflict,
			wantCode:   codes.Aborted,
			wantActor:  ateapi.ActorState_ACTOR_STATE_RUNNING,
			wantWorker: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			_, persistence := newWorkerDeleteWorkflow(t)
			seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
			actor := seedAPIActor(t, ctx, persistence, ateapi.ActorState_ACTOR_STATE_RUNNING)
			assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

			wf := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: tc.updateErr}, newNodeAteletDialer(t, &capturingTerminator{}))
			_, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{})
			if err == nil {
				t.Fatal("DeleteWorker() = nil error, want the release failure reported")
			}
			if got := apierror.Code(err); got != tc.wantCode {
				t.Errorf("DeleteWorker() code = %v (err %v), want %v", got, err, tc.wantCode)
			}

			worker, err := persistence.GetWorker(ctx, apiWorkerName)
			if err != nil {
				t.Fatalf("worker gone after a failed release: %v", err)
			}
			if worker.GetMetadata().GetDeleteTime() == nil {
				t.Errorf("a failed delete left delete_time unset")
			}
			if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err == nil {
				t.Fatal("retried DeleteWorker() = nil error, want the release failure reported")
			}
			retried, err := persistence.GetWorker(ctx, apiWorkerName)
			if err != nil {
				t.Fatalf("worker gone after a retried failed release: %v", err)
			}
			if got, want := retried.GetMetadata().GetVersion(), worker.GetMetadata().GetVersion(); got != want {
				t.Errorf("a retried delete moved the worker version to %d, want %d", got, want)
			}
			got, err := persistence.GetActor(ctx, apiActorRef)
			if err != nil {
				t.Fatalf("GetActor() failed: %v", err)
			}
			if got.GetStatus().GetState() != tc.wantActor {
				t.Errorf("actor state = %v, want %v: the release never landed", got.GetStatus().GetState(), tc.wantActor)
			}
		})
	}
}

// An Actor deleted between the read and the write leaves nothing pointing at
// the Worker, which is what the release was driving towards, so the delete
// carries on.
func TestDeleteWorkerWorkflow_ActorDeletedDuringRelease(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapi.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	wf := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: store.ErrNotFound}, newNodeAteletDialer(t, &capturingTerminator{}))
	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}

	if _, err := persistence.GetWorker(ctx, apiWorkerName); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetWorker() error = %v, want the worker deleted", err)
	}
}

// Steps wrap what they return, which has to leave the gRPC code the caller
// branches on intact — the worker-pod syncer reads NOT_FOUND off this call to
// decide a deregistration is already done.
func TestDeleteWorkerWorkflow_AbsentReportsNotFoundThroughStepWrap(t *testing.T) {
	ctx := context.Background()
	wf, _ := newWorkerDeleteWorkflow(t)

	_, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{})
	if got := apierror.Code(err); got != codes.NotFound {
		t.Fatalf("DeleteWorker() code = %v (err %v), want %v", got, err, codes.NotFound)
	}
	if want := "step MarkWorkerForDeletion"; !strings.Contains(err.Error(), want) {
		t.Errorf("DeleteWorker() error = %q, want it to name the step it failed at (%q)", err, want)
	}
}

// failingUpdateActorStore wraps a store and fails every UpdateActor call,
// simulating a state-store error while releasing an actor.
type failingUpdateActorStore struct {
	store.Interface
	err error
}

func (f failingUpdateActorStore) UpdateActor(context.Context, resources.ActorRef, store.Precondition, func(*ateapi.Actor) error) (*ateapi.Actor, error) {
	return nil, f.err
}

func TestDeleteWorkerWorkflow_DrainedWorkerIsStillAdmitted(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	worker := seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	drained, err := persistence.UpdateWorker(ctx, apiWorkerName, store.PreconditionFrom(worker), func(toUpdate *ateapi.Worker) error {
		if toUpdate.Status == nil {
			toUpdate.Status = &ateapi.WorkerStatus{}
		}
		toUpdate.Status.State = ateapi.WorkerState_WORKER_STATE_DRAINING
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}

	deleted, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{})
	if err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}
	if deleted.GetMetadata().GetDeleteTime() == nil {
		t.Fatal("an operator-drained worker was removed without being marked for deletion")
	}
	if got, want := deleted.GetMetadata().GetVersion(), drained.GetMetadata().GetVersion()+1; got != want {
		t.Errorf("version = %d, want %d: admission is one write", got, want)
	}
}

// A Worker is deleted because its pod is gone. The pod took the worker process
// with it, leaving the actor's state directory on the node. This delete
// reclaims it ahead of the actor's revert or delete.
func TestDeleteWorkerWorkflow_ReclaimsActorNodeState(t *testing.T) {
	tests := []struct {
		name string
		// state the actor is in when its pod disappears.
		state ateapi.ActorState
		// seed further shapes the stored actor.
		seed func(*ateapi.Actor)
		// terminateErr is what the atelet answers, if anything.
		terminateErr  error
		wantTerminate bool
	}{
		{
			name:          "running actor is reclaimed",
			state:         ateapi.ActorState_ACTOR_STATE_RUNNING,
			wantTerminate: true,
		},
		{
			// An actor resumed from a local snapshot keeps Locality on that
			// snapshot while running. When its worker pod vanishes, it crashes
			// and its node state is reclaimed along with any local snapshot.
			name:          "running actor with a local snapshot is reclaimed",
			state:         ateapi.ActorState_ACTOR_STATE_RUNNING,
			seed:          withPausedShape(t, "node-1"),
			wantTerminate: true,
		},
		{
			// A cleanly suspended actor already ran Checkpoint's teardown,
			// volume unmount, and resetActorDirs, and stays resumable.
			name:          "cleanly suspended actor is left alone",
			state:         ateapi.ActorState_ACTOR_STATE_SUSPENDED,
			wantTerminate: false,
		},
		{
			// An unreachable or unhappy node must not wedge deregistration:
			// the worker still goes, and the actor's revert or delete collects
			// what this could not.
			name:          "a failing terminate does not fail the delete",
			state:         ateapi.ActorState_ACTOR_STATE_RUNNING,
			terminateErr:  apierror.Internal("atelet is having a bad day"),
			wantTerminate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)

			atelet := &capturingTerminator{err: tt.terminateErr}
			wf := NewWorkerWorkflow(persistence, newNodeAteletDialer(t, atelet))

			seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
			seeds := []func(*ateapi.Actor){}
			if tt.seed != nil {
				seeds = append(seeds, tt.seed)
			}
			actor := seedAPIActor(t, ctx, persistence, tt.state, seeds...)
			assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

			if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteWorker() failed: %v", err)
			}

			got := atelet.requests()
			if !tt.wantTerminate {
				if len(got) != 0 {
					t.Fatalf("atelet received %d Terminate calls, want none: %v", len(got), got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("atelet received %d Terminate calls, want exactly 1", len(got))
			}
			req := got[0]
			// The request names the actor and the worker pod UID it ran on, so
			// atelet can check whether that worker is still present.
			if req.GetActorUid() != actor.GetMetadata().GetUid() {
				t.Errorf("Terminate actor_uid = %q, want %q", req.GetActorUid(), actor.GetMetadata().GetUid())
			}
			if req.GetAtespace() != apiActorRef.Atespace || req.GetActorName() != apiActorRef.Name {
				t.Errorf("Terminate named %s/%s, want %s", req.GetAtespace(), req.GetActorName(), apiActorRef)
			}
			if want := validWorker(apiWorkerName).GetWorkerPodUid(); req.GetWorkerPodUid() != want {
				t.Errorf("Terminate worker_pod_uid = %q, want the worker's pod uid %q", req.GetWorkerPodUid(), want)
			}
		})
	}
}

// A suspend holds the actor's lease from writing its snapshot to finalizing
// it. A release that reclaimed in between would race the checkpoint, so the
// release waits for the lease: while it is held, the actor and its worker are
// left alone and the delete fails ABORTED for a retry. Once it is free no
// suspend is running and the release reclaims and crashes the actor.
func TestDeleteWorkerWorkflow_ReleasesUnderTheActorLease(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	atelet := &capturingTerminator{}
	wf := NewWorkerWorkflow(persistence, newNodeAteletDialer(t, atelet))

	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapi.ActorState_ACTOR_STATE_SUSPENDING, func(a *ateapi.Actor) {
		withPauseInProgress(t)(a)
		a.Status.AssignedNode = "node-1"
	})
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	lease, err := persistence.AcquireLease(ctx, actorLeaseKey(apiActorRef))
	if err != nil {
		t.Fatalf("AcquireLease() failed: %v", err)
	}
	_, err = wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{})
	if got := atelet.requests(); len(got) != 0 {
		t.Errorf("atelet received %d Terminate calls while the actor's lease was held, want none", len(got))
	}
	if got := apierror.Code(err); got != codes.Aborted {
		t.Errorf("DeleteWorker() with the actor's lease held = %v, want ABORTED", err)
	}
	held := mustGetActor(t, ctx, persistence)
	if got := held.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDING {
		t.Errorf("actor state = %v, want SUSPENDING while its lease is held", got)
	}
	if got := held.GetStatus().GetWorkerAssignment().GetWorker().GetName(); got != apiWorkerName {
		t.Errorf("actor points at worker %q, want %q while its lease is held", got, apiWorkerName)
	}
	if _, err := persistence.GetWorker(ctx, apiWorkerName); err != nil {
		t.Errorf("GetWorker() = %v, want the worker kept for the retry", err)
	}

	lease.Close()
	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker() once the lease is free failed: %v", err)
	}
	if got := atelet.requests(); len(got) != 1 {
		t.Errorf("atelet received %d Terminate calls once the lease was free, want 1", len(got))
	}
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want CRASHED once the lease is free", got)
	}
}

// One actor with an operation in progress does not hold up the rest of the
// worker's actors: they are reclaimed and crashed in the same call, which then
// fails ABORTED so the delete is retried for the busy one.
func TestDeleteWorkerWorkflow_BusyActorDoesNotHoldUpOthers(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	atelet := &capturingTerminator{}
	wf := NewWorkerWorkflow(persistence, newNodeAteletDialer(t, atelet))

	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	var refs []resources.ActorRef
	var uids []string
	for _, name := range []string{"busy", "free"} {
		actor := seedAPIActor(t, ctx, persistence, ateapi.ActorState_ACTOR_STATE_RUNNING, func(a *ateapi.Actor) {
			a.Metadata.Name = name
		})
		ref := resources.ActorRefFromActor(actor)
		if err := persistence.BindActorToWorker(ctx, apiWorkerName, &ateapi.ActorAssignment{
			Actor:    &ateapi.ObjectRef{Atespace: ref.Atespace, Name: ref.Name},
			ActorUid: actor.GetMetadata().GetUid(),
		}, nil); err != nil {
			t.Fatalf("assigning %s to worker %s: %v", ref, apiWorkerName, err)
		}
		refs = append(refs, ref)
		uids = append(uids, actor.GetMetadata().GetUid())
	}

	lease, err := persistence.AcquireLease(ctx, actorLeaseKey(refs[0]))
	if err != nil {
		t.Fatalf("AcquireLease() failed: %v", err)
	}
	defer lease.Close()
	_, err = wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{})
	if got := apierror.Code(err); got != codes.Aborted {
		t.Errorf("DeleteWorker() with one actor's lease held = %v, want ABORTED", err)
	}
	got := atelet.requests()
	if len(got) != 1 || got[0].GetActorUid() != uids[1] {
		t.Errorf("atelet received %v, want exactly one Terminate, for the free actor %s", got, uids[1])
	}
	for i, want := range []ateapi.ActorState{ateapi.ActorState_ACTOR_STATE_RUNNING, ateapi.ActorState_ACTOR_STATE_CRASHED} {
		actor, err := persistence.GetActor(ctx, refs[i])
		if err != nil {
			t.Fatalf("GetActor(%s) failed: %v", refs[i], err)
		}
		if actor.GetStatus().GetState() != want {
			t.Errorf("actor %s state = %v, want %v", refs[i], actor.GetStatus().GetState(), want)
		}
	}
}

// An unmount that hangs, such as one against a dead NFS server, or an
// unreachable atelet returning Unavailable, must not wedge deregistration.
// Each reclaim gets w.reclaimTimeout, and once one times out or returns
// Unavailable the worker's other actors, and subsequent worker deletes on that
// node within w.reclaimTimeout, leave theirs to their revert or delete. Once
// w.reclaimTimeout elapses, a later delete tries the node again.
func TestDeleteWorkerWorkflow_HungReclaimDoesNotWedgeDelete(t *testing.T) {
	for _, tt := range []struct {
		name string
		hang bool
		err  error
	}{
		{name: "timeout", hang: true},
		{name: "unavailable", err: apierror.Unavailable("atelet shutting down")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)

			now := time.Now()
			atelet := &capturingTerminator{hang: tt.hang, err: tt.err}
			wf := NewWorkerWorkflow(persistence, newNodeAteletDialer(t, atelet))
			wf.reclaimTimeout = 50 * time.Millisecond
			wf.now = func() time.Time { return now }

			bindRunningActor := func(workerName, actorName string) resources.ActorRef {
				actor := seedAPIActor(t, ctx, persistence, ateapi.ActorState_ACTOR_STATE_RUNNING, func(a *ateapi.Actor) {
					a.Metadata.Name = actorName
					a.Status.WorkerAssignment.Worker = workerRef(workerName)
					a.Status.WorkerAssignment.WorkerPodUid = workerName
				})
				ref := resources.ActorRefFromActor(actor)
				if err := persistence.BindActorToWorker(ctx, workerName, &ateapi.ActorAssignment{
					Actor:    &ateapi.ObjectRef{Atespace: ref.Atespace, Name: ref.Name},
					ActorUid: actor.GetMetadata().GetUid(),
				}, nil); err != nil {
					t.Fatalf("assigning %s to worker %s: %v", ref, workerName, err)
				}
				return ref
			}

			seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
			refs := []resources.ActorRef{
				bindRunningActor(apiWorkerName, "actor-1"),
				bindRunningActor(apiWorkerName, "actor-2"),
			}

			// A deadline, as the RPC has. A Terminate without a timeout of its own runs
			// into it, and the release after it fails.
			rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if _, err := wf.DeleteWorker(rpcCtx, apiWorkerName, store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteWorker(first) failed: %v", err)
			}
			if got := len(atelet.requests()); got != 1 {
				t.Errorf("atelet received %d Terminate calls on first worker delete, want 1: the first failure gives up on the node", got)
			}

			// A second worker deleted from the same node within w.reclaimTimeout
			// skips Terminate without waiting out another timeout.
			seedAPIWorker(t, ctx, persistence, validWorker(apiOtherWorkerName))
			refs = append(refs, bindRunningActor(apiOtherWorkerName, "actor-3"))
			if _, err := wf.DeleteWorker(rpcCtx, apiOtherWorkerName, store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteWorker(second) failed: %v", err)
			}
			if got := len(atelet.requests()); got != 1 {
				t.Errorf("atelet received %d Terminate calls across two worker deletes on the same node, want 1", got)
			}

			for _, ref := range refs {
				got, err := persistence.GetActor(ctx, ref)
				if err != nil {
					t.Fatalf("GetActor(%s) failed: %v", ref, err)
				}
				if got.GetStatus().GetWorkerAssignment() != nil {
					t.Errorf("actor %s still points at a worker: %v", ref, got.GetStatus().GetWorkerAssignment())
				}
			}

			// Once w.reclaimTimeout elapses, a subsequent worker delete tries the node again.
			now = now.Add(wf.reclaimTimeout)
			atelet.mu.Lock()
			atelet.hang = false
			atelet.err = nil
			atelet.mu.Unlock()
			seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
			bindRunningActor(apiWorkerName, "actor-4")
			if _, err := wf.DeleteWorker(rpcCtx, apiWorkerName, store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteWorker(after timeout) failed: %v", err)
			}
			if got := len(atelet.requests()); got != 2 {
				t.Errorf("atelet received %d Terminate calls after w.reclaimTimeout elapsed, want 2", got)
			}
		})
	}
}

// capturingTerminator is an atelet that records the Terminate calls it is sent
// and answers them with err, or, with hang set, blocks until the caller gives
// up, like an atelet stuck in an unmount.
type capturingTerminator struct {
	ateletpb.UnimplementedAteletServer

	mu   sync.Mutex
	err  error
	hang bool
	reqs []*ateletpb.TerminateRequest
}

func (f *capturingTerminator) Terminate(ctx context.Context, req *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, proto.Clone(req).(*ateletpb.TerminateRequest))
	hang, err := f.hang, f.err
	f.mu.Unlock()
	if hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	return &ateletpb.TerminateResponse{}, nil
}

func (f *capturingTerminator) requests() []*ateletpb.TerminateRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.reqs)
}
