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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newWorkerDeleteWorkflow returns a workflow backed by a real store, which is
// what makes the release assertions below meaningful — a fake would decide the
// outcome the test is trying to observe.
func newWorkerDeleteWorkflow(t *testing.T) (*WorkerWorkflow, store.Interface) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	return NewWorkerWorkflow(persistence), persistence
}

// apiActorRef names the Actor seedAPIActor stores.
var apiActorRef = resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

// seedAPIActor stores an Actor bound to apiWorkerName in the given state — the
// shape the delete's release step acts on. Its coordinates line up with
// newAPIWorker and newAPIAssignment, so the two seeds agree about who is bound
// to whom.
func seedAPIActor(t *testing.T, ctx context.Context, persistence store.Interface, state ateapipb.ActorState, opts ...func(*ateapipb.Actor)) *ateapipb.Actor {
	t.Helper()
	actor := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: apiActorRef.Atespace, Name: apiActorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ate-system", Name: "tmpl"},
		Status: &ateapipb.ActorStatus{
			State: state,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          workerRef(apiWorkerName),
				WorkerNamespace: "ate-system",
				WorkerPool:      "pool-1",
				WorkerPod:       "worker-pod-1",
				WorkerPodUid:    apiWorkerName,
				WorkerPodIp:     "10.1.2.3",
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
func TestDeleteWorkerWorkflow_ReleasesBoundActor(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, newAPIWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING, func(a *ateapipb.Actor) {
		// Both in-progress checkpoints are set so the assertion covers the
		// shared crash path, which cannot know which workflow was in flight.
		a.Status.InProgressSnapshotName = "partial-snapshot"
		a.Status.InProgressLocalSnapshotName = "partial-local-snapshot"
		a.Status.LatestSnapshot = &ateapipb.ObjectRef{Atespace: apiActorRef.Atespace, Name: "last"}
	})
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}

	got, err := persistence.GetActor(ctx, apiActorRef)
	if err != nil {
		t.Fatalf("GetActor() failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want CRASHED: it never suspended cleanly", got.GetStatus().GetState())
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("actor worker assignment = %v, want it cleared", got.GetStatus().GetWorkerAssignment())
	}
	// The durable checkpoint was never uploaded and the local one lived on the
	// node that went away, so both die with the worker.
	if got.GetStatus().GetInProgressSnapshotName() != "" || got.GetStatus().GetInProgressLocalSnapshotName() != "" {
		t.Errorf("in-progress checkpoints not cleared: %v", got.GetStatus())
	}
	// The last completed snapshot is what makes the actor resumable, so it stays.
	if got.GetStatus().GetLatestSnapshot().GetName() != "last" {
		t.Errorf("latest snapshot = %v, want it preserved", got.GetStatus().GetLatestSnapshot())
	}
}

// The Actor's state when its pod vanished decides what the release does: one
// that had already suspended saved its state cleanly and stays resumable, while
// anything still in flight crashes and is counted. An Actor already counted as
// crashed is released again without being counted twice.
func TestDeleteWorkerWorkflow_ReleasedActorStateTransitions(t *testing.T) {
	tests := []struct {
		name       string
		start      ateapipb.ActorState
		wantState  ateapipb.ActorState
		wantOp     string
		wantMetric bool
	}{
		{name: "running becomes crashed", start: ateapipb.ActorState_ACTOR_STATE_RUNNING, wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantOp: ateattr.OperationUnknown, wantMetric: true},
		{name: "resuming becomes crashed", start: ateapipb.ActorState_ACTOR_STATE_RESUMING, wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantOp: ateattr.OperationResume, wantMetric: true},
		{name: "suspending becomes crashed", start: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantOp: ateattr.OperationSuspend, wantMetric: true},
		{name: "pausing becomes crashed", start: ateapipb.ActorState_ACTOR_STATE_PAUSING, wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantOp: ateattr.OperationPause, wantMetric: true},
		{name: "suspended stays suspended", start: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, wantMetric: false},
		{name: "crashed is not counted twice", start: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED, wantMetric: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			if err := RegisterActorCrashes(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("ateapi")); err != nil {
				t.Fatalf("RegisterActorCrashes: %v", err)
			}

			ctx := context.Background()
			wf, persistence := newWorkerDeleteWorkflow(t)
			seedAPIWorker(t, ctx, persistence, newAPIWorker(apiWorkerName))
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
			if tc.wantState == ateapipb.ActorState_ACTOR_STATE_CRASHED && got.GetStatus().GetWorkerAssignment() != nil {
				t.Errorf("crashed actor worker assignment = %v, want it cleared", got.GetStatus().GetWorkerAssignment())
			}
			if tc.wantMetric {
				assertCrashMetricDatapoint(t, reader, tc.wantOp, ateattr.ReasonWorkerPodGone, "ate-system", "tmpl", "pool-1", "gvisor", 1)
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
	seedAPIWorker(t, ctx, persistence, newAPIWorker(apiWorkerName))
	seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, "old-incarnation-uid")

	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}

	got, err := persistence.GetActor(ctx, apiActorRef)
	if err != nil {
		t.Fatalf("GetActor() failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING: the dead worker was bound to a different incarnation", got.GetStatus().GetState())
	}
}

// An Actor that has since been placed on another Worker is no longer this
// Worker's to release, even though this Worker still points at it.
func TestDeleteWorkerWorkflow_IgnoresActorMovedElsewhere(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, newAPIWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING, func(a *ateapipb.Actor) {
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
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
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
	seedAPIWorker(t, ctx, persistence, newAPIWorker(apiWorkerName))
	assignAPIWorker(t, ctx, persistence, apiWorkerName, "actor-uid-1")

	got, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{})
	if err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}
	if got.GetStatus().GetAssignment().GetActorUid() != "actor-uid-1" {
		t.Errorf("deleted worker assignment = %v, want the one it was holding", got.GetStatus().GetAssignment())
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
		wantActor  ateapipb.ActorState
		wantWorker bool
	}{
		{
			// A transient store failure says nothing about who owns what, so
			// both records stay as they were.
			name:       "store unavailable",
			updateErr:  errors.New("store is down"),
			wantCode:   codes.Unknown,
			wantActor:  ateapipb.ActorState_ACTOR_STATE_RUNNING,
			wantWorker: true,
		},
		{
			// A concurrent Suspend or Resume got there first. The caller
			// retries against the state that write left behind.
			name:       "lost the version guard",
			updateErr:  store.ErrVersionConflict,
			wantCode:   codes.Aborted,
			wantActor:  ateapipb.ActorState_ACTOR_STATE_RUNNING,
			wantWorker: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			_, persistence := newWorkerDeleteWorkflow(t)
			seedAPIWorker(t, ctx, persistence, newAPIWorker(apiWorkerName))
			actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
			assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

			wf := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: tc.updateErr})
			_, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{})
			if err == nil {
				t.Fatal("DeleteWorker() = nil error, want the release failure reported")
			}
			if got := status.Code(err); got != tc.wantCode {
				t.Errorf("DeleteWorker() code = %v (err %v), want %v", got, err, tc.wantCode)
			}

			if _, err := persistence.GetWorker(ctx, apiWorkerName); err != nil {
				t.Errorf("worker gone after a failed release: %v", err)
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
	seedAPIWorker(t, ctx, persistence, newAPIWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	wf := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: store.ErrNotFound})
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
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("DeleteWorker() code = %v (err %v), want %v", got, err, codes.NotFound)
	}
	if want := "step LoadWorkerForDelete"; !strings.Contains(err.Error(), want) {
		t.Errorf("DeleteWorker() error = %q, want it to name the step it failed at (%q)", err, want)
	}
}

// failingUpdateActorStore wraps a store and fails every UpdateActor call,
// simulating a state-store error while releasing an actor.
type failingUpdateActorStore struct {
	store.Interface
	err error
}

func (f failingUpdateActorStore) UpdateActor(context.Context, resources.ActorRef, store.Precondition, func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	return nil, f.err
}
