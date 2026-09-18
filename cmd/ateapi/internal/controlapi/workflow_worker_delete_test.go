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
	"net"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// newWorkerDeleteWorkflow returns a workflow backed by a real store, which is
// what makes the release assertions below meaningful — a fake would decide the
// outcome the test is trying to observe.
func newWorkerDeleteWorkflow(t *testing.T) (*WorkerWorkflow, store.Interface) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	return NewWorkerWorkflow(persistence, nil), persistence
}

// apiActorRef names the Actor seedAPIActor stores.
var apiActorRef = resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

// seedAPIActor stores an Actor bound to apiWorkerName in the given state — the
// shape the delete's release step acts on. Its coordinates line up with
// validWorker and newAPIAssignment, so the two seeds agree about who is bound
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
// A Worker left ACTIVE through the sweep can take a bind onto a page already
// passed, and the delete then cascades that assignment away while the Actor
// still points at it. Failing the release keeps the record around, so the
// state it held during the sweep can be read.
func TestDeleteWorkerWorkflow_DrainsBeforeSweeping(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	wf := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: errors.New("release failed")}, nil)
	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err == nil {
		t.Fatal("DeleteWorker() = nil error, want the release failure reported")
	}

	got, err := persistence.GetWorker(ctx, apiWorkerName)
	if err != nil {
		t.Fatalf("GetWorker() failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("worker state during the sweep = %v, want DRAINING so nothing new binds",
			got.GetStatus().GetState())
	}
}

func TestDeleteWorkerWorkflow_ReleasesBoundActor(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING, func(a *ateapipb.Actor) {
		// Both in-progress checkpoints are set so the assertion covers the
		// shared crash path, which cannot know which workflow was in flight.
		a.Status.InProgressSnapshotUri = someActorSnapshotURI(t, testStorageLocation, apiActorRef.Atespace, "partial-snapshot")
		a.Status.InProgressLocalSnapshotName = "partial-local-snapshot"
		a.Status.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: someActorSnapshotURI(t, testStorageLocation, apiActorRef.Atespace, "last")}
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
	// The local checkpoint lived on the node that went away, so it dies with
	// the worker.
	if got.GetStatus().GetInProgressLocalSnapshotName() != "" {
		t.Errorf("in-progress local checkpoint not cleared: %v", got.GetStatus())
	}
	// The durable one is kept: it names the prefix whatever atelet already
	// uploaded lives under, which the actor's delete needs to collect it.
	if want := someActorSnapshotURI(t, testStorageLocation, apiActorRef.Atespace, "partial-snapshot"); got.GetStatus().GetInProgressSnapshotUri() != want {
		t.Errorf("in-progress external checkpoint not preserved: %v", got.GetStatus())
	}
	// The last completed snapshot is what makes the actor resumable, so it stays.
	if want := someActorSnapshotURI(t, testStorageLocation, apiActorRef.Atespace, "last"); got.GetStatus().GetExternalSnapshot().GetSnapshotUri() != want {
		t.Errorf("external snapshot = %q, want it preserved as %q", got.GetStatus().GetExternalSnapshot().GetSnapshotUri(), want)
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
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
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
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
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
			seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
			actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
			assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

			wf := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: tc.updateErr}, nil)
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
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	wf := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: store.ErrNotFound}, nil)
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

// A Worker is deleted because its pod is gone, and the pod took the ateom with
// it but not the actor's state directory on the node — a durdir actor leaves
// gigabytes there. This delete is the last moment anything knows which node
// that is, so it is where the reclaim has to happen.
func TestDeleteWorkerWorkflow_ReclaimsActorNodeState(t *testing.T) {
	tests := []struct {
		name string
		// state the actor is in when its pod disappears.
		state ateapipb.ActorState
		// seed further shapes the stored actor.
		seed func(*ateapipb.Actor)
		// terminateErr is what the atelet answers, if anything.
		terminateErr  error
		wantTerminate bool
	}{
		{
			name:          "running actor is reclaimed",
			state:         ateapipb.ActorState_ACTOR_STATE_RUNNING,
			wantTerminate: true,
		},
		{
			// It saved its state externally and stays resumable, but it is
			// resumable somewhere else: what it left on this node is dead
			// weight, and the release path skips it for every other purpose.
			name:          "cleanly suspended actor is reclaimed too",
			state:         ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			wantTerminate: true,
		},
		{
			// Terminate prunes local checkpoints, and a local snapshot is the
			// one piece of actor state this node holds deliberately. Leaving
			// it costs disk a sweep can still reclaim; deleting it is
			// unrecoverable.
			name:  "actor with a local snapshot is left alone",
			state: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			seed: func(a *ateapipb.Actor) {
				a.Status.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{
					SnapshotName:              "pause-1",
					NodeVmsWithLocalSnapshots: []string{"node-1"},
				}
			},
			wantTerminate: false,
		},
		{
			// An unreachable or unhappy node must not wedge deregistration:
			// the worker still goes, and the orphan sweep collects what this
			// could not.
			name:          "a failing terminate does not fail the delete",
			state:         ateapipb.ActorState_ACTOR_STATE_RUNNING,
			terminateErr:  status.Error(codes.Internal, "atelet is having a bad day"),
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
			seeds := []func(*ateapipb.Actor){}
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
			// The actor the node has to be told about, and the ateom UID it
			// was hosted by — which is what atelet resolves the (now absent)
			// sandbox through.
			if req.GetActorUid() != actor.GetMetadata().GetUid() {
				t.Errorf("Terminate actor_uid = %q, want %q", req.GetActorUid(), actor.GetMetadata().GetUid())
			}
			if req.GetAtespace() != apiActorRef.Atespace || req.GetActorName() != apiActorRef.Name {
				t.Errorf("Terminate named %s/%s, want %s", req.GetAtespace(), req.GetActorName(), apiActorRef)
			}
			if want := validWorker(apiWorkerName).GetWorkerPodUid(); req.GetTargetAteomUid() != want {
				t.Errorf("Terminate target_ateom_uid = %q, want the worker's pod uid %q", req.GetTargetAteomUid(), want)
			}
		})
	}
}

// A Worker whose record names no node cannot be reclaimed against: the node is
// the only handle left on the atelet once the pod is gone. The delete carries
// on regardless.
func TestDeleteWorkerWorkflow_ReclaimSkippedWithoutANode(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	atelet := &capturingTerminator{}
	wf := NewWorkerWorkflow(persistence, newNodeAteletDialer(t, atelet))

	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.NodeName = "" }))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	if _, err := wf.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}
	if got := atelet.requests(); len(got) != 0 {
		t.Errorf("atelet received %d Terminate calls, want none: %v", len(got), got)
	}
}

// capturingTerminator is an atelet that records the Terminate calls it is sent
// and answers them with err.
type capturingTerminator struct {
	ateletpb.UnimplementedAteomHerderServer
	err error

	mu   sync.Mutex
	reqs []*ateletpb.TerminateRequest
}

func (f *capturingTerminator) Terminate(_ context.Context, req *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, proto.Clone(req).(*ateletpb.TerminateRequest))
	if f.err != nil {
		return nil, f.err
	}
	return &ateletpb.TerminateResponse{}, nil
}

func (f *capturingTerminator) requests() []*ateletpb.TerminateRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.reqs)
}

// newNodeAteletDialer resolves node-1's atelet to an in-process fake. The conn
// cache is pre-warmed by the atelet pod's UID, so the by-node lookup under test
// runs for real and only the transport is short-circuited. No worker pod is
// seeded: this is the state the reclaim exists for, where the pod is gone.
func newNodeAteletDialer(t *testing.T, srvImpl ateletpb.AteomHerderServer) *AteletDialer {
	t.Helper()

	srv := grpc.NewServer()
	ateletpb.RegisterAteomHerderServer(srv, srvImpl)
	lis := bufconn.Listen(1 << 20)
	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("fake atelet server exited: %v", err)
		}
	}()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("connecting to the fake atelet: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})

	goneWorkerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "worker-pod-gone", UID: "worker-pod-gone"},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	}
	ateletPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ateletNamespace, Name: "atelet-1", UID: "atelet-uid"},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	}
	dialer := newDialerForPods(t, goneWorkerPod, ateletPod)
	dialer.ateletConns.Add("atelet-uid", conn)
	return dialer
}
