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

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// newTestTransitionReaper builds a reaper over persistence whose deletes run
// the real delete workflow against atelet on node-1. pastGrace sets its clock
// past the grace period.
func newTestTransitionReaper(t *testing.T, persistence store.Interface, atelet ateletpb.AteletServer, pastGrace bool) *ActorTransitionReaper {
	t.Helper()
	workflow := newTestActorWorkflow(t, persistence, "ate-system", "tmpl")
	workflow.dialer = newNodeAteletDialer(t, atelet)
	svc := &RPCService{
		impl:          newServiceImpl(persistence, nil),
		actorWorkflow: workflow,
	}
	r := NewActorTransitionReaper(persistence, svc)
	if pastGrace {
		r.now = func() time.Time { return time.Now().Add(transitionGracePeriod) }
	}
	return r
}

// onNode1 seeds the actor with its sandbox on node-1, where the test atelet is.
func onNode1(a *ateapipb.Actor) {
	a.Status.AssignedNode = "node-1"
}

func TestActorTransitionReaper_Sweep(t *testing.T) {
	tests := []struct {
		name  string
		state ateapipb.ActorState
		// fresh writes the actor within the grace period.
		fresh bool
		// leased holds the actor's lease during the sweep, as a live workflow does.
		leased bool
		// terminateErr is what atelet answers Terminate with, if anything.
		terminateErr error
		// wantState is the state after the sweep; wantDeleted expects the
		// actor gone instead.
		wantState   ateapipb.ActorState
		wantDeleted bool
		// wantReleased expects the worker's assignment to the actor released.
		wantReleased bool
		// wantTerminated expects atelet asked to terminate the sandbox.
		wantTerminated bool
	}{
		{
			name:      "running actor is left alone",
			state:     ateapipb.ActorState_ACTOR_STATE_RUNNING,
			wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
		{
			name:      "suspending actor within the grace period is left alone",
			state:     ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			fresh:     true,
			wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		},
		{
			name:      "suspending actor whose lease is held is left alone",
			state:     ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			leased:    true,
			wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		},
		{
			name:           "suspending actor past the grace period is crashed",
			state:          ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			wantState:      ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantReleased:   true,
			wantTerminated: true,
		},
		{
			name:           "suspending actor is crashed when atelet is unavailable",
			state:          ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			terminateErr:   apierror.Unavailable("atelet is restarting"),
			wantState:      ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantReleased:   true,
			wantTerminated: true,
		},
		{
			name:           "resuming actor past the grace period is crashed",
			state:          ateapipb.ActorState_ACTOR_STATE_RESUMING,
			wantState:      ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantReleased:   true,
			wantTerminated: true,
		},
		{
			name:           "reverting actor past the grace period is crashed",
			state:          ateapipb.ActorState_ACTOR_STATE_REVERTING,
			wantState:      ateapipb.ActorState_ACTOR_STATE_CRASHED,
			wantReleased:   true,
			wantTerminated: true,
		},
		{
			name:      "deleting actor whose lease is held is left alone",
			state:     ateapipb.ActorState_ACTOR_STATE_DELETING,
			leased:    true,
			wantState: ateapipb.ActorState_ACTOR_STATE_DELETING,
		},
		{
			name:           "deleting actor past the grace period is deleted",
			state:          ateapipb.ActorState_ACTOR_STATE_DELETING,
			wantDeleted:    true,
			wantReleased:   true,
			wantTerminated: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
			actor := seedAPIActor(t, ctx, persistence, tc.state, onNode1)
			if tc.state == ateapipb.ActorState_ACTOR_STATE_DELETING {
				if _, err := persistence.MarkActorForDeletion(ctx, apiActorRef, store.DeletePreconditions{}, nil); err != nil {
					t.Fatalf("MarkActorForDeletion() failed: %v", err)
				}
			}
			assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())
			if tc.leased {
				lease, err := persistence.AcquireLease(ctx, actorLeaseKey(apiActorRef))
				if err != nil {
					t.Fatalf("AcquireLease() failed: %v", err)
				}
				defer lease.Close()
			}

			atelet := &capturingTerminator{err: tc.terminateErr}
			newTestTransitionReaper(t, persistence, atelet, !tc.fresh).sweep(ctx)

			if terminated := len(atelet.requests()) > 0; terminated != tc.wantTerminated {
				t.Errorf("atelet asked to terminate the sandbox = %t, want %t", terminated, tc.wantTerminated)
			}
			got, err := persistence.GetActor(ctx, apiActorRef)
			switch {
			case tc.wantDeleted:
				if !errors.Is(err, store.ErrNotFound) {
					t.Errorf("GetActor() = %v, %v, want ErrNotFound: the delete should have been resumed", got.GetStatus().GetState(), err)
				}
			case err != nil:
				t.Fatalf("GetActor() failed: %v", err)
			case got.GetStatus().GetState() != tc.wantState:
				t.Errorf("actor state = %v, want %v", got.GetStatus().GetState(), tc.wantState)
			case tc.wantState == ateapipb.ActorState_ACTOR_STATE_CRASHED:
				want := transitionOperation(tc.state) + " failed: " + crashMessageTransitionAbandoned
				if msg := got.GetStatus().GetCrash().GetMessage(); msg != want {
					t.Errorf("crash message = %q, want %q", msg, want)
				}
				if got.GetStatus().GetWorkerAssignment() != nil {
					t.Errorf("actor worker assignment = %v, want it cleared", got.GetStatus().GetWorkerAssignment())
				}
				// RevertActor and DeleteActor reach the sandbox through the node.
				if got.GetStatus().GetAssignedNode() != "node-1" {
					t.Errorf("actor assigned node = %q, want node-1 kept", got.GetStatus().GetAssignedNode())
				}
			}

			wantAllocated := int32(1)
			if tc.wantReleased {
				wantAllocated = 0
			}
			if allocated := mustGetWorker(t, ctx, persistence).GetStatus().GetAllocated().GetActors(); allocated != wantAllocated {
				t.Errorf("worker allocated actors = %d, want %d", allocated, wantAllocated)
			}
			if hosted := firstAssignment(t, persistence, apiWorkerName) != nil; hosted == tc.wantReleased {
				t.Errorf("worker hosts the actor = %t, want %t", hosted, !tc.wantReleased)
			}
		})
	}
}

// A sweep that finds nothing to do, or an actor another replica already
// resolved, writes nothing.
func TestActorTransitionReaper_SweepIsIdempotent(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_SUSPENDING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())
	r := newTestTransitionReaper(t, persistence, &capturingTerminator{}, true)

	r.sweep(ctx)
	crashed := mustGetActor(t, ctx, persistence)
	if crashed.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Fatalf("actor state = %v, want CRASHED", crashed.GetStatus().GetState())
	}

	r.sweep(ctx)
	if again := mustGetActor(t, ctx, persistence); again.GetMetadata().GetVersion() != crashed.GetMetadata().GetVersion() {
		t.Errorf("actor version = %d after a second sweep, want %d: a CRASHED actor is not transitional", again.GetMetadata().GetVersion(), crashed.GetMetadata().GetVersion())
	}
}

// A reap that hangs in atelet ends at reapTimeout and the sweep goes on to
// the next actor.
func TestActorTransitionReaper_HangingReapDoesNotStallSweep(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))

	// The delete of this actor hangs in Terminate until its context ends.
	deleting := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_DELETING, onNode1)
	deletingRef := apiActorRef
	if _, err := persistence.MarkActorForDeletion(ctx, deletingRef, store.DeletePreconditions{}, nil); err != nil {
		t.Fatalf("MarkActorForDeletion() failed: %v", err)
	}
	assignAPIWorker(t, ctx, persistence, apiWorkerName, deleting.GetMetadata().GetUid())
	// This one has no node, so its reap never calls atelet.
	suspendingRef := resources.ActorRef{Atespace: apiActorRef.Atespace, Name: "actor-2"}
	seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_SUSPENDING, func(a *ateapipb.Actor) {
		a.Metadata.Name = suspendingRef.Name
	})

	r := newTestTransitionReaper(t, persistence, &capturingTerminator{hang: true}, true)
	r.reapTimeout = time.Second
	r.sweep(ctx)

	got, err := persistence.GetActor(ctx, deletingRef)
	if err != nil {
		t.Fatalf("GetActor(%s) failed: %v", deletingRef, err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_DELETING {
		t.Errorf("actor %s state = %v, want DELETING kept for the next sweep", deletingRef, got.GetStatus().GetState())
	}
	got, err = persistence.GetActor(ctx, suspendingRef)
	if err != nil {
		t.Fatalf("GetActor(%s) failed: %v", suspendingRef, err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor %s state = %v, want CRASHED", suspendingRef, got.GetStatus().GetState())
	}
}
