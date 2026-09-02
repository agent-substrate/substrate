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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestUpdateActor(t *testing.T) {
	const templateNS, templateName = "ns1", "tmpl1"

	tests := []struct {
		name     string
		stored   *ateapipb.Actor
		req      *ateapipb.Actor
		want     *ateapipb.Actor
		wantCode codes.Code
	}{
		{
			name:   "sets a worker_selector the stored actor does not have",
			stored: &ateapipb.Actor{},
			req: &ateapipb.Actor{
				ActorTemplate:  &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
		},
		{
			name:   "overwrites an existing worker_selector",
			stored: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}}},
			req: &ateapipb.Actor{
				ActorTemplate:  &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
		},
		{
			name:   "an omitted worker_selector is cleared",
			stored: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}}},
			req: &ateapipb.Actor{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
			},
			want: &ateapipb.Actor{},
		},
		{
			name:   "SourceSnapshotTag immutable field is kept",
			stored: &ateapipb.Actor{SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplate:     &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"},
				WorkerSelector:    &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{
				SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"},
				WorkerSelector:    &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
		},
		{
			name:   "changes to status in the request are ignored",
			stored: &ateapipb.Actor{},
			req: &ateapipb.Actor{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
			},
			want: &ateapipb.Actor{},
		},
		{
			name:   "an omitted immutable field is rejected",
			stored: &ateapipb.Actor{SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				// Omitted SourceSnapshotTag
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name:   "an immutable field the request rewrites is rejected",
			stored: &ateapipb.Actor{SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplate:     &ateapipb.ObjectRef{Atespace: "attacker-ns", Name: "attacker-tmpl"},
				SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag2"},
			},
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.stored.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID}
			tt.stored.ActorTemplate = &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName}
			tt.stored.Status = &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			}

			svc, created := rpcServiceWithActor(t, tt.stored)

			tt.req.Metadata = created.GetMetadata()
			updated, err := svc.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{Actor: tt.req})

			if tt.wantCode != codes.OK {
				if code := status.Code(err); code != tt.wantCode {
					t.Errorf("UpdateActor error = %v (code %v), want code %v", err, code, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("UpdateActor failed: %v", err)
			}

			tt.want.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID, Version: 2}
			tt.want.ActorTemplate = &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName}
			tt.want.Status = &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			}
			if diff := cmp.Diff(tt.want, updated, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
				t.Errorf("UpdateActor response mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestUpdateActor_DeleteRecreateRace checks that an update is not applied
// if an actor was deleted and recreated during the update operation.
func TestUpdateActor_DeleteRecreateRace(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorID}

	// Actor A: what the client reads, and what its uid precondition names.
	// Freshly created, so it sits at version 1.
	original := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPod: "pod-a"},
		},
	})

	// A concurrent client deletes A and recreates the same atespace/name as a
	// brand new actor B, in the window the handler used to leave open between
	// its own read and the store's WATCH.
	var recreated *ateapipb.Actor
	var err error
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(toUpdate *ateapipb.Actor) error {
				toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_DELETING
				return nil
			}); err != nil {
				t.Fatalf("racing writer: mark deleting: %v", err)
			}
			if _, err := persistence.DeleteActor(ctx, actorRef); err != nil {
				t.Fatalf("racing writer: DeleteActor: %v", err)
			}
			recreated, err = persistence.CreateActor(ctx, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			})
			if err != nil {
				t.Fatalf("racing writer: recreate CreateActor: %v", err)
			}
		},
	}
	svc := &RPCService{impl: newServiceImpl(racing, nil)}

	// The client asserts "only update the actor with uid A".
	original.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
	_, err = svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: original})
	if code := status.Code(err); code != codes.Aborted {
		t.Errorf("UpdateActor error = %v (code %v), want code Aborted: the actor holding uid %s was deleted mid-update",
			err, code, original.GetMetadata().GetUid())
	}

	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got, want := stored.GetMetadata().GetUid(), recreated.GetMetadata().GetUid(); got != want {
		t.Fatalf("stored uid = %s, want recreated actor's uid %s", got, want)
	}
	// The stored record must still be actor B as its creator left it. Any of A's
	// state showing up here is the clobber.
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("stored state = %v, want %v: recreated actor was overwritten with the deleted actor's state",
			got, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	}
	if got := stored.GetStatus().GetWorkerAssignment(); got != nil {
		t.Errorf("stored worker_assignment = %v, want nil: recreated actor inherited the deleted actor's worker", got)
	}
	if got := stored.GetWorkerSelector(); got != nil {
		t.Errorf("stored worker_selector = %v, want nil: update meant for the deleted actor was applied", got)
	}
}

// TestUpdateActor_ConcurrentDisjointUpdates checks that a concurrent write is
// reported even when it touched a field the update does not. The version guards
// the whole actor, not a single field, so the server cannot know the two
// writes commute: it reports the conflict and leaves reconciling to the client.
func TestUpdateActor_ConcurrentDisjointUpdates(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorID}

	original := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	// A suspend workflow bumps state (a field that a later update operation will not touch)
	// inside the handler's read-modify-write window.
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(toUpdate *ateapipb.Actor) error {
				toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
				return nil
			}); err != nil {
				t.Fatalf("racing writer: mark suspending: %v", err)
			}
		},
	}
	svc := &RPCService{impl: newServiceImpl(racing, nil)}

	// Update operation is changing the worker_selector field, not the actor's state (like the concurrent op)
	// This update must fail: the racing update bumped the version.
	original.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
	_, err := svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: original})
	if code := status.Code(err); code != codes.Aborted {
		t.Errorf("UpdateActor error = %v (code %v), want code Aborted: the guarded version moved under the update", err, code)
	}

	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	// The concurrent writer's field survives; the rejected update wrote nothing.
	if got := stored.GetWorkerSelector(); got != nil {
		t.Errorf("stored worker_selector = %v, want nil: the rejected update was applied anyway", got)
	}
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		t.Errorf("stored state = %v, want %v: the concurrent writer's field must survive", got, ateapipb.ActorState_ACTOR_STATE_SUSPENDING)
	}
}

// rpcServiceWithActor seeds one actor in a PostgreSQL-backed store and returns an
// RPCService over it.
func rpcServiceWithActor(t *testing.T, actor *ateapipb.Actor) (*RPCService, *ateapipb.Actor) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	created := storetest.MustCreateActor(t, context.Background(), persistence, actor)
	return &RPCService{impl: newServiceImpl(persistence, nil)}, created
}
