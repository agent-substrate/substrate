// Copyright 2026 The Agent Substrate Authors
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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
)

var _ authz.ActorLocator = ActorLocator{}

// fakeHostingStore serves the one read ActorLocator makes: GetActor, for one
// Actor.
type fakeHostingStore struct {
	store.Interface
	actor    *ateapi.Actor
	actorErr error
}

func (f *fakeHostingStore) GetActor(_ context.Context, ref resources.ActorRef) (*ateapi.Actor, error) {
	if f.actorErr != nil {
		return nil, f.actorErr
	}
	if f.actor == nil || resources.ActorRefFromActor(f.actor) != ref {
		return nil, store.ErrNotFound
	}
	return f.actor, nil
}

func TestActorLocator(t *testing.T) {
	const actorUID = "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "runner"}
	running := func() *ateapi.Actor {
		return &ateapi.Actor{
			Metadata: &ateapi.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name, Uid: actorUID},
			Status: &ateapi.ActorStatus{
				State:            ateapi.ActorState_ACTOR_STATE_RUNNING,
				WorkerAssignment: &ateapi.WorkerAssignment{Worker: &ateapi.ObjectRef{Name: "worker-1"}, NodeName: "node-1"},
			},
		}
	}
	// hosting is a store in which worker-1, on node-1, hosts the running Actor.
	hosting := func() *fakeHostingStore {
		return &fakeHostingStore{actor: running()}
	}

	tests := []struct {
		name    string
		store   func() *fakeHostingStore
		actor   resources.ActorRef
		node    string
		want    bool
		wantErr bool
	}{
		{name: "hosting node", store: hosting, actor: actorRef, node: "node-1", want: true},
		{name: "another node", store: hosting, actor: actorRef, node: "node-2", want: false},
		{name: "no such actor", store: hosting, actor: resources.ActorRef{Atespace: "team-a", Name: "other"}, node: "node-1", want: false},
		{
			// A suspended Actor whose snapshot no longer holds files on any node.
			name: "not hosted",
			store: func() *fakeHostingStore {
				s := hosting()
				s.actor.Status = &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}
				return s
			},
			actor: actorRef, node: "node-1", want: false,
		},
		{
			name: "actor lookup fails",
			store: func() *fakeHostingStore {
				s := hosting()
				s.actorErr = errors.New("connection reset")
				return s
			},
			actor: actorRef, node: "node-1", wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ActorLocator{Store: tc.store()}.ActorHostedOnNode(context.Background(), tc.actor, tc.node)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ActorHostedOnNode() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("ActorHostedOnNode() = %v, want %v", got, tc.want)
			}
		})
	}
}
