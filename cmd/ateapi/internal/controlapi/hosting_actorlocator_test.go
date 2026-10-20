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

	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

var _ authz.ActorLocator = ActorLocator{}

// fakeHostingStore serves the reads HostingNode makes: one Actor, the Workers
// in workers, and the worker assignments in assignments (actor UID to worker).
type fakeHostingStore struct {
	store.Interface
	actor       *ateapipb.Actor
	actorErr    error
	workers     map[string]*ateapipb.Worker
	assignments map[string]string
}

func (f *fakeHostingStore) GetActor(_ context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	if f.actorErr != nil {
		return nil, f.actorErr
	}
	if f.actor == nil || resources.ActorRefFromActor(f.actor) != ref {
		return nil, store.ErrNotFound
	}
	return f.actor, nil
}

func (f *fakeHostingStore) GetWorker(_ context.Context, name string) (*ateapipb.Worker, error) {
	w, ok := f.workers[name]
	if !ok {
		return nil, store.ErrNotFound
	}
	return w, nil
}

func (f *fakeHostingStore) GetWorkerAssignment(_ context.Context, workerName, actorUID string) (*ateapipb.ActorAssignment, error) {
	if f.assignments[actorUID] != workerName {
		return nil, store.ErrNotFound
	}
	return &ateapipb.ActorAssignment{ActorUid: actorUID}, nil
}

func TestActorLocator(t *testing.T) {
	const actorUID = "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "runner"}
	running := func() *ateapipb.Actor {
		return &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name, Uid: actorUID},
			Status: &ateapipb.ActorStatus{
				State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
				WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "worker-1"}},
			},
		}
	}
	// hosting is a store in which worker-1, on node-1, hosts the running Actor.
	hosting := func() *fakeHostingStore {
		return &fakeHostingStore{
			actor:       running(),
			workers:     map[string]*ateapipb.Worker{"worker-1": {Metadata: &ateapipb.ResourceMetadata{Name: "worker-1"}, NodeName: "node-1"}},
			assignments: map[string]string{actorUID: "worker-1"},
		}
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
			// The status still names worker-1, but its assignment was released.
			name: "worker released",
			store: func() *fakeHostingStore {
				s := hosting()
				s.assignments = nil
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
