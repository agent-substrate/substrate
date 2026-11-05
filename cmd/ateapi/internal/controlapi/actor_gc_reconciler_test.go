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
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
)

type fakeGCStore struct {
	mu     sync.Mutex
	actors map[string]*ateapi.Actor
}

func newFakeGCStore(actors ...*ateapi.Actor) *fakeGCStore {
	m := make(map[string]*ateapi.Actor)
	for _, a := range actors {
		m[resources.ActorRefFromActor(a).String()] = a
	}
	return &fakeGCStore{actors: m}
}

func (s *fakeGCStore) ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.Actor], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var items []*ateapi.Actor
	for _, a := range s.actors {
		if atespace == "" || a.GetMetadata().GetAtespace() == atespace {
			items = append(items, a)
		}
	}
	return store.ListResponse[*ateapi.Actor]{Items: items}, nil
}

func (s *fakeGCStore) GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapi.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.actors[actorRef.String()]
	if !ok {
		return nil, store.ErrNotFound
	}
	return a, nil
}

type fakeGCControl struct {
	mu          sync.Mutex
	deletedReqs []*ateapi.DeleteActorRequest
	deleteErr   error
}

func (c *fakeGCControl) DeleteActor(ctx context.Context, req *ateapi.DeleteActorRequest) (*ateapi.Actor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletedReqs = append(c.deletedReqs, req)
	if c.deleteErr != nil {
		return nil, c.deleteErr
	}
	return &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Name: req.GetActor().GetName(), Atespace: req.GetActor().GetAtespace()}}, nil
}

func TestActorGCReconciler_ReconcilesDeletingActor(t *testing.T) {
	actor := &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-1", Uid: "uid-1"},
		Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_DELETING},
	}
	st := newFakeGCStore(actor)
	ctrl := &fakeGCControl{}

	r := NewActorGCReconciler(st, ctrl, 100*time.Millisecond)
	ref := resources.ActorRefFromActor(actor)
	r.Enqueue(ref)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !r.processNextWorkItem(ctx) {
		t.Fatal("processNextWorkItem returned false")
	}

	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	if len(ctrl.deletedReqs) != 1 {
		t.Fatalf("expected 1 delete request, got %d", len(ctrl.deletedReqs))
	}
	req := ctrl.deletedReqs[0]
	if req.GetActor().GetName() != "actor-1" || req.GetActor().GetAtespace() != "team-a" {
		t.Errorf("unexpected actor in delete request: %v", req.GetActor())
	}
	if req.GetOptions().GetUid() != "uid-1" {
		t.Errorf("expected UID 'uid-1', got %q", req.GetOptions().GetUid())
	}
}

func TestActorGCReconciler_SkipsNonDeletingActor(t *testing.T) {
	actor := &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-running", Uid: "uid-2"},
		Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RUNNING},
	}
	st := newFakeGCStore(actor)
	ctrl := &fakeGCControl{}

	r := NewActorGCReconciler(st, ctrl, 100*time.Millisecond)
	ref := resources.ActorRefFromActor(actor)
	r.Enqueue(ref)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !r.processNextWorkItem(ctx) {
		t.Fatal("processNextWorkItem returned false")
	}

	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	if len(ctrl.deletedReqs) != 0 {
		t.Fatalf("expected 0 delete requests for running actor, got %d", len(ctrl.deletedReqs))
	}
}

func TestActorGCReconciler_Resync(t *testing.T) {
	actorDeleting := &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "deleting-1", Uid: "uid-del"},
		Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_DELETING},
	}
	actorRunning := &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "running-1", Uid: "uid-run"},
		Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RUNNING},
	}
	st := newFakeGCStore(actorDeleting, actorRunning)
	ctrl := &fakeGCControl{}

	r := NewActorGCReconciler(st, ctrl, 100*time.Millisecond)
	r.resync(context.Background())

	if r.queue.Len() != 1 {
		t.Fatalf("expected queue length 1 after resync, got %d", r.queue.Len())
	}
	ref, _ := r.queue.Get()
	if ref.Name != "deleting-1" {
		t.Errorf("expected queued actor to be 'deleting-1', got %q", ref.Name)
	}
	r.queue.Done(ref)
}
