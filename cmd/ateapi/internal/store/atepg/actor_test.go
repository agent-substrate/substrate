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

package atepg

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
)

func TestUpdateActor_ConcurrentWriteReturnsConflict(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	created, err := s.CreateActor(ctx, &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-a"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: "default", Name: "template-a"},
		Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	actorRef := resources.ActorRefFromActor(created)

	mutations := 0
	_, err = s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(toUpdate *ateapi.Actor) error {
		mutations++
		if _, err := s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(concurrent *ateapi.Actor) error {
			concurrent.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"tier": "paid"}}
			return nil
		}); err != nil {
			return fmt.Errorf("concurrent actor update: %w", err)
		}
		toUpdate.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
		return nil
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("UpdateActor error = %v, want ErrVersionConflict", err)
	}
	if mutations != 1 {
		t.Errorf("mutation ran %d times, want 1", mutations)
	}
	stored, err := s.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if stored.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED: losing update was persisted", stored.GetStatus().GetState())
	}
	if got := stored.GetWorkerSelector().GetMatchLabels()["tier"]; got != "paid" {
		t.Errorf("worker selector tier = %q, want paid", got)
	}
	if got, want := stored.GetMetadata().GetVersion(), created.GetMetadata().GetVersion()+1; got != want {
		t.Errorf("version = %d, want %d", got, want)
	}
}

func TestUpdateObservedActor(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "team-a")
	input := &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-a"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: "default", Name: "template-a"},
		Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
	}
	created, err := s.CreateActor(ctx, input)
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	actorRef := resources.ActorRefFromActor(created)
	original := proto.CloneOf(created)
	markRunning := func(actor *ateapi.Actor) error {
		actor.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
		return nil
	}

	tracer, tp, spans := newTestQueryTracer(t)
	traced := &Persistence{pool: openPool(t, "public", tracer)}
	updateCtx, parent := tp.Tracer("test").Start(ctx, "update actor status")
	updated, err := traced.UpdateObservedActor(updateCtx, actorRef, created, markRunning)
	statements := spans.Ended()
	parent.End()
	if err != nil {
		t.Fatalf("UpdateObservedActor: %v", err)
	}
	if len(statements) != 1 || statements[0].Name() != "UPDATE actors" {
		var names []string
		for _, statement := range statements {
			names = append(names, statement.Name())
		}
		t.Errorf("statements = %v, want [UPDATE actors]", names)
	}
	if !proto.Equal(created, original) {
		t.Error("UpdateObservedActor mutated its input")
	}
	if got := updated.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("state = %v, want RUNNING", got)
	}
	if got, want := updated.GetMetadata().GetVersion(), original.GetMetadata().GetVersion()+1; got != want {
		t.Errorf("version = %d, want %d", got, want)
	}

	if _, err := s.UpdateObservedActor(ctx, actorRef, original, markRunning); !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("update with stale version = %v, want ErrVersionConflict", err)
	}
	stored, err := s.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if !proto.Equal(stored, updated) {
		t.Error("stored actor differs from the result of the successful update")
	}
	if _, err := s.DeleteActor(ctx, actorRef, store.PreconditionFrom(updated)); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	if _, err := s.UpdateObservedActor(ctx, actorRef, original, markRunning); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("update after delete = %v, want ErrNotFound", err)
	}

	recreated, err := s.CreateActor(ctx, input)
	if err != nil {
		t.Fatalf("recreating actor: %v", err)
	}
	if recreated.GetMetadata().GetVersion() != original.GetMetadata().GetVersion() {
		t.Fatal("recreated actor must have the same version to test the UID guard")
	}
	if _, err := s.UpdateObservedActor(ctx, actorRef, original, markRunning); !errors.Is(err, store.ErrUIDConflict) {
		t.Errorf("update after recreation = %v, want ErrUIDConflict", err)
	}
	stored, err = s.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor after recreation: %v", err)
	}
	if !proto.Equal(stored, recreated) {
		t.Error("stale update changed the recreated actor")
	}
}

// TestCreateActor_MissingAtespace_FailedPrecondition exercises the
// foreign-key race the doc calls out: CreateActor rejects an actor whose
// atespace doesn't exist (including a concurrently-deleted one), with the
// foreign key closing the TOCTOU window around a separate existence check.
func TestCreateActor_MissingAtespace_FailedPrecondition(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	actor := &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Name: "id1", Atespace: "no-such-atespace"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
		Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
	}
	if _, err := s.CreateActor(ctx, actor); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("CreateActor with missing atespace = %v, want ErrFailedPrecondition", err)
	}
}

func TestListActors_InvalidPageToken(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	if _, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 10, PageToken: "not-valid-base64!!"}); err == nil {
		t.Errorf("ListActors with malformed page token = nil error, want an error")
	}
}

func TestListActors_CrossScopePageToken(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace(team-a) failed: %v", err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-b")); err != nil {
		t.Fatalf("CreateAtespace(team-b) failed: %v", err)
	}
	for _, name := range []string{"a1", "a2"} {
		if _, err := s.CreateActor(ctx, &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Name: name, Atespace: "team-a"}, Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED}}); err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
	}

	page, err := s.ListActors(ctx, "team-a", store.ListOptions{PageSize: 1})
	if err != nil {
		t.Fatalf("ListActors(team-a) failed: %v", err)
	}
	if page.NextPageToken == "" {
		t.Fatalf("expected a next page token")
	}

	// A token minted for team-a must be rejected when replayed against team-b
	// or against the unscoped (global) listing.
	if _, err := s.ListActors(ctx, "team-b", store.ListOptions{PageSize: 1, PageToken: page.NextPageToken}); err == nil {
		t.Errorf("ListActors(team-b) with team-a's token = nil error, want an error")
	}
	if _, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 1, PageToken: page.NextPageToken}); err == nil {
		t.Errorf("ListActors(all) with team-a's token = nil error, want an error")
	}

	// A worker-list token must be rejected by ListAtespaces (different kind).
	workerPage, err := s.ListWorkers(ctx, store.ListOptions{PageSize: 1})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	if workerPage.NextPageToken != "" {
		if _, err := s.ListAtespaces(ctx, store.ListOptions{PageSize: 1, PageToken: workerPage.NextPageToken}); err == nil {
			t.Errorf("ListAtespaces with a worker page token = nil error, want an error")
		}
	}
}

func TestUpdateActor_DiscardsMetadataEdits(t *testing.T) {
	ref := resources.ActorRef{Atespace: "team-a", Name: "a"}
	testUpdateDiscardsMetadataEdits(t,
		func(t *testing.T, s *Persistence) *ateapi.Actor {
			created, err := s.CreateActor(t.Context(), &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
				Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
			})
			if err != nil {
				t.Fatalf("CreateActor failed: %v", err)
			}
			return created
		},
		func(ctx context.Context, s *Persistence, pc store.Precondition, mutate func(*ateapi.Actor) error) (*ateapi.Actor, error) {
			return s.UpdateActor(ctx, ref, pc, mutate)
		},
		func(ctx context.Context, s *Persistence) (*ateapi.Actor, error) {
			return s.GetActor(ctx, ref)
		})
}
