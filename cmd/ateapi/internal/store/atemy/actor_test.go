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

package atemy

import (
	"errors"
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func newTestActor(atespace, name string) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: "template-a"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}
}

func TestUpdateActor_ConcurrentWriteReturnsConflict(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "team-a")
	created, err := s.CreateActor(ctx, newTestActor("team-a", "actor-a"))
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	actorRef := resources.ActorRefFromActor(created)

	mutations := 0
	_, err = s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.Actor) error {
		mutations++
		if _, err := s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(concurrent *ateapipb.Actor) error {
			concurrent.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
			return nil
		}); err != nil {
			return fmt.Errorf("concurrent actor update: %w", err)
		}
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
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
	if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED: losing update was persisted", stored.GetStatus().GetState())
	}
	if got := stored.GetWorkerSelector().GetMatchLabels()["tier"]; got != "paid" {
		t.Errorf("worker selector tier = %q, want paid", got)
	}
	if got, want := stored.GetMetadata().GetVersion(), created.GetMetadata().GetVersion()+1; got != want {
		t.Errorf("version = %d, want %d", got, want)
	}
}

func TestCreateActor_MissingAtespace_FailedPrecondition(t *testing.T) {
	s := setupMySQLPersistence(t)
	if _, err := s.CreateActor(t.Context(), newTestActor("no-such-atespace", "id1")); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("CreateActor with missing atespace = %v, want ErrFailedPrecondition", err)
	}
	if n := countRows(t, s, `SELECT COUNT(*) FROM actors`); n != 0 {
		t.Errorf("%d actor rows after a refused create, want 0", n)
	}
}

func TestListActors_InvalidPageToken(t *testing.T) {
	s := setupMySQLPersistence(t)
	if _, err := s.ListActors(t.Context(), "", store.ListOptions{PageSize: 10, PageToken: "not-valid-base64!!"}); err == nil {
		t.Errorf("ListActors with malformed page token = nil error, want an error")
	}
}

func TestListActors_CrossScopePageToken(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "team-a")
	createTestAtespace(t, s, "team-b")
	for _, name := range []string{"a1", "a2"} {
		if _, err := s.CreateActor(ctx, newTestActor("team-a", name)); err != nil {
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
	// A token of another kind must be rejected too.
	if _, err := s.ListAtespaces(ctx, store.ListOptions{PageSize: 1, PageToken: page.NextPageToken}); err == nil {
		t.Errorf("ListAtespaces with an actor page token = nil error, want an error")
	}
	if _, err := s.ListActorTemplates(ctx, "team-a", store.ListOptions{PageSize: 1, PageToken: page.NextPageToken}); err == nil {
		t.Errorf("ListActorTemplates with an actor page token = nil error, want an error")
	}
}
