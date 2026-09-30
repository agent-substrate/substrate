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

package atepg

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestUpdateActor_ConcurrentWriteReturnsConflict(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	created, err := s.CreateActor(ctx, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-a"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "template-a"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
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

// TestCreateActor_MissingAtespace_FailedPrecondition exercises the
// foreign-key race the doc calls out: CreateActor rejects an actor whose
// atespace doesn't exist (including a concurrently-deleted one), with the
// foreign key closing the TOCTOU window around a separate existence check.
func TestCreateActor_MissingAtespace_FailedPrecondition(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	actor := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: "id1", Atespace: "no-such-atespace"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
		if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name, Atespace: "team-a"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}}); err != nil {
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

// A move to DELETING still in flight holds the tag row locked, so a new borrow
// waits for it and then checks the state it left behind.
func TestCreateActor_BorrowWaitsForInFlightMarkDeleting(t *testing.T) {
	tests := []struct {
		name               string
		commit             bool
		wantCreateActorErr error
	}{
		{
			name:               "deleting tag tx commits",
			commit:             true,
			wantCreateActorErr: store.ErrTagNotReady,
		},
		{
			name:               "deleting tag tx rolls back",
			commit:             false,
			wantCreateActorErr: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := setupPostgresPersistence(t)
			ctx := context.Background()
			createTestAtespace(t, s, "team-a")
			tag := createReadyTestTag(t, s, "team-a", "tag-a")

			deleting := proto.CloneOf(tag)
			deleting.Status.State = ateapipb.TagState_TAG_STATE_DELETING
			deletingBytes, err := proto.Marshal(deleting)
			if err != nil {
				t.Fatalf("marshaling tag: %v", err)
			}
			updateTagTx, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatalf("beginning mark: %v", err)
			}
			defer updateTagTx.Rollback(ctx) //nolint:errcheck // no-op once committed
			if _, err := updateTagTx.Exec(ctx, `UPDATE tags SET proto = $1 WHERE uid = $2`, deletingBytes, tag.GetMetadata().GetUid()); err != nil {
				t.Fatalf("marking tag deleting: %v", err)
			}

			done := make(chan error, 1)
			go func() {
				_, err := s.CreateActor(ctx, &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "session-1"},
					Status: &ateapipb.ActorStatus{
						State:            ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
						ExternalSnapshot: proto.CloneOf(tag.GetStatus().GetSnapshot()),
					},
				})
				done <- err
			}()
			waitUntilBlockedOnLock(t, s, done)

			if test.commit {
				err = updateTagTx.Commit(ctx)
			} else {
				err = updateTagTx.Rollback(ctx)
			}
			if err != nil {
				t.Fatalf("ending mark: %v", err)
			}
			if err := <-done; !errors.Is(err, test.wantCreateActorErr) {
				t.Errorf("CreateActor borrowing the tag = %v, want %v", err, test.wantCreateActorErr)
			}
		})
	}
}
