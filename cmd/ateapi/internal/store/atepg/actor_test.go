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
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func TestCreateActorKeepsPreassignedMetadata(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	preassignedTime := timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))

	tests := []struct {
		name       string
		actor      string
		uid        string
		createTime *timestamppb.Timestamp
	}{
		{name: "assigns uid and create_time when empty", actor: "actor-a"},
		{
			name:       "keeps caller-assigned uid and create_time",
			actor:      "actor-b",
			uid:        "11111111-2222-4333-8444-555555555555",
			createTime: preassignedTime,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			created, err := s.CreateActor(ctx, &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{
					Atespace:   "team-a",
					Name:       tc.actor,
					Uid:        tc.uid,
					CreateTime: tc.createTime,
				},
				ActorTemplate: &ateapi.ObjectRef{Atespace: "default", Name: "template-a"},
				Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
			})
			if err != nil {
				t.Fatalf("CreateActor failed: %v", err)
			}
			stored, err := s.GetActor(ctx, resources.ActorRefFromActor(created))
			if err != nil {
				t.Fatalf("GetActor failed: %v", err)
			}
			for _, a := range []*ateapi.Actor{created, stored} {
				meta := a.GetMetadata()
				if meta.GetUid() == "" {
					t.Fatal("uid is empty")
				}
				if tc.uid != "" && meta.GetUid() != tc.uid {
					t.Errorf("uid = %q, want %q", meta.GetUid(), tc.uid)
				}
				if meta.GetCreateTime() == nil || meta.GetUpdateTime() == nil {
					t.Fatal("timestamps are nil")
				}
				if !meta.GetCreateTime().AsTime().Equal(meta.GetUpdateTime().AsTime()) {
					t.Errorf("create_time %v != update_time %v", meta.GetCreateTime().AsTime(), meta.GetUpdateTime().AsTime())
				}
				if tc.createTime != nil && !meta.GetCreateTime().AsTime().Equal(tc.createTime.AsTime()) {
					t.Errorf("create_time = %v, want %v", meta.GetCreateTime().AsTime(), tc.createTime.AsTime())
				}
			}
		})
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
