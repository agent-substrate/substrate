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

	"github.com/jackc/pgx/v5"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
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

// A MarkTagForDeletion still in flight holds the tag row locked, so a new
// borrow waits for it and then checks the delete_time it left behind.
func TestCreateActor_BorrowWaitsForInFlightMarkForDeletion(t *testing.T) {
	tests := []struct {
		name               string
		commit             bool
		wantCreateActorErr error
	}{
		{
			name:               "mark tx commits",
			commit:             true,
			wantCreateActorErr: store.ErrTagNotReady,
		},
		{
			name:               "mark tx rolls back",
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
			deleting.Metadata.DeleteTime = timestamppb.Now()
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
				t.Fatalf("marking tag for deletion: %v", err)
			}

			done := make(chan error, 1)
			go func() {
				_, err := s.CreateActor(ctx, &ateapi.Actor{
					Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "session-1"},
					Status: &ateapi.ActorStatus{
						State:     ateapi.ActorState_ACTOR_STATE_SUSPENDED,
						Snapshots: []*ateapi.Snapshot{proto.CloneOf(tag.GetStatus().GetSnapshot())},
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

// tagBorrowOf returns the UID of the Tag the actor borrows according to
// tag_borrows, or "" when the actor has no row.
func tagBorrowOf(t *testing.T, s *Persistence, actorUID string) string {
	t.Helper()
	var tagUID string
	err := s.pool.QueryRow(context.Background(), `SELECT tag_uid FROM tag_borrows WHERE actor_uid = $1`, actorUID).Scan(&tagUID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatalf("reading the tag borrow of actor %s: %v", actorUID, err)
	}
	return tagUID
}

// testActorSnapshot is a snapshot an actor took itself, under its own prefix.
func testActorSnapshot(t *testing.T, gen int32, storageStatus ateapi.SnapshotStorageStatus) *ateapi.Snapshot {
	t.Helper()
	name := fmt.Sprintf("snapshot-%d", gen)
	uri, err := resources.NewActorSnapshotURI("gs://bucket", "team-a", "6f1c2d9e-4b7a-4e3f-9a10-2c5d8e7b3f41", name)
	if err != nil {
		t.Fatalf("NewActorSnapshotURI failed: %v", err)
	}
	return &ateapi.Snapshot{
		Uuid:       name,
		Generation: gen,
		Owner:      ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
		Fidelity:   ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		DurableSnapshot: &ateapi.SnapshotStorage{
			Status: storageStatus,
			Object: &ateapi.ObjectSnapshot{SnapshotUri: uri.String()},
		},
	}
}

func TestCreateActor_RecordsTagBorrow(t *testing.T) {
	tests := []struct {
		name      string
		snapshots func(t *testing.T, tag *ateapi.Tag) []*ateapi.Snapshot
		wantTag   string
	}{
		{
			name: "a tag's snapshot borrows the tag",
			snapshots: func(_ *testing.T, tag *ateapi.Tag) []*ateapi.Snapshot {
				return []*ateapi.Snapshot{proto.CloneOf(tag.GetStatus().GetSnapshot())}
			},
			wantTag: "tag-a",
		},
		{
			name: "a snapshot of its own borrows nothing",
			snapshots: func(t *testing.T, _ *ateapi.Tag) []*ateapi.Snapshot {
				return []*ateapi.Snapshot{testActorSnapshot(t, 1, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)}
			},
			wantTag: "",
		},
		{
			name: "a tag's snapshot still being written borrows nothing",
			snapshots: func(_ *testing.T, tag *ateapi.Tag) []*ateapi.Snapshot {
				inProgress := proto.CloneOf(tag.GetStatus().GetSnapshot())
				inProgress.DurableSnapshot.Status = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
				return []*ateapi.Snapshot{inProgress}
			},
			wantTag: "",
		},
		{
			name: "a tag's snapshot newer than the actor's own borrows the tag",
			snapshots: func(t *testing.T, tag *ateapi.Tag) []*ateapi.Snapshot {
				newer := proto.CloneOf(tag.GetStatus().GetSnapshot())
				newer.Generation = 2
				return []*ateapi.Snapshot{newer, testActorSnapshot(t, 1, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)}
			},
			wantTag: "tag-a",
		},
		{
			name: "a newer tag's snapshot still being written borrows nothing",
			snapshots: func(t *testing.T, tag *ateapi.Tag) []*ateapi.Snapshot {
				newer := proto.CloneOf(tag.GetStatus().GetSnapshot())
				newer.Generation = 2
				newer.DurableSnapshot.Status = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
				return []*ateapi.Snapshot{newer, testActorSnapshot(t, 1, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)}
			},
			wantTag: "",
		},
		{
			name:      "no snapshot borrows nothing",
			snapshots: func(*testing.T, *ateapi.Tag) []*ateapi.Snapshot { return nil },
			wantTag:   "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := setupPostgresPersistence(t)
			createTestAtespace(t, s, "team-a")
			tags := map[string]*ateapi.Tag{"tag-a": createReadyTestTag(t, s, "team-a", "tag-a")}

			created, err := s.CreateActor(context.Background(), &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "session-1"},
				Status: &ateapi.ActorStatus{
					State:     ateapi.ActorState_ACTOR_STATE_SUSPENDED,
					Snapshots: test.snapshots(t, tags["tag-a"]),
				},
			})
			if err != nil {
				t.Fatalf("CreateActor failed: %v", err)
			}
			want := tags[test.wantTag].GetMetadata().GetUid()
			if got := tagBorrowOf(t, s, created.GetMetadata().GetUid()); got != want {
				t.Errorf("tag_borrows tag_uid = %q, want %q", got, want)
			}
		})
	}
}

func TestUpdateActor_RecordsTagBorrow(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(t *testing.T, a *ateapi.Actor, otherTag *ateapi.Tag)
		wantTag string
	}{
		// In the tests below, the actor starts borrowing tag-a's snapshot.
		{
			name: "a snapshot of its own not uploaded yet keeps the borrow",
			mutate: func(t *testing.T, a *ateapi.Actor, _ *ateapi.Tag) {
				a.Status.Snapshots = append(a.Status.Snapshots, testActorSnapshot(t, 2, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING))
			},
			wantTag: "tag-a",
		},
		{
			name: "an upload of its own snapshot ends the borrow, with the tag's still listed",
			mutate: func(t *testing.T, a *ateapi.Actor, _ *ateapi.Tag) {
				a.Status.Snapshots = append(a.Status.Snapshots, testActorSnapshot(t, 2, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED))
			},
			wantTag: "",
		},
		{
			name: "a suspend replacing the tag's snapshot ends the borrow",
			mutate: func(t *testing.T, a *ateapi.Actor, _ *ateapi.Tag) {
				a.Status.Snapshots = []*ateapi.Snapshot{testActorSnapshot(t, 2, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)}
			},
			wantTag: "",
		},
		{
			name:    "dropping the snapshot ends the borrow",
			mutate:  func(_ *testing.T, a *ateapi.Actor, _ *ateapi.Tag) { a.Status.Snapshots = nil },
			wantTag: "",
		},
		{
			name: "a suspend that writes no snapshot keeps the borrow",
			mutate: func(_ *testing.T, a *ateapi.Actor, _ *ateapi.Tag) {
				a.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
			},
			wantTag: "tag-a",
		},
		{
			name: "moving to another tag moves the borrow",
			mutate: func(_ *testing.T, a *ateapi.Actor, otherTag *ateapi.Tag) {
				a.Status.Snapshots = []*ateapi.Snapshot{proto.CloneOf(otherTag.GetStatus().GetSnapshot())}
			},
			wantTag: "tag-b",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := setupPostgresPersistence(t)
			ctx := context.Background()
			createTestAtespace(t, s, "team-a")
			tags := map[string]*ateapi.Tag{
				"tag-a": createReadyTestTag(t, s, "team-a", "tag-a"),
				"tag-b": createReadyTestTag(t, s, "team-a", "tag-b"),
			}

			created, err := s.CreateActor(ctx, &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "session-1"},
				Status: &ateapi.ActorStatus{
					State:                  ateapi.ActorState_ACTOR_STATE_SUSPENDED,
					LastAssignedGeneration: 1,
					Snapshots:              []*ateapi.Snapshot{proto.CloneOf(tags["tag-a"].GetStatus().GetSnapshot())},
				},
			})
			if err != nil {
				t.Fatalf("CreateActor failed: %v", err)
			}
			if _, err := s.UpdateActor(ctx, resources.ActorRefFromActor(created), store.PreconditionFrom(created), func(toUpdate *ateapi.Actor) error {
				test.mutate(t, toUpdate, tags["tag-b"])
				return nil
			}); err != nil {
				t.Fatalf("UpdateActor failed: %v", err)
			}
			want := tags[test.wantTag].GetMetadata().GetUid()
			if got := tagBorrowOf(t, s, created.GetMetadata().GetUid()); got != want {
				t.Errorf("tag_borrows tag_uid = %q, want %q", got, want)
			}
		})
	}
}
