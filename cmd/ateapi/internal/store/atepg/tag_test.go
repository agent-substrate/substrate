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
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/testing/protocmp"
)

// createTestTag creates tagName over its own copy of an actor's
// external snapshot, already finalized.
func createTestTag(t *testing.T, s *Persistence, tagAtespace, tagName string) *ateapi.Tag {
	t.Helper()
	tag, err := s.CreateTag(context.Background(), &ateapi.Tag{
		Metadata: &ateapi.ResourceMetadata{Atespace: tagAtespace, Name: tagName},
		Scope:    ateapi.TagScope_TAG_SCOPE_ATESPACE,
		Status: &ateapi.TagStatus{
			Snapshot: &ateapi.Snapshot{
				Generation: 1,
				Owner:      ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG,
				Fidelity:   ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				DurableSnapshot: &ateapi.SnapshotStorage{
					Status: ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
					Object: &ateapi.ObjectSnapshot{SnapshotUri: "gs://bucket/atespaces/" + tagAtespace + "/tags/" + tagName},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTag(%s/%s) failed: %v", tagAtespace, tagName, err)
	}
	return tag
}

func TestUpdateTag_CASPreventsDeleteRecreateABA(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	original := createTestTag(t, s, "team-a", "tag-a")

	mutations := 0
	var recreated *ateapi.Tag
	_, err := s.UpdateTag(ctx, resources.TagRef{Atespace: "team-a", Name: "tag-a"}, store.PreconditionFrom(original), func(toUpdate *ateapi.Tag) error {
		mutations++
		if _, err := s.DeleteTag(ctx, resources.TagRef{Atespace: "team-a", Name: "tag-a"}, store.DeletePreconditions{}); err != nil {
			return fmt.Errorf("deleting original tag: %w", err)
		}
		recreated = createTestTag(t, s, "team-a", "tag-a")
		toUpdate.Scope = ateapi.TagScope_TAG_SCOPE_PUBLISHED
		return nil
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("UpdateTag error = %v, want ErrVersionConflict", err)
	}
	if mutations != 1 {
		t.Errorf("guarded mutation ran %d times, want 1", mutations)
	}
	stored, err := s.GetTag(ctx, resources.TagRef{Atespace: "team-a", Name: "tag-a"})
	if err != nil {
		t.Fatalf("GetTag failed: %v", err)
	}
	if diff := cmp.Diff(recreated, stored, protocmp.Transform()); diff != "" {
		t.Errorf("recreated tag was overwritten (-want +got):\n%s", diff)
	}
}

func TestCreateTag_TagForeignKeyErrors(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")

	// A tag in an atespace that does not exist trips the tag's atespace FK.
	_, err := s.CreateTag(ctx, &ateapi.Tag{
		Metadata: &ateapi.ResourceMetadata{Atespace: "gone", Name: "latest"},
		Status:   &ateapi.TagStatus{},
	})
	if !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("missing tag atespace error = %v, want ErrFailedPrecondition", err)
	}
}

// createReadyTestTag reserves tagName, records its PENDING snapshot under the
// prefix of the UID the store assigned, and marks it COMPLETED, the only state
// a new borrow is accepted against.
func createReadyTestTag(t *testing.T, s *Persistence, atespace, tagName string) *ateapi.Tag {
	t.Helper()
	ctx := context.Background()
	reserved, err := s.CreateTag(ctx, &ateapi.Tag{
		Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: tagName},
		Scope:    ateapi.TagScope_TAG_SCOPE_ATESPACE,
		Status:   &ateapi.TagStatus{},
	})
	if err != nil {
		t.Fatalf("CreateTag(%s/%s) failed: %v", atespace, tagName, err)
	}
	uri, err := resources.NewTagSnapshotURI("gs://bucket", atespace, reserved.GetMetadata().GetUid())
	if err != nil {
		t.Fatalf("NewTagSnapshotURI failed: %v", err)
	}
	recorded, err := s.UpdateTag(ctx, resources.TagRefFromTag(reserved), store.PreconditionFrom(reserved), func(toUpdate *ateapi.Tag) error {
		toUpdate.Status.Snapshot = &ateapi.Snapshot{
			Uuid:       uri.Name(),
			Generation: 1,
			Owner:      ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG,
			Fidelity:   ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			DurableSnapshot: &ateapi.SnapshotStorage{
				Status: ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING,
				Object: &ateapi.ObjectSnapshot{SnapshotUri: uri.String()},
			},
		}
		return nil
	})
	if err != nil {
		t.Fatalf("recording the snapshot of tag %s/%s failed: %v", atespace, tagName, err)
	}
	ready, err := s.UpdateTag(ctx, resources.TagRefFromTag(recorded), store.PreconditionFrom(recorded), func(toUpdate *ateapi.Tag) error {
		toUpdate.Status.Snapshot.DurableSnapshot.Status = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
		return nil
	})
	if err != nil {
		t.Fatalf("finalizing tag %s/%s failed: %v", atespace, tagName, err)
	}
	return ready
}

// waitUntilBlockedOnLock returns once some connection is waiting on a row
// lock, failing if done reports the call under test returned first.
func waitUntilBlockedOnLock(t *testing.T, s *Persistence, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("returned %v without waiting for the open transaction", err)
		default:
		}
		var blocked bool
		if err := s.pool.QueryRow(context.Background(), `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database() AND wait_event_type = 'Lock')`).Scan(&blocked); err != nil {
			t.Fatalf("reading pg_stat_activity: %v", err)
		}
		if blocked {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the call under test to block on a lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A borrow still in flight holds the tag row share-locked, so
// MarkTagForDeletion waits for it and then sees whatever it left in
// tag_borrows.
func TestMarkTagForDeletion_WaitsForInFlightBorrow(t *testing.T) {
	tests := []struct {
		name        string
		commitTx    bool
		wantMarkErr error
	}{
		{
			name:        "concurrent tag borrow commits",
			commitTx:    true,
			wantMarkErr: store.ErrTagBorrowed,
		},
		{
			name:        "concurrent tag borrow rolls back",
			commitTx:    false,
			wantMarkErr: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := setupPostgresPersistence(t)
			ctx := context.Background()
			createTestAtespace(t, s, "team-a")
			tag := createReadyTestTag(t, s, "team-a", "tag-a")

			updateActorTx, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatalf("beginning borrow: %v", err)
			}
			defer updateActorTx.Rollback(ctx) //nolint:errcheck // no-op once committed
			// Simulate an actor trying to borrow this tag's snapshot.
			if err := updateTagBorrow(ctx, updateActorTx, "borrower-uid", tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetUid()); err != nil {
				t.Fatalf("updateTagBorrow failed: %v", err)
			}

			done := make(chan error, 1)
			go func() {
				_, err := s.MarkTagForDeletion(ctx, resources.TagRefFromTag(tag), store.DeletePreconditions{UID: tag.GetMetadata().GetUid()}, nil)
				done <- err
			}()
			waitUntilBlockedOnLock(t, s, done)

			if test.commitTx {
				err = updateActorTx.Commit(ctx)
			} else {
				err = updateActorTx.Rollback(ctx)
			}
			if err != nil {
				t.Fatalf("ending borrow: %v", err)
			}
			if err := <-done; !errors.Is(err, test.wantMarkErr) {
				t.Errorf("MarkTagForDeletion = %v, want %v", err, test.wantMarkErr)
			}
		})
	}
}
