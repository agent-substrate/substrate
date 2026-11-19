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
	"testing"

	"github.com/jackc/pgx/v5"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestExternalVolumeSnapshot(tag *ateapi.Tag) *ateapi.ExternalVolumeSnapshot {
	md := tag.GetMetadata()
	return &ateapi.ExternalVolumeSnapshot{
		Metadata:   &ateapi.ResourceMetadata{Atespace: md.GetAtespace(), Name: "substrate-snap-" + md.GetUid() + "-data"},
		DriverName: "pd.csi.storage.gke.io",
		Origin: &ateapi.ExternalVolumeSnapshotOrigin{TagCapture: &ateapi.TagVolumeSnapshotOrigin{
			Tag:              &ateapi.ObjectRef{Atespace: md.GetAtespace(), Name: md.GetName()},
			TagUid:           md.GetUid(),
			SourceVolumeName: "data",
			SourceVolumeId:   "projects/p/zones/z/disks/data",
		}},
	}
}

// pendingTag leaves a test tag still being created, the only state in which a
// tag takes new snapshots.
func pendingTag(tag *ateapi.Tag) {
	tag.Status.Snapshot.DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
}

// waitBlockedBy waits until another backend waits on a lock tx holds. It fails
// the test if done reports first.
func waitBlockedBy(t *testing.T, s *Persistence, tx pgx.Tx, done <-chan error) {
	t.Helper()
	for blocked := false; !blocked; {
		select {
		case err := <-done:
			t.Fatalf("returned %v before waiting on the lock", err)
		default:
		}
		if err := s.pool.QueryRow(t.Context(), `
			SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE $1::int = ANY(pg_blocking_pids(pid)))`, int32(tx.Conn().PgConn().PID())).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCreateExternalVolumeSnapshot_TagChangedConcurrently changes the tag in a
// transaction that commits only once the create waits on the tag's row lock.
// The create must then see the change, rather than record a snapshot for a
// tag that is being deleted or is gone.
func TestCreateExternalVolumeSnapshot_TagChangedConcurrently(t *testing.T) {
	tests := []struct {
		name   string
		change func(ctx context.Context, tx pgx.Tx, tag *ateapi.Tag) error
	}{
		{
			name: "marked for deletion",
			change: func(ctx context.Context, tx pgx.Tx, tag *ateapi.Tag) error {
				marked := proto.CloneOf(tag)
				marked.Metadata.DeleteTime = timestamppb.Now()
				b, err := proto.Marshal(marked)
				if err != nil {
					return err
				}
				_, err = tx.Exec(ctx, `UPDATE tags SET version = version + 1, proto = $1 WHERE atespace = $2 AND name = $3`, b, "team-a", "v1")
				return err
			},
		},
		{
			name: "deleted",
			change: func(ctx context.Context, tx pgx.Tx, _ *ateapi.Tag) error {
				_, err := tx.Exec(ctx, `DELETE FROM tags WHERE atespace = $1 AND name = $2`, "team-a", "v1")
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := setupPostgresPersistence(t)
			ctx := t.Context()
			createTestAtespace(t, s, "team-a")
			tag := createTestTag(t, s, "team-a", "v1", pendingTag)

			tx, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background()) //nolint:errcheck // no-op once committed
			if err := tt.change(ctx, tx, tag); err != nil {
				t.Fatalf("changing the tag: %v", err)
			}

			created := make(chan error, 1)
			go func() {
				_, err := s.CreateExternalVolumeSnapshot(ctx, newTestExternalVolumeSnapshot(tag))
				created <- err
			}()
			waitBlockedBy(t, s, tx, created)
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("committing the tag change: %v", err)
			}
			if err := <-created; !errors.Is(err, store.ErrFailedPrecondition) {
				t.Errorf("CreateExternalVolumeSnapshot = %v, want ErrFailedPrecondition", err)
			}
			if _, err := s.GetExternalVolumeSnapshot(ctx, resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(newTestExternalVolumeSnapshot(tag))); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("GetExternalVolumeSnapshot = %v, want ErrNotFound", err)
			}
		})
	}
}

// TestUpdateExternalVolumeSnapshot_DeletedDuringUpdate deletes the row from
// another connection after the update read it.
func TestUpdateExternalVolumeSnapshot_DeletedDuringUpdate(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "team-a")
	tag := createTestTag(t, s, "team-a", "v1", pendingTag)
	created, err := s.CreateExternalVolumeSnapshot(ctx, newTestExternalVolumeSnapshot(tag))
	if err != nil {
		t.Fatalf("CreateExternalVolumeSnapshot failed: %v", err)
	}
	ref := resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(created)
	_, err = s.UpdateExternalVolumeSnapshot(ctx, ref, store.PreconditionFrom(created), func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
		if _, err := s.pool.Exec(ctx, `DELETE FROM external_volume_snapshots WHERE atespace = $1 AND name = $2`, ref.Atespace, ref.Name); err != nil {
			return err
		}
		toUpdate.Status = &ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap-handle"}
		return nil
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("UpdateExternalVolumeSnapshot = %v, want ErrVersionConflict", err)
	}
}

// TestDeleteAtespace_BlockedBySnapshotWithoutTag checks the atespace foreign
// key on its own. The store only creates snapshots that a tag owns, and a tag
// that owns snapshots blocks the atespace first, so the row is written
// directly, with the NULL tag columns the schema allows for other origins.
func TestDeleteAtespace_BlockedBySnapshotWithoutTag(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "team-a")
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO external_volume_snapshots (atespace, name, uid, version, proto)
		VALUES ('team-a', 'snap', '7c8d9e0f-1a2b-4c3d-8e4f-5a6b7c8d9e0f', 1, '')`); err != nil {
		t.Fatalf("inserting a snapshot without a tag: %v", err)
	}
	if _, err := s.DeleteAtespace(ctx, "team-a", store.DeletePreconditions{}); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("DeleteAtespace with a snapshot = %v, want ErrFailedPrecondition", err)
	}
}
