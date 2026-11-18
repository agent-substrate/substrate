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

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/testing/protocmp"
)

// createTestTag creates tagName over its own copy of an actor's
// external snapshot, already finalized, with opts applied.
func createTestTag(t *testing.T, s *Persistence, tagAtespace, tagName string, opts ...func(*ateapi.Tag)) *ateapi.Tag {
	t.Helper()
	input := &ateapi.Tag{
		Metadata: &ateapi.ResourceMetadata{Atespace: tagAtespace, Name: tagName},
		Scope:    ateapi.TagScope_TAG_SCOPE_ATESPACE,
		Status: &ateapi.TagStatus{
			Snapshot: &ateapi.Snapshot{
				Generation: 1,
				Owner:      ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG,
				Fidelity:   ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				DurableSnapshot: &ateapi.SnapshotStorage{
					State:  ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
					Object: &ateapi.ObjectSnapshot{SnapshotUri: "gs://bucket/atespaces/" + tagAtespace + "/tags/" + tagName},
				},
			},
		},
	}
	for _, opt := range opts {
		opt(input)
	}
	tag, err := s.CreateTag(context.Background(), input)
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

// TestDeleteTag_UnknownForeignKey checks that DeleteTag reports a foreign key
// other than external_volume_snapshots_tag_fk as an error of its own, even
// when its name starts with that one.
func TestDeleteTag_UnknownForeignKey(t *testing.T) {
	pool := migratedPool(t, "unknown-tag-fk")
	s, err := NewPersistence(t.Context(), pool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO atespaces (name, uid, version, proto) VALUES ('team-a', 'u1', 1, '');
		INSERT INTO tags (atespace, name, uid, version, proto) VALUES ('team-a', 'v1', 'u2', 1, '');
		CREATE TABLE tag_refs (atespace text, tag_name text,
			CONSTRAINT external_volume_snapshots_tag_fkey FOREIGN KEY (atespace, tag_name) REFERENCES tags(atespace, name));
		INSERT INTO tag_refs VALUES ('team-a', 'v1')`); err != nil {
		t.Fatal(err)
	}
	_, err = s.DeleteTag(t.Context(), resources.TagRef{Atespace: "team-a", Name: "v1"}, store.DeletePreconditions{})
	if err == nil || errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("DeleteTag = %v, want an error other than ErrFailedPrecondition", err)
	}
}
