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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// createTestTag creates tagName over its own copy of an actor's
// external snapshot, already finalized.
func createTestTag(t *testing.T, s *Persistence, tagAtespace, tagName string) *ateapipb.Tag {
	t.Helper()
	tag, err := s.CreateTag(context.Background(), &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: tagAtespace, Name: tagName},
		Scope:    ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		Status: &ateapipb.TagStatus{
			Snapshot: &ateapipb.ExternalSnapshot{SnapshotUri: "gs://bucket/atespaces/" + tagAtespace + "/tags/" + tagName},
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
	var recreated *ateapipb.Tag
	_, err := s.UpdateTag(ctx, resources.TagRef{Atespace: "team-a", Name: "tag-a"}, store.PreconditionFrom(original), func(toUpdate *ateapipb.Tag) error {
		mutations++
		if _, err := s.DeleteTag(ctx, resources.TagRef{Atespace: "team-a", Name: "tag-a"}, store.DeletePreconditions{}); err != nil {
			return fmt.Errorf("deleting original tag: %w", err)
		}
		recreated = createTestTag(t, s, "team-a", "tag-a")
		toUpdate.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED
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
	_, err := s.CreateTag(ctx, &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "gone", Name: "latest"},
		Status: &ateapipb.TagStatus{
			StorageLocation: "gs://bucket",
		},
	})
	if !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("missing tag atespace error = %v, want ErrFailedPrecondition", err)
	}
}
