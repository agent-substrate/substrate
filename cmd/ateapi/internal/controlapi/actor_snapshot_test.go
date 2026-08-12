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
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestValidateUpdateActorSnapshotTagRequest(t *testing.T) {
	mutableFields := []string{"scope"}
	scopes := []string{
		ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE.String(),
		ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED.String(),
	}

	tests := []struct {
		name      string
		req       *ateapipb.UpdateActorSnapshotTagRequest
		wantError field.ErrorList
	}{
		{
			name: "valid",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: nil,
		},
		{
			name:      "missing tag",
			req:       &ateapipb.UpdateActorSnapshotTagRequest{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}}},
			wantError: field.ErrorList{field.Required(field.NewPath("tag"), "")},
		},
		{
			name: "missing tag.metadata.atespace",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata", "atespace"), "")},
		},
		{
			name: "invalid tag.metadata.atespace",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "NS1", Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "atespace"), "NS1", "")},
		},
		{
			name: "missing tag.metadata.name",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata", "name"), "")},
		},
		{
			name: "invalid tag.metadata.name",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "TAG1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "name"), "TAG1", "")},
		},
		{
			name: "valid tag.metadata.uid precondition",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{
						Atespace: "ns1", Name: "tag1", Uid: "2a5f8c1e-9b3d-4f7a-8e6c-1d0b4a7f2e93",
					},
					Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: nil,
		},
		{
			name: "invalid tag.metadata.uid precondition",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: "not-a-uuid"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "uid"), "not-a-uuid", "")},
		},
		{
			name: "valid tag.metadata.version precondition",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Version: 7},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: nil,
		},
		{
			name: "negative tag.metadata.version precondition",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Version: -1},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "version"), int64(-1), "")},
		},
		{
			name: "missing update_mask",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("update_mask"), "")},
		},
		{
			name: "empty update_mask",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("update_mask"), "")},
		},
		{
			name: "wildcard update_mask",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"*"}},
			},
			wantError: field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "*", mutableFields)},
		},
		{
			name: "output-only field in update_mask",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata.version"}},
			},
			wantError: field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "metadata.version", mutableFields)},
		},
		{
			name: "immutable field in update_mask",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"snapshot"}},
			},
			wantError: field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "snapshot", mutableFields)},
		},
		{
			name: "unset tag.scope",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "scope"), "")},
		},
		{
			name: "explicit tag.scope UNSPECIFIED",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_UNSPECIFIED,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "scope"), "")},
		},
		{
			name: "tag.scope ATESPACE explicitly unpublishes",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: nil,
		},
		{
			name: "tag.scope outside the enum",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope(7),
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
			},
			wantError: field.ErrorList{field.NotSupported(field.NewPath("tag", "scope"), "7", scopes)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateActorSnapshotTagRequest(tt.req), tt.wantError)
		})
	}
}

func TestUpdateActorSnapshotTag_FieldMasks(t *testing.T) {
	tests := []struct {
		name      string
		stored    *ateapipb.ActorSnapshotTag
		req       *ateapipb.ActorSnapshotTag
		maskPaths []string
		want      *ateapipb.ActorSnapshotTag
	}{
		{
			name:      "mask publishes an atespace-scoped tag",
			stored:    &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE},
			req:       &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED},
			maskPaths: []string{"scope"},
			want:      &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED},
		},
		{
			name:      "mask unpublishes a published tag",
			stored:    &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED},
			req:       &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE},
			maskPaths: []string{"scope"},
			want:      &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.stored.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"}
			svc, stored := serviceWithActorSnapshotTag(t, tt.stored)

			tt.req.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"}
			// Sent but not in the mask - must be ignored.
			tt.req.Snapshot = &ateapipb.ObjectRef{Atespace: testAtespace, Name: "some-other-snapshot"}

			updated, err := svc.UpdateActorSnapshotTag(context.Background(), &ateapipb.UpdateActorSnapshotTagRequest{
				Tag:        tt.req,
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.maskPaths},
			})
			if err != nil {
				t.Fatalf("UpdateActorSnapshotTag failed: %v", err)
			}

			tt.want.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1", Version: 2}
			tt.want.Snapshot = stored.GetSnapshot()
			if diff := cmp.Diff(tt.want, updated, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
				t.Errorf("UpdateActorSnapshotTag response mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestUpdateActorSnapshotTag_UnsetScopeDoesNotUnpublish checks that masking
// scope without populating it is rejected.
func TestUpdateActorSnapshotTag_UnsetScopeDoesNotUnpublish(t *testing.T) {
	ctx := context.Background()
	svc, stored := serviceWithActorSnapshotTag(t, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
	})

	_, err := svc.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
	})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("UpdateActorSnapshotTag error = %v (code %v), want code InvalidArgument", err, code)
	}

	_, current, err := svc.persistence.GetActorSnapshotByTag(ctx, testAtespace, "tag1")
	if err != nil {
		t.Fatalf("GetActorSnapshotByTag: %v", err)
	}
	if got, want := current.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED; got != want {
		t.Errorf("stored scope = %v, want %v: the rejected update must not have unpublished the tag", got, want)
	}
	if got, want := current.GetMetadata().GetVersion(), stored.GetMetadata().GetVersion(); got != want {
		t.Errorf("stored version = %d, want %d: the rejected update must not have written", got, want)
	}
}

// TestTagActorSnapshot_RejectsUnsetScope checks that scope is required at
// creation.
func TestTagActorSnapshot_RejectsUnsetScope(t *testing.T) {
	ctx := context.Background()
	svc, stored := serviceWithActorSnapshotTag(t, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})

	_, err := svc.TagActorSnapshot(ctx, &ateapipb.TagActorSnapshotRequest{
		Snapshot: &ateapipb.ActorSnapshotRef{
			Reference: &ateapipb.ActorSnapshotRef_Snapshot{Snapshot: stored.GetSnapshot()},
		},
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag2"},
		},
	})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("TagActorSnapshot error = %v (code %v), want code InvalidArgument", err, code)
	}
}

// serviceWithActorSnapshotTag seeds an ActorSnapshot and a tag pointing at it
// in a miniredis-backed store, and returns a Service over it.
func serviceWithActorSnapshotTag(t *testing.T, tag *ateapipb.ActorSnapshotTag) (*Service, *ateapipb.ActorSnapshotTag) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	atespace, name := tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName()
	snapshot, err := persistence.CreateActorSnapshot(context.Background(), &ateapipb.ActorSnapshot{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: atespace, Name: "snapshot-" + name},
		SnapshotUri: "gs://my-bucket/snapshots/" + atespace + "/snapshot-" + name,
	})
	if err != nil {
		t.Fatalf("Failed to CreateActorSnapshot: %v", err)
	}
	tag.Snapshot = &ateapipb.ObjectRef{Atespace: snapshot.GetMetadata().GetAtespace(), Name: snapshot.GetMetadata().GetName()}
	created, err := persistence.TagActorSnapshot(context.Background(), atespace, snapshot.GetMetadata().GetName(), tag)
	if err != nil {
		t.Fatalf("Failed to TagActorSnapshot: %v", err)
	}
	return &Service{persistence: persistence}, created
}

// TestUpdateActorSnapshotTag_DeleteRecreateRace checks that an update is not
// applied if a tag was deleted and re-created during the update operation.
func TestUpdateActorSnapshotTag_DeleteRecreateRace(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	for _, name := range []string{"snapshot-1", "snapshot-2"} {
		if _, err := persistence.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
			Metadata:    &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			SnapshotUri: "gs://bucket/root/snapshots/" + testAtespace + "/" + name,
		}); err != nil {
			t.Fatalf("Failed to CreateActorSnapshot(%s): %v", name, err)
		}
	}

	const tagName = "before-upgrade"
	// Tag A: what the client reads, and what its uid precondition names.
	// Freshly created, so it sits at version 1.
	originalTag, err := persistence.TagActorSnapshot(ctx, testAtespace, "snapshot-1", &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})
	if err != nil {
		t.Fatalf("Failed to TagActorSnapshot(snapshot-1): %v", err)
	}

	// A concurrent client deletes A and re-tags the same atespace/name as a
	// brand new tag B, pointed at another snapshot.
	var recreatedTag *ateapipb.ActorSnapshotTag
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.DeleteActorSnapshotTag(ctx, testAtespace, tagName); err != nil {
				t.Fatalf("Racing writer: DeleteActorSnapshotTag: %v", err)
			}
			recreatedTag, err = persistence.TagActorSnapshot(ctx, testAtespace, "snapshot-2", &ateapipb.ActorSnapshotTag{
				Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName},
				Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
			})
			if err != nil {
				t.Fatalf("Racing writer: re-tag TagActorSnapshot: %v", err)
			}
		},
	}
	svc := &Service{persistence: racing}

	// The client asserts "only update the tag with uid A". Its version guard is
	// satisfied by B as well, because re-tagging resets the version to 1: the
	// uid is the only thing that can tell the two lifecycles apart.
	_, err = svc.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: testAtespace,
				Name:     tagName,
				Uid:      originalTag.GetMetadata().GetUid(),
				Version:  originalTag.GetMetadata().GetVersion(),
			},
			Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
	})
	if code := status.Code(err); code != codes.Aborted {
		t.Errorf("UpdateActorSnapshotTag error = %v (code %v), want code Aborted: the tag holding uid %s was deleted mid-update",
			err, code, originalTag.GetMetadata().GetUid())
	}

	_, storedTag, err := persistence.GetActorSnapshotByTag(ctx, testAtespace, tagName)
	if err != nil {
		t.Fatalf("GetActorSnapshotByTag: %v", err)
	}
	// The stored record must still be tag B as its creator left it. Any of A's
	// state showing up here is the clobber.
	if diff := cmp.Diff(recreatedTag, storedTag, protocmp.Transform()); diff != "" {
		t.Errorf("Update meant for the deleted tag was applied to the recreated one (-recreated +stored):\n%s", diff)
	}
}

// TestUpdateActorSnapshotTag_ConcurrentUnguardedUpdate checks that an update
// pinning nothing is the server's conflict to resolve: a write landing in the
// handler's read-modify-write window is absorbed, not reported as Aborted.
func TestUpdateActorSnapshotTag_ConcurrentUnguardedUpdate(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	if _, err := persistence.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "snapshot-1"},
		SnapshotUri: "gs://bucket/root/snapshots/" + testAtespace + "/snapshot-1",
	}); err != nil {
		t.Fatalf("Failed to CreateActorSnapshot: %v", err)
	}

	const tagName = "before-upgrade"
	originalTag, err := persistence.TagActorSnapshot(ctx, testAtespace, "snapshot-1", &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})
	if err != nil {
		t.Fatalf("Failed to TagActorSnapshot(snapshot-1): %v", err)
	}

	// A concurrent client moves the tag past the version the caller could have
	// observed, in the window the handler used to leave open between its own
	// read and the store's WATCH.
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActorSnapshotTag(ctx, testAtespace, tagName, func(toUpdate *ateapipb.ActorSnapshotTag) error {
				toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE
				return nil
			}); err != nil {
				t.Fatalf("Racing writer: UpdateActorSnapshotTag: %v", err)
			}
		},
	}
	svc := &Service{persistence: racing}

	if _, err := svc.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
	}); err != nil {
		t.Fatalf("UpdateActorSnapshotTag error = %v, want success: no precondition was set, so the conflict is the server's to resolve", err)
	}

	_, storedTag, err := persistence.GetActorSnapshotByTag(ctx, testAtespace, tagName)
	if err != nil {
		t.Fatalf("Failed to GetActorSnapshotByTag(%s/%s): %v", testAtespace, tagName, err)
	}
	if got, want := storedTag.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED; got != want {
		t.Errorf("Stored scope = %v, want %v", got, want)
	}
	// Seed, concurrent writer, then this update: the write landed on top of the
	// concurrent one rather than on the state the handler read first.
	if got, want := storedTag.GetMetadata().GetVersion(), originalTag.GetMetadata().GetVersion()+2; got != want {
		t.Errorf("stored version = %d, want %d", got, want)
	}
}
