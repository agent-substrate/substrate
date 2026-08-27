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
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestValidateUpdateActorSnapshotTagRequest(t *testing.T) {
	// validUID is a well-formed uid to pass validation.
	const validUID = "2a5f8c1e-9b3d-4f7a-8e6c-1d0b4a7f2e93"
	scopes := []string{
		ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE.String(),
		ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED.String(),
	}
	// Every case carries a uid and version guard, because an update that carries
	// neither is rejected as a blind write before anything else is checked.
	tests := []struct {
		name      string
		req       *ateapipb.UpdateActorSnapshotTagRequest
		wantError field.ErrorList
	}{
		{
			name: "valid",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: validUID, Version: 7},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
			},
			wantError: nil,
		},
		{
			name:      "missing tag",
			req:       &ateapipb.UpdateActorSnapshotTagRequest{},
			wantError: field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag"), "")},
		},
		{
			name: "missing tag.metadata.atespace",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Name: "tag1", Uid: validUID, Version: 7},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "metadata", "atespace"), "")},
		},
		{
			name: "invalid tag.metadata.atespace",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "NS1", Name: "tag1", Uid: validUID, Version: 7},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "metadata", "atespace"), "NS1", "")},
		},
		{
			name: "missing tag.metadata.name",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Uid: validUID, Version: 7},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "metadata", "name"), "")},
		},
		{
			name: "invalid tag.metadata.name",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "TAG1", Uid: validUID, Version: 7},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "metadata", "name"), "TAG1", "")},
		},
		{
			name: "missing tag.metadata.uid precondition",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Version: 7},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "metadata", "uid"), "")},
		},
		{
			name: "invalid tag.metadata.uid precondition",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: "not-a-uuid", Version: 7},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "metadata", "uid"), "not-a-uuid", "")},
		},
		{
			name: "missing tag.metadata.version precondition",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: validUID},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "metadata", "version"), "")},
		},
		{
			name: "negative tag.metadata.version precondition",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: validUID, Version: -1},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "metadata", "version"), int64(-1), "")},
		},
		{
			// A blind write: the caller never read the tag it is updating.
			name: "guards on neither uid nor version",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				},
			},
			wantError: field.ErrorList{
				field.Required(field.NewPath("actor_snapshot_tag", "metadata", "uid"), ""),
				field.Required(field.NewPath("actor_snapshot_tag", "metadata", "version"), ""),
			},
		},
		{
			name: "unset tag.scope",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: validUID, Version: 7},
				},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "scope"), "")},
		},
		{
			name: "explicit tag.scope UNSPECIFIED",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: validUID, Version: 7},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_UNSPECIFIED,
				},
			},
			wantError: field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "scope"), "")},
		},
		{
			name: "tag.scope ATESPACE explicitly unpublishes",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: validUID, Version: 7},
					Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
				},
			},
			wantError: nil,
		},
		{
			name: "tag.scope outside the enum",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: validUID, Version: 7},
					Scope:    ateapipb.ActorSnapshotTagScope(7),
				},
			},
			wantError: field.ErrorList{field.NotSupported(field.NewPath("actor_snapshot_tag", "scope"), "7", scopes)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateActorSnapshotTagRequest(tt.req), tt.wantError)
		})
	}
}

func TestCreateActorSnapshotTag_MissingSnapshotIsNotFound(t *testing.T) {
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	storetest.MustCreateAtespace(t, context.Background(), persistence, "team-a")
	s := &RPCService{impl: persistence}

	_, err := s.CreateActorSnapshotTag(context.Background(), &ateapipb.CreateActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "latest"},
			Snapshot: &ateapipb.ObjectRef{Atespace: "team-a", Name: "missing"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("CreateActorSnapshotTag status = %v, want NotFound (error: %v)", status.Code(err), err)
	}
}

func TestUpdateActorSnapshotTag(t *testing.T) {
	tests := []struct {
		name     string
		stored   *ateapipb.ActorSnapshotTag
		req      *ateapipb.ActorSnapshotTag
		want     *ateapipb.ActorSnapshotTag
		wantCode codes.Code
	}{
		{
			name:   "publishes an atespace-scoped tag",
			stored: &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE},
			req:    &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED},
			want:   &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED},
		},
		{
			name:   "unpublishes a published tag",
			stored: &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED},
			req:    &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE},
			want:   &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE},
		},
		{
			name:   "immutable snapshot cant be unset",
			stored: &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE},
			req: &ateapipb.ActorSnapshotTag{
				Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				Snapshot: &ateapipb.ObjectRef{},
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name:   "immutable snapshot cant be updated",
			stored: &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE},
			req: &ateapipb.ActorSnapshotTag{
				Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				Snapshot: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "some-other-snapshot"},
			},
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.stored.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"}
			svc, stored := rpcServiceWithActorSnapshotTag(t, tt.stored)

			tt.req.Metadata = stored.GetMetadata()
			if tt.req.GetSnapshot() == nil {
				tt.req.Snapshot = stored.GetSnapshot()
			}

			updated, err := svc.UpdateActorSnapshotTag(context.Background(), &ateapipb.UpdateActorSnapshotTagRequest{ActorSnapshotTag: tt.req})

			if tt.wantCode != codes.OK {
				if code := status.Code(err); code != tt.wantCode {
					t.Errorf("UpdateActorSnapshotTag error = %v (code %v), want code %v", err, code, tt.wantCode)
				}
				return
			}
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

// TestUpdateActorSnapshotTag_UnsetScopeDoesNotUnpublish checks that an update
// leaving scope unset is rejected.
func TestUpdateActorSnapshotTag_UnsetScopeDoesNotUnpublish(t *testing.T) {
	ctx := context.Background()
	svc, stored := rpcServiceWithActorSnapshotTag(t, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
	})

	// The guards are the ones the client read, so the rejection can only come
	// from the unset scope.
	stored.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_UNSPECIFIED
	_, err := svc.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{
		ActorSnapshotTag: stored,
	})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("UpdateActorSnapshotTag error = %v (code %v), want code InvalidArgument", err, code)
	}

	current, err := svc.impl.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: testAtespace, Name: "tag1"})
	if err != nil {
		t.Fatalf("GetActorSnapshotTag: %v", err)
	}
	if got, want := current.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED; got != want {
		t.Errorf("stored scope = %v, want %v: the rejected update must not have unpublished the tag", got, want)
	}
	if got, want := current.GetMetadata().GetVersion(), stored.GetMetadata().GetVersion(); got != want {
		t.Errorf("stored version = %d, want %d: the rejected update must not have written", got, want)
	}
}

// TestCreateActorSnapshotTag_RejectsUnsetScope checks that scope is required at
// creation.
func TestCreateActorSnapshotTag_RejectsUnsetScope(t *testing.T) {
	ctx := context.Background()
	svc, stored := rpcServiceWithActorSnapshotTag(t, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})

	_, err := svc.CreateActorSnapshotTag(ctx, &ateapipb.CreateActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag2"},
			Snapshot: stored.GetSnapshot(),
		},
	})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("CreateActorSnapshotTag error = %v (code %v), want code InvalidArgument", err, code)
	}
}

// rpcServiceWithActorSnapshotTag seeds an ActorSnapshot and a tag pointing at it
// in a PostgreSQL-backed store, and returns an RPCService over it.
func rpcServiceWithActorSnapshotTag(t *testing.T, tag *ateapipb.ActorSnapshotTag) (*RPCService, *ateapipb.ActorSnapshotTag) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	atespace, name := tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName()
	snapshot := storetest.MustCreateActorSnapshot(t, context.Background(), persistence, &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: "snapshot-" + name},
		Status:   &ateapipb.ActorSnapshotStatus{SnapshotUri: "gs://my-bucket/snapshots/" + atespace + "/snapshot-" + name},
	})
	tag.Snapshot = &ateapipb.ObjectRef{Atespace: snapshot.GetMetadata().GetAtespace(), Name: snapshot.GetMetadata().GetName()}
	created, err := persistence.CreateActorSnapshotTag(context.Background(), resources.ActorSnapshotRef{Atespace: atespace, Name: snapshot.GetMetadata().GetName()}, tag)
	if err != nil {
		t.Fatalf("Failed to CreateActorSnapshotTag: %v", err)
	}
	return &RPCService{impl: persistence}, created
}

// TestUpdateActorSnapshotTag_DeleteRecreateRace checks that an update is not
// applied if a tag was deleted and re-created during the update operation.
func TestUpdateActorSnapshotTag_DeleteRecreateRace(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	for _, name := range []string{"snapshot-1", "snapshot-2"} {
		storetest.MustCreateActorSnapshot(t, ctx, persistence, &ateapipb.ActorSnapshot{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			Status:   &ateapipb.ActorSnapshotStatus{SnapshotUri: "gs://bucket/root/snapshots/" + testAtespace + "/" + name},
		})
	}

	const tagName = "before-upgrade"
	// Tag A: what the client reads, and what its uid precondition names.
	// Freshly created, so it sits at version 1.
	originalTag, err := persistence.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: testAtespace, Name: "snapshot-1"}, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})
	if err != nil {
		t.Fatalf("Failed to CreateActorSnapshotTag(snapshot-1): %v", err)
	}

	// A concurrent client deletes A and re-tags the same atespace/name as a
	// brand new tag B, pointed at another snapshot.
	var recreatedTag *ateapipb.ActorSnapshotTag
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.DeleteActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: testAtespace, Name: tagName}); err != nil {
				t.Fatalf("Racing writer: DeleteActorSnapshotTag: %v", err)
			}
			recreatedTag, err = persistence.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: testAtespace, Name: "snapshot-2"}, &ateapipb.ActorSnapshotTag{
				Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName},
				Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
			})
			if err != nil {
				t.Fatalf("Racing writer: re-tag CreateActorSnapshotTag: %v", err)
			}
		},
	}
	svc := &RPCService{impl: racing}

	// The client asserts "only update the tag with uid A". Its version guard is
	// satisfied by B as well, because re-tagging resets the version to 1: the
	// uid is the only thing that can tell the two lifecycles apart.
	originalTag.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
	_, err = svc.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{
		ActorSnapshotTag: originalTag,
	})
	if code := status.Code(err); code != codes.Aborted {
		t.Errorf("UpdateActorSnapshotTag error = %v (code %v), want code Aborted: the tag holding uid %s was deleted mid-update",
			err, code, originalTag.GetMetadata().GetUid())
	}

	storedTag, err := persistence.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: testAtespace, Name: tagName})
	if err != nil {
		t.Fatalf("GetActorSnapshotTag: %v", err)
	}
	// The stored record must still be tag B as its creator left it. Any of A's
	// state showing up here is the clobber.
	if diff := cmp.Diff(recreatedTag, storedTag, protocmp.Transform()); diff != "" {
		t.Errorf("Update meant for the deleted tag was applied to the recreated one (-recreated +stored):\n%s", diff)
	}
}

// TestUpdateActorSnapshotTag_ConcurrentUpdate checks that a write landing in
// the handler's read-modify-write window is reported as Aborted rather than
// absorbed. Every update guards on a version, so there is no unguarded update left
// for the server to resolve on the client's behalf.
func TestUpdateActorSnapshotTag_ConcurrentUpdate(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	storetest.MustCreateActorSnapshot(t, ctx, persistence, &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "snapshot-1"},
		Status:   &ateapipb.ActorSnapshotStatus{SnapshotUri: "gs://bucket/root/snapshots/" + testAtespace + "/snapshot-1"},
	})

	const tagName = "before-upgrade"
	originalTag, err := persistence.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: testAtespace, Name: "snapshot-1"}, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})
	if err != nil {
		t.Fatalf("Failed to CreateActorSnapshotTag(snapshot-1): %v", err)
	}

	// A concurrent client moves the tag past the version the caller could have
	// observed, in the window the handler used to leave open between its own
	// read and the store's WATCH.
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: testAtespace, Name: tagName}, store.PreconditionFrom(originalTag), func(toUpdate *ateapipb.ActorSnapshotTag) error {
				toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE
				return nil
			}); err != nil {
				t.Fatalf("Racing writer: UpdateActorSnapshotTag: %v", err)
			}
		},
	}
	svc := &RPCService{impl: racing}

	originalTag.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
	_, err = svc.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{
		ActorSnapshotTag: originalTag,
	})
	if code := status.Code(err); code != codes.Aborted {
		t.Errorf("UpdateActorSnapshotTag error = %v (code %v), want code Aborted: the guarded version moved under the update", err, code)
	}

	storedTag, err := persistence.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: testAtespace, Name: tagName})
	if err != nil {
		t.Fatalf("Failed to GetActorSnapshotTag(%s/%s): %v", testAtespace, tagName, err)
	}
	// Only the concurrent writer's version bump landed: the rejected update
	// wrote nothing.
	if got, want := storedTag.GetMetadata().GetVersion(), originalTag.GetMetadata().GetVersion()+1; got != want {
		t.Errorf("stored version = %d, want %d", got, want)
	}
	if got, want := storedTag.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE; got != want {
		t.Errorf("Stored scope = %v, want %v: the rejected update was applied anyway", got, want)
	}
}
