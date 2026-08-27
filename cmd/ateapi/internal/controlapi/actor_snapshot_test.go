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
	"strings"
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
	// This test verifies validation of user input for update. The tag body is
	// deliberately not descended into here (updates are validated in two
	// steps); only the metadata that addresses the resource is checked. Scope
	// and snapshot rules are enforced against the stored tag inside
	// ServiceImpl.UpdateActorSnapshotTag, which the RPC-level tests cover.
	const validUID = "2a5f8c1e-9b3d-4f7a-8e6c-1d0b4a7f2e93"
	validReq := func(mods ...func(md *ateapipb.ResourceMetadata)) *ateapipb.UpdateActorSnapshotTagRequest {
		md := &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: validUID, Version: 7}
		for _, m := range mods {
			m(md)
		}
		return &ateapipb.UpdateActorSnapshotTagRequest{
			ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
				Metadata: md,
				Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
			},
		}
	}

	tests := []struct {
		name      string
		req       *ateapipb.UpdateActorSnapshotTagRequest
		wantError field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		// uid and version are preconditions the store requires; the request
		// validation deliberately leaves their presence to the store.
		"missing uid and version pass request validation",
		validReq(func(md *ateapipb.ResourceMetadata) { md.Uid = ""; md.Version = 0 }),
		nil,
	}, {
		"missing tag",
		&ateapipb.UpdateActorSnapshotTagRequest{},
		field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag"), "")},
	}, {
		"missing tag.metadata",
		&ateapipb.UpdateActorSnapshotTagRequest{ActorSnapshotTag: &ateapipb.ActorSnapshotTag{}},
		field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "metadata"), "")},
	}, {
		"missing tag.metadata.atespace",
		validReq(func(md *ateapipb.ResourceMetadata) { md.Atespace = "" }),
		field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "metadata", "atespace"), "")},
	}, {
		"invalid tag.metadata.atespace",
		validReq(func(md *ateapipb.ResourceMetadata) { md.Atespace = "NS1" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "metadata", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing tag.metadata.name",
		validReq(func(md *ateapipb.ResourceMetadata) { md.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "metadata", "name"), "")},
	}, {
		"invalid tag.metadata.name",
		validReq(func(md *ateapipb.ResourceMetadata) { md.Name = "TAG1" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid tag.metadata.uid precondition",
		validReq(func(md *ateapipb.ResourceMetadata) { md.Uid = "not-a-uuid" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "metadata", "uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		"negative tag.metadata.version precondition",
		validReq(func(md *ateapipb.ResourceMetadata) { md.Version = -1 }),
		field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "metadata", "version"), nil, "").WithOrigin("minimum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateActorSnapshotTagRequest(context.Background(), tt.req), tt.wantError)
		})
	}
}

func TestCreateActorSnapshotTag_MissingSnapshotIsNotFound(t *testing.T) {
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	storetest.MustCreateAtespace(t, context.Background(), persistence, "team-a")
	s := &RPCService{impl: newServiceImpl(persistence, nil, nil)}

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
	return &RPCService{impl: newServiceImpl(persistence, nil, nil)}, created
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
	svc := &RPCService{impl: newServiceImpl(racing, nil, nil)}

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
	svc := &RPCService{impl: newServiceImpl(racing, nil, nil)}

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

func TestValidateCreateActorSnapshotTagRequest(t *testing.T) {
	// This test verifies validation of user input for creation.
	validReq := func(mods ...func(tag *ateapipb.ActorSnapshotTag)) *ateapipb.CreateActorSnapshotTagRequest {
		tag := &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "tag-1"},
			Snapshot: &ateapipb.ObjectRef{Atespace: "team-a", Name: "snap-1"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		}
		for _, m := range mods {
			m(tag)
		}
		return &ateapipb.CreateActorSnapshotTagRequest{ActorSnapshotTag: tag}
	}

	tests := []struct {
		name string
		req  *ateapipb.CreateActorSnapshotTagRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		"missing tag",
		&ateapipb.CreateActorSnapshotTagRequest{},
		field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag"), "")},
	}, {
		"missing metadata",
		validReq(func(tag *ateapipb.ActorSnapshotTag) { tag.Metadata = nil }),
		field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "metadata"), "")},
	}, {
		"missing metadata.atespace",
		validReq(func(tag *ateapipb.ActorSnapshotTag) { tag.Metadata.Atespace = "" }),
		field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "metadata", "atespace"), "")},
	}, {
		"invalid metadata.name",
		validReq(func(tag *ateapipb.ActorSnapshotTag) { tag.Metadata.Name = "TAG-1" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing snapshot",
		validReq(func(tag *ateapipb.ActorSnapshotTag) { tag.Snapshot = nil }),
		field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "snapshot"), "")},
	}, {
		"missing snapshot.atespace",
		validReq(func(tag *ateapipb.ActorSnapshotTag) { tag.Snapshot.Atespace = "" }),
		field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "snapshot", "atespace"), "")},
	}, {
		"invalid snapshot.name",
		validReq(func(tag *ateapipb.ActorSnapshotTag) { tag.Snapshot.Name = "SNAP 1" }),
		field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "snapshot", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"unset scope",
		validReq(func(tag *ateapipb.ActorSnapshotTag) {
			tag.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_UNSPECIFIED
		}),
		field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "scope"), "")},
	}, {
		"scope outside the enum",
		validReq(func(tag *ateapipb.ActorSnapshotTag) { tag.Scope = ateapipb.ActorSnapshotTagScope(7) }),
		field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "scope"), nil, "").WithOrigin("maximum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateCreateActorSnapshotTagRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateActorSnapshotRefRequests(t *testing.T) {
	// Get/Delete requests carry a single atespaced ref; all three share the
	// same generated rules, so one table drives them all.
	type refCase struct {
		name string
		ref  *ateapipb.ObjectRef
		want func(root string) field.ErrorList
	}
	cases := []refCase{{
		"valid",
		&ateapipb.ObjectRef{Atespace: "team-a", Name: "obj-1"},
		func(string) field.ErrorList { return nil },
	}, {
		"missing ref",
		nil,
		func(root string) field.ErrorList { return field.ErrorList{field.Required(field.NewPath(root), "")} },
	}, {
		"missing atespace",
		&ateapipb.ObjectRef{Name: "obj-1"},
		func(root string) field.ErrorList {
			return field.ErrorList{field.Required(field.NewPath(root, "atespace"), "")}
		},
	}, {
		"invalid atespace",
		&ateapipb.ObjectRef{Atespace: "TEAM A", Name: "obj-1"},
		func(root string) field.ErrorList {
			return field.ErrorList{field.Invalid(field.NewPath(root, "atespace"), nil, "").WithOrigin("format=k8s-short-name")}
		},
	}, {
		"missing name",
		&ateapipb.ObjectRef{Atespace: "team-a"},
		func(root string) field.ErrorList {
			return field.ErrorList{field.Required(field.NewPath(root, "name"), "")}
		},
	}, {
		"invalid name",
		&ateapipb.ObjectRef{Atespace: "team-a", Name: "OBJ 1"},
		func(root string) field.ErrorList {
			return field.ErrorList{field.Invalid(field.NewPath(root, "name"), nil, "").WithOrigin("format=k8s-short-name")}
		},
	}}
	for _, tc := range cases {
		t.Run("GetActorSnapshot/"+tc.name, func(t *testing.T) {
			got := validateGetActorSnapshotRequest(context.Background(), &ateapipb.GetActorSnapshotRequest{ActorSnapshot: tc.ref})
			assertValidateErr(t, got, tc.want("actor_snapshot"))
		})
		t.Run("GetActorSnapshotTag/"+tc.name, func(t *testing.T) {
			got := validateGetActorSnapshotTagRequest(context.Background(), &ateapipb.GetActorSnapshotTagRequest{ActorSnapshotTag: tc.ref})
			assertValidateErr(t, got, tc.want("actor_snapshot_tag"))
		})
		t.Run("DeleteActorSnapshotTag/"+tc.name, func(t *testing.T) {
			got := validateDeleteActorSnapshotTagRequest(context.Background(), &ateapipb.DeleteActorSnapshotTagRequest{ActorSnapshotTag: tc.ref})
			assertValidateErr(t, got, tc.want("actor_snapshot_tag"))
		})
	}
}

func TestValidateListActorSnapshotsRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ListActorSnapshotsRequest
		want field.ErrorList
	}{{
		"valid, atespace scoped",
		&ateapipb.ListActorSnapshotsRequest{Atespace: "team-a"},
		nil,
	}, {
		"valid, empty atespace means all atespaces",
		&ateapipb.ListActorSnapshotsRequest{},
		nil,
	}, {
		"invalid atespace",
		&ateapipb.ListActorSnapshotsRequest{Atespace: "TEAM-A"},
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"negative page_size",
		&ateapipb.ListActorSnapshotsRequest{PageSize: -1},
		field.ErrorList{field.Invalid(field.NewPath("page_size"), nil, "").WithOrigin("minimum")},
	}, {
		"valid page_token",
		&ateapipb.ListActorSnapshotsRequest{PageToken: strings.Repeat("x", 256)},
		nil,
	}, {
		"too-large page_token",
		&ateapipb.ListActorSnapshotsRequest{PageToken: strings.Repeat("x", 257)},
		field.ErrorList{field.TooLongCharacters(field.NewPath("page_token"), "", 256).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateListActorSnapshotsRequest(context.Background(), tt.req), tt.want)
		})
	}
}

// TestServiceImplUpdateActorSnapshotTag_ImmutableFields pins the
// immutable-snapshot rule at the layer that now owns it: declarative
// validation in ServiceImpl, which every write path shares. The store no
// longer enforces it.
func TestServiceImplUpdateActorSnapshotTag_ImmutableFields(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	impl := newServiceImpl(persistence, nil, nil)

	snapshot := storetest.MustCreateActorSnapshot(t, ctx, persistence, &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "snap-1"},
		Status:   &ateapipb.ActorSnapshotStatus{SnapshotUri: "gs://my-bucket/snap-1"},
	})
	created, err := persistence.CreateActorSnapshotTag(ctx,
		resources.ActorSnapshotRef{Atespace: testAtespace, Name: snapshot.GetMetadata().GetName()},
		&ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag-1"},
			Snapshot: &ateapipb.ObjectRef{Atespace: testAtespace, Name: snapshot.GetMetadata().GetName()},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		})
	if err != nil {
		t.Fatalf("CreateActorSnapshotTag failed: %v", err)
	}

	tagRef := resources.ActorSnapshotTagRef{Atespace: testAtespace, Name: "tag-1"}
	for _, tc := range []struct {
		name   string
		mutate func(*ateapipb.ActorSnapshotTag)
	}{
		{"snapshot changed", func(tag *ateapipb.ActorSnapshotTag) { tag.Snapshot.Name = "some-other-snapshot" }},
		{"snapshot cleared", func(tag *ateapipb.ActorSnapshotTag) { tag.Snapshot = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := impl.UpdateActorSnapshotTag(ctx, tagRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.ActorSnapshotTag) error {
				tc.mutate(toUpdate)
				return nil
			})
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("%s returned %v (err %v), want %v", tc.name, got, err, codes.InvalidArgument)
			}
			got, err := persistence.GetActorSnapshotTag(ctx, tagRef)
			if err != nil {
				t.Fatalf("GetActorSnapshotTag failed: %v", err)
			}
			if got.GetMetadata().GetVersion() != 1 {
				t.Errorf("rejected mutation bumped the version to %d, want 1", got.GetMetadata().GetVersion())
			}
		})
	}
}

// A blind write: the caller never read the tag it is updating. Presence of
// the uid and version guards is the store's to enforce, so the rejection
// arrives from below the request validation, with the same code as before.
func TestUpdateActorSnapshotTag_BlindWriteRejected(t *testing.T) {
	ctx := context.Background()
	svc, stored := rpcServiceWithActorSnapshotTag(t, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag-1"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})

	req := &ateapipb.UpdateActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag-1"},
			Snapshot: stored.GetSnapshot(),
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
		},
	}
	_, err := svc.UpdateActorSnapshotTag(ctx, req)
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("UpdateActorSnapshotTag() code = %v (err %v), want %v", got, err, codes.InvalidArgument)
	}
}

// TestCreateActorSnapshotTag_RejectsCrossAtespace pins the cross-field rule
// that declarative validation cannot express: a tag must be created in its
// snapshot's Atespace. ServiceImpl rejects it before the store is touched, so
// no store is needed here.
func TestCreateActorSnapshotTag_RejectsCrossAtespace(t *testing.T) {
	svc := &RPCService{impl: newServiceImpl(nil, nil, nil)}

	_, err := svc.CreateActorSnapshotTag(context.Background(), &ateapipb.CreateActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "tag-1"},
			Snapshot: &ateapipb.ObjectRef{Atespace: "team-b", Name: "snap-1"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		},
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("CreateActorSnapshotTag() code = %v (err %v), want %v", got, err, codes.FailedPrecondition)
	}
}

// Server-assigned metadata carried on a create request is scrubbed rather than
// rejected: the fields are documented as ignored on input, so even garbage in
// them must not fail validation.
func TestCreateActorSnapshotTag_IgnoresRequestMetadataServerFields(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	svc := &RPCService{impl: newServiceImpl(persistence, nil, nil)}

	snapshot := storetest.MustCreateActorSnapshot(t, ctx, persistence, &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "snap-1"},
		Status:   &ateapipb.ActorSnapshotStatus{SnapshotUri: "gs://my-bucket/snap-1"},
	})

	got, err := svc.CreateActorSnapshotTag(ctx, &ateapipb.CreateActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: testAtespace,
				Name:     "tag-1",
				Uid:      "not-a-uuid",
				Version:  -5,
			},
			Snapshot: &ateapipb.ObjectRef{Atespace: testAtespace, Name: snapshot.GetMetadata().GetName()},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		},
	})
	if err != nil {
		t.Fatalf("CreateActorSnapshotTag() failed: %v", err)
	}
	if uid := got.GetMetadata().GetUid(); uid == "" || uid == "not-a-uuid" {
		t.Errorf("created tag uid = %q, want a server-assigned uid", uid)
	}
	if got.GetMetadata().GetVersion() != 1 {
		t.Errorf("created tag version = %d, want 1", got.GetMetadata().GetVersion())
	}
}

// TestReadActorSnapshotRPCs pins the Get and Delete RPC paths end to end:
// a present resource round-trips, invalid refs are rejected by the generated
// validation, and absent resources map to NOT_FOUND.
func TestReadActorSnapshotRPCs(t *testing.T) {
	ctx := context.Background()
	svc, stored := rpcServiceWithActorSnapshotTag(t, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag-1"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})

	if _, err := svc.GetActorSnapshot(ctx, &ateapipb.GetActorSnapshotRequest{ActorSnapshot: stored.GetSnapshot()}); err != nil {
		t.Errorf("GetActorSnapshot(existing) failed: %v", err)
	}
	got, err := svc.GetActorSnapshotTag(ctx, &ateapipb.GetActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag-1"},
	})
	if err != nil {
		t.Errorf("GetActorSnapshotTag(existing) failed: %v", err)
	} else if got.GetMetadata().GetUid() != stored.GetMetadata().GetUid() {
		t.Errorf("GetActorSnapshotTag() uid = %q, want the stored %q", got.GetMetadata().GetUid(), stored.GetMetadata().GetUid())
	}

	absent := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "no-such-thing"}
	for _, tc := range []struct {
		name string
		call func() error
		want codes.Code
	}{{
		"GetActorSnapshot absent",
		func() error {
			_, err := svc.GetActorSnapshot(ctx, &ateapipb.GetActorSnapshotRequest{ActorSnapshot: absent})
			return err
		},
		codes.NotFound,
	}, {
		"GetActorSnapshot no ref",
		func() error {
			_, err := svc.GetActorSnapshot(ctx, &ateapipb.GetActorSnapshotRequest{})
			return err
		},
		codes.InvalidArgument,
	}, {
		"GetActorSnapshotTag absent",
		func() error {
			_, err := svc.GetActorSnapshotTag(ctx, &ateapipb.GetActorSnapshotTagRequest{ActorSnapshotTag: absent})
			return err
		},
		codes.NotFound,
	}, {
		"GetActorSnapshotTag no ref",
		func() error {
			_, err := svc.GetActorSnapshotTag(ctx, &ateapipb.GetActorSnapshotTagRequest{})
			return err
		},
		codes.InvalidArgument,
	}, {
		"DeleteActorSnapshotTag absent",
		func() error {
			_, err := svc.DeleteActorSnapshotTag(ctx, &ateapipb.DeleteActorSnapshotTagRequest{ActorSnapshotTag: absent})
			return err
		},
		codes.NotFound,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.Code(tc.call()); got != tc.want {
				t.Errorf("code = %v, want %v", got, tc.want)
			}
		})
	}
}
