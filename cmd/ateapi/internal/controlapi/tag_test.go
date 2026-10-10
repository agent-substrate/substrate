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
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestListTags checks the atespace scoping and the paging of the
// list handler.
func TestListTags(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	svc := &RPCService{impl: newServiceImpl(persistence, nil)}

	const otherAtespace = "other-atespace"
	seedTag := func(atespace, name string) {
		t.Helper()
		actor := newTestSuspendedActor(t, ctx, persistence, atespace, "actor-"+name)
		storetest.MustCreateTag(t, ctx, persistence, newTestTag(t, name, actor))
	}
	seedTag(testAtespace, "v1")
	seedTag(testAtespace, "v2")
	seedTag(otherAtespace, "v1")

	// list collects every page, so the assertions below cover the page token
	// round-trip as well as the contents.
	list := func(req *ateapipb.ListTagsRequest) []string {
		t.Helper()
		var got []string
		for {
			resp, err := svc.ListTags(ctx, req)
			if err != nil {
				t.Fatalf("ListTags(%v) failed: %v", req, err)
			}
			for _, tag := range resp.GetTags() {
				got = append(got, tag.GetMetadata().GetAtespace()+"/"+tag.GetMetadata().GetName())
			}
			if resp.GetNextPageToken() == "" {
				return got
			}
			req.PageToken = resp.GetNextPageToken()
		}
	}

	tests := []struct {
		name string
		req  *ateapipb.ListTagsRequest
		want []string
	}{
		{
			name: "atespace scoped",
			req:  &ateapipb.ListTagsRequest{Atespace: testAtespace},
			want: []string{testAtespace + "/v1", testAtespace + "/v2"},
		},
		{
			name: "empty atespace lists all atespaces",
			req:  &ateapipb.ListTagsRequest{},
			want: []string{otherAtespace + "/v1", testAtespace + "/v1", testAtespace + "/v2"},
		},
		{
			name: "one tag per page",
			req:  &ateapipb.ListTagsRequest{PageSize: 1},
			want: []string{otherAtespace + "/v1", testAtespace + "/v1", testAtespace + "/v2"},
		},
		{
			name: "atespace with no tags",
			req:  &ateapipb.ListTagsRequest{Atespace: "empty-atespace"},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, list(tt.req)); diff != "" {
				t.Errorf("ListTags mismatch (-want +got):\n%s", diff)
			}
		})
	}

	_, err := svc.ListTags(ctx, &ateapipb.ListTagsRequest{PageToken: "not-a-token"})
	if code := apierror.Code(err); code != codes.InvalidArgument {
		t.Errorf("ListTags(bad page_token) error = %v (code %v), want code InvalidArgument", err, code)
	}
}

func TestUpdateTag(t *testing.T) {
	tests := []struct {
		name     string
		stored   *ateapipb.Tag
		req      *ateapipb.Tag
		want     *ateapipb.Tag
		wantCode codes.Code
	}{
		{
			name:   "publishes an atespace-scoped tag",
			stored: &ateapipb.Tag{Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE},
			req:    &ateapipb.Tag{Scope: ateapipb.TagScope_TAG_SCOPE_PUBLISHED},
			want:   &ateapipb.Tag{Scope: ateapipb.TagScope_TAG_SCOPE_PUBLISHED},
		},
		{
			name:   "unpublishes a published tag",
			stored: &ateapipb.Tag{Scope: ateapipb.TagScope_TAG_SCOPE_PUBLISHED},
			req:    &ateapipb.Tag{Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE},
			want:   &ateapipb.Tag{Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE},
		},
		{
			// scope is the only field a client owns: a request rewriting
			// server-owned fields is applied as a scope change alone.
			name:   "server-owned fields in the request are ignored",
			stored: &ateapipb.Tag{Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE},
			req: &ateapipb.Tag{
				Scope: ateapipb.TagScope_TAG_SCOPE_PUBLISHED,
				Status: &ateapipb.TagStatus{
					Snapshot:         newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, "", "gs://attacker/elsewhere", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					ActorTemplateUid: "other-template-uid",
				},
			},
			want: &ateapipb.Tag{Scope: ateapipb.TagScope_TAG_SCOPE_PUBLISHED},
		},
		{
			// A tag never moves between snapshots, so it never moves between
			// sources either.
			name:   "repointing source_actor is rejected",
			stored: &ateapipb.Tag{Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE},
			req: &ateapipb.Tag{
				Scope:       ateapipb.TagScope_TAG_SCOPE_PUBLISHED,
				SourceActor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "someone-elses-actor"},
			},
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.stored.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"}
			svc, stored := rpcServiceWithTag(t, tt.stored)

			tt.req.Metadata = stored.GetMetadata()
			// source_actor is immutable, so an ordinary update echoes back the
			// one it read; only the case that repoints it names its own.
			if tt.req.GetSourceActor() == nil {
				tt.req.SourceActor = stored.GetSourceActor()
			}

			updated, err := svc.UpdateTag(context.Background(), &ateapipb.UpdateTagRequest{Tag: tt.req})

			if tt.wantCode != codes.OK {
				if code := apierror.Code(err); code != tt.wantCode {
					t.Errorf("UpdateTag error = %v (code %v), want code %v", err, code, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("UpdateTag failed: %v", err)
			}

			tt.want.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1", Version: 2}
			tt.want.Status, tt.want.SourceActor = stored.GetStatus(), stored.GetSourceActor()
			if diff := cmp.Diff(tt.want, updated, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
				t.Errorf("UpdateTag response mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestUpdateTag_BlindWrite checks that an update naming neither a
// uid nor a version is rejected. The schema does not require them — the store
// does, because a caller that read nothing has nothing to lose a race with.
func TestUpdateTag_BlindWrite(t *testing.T) {
	svc, stored := rpcServiceWithTag(t, &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"},
		Scope:    ateapipb.TagScope_TAG_SCOPE_ATESPACE,
	})

	stored.Metadata.Uid, stored.Metadata.Version = "", 0
	stored.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED
	_, err := svc.UpdateTag(context.Background(), &ateapipb.UpdateTagRequest{Tag: stored})
	if code := apierror.Code(err); code != codes.InvalidArgument {
		t.Errorf("UpdateTag error = %v (code %v), want code InvalidArgument", err, code)
	}
}

// TestUpdateTag_UnsetScopeDoesNotUnpublish checks that an update
// leaving scope unset is rejected.
func TestUpdateTag_UnsetScopeDoesNotUnpublish(t *testing.T) {
	ctx := context.Background()
	svc, stored := rpcServiceWithTag(t, &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"},
		Scope:    ateapipb.TagScope_TAG_SCOPE_PUBLISHED,
	})

	// The guards are the ones the client read, so the rejection can only come
	// from the unset scope.
	stored.Scope = ateapipb.TagScope_TAG_SCOPE_UNSPECIFIED
	_, err := svc.UpdateTag(ctx, &ateapipb.UpdateTagRequest{
		Tag: stored,
	})
	if code := apierror.Code(err); code != codes.InvalidArgument {
		t.Errorf("UpdateTag error = %v (code %v), want code InvalidArgument", err, code)
	}

	current, err := svc.impl.GetTag(ctx, resources.TagRef{Atespace: testAtespace, Name: "tag1"})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if got, want := current.GetScope(), ateapipb.TagScope_TAG_SCOPE_PUBLISHED; got != want {
		t.Errorf("stored scope = %v, want %v: the rejected update must not have unpublished the tag", got, want)
	}
	if got, want := current.GetMetadata().GetVersion(), stored.GetMetadata().GetVersion(); got != want {
		t.Errorf("stored version = %d, want %d: the rejected update must not have written", got, want)
	}
}

// newTestSuspendedActor creates a suspended actor holding an external snapshot.
func newTestSuspendedActor(t *testing.T, ctx context.Context, st store.Interface, atespace, name string) *ateapipb.Actor {
	t.Helper()
	actor := storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "template-1"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	// The snapshot sits under the actor's own prefix, which is keyed on the UID
	// the store assigns, so it can only be recorded once the row exists.
	uri, err := resources.NewActorSnapshotURI(testStorageLocation, atespace, actor.GetMetadata().GetUid(), name)
	if err != nil {
		t.Fatalf("NewActorSnapshotURI: %v", err)
	}
	return mustUpdateActorStatus(t, ctx, st, actor, func(status *ateapipb.ActorStatus) {
		status.LastAssignedGeneration = 1
		status.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", uri.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
	})
}

// newTestTag builds a finished tag of actor: the shape
// CreateTag leaves behind, pointing at the tag's own copy of the
// actor's external snapshot rather than at the actor's.
func newTestTag(t *testing.T, name string, actor *ateapipb.Actor) *ateapipb.Tag {
	t.Helper()
	atespace := actor.GetMetadata().GetAtespace()
	uri, err := resources.NewTagSnapshotURI(testStorageLocation, atespace, name)
	if err != nil {
		t.Fatalf("NewTagSnapshotURI: %v", err)
	}
	actorSnap, actorSt := findLatestSnapshotStorage(actor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	return &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: resources.ActorRefFromActor(actor).ToObjectRef(),
		Status: &ateapipb.TagStatus{
			Snapshot: newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, actorSt.GetFidelity(), actorSnap.GetActorTemplateUid(), uri.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		},
	}
}

// newPendingTestTag builds the tag a create leaves behind when it dies between
// reserving the name and finishing the copy: the row names the prefix it was
// writing into as IN_PROGRESS.
func newPendingTestTag(t *testing.T, name string, actor *ateapipb.Actor) *ateapipb.Tag {
	t.Helper()
	tag := newTestTag(t, name, actor)
	tag.Status.Snapshot.Storage[0].Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS
	return tag
}

// rpcServiceWithTag seeds a suspended actor and a tag over its
// external snapshot in a PostgreSQL-backed store, and returns an RPCService
// over it.
func rpcServiceWithTag(t *testing.T, tag *ateapipb.Tag) (*RPCService, *ateapipb.Tag) {
	t.Helper()
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	atespace, name := tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName()
	actor := newTestSuspendedActor(t, ctx, persistence, atespace, "actor-"+name)
	seeded := newTestTag(t, name, actor)
	seeded.Scope = tag.GetScope()
	created := storetest.MustCreateTag(t, ctx, persistence, seeded)
	return &RPCService{impl: newServiceImpl(persistence, nil)}, created
}

// TestUpdateTag_DeleteRecreateRace checks that an update is not
// applied if a tag was deleted and re-created during the update operation.
func TestUpdateTag_DeleteRecreateRace(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	actorOne := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-1")
	actorTwo := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-2")

	const tagName = "before-upgrade"
	// Tag A: what the client reads, and what its uid precondition names.
	// Freshly created, so it sits at version 1.
	originalTag := storetest.MustCreateTag(t, ctx, persistence, newTestTag(t, tagName, actorOne))

	// A concurrent client deletes A and re-tags the same atespace/name as a
	// brand new tag B, pointed at another snapshot.
	var recreatedTag *ateapipb.Tag
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.DeleteTag(ctx, resources.TagRef{Atespace: testAtespace, Name: tagName}, store.DeletePreconditions{}); err != nil {
				t.Fatalf("Racing writer: DeleteTag: %v", err)
			}
			recreatedTag = storetest.MustCreateTag(t, ctx, persistence, newTestTag(t, tagName, actorTwo))
		},
	}
	svc := &RPCService{impl: newServiceImpl(racing, nil)}

	// The client asserts "only update the tag with uid A". Its version guard is
	// satisfied by B as well, because re-tagging resets the version to 1: the
	// uid is the only thing that can tell the two lifecycles apart.
	originalTag.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED
	_, err := svc.UpdateTag(ctx, &ateapipb.UpdateTagRequest{
		Tag: originalTag,
	})
	if code := apierror.Code(err); code != codes.Aborted {
		t.Errorf("UpdateTag error = %v (code %v), want code Aborted: the tag holding uid %s was deleted mid-update",
			err, code, originalTag.GetMetadata().GetUid())
	}

	storedTag, err := persistence.GetTag(ctx, resources.TagRef{Atespace: testAtespace, Name: tagName})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	// The stored record must still be tag B as its creator left it. Any of A's
	// state showing up here is the clobber.
	if diff := cmp.Diff(recreatedTag, storedTag, protocmp.Transform()); diff != "" {
		t.Errorf("Update meant for the deleted tag was applied to the recreated one (-recreated +stored):\n%s", diff)
	}
}

// TestUpdateTag_ConcurrentUpdate checks that a write landing in
// the handler's read-modify-write window is reported as Aborted rather than
// absorbed. Every update guards on a version, so there is no unguarded update left
// for the server to resolve on the client's behalf.
func TestUpdateTag_ConcurrentUpdate(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	actor := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-1")

	const tagName = "before-upgrade"
	originalTag := storetest.MustCreateTag(t, ctx, persistence, newTestTag(t, tagName, actor))

	// A concurrent client moves the tag past the version the caller could have
	// observed, in the window the handler used to leave open between its own
	// read and the store's WATCH.
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateTag(ctx, resources.TagRef{Atespace: testAtespace, Name: tagName}, store.PreconditionFrom(originalTag), func(toUpdate *ateapipb.Tag) error {
				toUpdate.Scope = ateapipb.TagScope_TAG_SCOPE_ATESPACE
				return nil
			}); err != nil {
				t.Fatalf("Racing writer: UpdateTag: %v", err)
			}
		},
	}
	svc := &RPCService{impl: newServiceImpl(racing, nil)}

	originalTag.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED
	_, err := svc.UpdateTag(ctx, &ateapipb.UpdateTagRequest{
		Tag: originalTag,
	})
	if code := apierror.Code(err); code != codes.Aborted {
		t.Errorf("UpdateTag error = %v (code %v), want code Aborted: the guarded version moved under the update", err, code)
	}

	storedTag, err := persistence.GetTag(ctx, resources.TagRef{Atespace: testAtespace, Name: tagName})
	if err != nil {
		t.Fatalf("Failed to GetTag(%s/%s): %v", testAtespace, tagName, err)
	}
	// Only the concurrent writer's version bump landed: the rejected update
	// wrote nothing.
	if got, want := storedTag.GetMetadata().GetVersion(), originalTag.GetMetadata().GetVersion()+1; got != want {
		t.Errorf("stored version = %d, want %d", got, want)
	}
	if got, want := storedTag.GetScope(), ateapipb.TagScope_TAG_SCOPE_ATESPACE; got != want {
		t.Errorf("Stored scope = %v, want %v: the rejected update was applied anyway", got, want)
	}
}

// TestUpdateTag_PendingTag verifies a tag whose create never
// finished cannot be published: it names a copy that may be partial, so it must
// not become usable until the create completes.
func TestUpdateTag_PendingTag(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	svc := &RPCService{impl: newServiceImpl(persistence, nil)}

	actor := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-1")
	tag := storetest.MustCreateTag(t, ctx, persistence, newPendingTestTag(t, "v1", actor))

	tag.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED
	_, err := svc.UpdateTag(ctx, &ateapipb.UpdateTagRequest{Tag: tag})
	if code := apierror.Code(err); code != codes.FailedPrecondition {
		t.Fatalf("UpdateTag error = %v (code %v), want code FailedPrecondition", err, code)
	}

	stored, err := persistence.GetTag(ctx, resources.TagRefFromTag(tag))
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if got, want := stored.GetScope(), ateapipb.TagScope_TAG_SCOPE_ATESPACE; got != want {
		t.Errorf("stored scope = %v, want %v: the rejected update published a pending tag", got, want)
	}
}
