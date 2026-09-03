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
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestValidateCreateActorSnapshotTagRequest(t *testing.T) {
	scopes := []string{
		ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE.String(),
		ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED.String(),
	}
	validTag := func(opts ...func(*ateapipb.ActorSnapshotTag)) *ateapipb.ActorSnapshotTag {
		tag := &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		}
		for _, opt := range opts {
			opt(tag)
		}
		return tag
	}
	tests := []struct {
		name      string
		req       *ateapipb.CreateActorSnapshotTagRequest
		wantError field.ErrorList
	}{
		{
			name: "valid",
			req: &ateapipb.CreateActorSnapshotTagRequest{
				Actor:            &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
				ActorSnapshotTag: validTag(),
			},
			wantError: nil,
		},
		{
			// The tag lands in the Actor's atespace, so leaving it out is how a
			// client says "wherever the Actor is".
			name: "empty tag.metadata.atespace",
			req: &ateapipb.CreateActorSnapshotTagRequest{
				Actor:            &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
				ActorSnapshotTag: validTag(func(tag *ateapipb.ActorSnapshotTag) { tag.Metadata.Atespace = "" }),
			},
			wantError: nil,
		},
		{
			name: "missing actor",
			req: &ateapipb.CreateActorSnapshotTagRequest{
				ActorSnapshotTag: validTag(func(tag *ateapipb.ActorSnapshotTag) { tag.Metadata.Atespace = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("actor"), "")},
		},
		{
			name: "invalid actor.name",
			req: &ateapipb.CreateActorSnapshotTagRequest{
				Actor:            &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"},
				ActorSnapshotTag: validTag(),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
		},
		{
			name:      "missing tag",
			req:       &ateapipb.CreateActorSnapshotTagRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
			wantError: field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag"), "")},
		},
		{
			name: "missing tag.metadata.name",
			req: &ateapipb.CreateActorSnapshotTagRequest{
				Actor:            &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
				ActorSnapshotTag: validTag(func(tag *ateapipb.ActorSnapshotTag) { tag.Metadata.Name = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "metadata", "name"), "")},
		},
		{
			name: "invalid tag.metadata.name",
			req: &ateapipb.CreateActorSnapshotTagRequest{
				Actor:            &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
				ActorSnapshotTag: validTag(func(tag *ateapipb.ActorSnapshotTag) { tag.Metadata.Name = "TAG1" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "metadata", "name"), "TAG1", "")},
		},
		{
			// A tag somewhere else than its source Actor could not be resolved
			// back to it.
			name: "tag.metadata.atespace is not the actor's",
			req: &ateapipb.CreateActorSnapshotTagRequest{
				Actor:            &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
				ActorSnapshotTag: validTag(func(tag *ateapipb.ActorSnapshotTag) { tag.Metadata.Atespace = "ns2" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("actor_snapshot_tag", "metadata", "atespace"), "ns2", "")},
		},
		{
			name: "unset tag.scope",
			req: &ateapipb.CreateActorSnapshotTagRequest{
				Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
				ActorSnapshotTag: validTag(func(tag *ateapipb.ActorSnapshotTag) {
					tag.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_UNSPECIFIED
				}),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("actor_snapshot_tag", "scope"), "")},
		},
		{
			name: "tag.scope outside the enum",
			req: &ateapipb.CreateActorSnapshotTagRequest{
				Actor:            &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
				ActorSnapshotTag: validTag(func(tag *ateapipb.ActorSnapshotTag) { tag.Scope = ateapipb.ActorSnapshotTagScope(7) }),
			},
			wantError: field.ErrorList{field.NotSupported(field.NewPath("actor_snapshot_tag", "scope"), "7", scopes)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateCreateActorSnapshotTagRequest(tt.req), tt.wantError)
		})
	}
}

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

func TestValidateListActorSnapshotTagsRequest(t *testing.T) {
	tests := []struct {
		name      string
		req       *ateapipb.ListActorSnapshotTagsRequest
		wantError field.ErrorList
	}{
		{
			name:      "valid, atespace scoped",
			req:       &ateapipb.ListActorSnapshotTagsRequest{Atespace: "ns1"},
			wantError: nil,
		},
		{
			// Empty atespace means "all atespaces"
			// (kubectl ate get actor-snapshot-tags -A).
			name:      "valid, empty atespace means all atespaces",
			req:       &ateapipb.ListActorSnapshotTagsRequest{},
			wantError: nil,
		},
		{
			name:      "invalid atespace",
			req:       &ateapipb.ListActorSnapshotTagsRequest{Atespace: "NS1"},
			wantError: field.ErrorList{field.Invalid(field.NewPath("atespace"), "NS1", "")},
		},
		{
			name:      "valid, positive page_size",
			req:       &ateapipb.ListActorSnapshotTagsRequest{Atespace: "ns1", PageSize: 10},
			wantError: nil,
		},
		{
			name:      "negative page_size",
			req:       &ateapipb.ListActorSnapshotTagsRequest{Atespace: "ns1", PageSize: -1},
			wantError: field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateListActorSnapshotTagsRequest(tt.req), tt.wantError)
		})
	}
}

// TestListActorSnapshotTags checks the atespace scoping and the paging of the
// list handler.
func TestListActorSnapshotTags(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	svc := &RPCService{impl: persistence}

	const otherAtespace = "other-atespace"
	seedTag := func(atespace, name string) {
		t.Helper()
		actor := newTestSuspendedActor(t, ctx, persistence, atespace, "actor-"+name)
		storetest.MustCreateActorSnapshotTag(t, ctx, persistence, newTestTag(t, name, actor))
	}
	seedTag(testAtespace, "v1")
	seedTag(testAtespace, "v2")
	seedTag(otherAtespace, "v1")

	// list collects every page, so the assertions below cover the page token
	// round-trip as well as the contents.
	list := func(req *ateapipb.ListActorSnapshotTagsRequest) []string {
		t.Helper()
		var got []string
		for {
			resp, err := svc.ListActorSnapshotTags(ctx, req)
			if err != nil {
				t.Fatalf("ListActorSnapshotTags(%v) failed: %v", req, err)
			}
			for _, tag := range resp.GetActorSnapshotTags() {
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
		req  *ateapipb.ListActorSnapshotTagsRequest
		want []string
	}{
		{
			name: "atespace scoped",
			req:  &ateapipb.ListActorSnapshotTagsRequest{Atespace: testAtespace},
			want: []string{testAtespace + "/v1", testAtespace + "/v2"},
		},
		{
			name: "empty atespace lists all atespaces",
			req:  &ateapipb.ListActorSnapshotTagsRequest{},
			want: []string{otherAtespace + "/v1", testAtespace + "/v1", testAtespace + "/v2"},
		},
		{
			name: "one tag per page",
			req:  &ateapipb.ListActorSnapshotTagsRequest{PageSize: 1},
			want: []string{otherAtespace + "/v1", testAtespace + "/v1", testAtespace + "/v2"},
		},
		{
			name: "atespace with no tags",
			req:  &ateapipb.ListActorSnapshotTagsRequest{Atespace: "empty-atespace"},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, list(tt.req)); diff != "" {
				t.Errorf("ListActorSnapshotTags mismatch (-want +got):\n%s", diff)
			}
		})
	}

	_, err := svc.ListActorSnapshotTags(ctx, &ateapipb.ListActorSnapshotTagsRequest{PageToken: "not-a-token"})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("ListActorSnapshotTags(bad page_token) error = %v (code %v), want code InvalidArgument", err, code)
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
			// scope is the only field a client owns: a request rewriting
			// server-owned fields is applied as a scope change alone.
			name:   "server-owned fields in the request are ignored",
			stored: &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE},
			req: &ateapipb.ActorSnapshotTag{
				Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
				Status: &ateapipb.ActorSnapshotTagStatus{
					Snapshot:         &ateapipb.ExternalSnapshot{SnapshotUri: "gs://attacker/elsewhere", ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA},
					ActorTemplateUid: "other-template-uid",
				},
			},
			want: &ateapipb.ActorSnapshotTag{Scope: ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.stored.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tag1"}
			svc, stored := rpcServiceWithActorSnapshotTag(t, tt.stored)

			tt.req.Metadata = stored.GetMetadata()

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
			tt.want.Status = stored.GetStatus()
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
		status.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: uri.String(), ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL}
	})
}

// newTestTag builds a finished tag of actor: the shape
// CreateActorSnapshotTag leaves behind, pointing at the tag's own copy of the
// actor's external snapshot rather than at the actor's.
func newTestTag(t *testing.T, name string, actor *ateapipb.Actor) *ateapipb.ActorSnapshotTag {
	t.Helper()
	atespace := actor.GetMetadata().GetAtespace()
	uri, err := resources.NewTagSnapshotURI(testStorageLocation, atespace, name)
	if err != nil {
		t.Fatalf("NewTagSnapshotURI: %v", err)
	}
	return &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		Status: &ateapipb.ActorSnapshotTagStatus{
			Snapshot: &ateapipb.ExternalSnapshot{
				SnapshotUri:  uri.String(),
				ContentScope: actor.GetStatus().GetExternalSnapshot().GetContentScope(),
			},
			SourceActorUid: actor.GetMetadata().GetUid(),
		},
	}
}

// newPendingTestTag builds the tag a create leaves behind when it dies between
// reserving the name and finishing the copy: the row names the prefix it was
// writing into, and nothing else.
func newPendingTestTag(t *testing.T, name string, actor *ateapipb.Actor) *ateapipb.ActorSnapshotTag {
	t.Helper()
	tag := newTestTag(t, name, actor)
	tag.Status.InProgressSnapshotUri = tag.GetStatus().GetSnapshot().GetSnapshotUri()
	tag.Status.Snapshot = nil
	return tag
}

// rpcServiceWithActorSnapshotTag seeds a suspended actor and a tag over its
// external snapshot in a PostgreSQL-backed store, and returns an RPCService
// over it.
func rpcServiceWithActorSnapshotTag(t *testing.T, tag *ateapipb.ActorSnapshotTag) (*RPCService, *ateapipb.ActorSnapshotTag) {
	t.Helper()
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	atespace, name := tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName()
	actor := newTestSuspendedActor(t, ctx, persistence, atespace, "actor-"+name)
	seeded := newTestTag(t, name, actor)
	seeded.Scope = tag.GetScope()
	created := storetest.MustCreateActorSnapshotTag(t, ctx, persistence, seeded)
	return &RPCService{impl: persistence}, created
}

// TestUpdateActorSnapshotTag_DeleteRecreateRace checks that an update is not
// applied if a tag was deleted and re-created during the update operation.
func TestUpdateActorSnapshotTag_DeleteRecreateRace(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	actorOne := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-1")
	actorTwo := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-2")

	const tagName = "before-upgrade"
	// Tag A: what the client reads, and what its uid precondition names.
	// Freshly created, so it sits at version 1.
	originalTag := storetest.MustCreateActorSnapshotTag(t, ctx, persistence, newTestTag(t, tagName, actorOne))

	// A concurrent client deletes A and re-tags the same atespace/name as a
	// brand new tag B, pointed at another snapshot.
	var recreatedTag *ateapipb.ActorSnapshotTag
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.DeleteActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: testAtespace, Name: tagName}); err != nil {
				t.Fatalf("Racing writer: DeleteActorSnapshotTag: %v", err)
			}
			recreatedTag = storetest.MustCreateActorSnapshotTag(t, ctx, persistence, newTestTag(t, tagName, actorTwo))
		},
	}
	svc := &RPCService{impl: racing}

	// The client asserts "only update the tag with uid A". Its version guard is
	// satisfied by B as well, because re-tagging resets the version to 1: the
	// uid is the only thing that can tell the two lifecycles apart.
	originalTag.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
	_, err := svc.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{
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
	actor := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-1")

	const tagName = "before-upgrade"
	originalTag := storetest.MustCreateActorSnapshotTag(t, ctx, persistence, newTestTag(t, tagName, actor))

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
	_, err := svc.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{
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

// TestDeleteActorSnapshotTag_ReleasesExternalSnapshot verifies the delete
// collects the external snapshot the tag owns before dropping the row that
// names it, and that a failure to collect leaves the row intact so a retry can
// finish the job.
func TestDeleteActorSnapshotTag_ReleasesExternalSnapshot(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actor := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-1")
	tag := storetest.MustCreateActorSnapshotTag(t, ctx, persistence, newTestTag(t, "v1", actor))
	tagRef := resources.ActorSnapshotTagRefFromActorSnapshotTag(tag)

	objects := objectstoretest.New()
	uri, err := resources.ParseSnapshotURI(tag.GetStatus().GetSnapshot().GetSnapshotUri())
	if err != nil {
		t.Fatalf("ParseSnapshotURI: %v", err)
	}
	objects.PutSnapshot(t, uri, "manifest.json", "memory.zst")
	svc := &RPCService{impl: persistence, objectStore: objects}

	// A delete that cannot reach object storage must not drop the row: it is
	// the only handle left on the snapshot.
	objects.OnDelete = func(string, string) error { return errObjectStore }
	req := &ateapipb.DeleteActorSnapshotTagRequest{ActorSnapshotTag: tagRef.ToObjectRef()}
	if _, err := svc.DeleteActorSnapshotTag(ctx, req); !errors.Is(err, errObjectStore) {
		t.Fatalf("DeleteActorSnapshotTag = %v, want an error wrapping %v", err, errObjectStore)
	}
	if _, err := persistence.GetActorSnapshotTag(ctx, tagRef); err != nil {
		t.Fatalf("GetActorSnapshotTag after the failure: %v", err)
	}

	// Simulates a retried deletion. Now, the object deletion succeeds,
	// so we can remove the row from the DB.
	objects.OnDelete = nil
	if _, err := svc.DeleteActorSnapshotTag(ctx, req); err != nil {
		t.Fatalf("retried DeleteActorSnapshotTag: %v", err)
	}
	if got := objects.Snapshot(t, uri); len(got) != 0 {
		t.Errorf("the tag's external snapshot still holds %v, want it collected", got)
	}
	if _, err := persistence.GetActorSnapshotTag(ctx, tagRef); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActorSnapshotTag after the delete = %v, want ErrNotFound", err)
	}
}

// TestDeleteActorSnapshotTag_ReleasesPendingSnapshot verifies that deleting a
// tag whose create never finished collects what that create stranded. The
// pending row names the prefix the copy was writing into, and it is the only
// handle left on those objects.
func TestDeleteActorSnapshotTag_ReleasesPendingSnapshot(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actor := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-1")
	tag := storetest.MustCreateActorSnapshotTag(t, ctx, persistence, newPendingTestTag(t, "v1", actor))
	tagRef := resources.ActorSnapshotTagRefFromActorSnapshotTag(tag)

	objects := objectstoretest.New()
	uri, err := resources.ParseSnapshotURI(tag.GetStatus().GetInProgressSnapshotUri())
	if err != nil {
		t.Fatalf("ParseSnapshotURI: %v", err)
	}
	// What a copy that died halfway through left behind.
	objects.PutSnapshot(t, uri, "manifest.json")
	svc := &RPCService{impl: persistence, objectStore: objects}

	if _, err := svc.DeleteActorSnapshotTag(ctx, &ateapipb.DeleteActorSnapshotTagRequest{ActorSnapshotTag: tagRef.ToObjectRef()}); err != nil {
		t.Fatalf("DeleteActorSnapshotTag: %v", err)
	}
	if got := objects.Snapshot(t, uri); len(got) != 0 {
		t.Errorf("the pending tag's stranded objects are still %v, want them collected", got)
	}
	if _, err := persistence.GetActorSnapshotTag(ctx, tagRef); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetActorSnapshotTag after the delete = %v, want ErrNotFound", err)
	}
}

// TestUpdateActorSnapshotTag_PendingTag verifies a tag whose create never
// finished cannot be published: it names a copy that may be partial, so it must
// not become usable until the create completes.
func TestUpdateActorSnapshotTag_PendingTag(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	svc := &RPCService{impl: persistence}

	actor := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-1")
	tag := storetest.MustCreateActorSnapshotTag(t, ctx, persistence, newPendingTestTag(t, "v1", actor))

	tag.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
	_, err := svc.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{ActorSnapshotTag: tag})
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Fatalf("UpdateActorSnapshotTag error = %v (code %v), want code FailedPrecondition", err, code)
	}

	stored, err := persistence.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRefFromActorSnapshotTag(tag))
	if err != nil {
		t.Fatalf("GetActorSnapshotTag: %v", err)
	}
	if got, want := stored.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE; got != want {
		t.Errorf("stored scope = %v, want %v: the rejected update published a pending tag", got, want)
	}
}
