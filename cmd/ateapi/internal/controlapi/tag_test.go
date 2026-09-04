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
	"strings"
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

func TestValidateCreateTagRequest(t *testing.T) {
	ctx := context.Background()
	validTag := func(opts ...func(*ateapipb.Tag)) *ateapipb.Tag {
		tag := &ateapipb.Tag{
			Metadata:    &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
			Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
		}
		for _, opt := range opts {
			opt(tag)
		}
		return tag
	}
	tests := []struct {
		name      string
		req       *ateapipb.CreateTagRequest
		wantError field.ErrorList
	}{
		{
			name:      "valid",
			req:       &ateapipb.CreateTagRequest{Tag: validTag()},
			wantError: nil,
		},
		{
			// Status is server-owned and scrubbed before this runs, but a
			// client that echoes back a tag it read is not to be tripped up by
			// one either.
			name: "valid with status",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) {
					tag.Status = &ateapipb.TagStatus{Snapshot: validExternalSnapshot()}
				}),
			},
			wantError: nil,
		},
		{
			// The tag has to name the atespace it lands in, like every other
			// create: the source Actor's atespace is checked against it, not
			// used as a default.
			name: "empty tag.metadata.atespace",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Atespace = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata", "atespace"), "")},
		},
		{
			// Malformed, so it is both rejected on its own terms and unable to
			// match the source actor's.
			name: "invalid tag.metadata.atespace",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Atespace = "NS1" }),
			},
			wantError: field.ErrorList{
				field.Invalid(field.NewPath("tag", "metadata", "atespace"), nil, ""),
				field.Invalid(field.NewPath("tag", "metadata", "atespace"), nil, "").WithOrigin("format=k8s-short-name"),
			},
		},
		{
			name: "missing tag.source_actor",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.SourceActor = nil }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "source_actor"), "")},
		},
		{
			// Only source_actor is reported: tag.metadata.atespace cannot be
			// held against a source that has none to compare it to.
			name: "missing tag.source_actor.atespace",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.SourceActor.Atespace = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "source_actor", "atespace"), "")},
		},
		{
			// A malformed source atespace is also one the tag's own atespace
			// cannot match, so both are reported.
			name: "invalid tag.source_actor.atespace",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.SourceActor.Atespace = "NS1" }),
			},
			wantError: field.ErrorList{
				field.Invalid(field.NewPath("tag", "metadata", "atespace"), nil, ""),
				field.Invalid(field.NewPath("tag", "source_actor", "atespace"), nil, "").WithOrigin("format=k8s-short-name"),
			},
		},
		{
			name: "missing tag.source_actor.name",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.SourceActor.Name = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "source_actor", "name"), "")},
		},
		{
			name: "invalid tag.source_actor.name",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.SourceActor.Name = "ID1" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "source_actor", "name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "missing tag",
			req:       &ateapipb.CreateTagRequest{},
			wantError: field.ErrorList{field.Required(field.NewPath("tag"), "")},
		},
		{
			name: "missing tag.metadata",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata = nil }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata"), "")},
		},
		{
			name: "missing tag.metadata.name",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Name = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata", "name"), "")},
		},
		{
			name: "invalid tag.metadata.name",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Name = "TAG1" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			// A tag somewhere else than its source Actor could not be resolved
			// back to it.
			name: "tag.metadata.atespace is not the source actor's",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Atespace = "ns2" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "atespace"), nil, "")},
		},
		{
			name: "unset tag.scope",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) {
					tag.Scope = ateapipb.TagScope_TAG_SCOPE_UNSPECIFIED
				}),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "scope"), "")},
		},
		{
			name: "tag.scope above the enum",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Scope = ateapipb.TagScope(7) }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "scope"), nil, "").WithOrigin("maximum")},
		},
		{
			name: "negative tag.scope",
			req: &ateapipb.CreateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Scope = ateapipb.TagScope(-1) }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "scope"), nil, "").WithOrigin("minimum")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateCreateTagRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateUpdateTagRequest(t *testing.T) {
	ctx := context.Background()
	// validUID is a well-formed uid to pass validation.
	const validUID = "2a5f8c1e-9b3d-4f7a-8e6c-1d0b4a7f2e93"
	validTag := func(opts ...func(*ateapipb.Tag)) *ateapipb.Tag {
		tag := &ateapipb.Tag{
			Metadata:    &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: validUID, Version: 7},
			Scope:       ateapipb.TagScope_TAG_SCOPE_PUBLISHED,
			SourceActor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
		}
		for _, opt := range opts {
			opt(tag)
		}
		return tag
	}
	tests := []struct {
		name      string
		req       *ateapipb.UpdateTagRequest
		wantError field.ErrorList
	}{
		{
			name:      "valid",
			req:       &ateapipb.UpdateTagRequest{Tag: validTag()},
			wantError: nil,
		},
		{
			name:      "missing tag",
			req:       &ateapipb.UpdateTagRequest{},
			wantError: field.ErrorList{field.Required(field.NewPath("tag"), "")},
		},
		{
			name: "missing tag.metadata",
			req: &ateapipb.UpdateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata = nil }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata"), "")},
		},
		{
			name: "missing tag.metadata.atespace",
			req: &ateapipb.UpdateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Atespace = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata", "atespace"), "")},
		},
		{
			name: "invalid tag.metadata.atespace",
			req: &ateapipb.UpdateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Atespace = "NS1" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name: "missing tag.metadata.name",
			req: &ateapipb.UpdateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Name = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata", "name"), "")},
		},
		{
			name: "invalid tag.metadata.name",
			req: &ateapipb.UpdateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Name = "TAG1" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			// The preconditions are the store's to insist on, not the schema's:
			// an update carrying neither is rejected as a blind write once it
			// reaches the store. See TestUpdateTag_BlindWrite.
			name: "missing tag.metadata.uid precondition",
			req: &ateapipb.UpdateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Uid = "" }),
			},
			wantError: nil,
		},
		{
			name: "invalid tag.metadata.uid precondition",
			req: &ateapipb.UpdateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Uid = "not-a-uuid" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "uid"), nil, "").WithOrigin("format=k8s-uuid")},
		},
		{
			name: "missing tag.metadata.version precondition",
			req: &ateapipb.UpdateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Version = 0 }),
			},
			wantError: nil,
		},
		{
			name: "negative tag.metadata.version precondition",
			req: &ateapipb.UpdateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) { tag.Metadata.Version = -1 }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "version"), nil, "").WithOrigin("minimum")},
		},
		{
			// Nothing about the tag body is held against the request: it is
			// checked against the stored tag instead, so a source or a scope
			// the request gets wrong is caught there.
			name: "tag body is not checked against the request",
			req: &ateapipb.UpdateTagRequest{
				Tag: validTag(func(tag *ateapipb.Tag) {
					tag.SourceActor = nil
					tag.Scope = ateapipb.TagScope_TAG_SCOPE_UNSPECIFIED
				}),
			},
			wantError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateTagRequest(ctx, tt.req), tt.wantError)
		})
	}
}

// TestValidateTagUpdateResult covers the second half of an update:
// the merged tag the server is about to write, checked against the one it
// replaces. This is where everything the request could not be held to lands.
func TestValidateTagUpdateResult(t *testing.T) {
	ctx := context.Background()
	tagPath := field.NewPath("tag")
	storedTag := func(opts ...func(*ateapipb.Tag)) *ateapipb.Tag {
		tag := &ateapipb.Tag{
			Metadata:    &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: "2a5f8c1e-9b3d-4f7a-8e6c-1d0b4a7f2e93", Version: 7},
			Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"},
		}
		for _, opt := range opts {
			opt(tag)
		}
		return tag
	}
	tests := []struct {
		name      string
		newVal    *ateapipb.Tag
		wantError field.ErrorList
	}{
		{
			name:      "unchanged",
			newVal:    storedTag(),
			wantError: nil,
		},
		{
			name: "publishing is allowed",
			newVal: storedTag(func(tag *ateapipb.Tag) {
				tag.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED
			}),
			wantError: nil,
		},
		{
			name: "unset tag.scope",
			newVal: storedTag(func(tag *ateapipb.Tag) {
				tag.Scope = ateapipb.TagScope_TAG_SCOPE_UNSPECIFIED
			}),
			wantError: field.ErrorList{field.Required(tagPath.Child("scope"), "")},
		},
		{
			name:      "tag.scope above the enum",
			newVal:    storedTag(func(tag *ateapipb.Tag) { tag.Scope = ateapipb.TagScope(7) }),
			wantError: field.ErrorList{field.Invalid(tagPath.Child("scope"), nil, "").WithOrigin("maximum")},
		},
		{
			name:      "negative tag.scope",
			newVal:    storedTag(func(tag *ateapipb.Tag) { tag.Scope = ateapipb.TagScope(-1) }),
			wantError: field.ErrorList{field.Invalid(tagPath.Child("scope"), nil, "").WithOrigin("minimum")},
		},
		{
			// A tag never moves between snapshots, so it never moves between
			// sources either.
			name: "repointed tag.source_actor",
			newVal: storedTag(func(tag *ateapipb.Tag) {
				tag.SourceActor = &ateapipb.ObjectRef{Atespace: "ns1", Name: "id2"}
			}),
			wantError: field.ErrorList{field.Invalid(tagPath.Child("source_actor"), nil, "").WithOrigin("immutable")},
		},
		{
			// An update is a whole-object replace, so a client that drops the
			// source it read back would clear it.
			name:   "missing tag.source_actor",
			newVal: storedTag(func(tag *ateapipb.Tag) { tag.SourceActor = nil }),
			wantError: field.ErrorList{
				field.Invalid(tagPath.Child("source_actor"), nil, "").WithOrigin("immutable"),
				field.Required(tagPath.Child("source_actor"), ""),
			},
		},
		{
			// The tag is addressed through its atespace and name; moving it
			// would strand every reference to it.
			name:      "renamed tag",
			newVal:    storedTag(func(tag *ateapipb.Tag) { tag.Metadata.Name = "tag2" }),
			wantError: field.ErrorList{field.Invalid(tagPath.Child("metadata", "name"), nil, "").WithOrigin("immutable")},
		},
		{
			name:      "tag moved to another atespace",
			newVal:    storedTag(func(tag *ateapipb.Tag) { tag.Metadata.Atespace = "ns2" }),
			wantError: field.ErrorList{field.Invalid(tagPath.Child("metadata", "atespace"), nil, "").WithOrigin("immutable")},
		},
		{
			name:   "cleared tag.metadata.atespace",
			newVal: storedTag(func(tag *ateapipb.Tag) { tag.Metadata.Atespace = "" }),
			wantError: field.ErrorList{
				field.Required(tagPath.Child("metadata", "atespace"), ""),
				field.Invalid(tagPath.Child("metadata", "atespace"), nil, "").WithOrigin("immutable"),
			},
		},
		{
			name:      "missing tag.metadata",
			newVal:    storedTag(func(tag *ateapipb.Tag) { tag.Metadata = nil }),
			wantError: field.ErrorList{field.Required(tagPath.Child("metadata"), "")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateTagUpdate(ctx, tagPath, tt.newVal, storedTag()), tt.wantError)
		})
	}
}

func TestValidateListTagsRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapipb.ListTagsRequest
		wantError field.ErrorList
	}{
		{
			name:      "valid, atespace scoped",
			req:       &ateapipb.ListTagsRequest{Atespace: "ns1"},
			wantError: nil,
		},
		{
			// Empty atespace means "all atespaces"
			// (kubectl ate get tags -A).
			name:      "valid, empty atespace means all atespaces",
			req:       &ateapipb.ListTagsRequest{},
			wantError: nil,
		},
		{
			name:      "invalid atespace",
			req:       &ateapipb.ListTagsRequest{Atespace: "NS1"},
			wantError: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "valid, positive page_size",
			req:       &ateapipb.ListTagsRequest{Atespace: "ns1", PageSize: 10},
			wantError: nil,
		},
		{
			name:      "negative page_size",
			req:       &ateapipb.ListTagsRequest{Atespace: "ns1", PageSize: -1},
			wantError: field.ErrorList{field.Invalid(field.NewPath("page_size"), nil, "").WithOrigin("minimum")},
		},
		{
			name:      "valid page_token",
			req:       &ateapipb.ListTagsRequest{Atespace: "ns1", PageToken: strings.Repeat("x", 256)},
			wantError: nil,
		},
		{
			name:      "too-large page_token",
			req:       &ateapipb.ListTagsRequest{Atespace: "ns1", PageToken: strings.Repeat("x", 257)},
			wantError: field.ErrorList{field.TooLongCharacters(field.NewPath("page_token"), "", 256).WithOrigin("maxLength")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateListTagsRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateGetTagRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapipb.GetTagRequest
		wantError field.ErrorList
	}{
		{
			name:      "valid",
			req:       &ateapipb.GetTagRequest{Tag: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tag1"}},
			wantError: nil,
		},
		{
			name:      "missing tag",
			req:       &ateapipb.GetTagRequest{},
			wantError: field.ErrorList{field.Required(field.NewPath("tag"), "")},
		},
		{
			name:      "missing tag.atespace",
			req:       &ateapipb.GetTagRequest{Tag: &ateapipb.ObjectRef{Name: "tag1"}},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "atespace"), "")},
		},
		{
			name:      "invalid tag.atespace",
			req:       &ateapipb.GetTagRequest{Tag: &ateapipb.ObjectRef{Atespace: "NS1", Name: "tag1"}},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "missing tag.name",
			req:       &ateapipb.GetTagRequest{Tag: &ateapipb.ObjectRef{Atespace: "ns1"}},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "name"), "")},
		},
		{
			name:      "invalid tag.name",
			req:       &ateapipb.GetTagRequest{Tag: &ateapipb.ObjectRef{Atespace: "ns1", Name: "TAG1"}},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateGetTagRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateDeleteTagRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapipb.DeleteTagRequest
		wantError field.ErrorList
	}{
		{
			name:      "valid",
			req:       &ateapipb.DeleteTagRequest{Tag: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tag1"}},
			wantError: nil,
		},
		{
			name:      "missing tag",
			req:       &ateapipb.DeleteTagRequest{},
			wantError: field.ErrorList{field.Required(field.NewPath("tag"), "")},
		},
		{
			name:      "missing tag.atespace",
			req:       &ateapipb.DeleteTagRequest{Tag: &ateapipb.ObjectRef{Name: "tag1"}},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "atespace"), "")},
		},
		{
			name:      "invalid tag.atespace",
			req:       &ateapipb.DeleteTagRequest{Tag: &ateapipb.ObjectRef{Atespace: "NS1", Name: "tag1"}},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "missing tag.name",
			req:       &ateapipb.DeleteTagRequest{Tag: &ateapipb.ObjectRef{Atespace: "ns1"}},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "name"), "")},
		},
		{
			name:      "invalid tag.name",
			req:       &ateapipb.DeleteTagRequest{Tag: &ateapipb.ObjectRef{Atespace: "ns1", Name: "TAG1"}},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateDeleteTagRequest(ctx, tt.req), tt.wantError)
		})
	}
}

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
	if code := status.Code(err); code != codes.InvalidArgument {
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
					Snapshot:         &ateapipb.ExternalSnapshot{SnapshotUri: "gs://attacker/elsewhere", ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA},
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
				if code := status.Code(err); code != tt.wantCode {
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
	if code := status.Code(err); code != codes.InvalidArgument {
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
	if code := status.Code(err); code != codes.InvalidArgument {
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
		status.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: uri.String(), ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL}
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
	return &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: resources.ActorRefFromActor(actor).ToObjectRef(),
		Status: &ateapipb.TagStatus{
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
func newPendingTestTag(t *testing.T, name string, actor *ateapipb.Actor) *ateapipb.Tag {
	t.Helper()
	tag := newTestTag(t, name, actor)
	tag.Status.InProgressSnapshotUri = tag.GetStatus().GetSnapshot().GetSnapshotUri()
	tag.Status.Snapshot = nil
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
			if _, err := persistence.DeleteTag(ctx, resources.TagRef{Atespace: testAtespace, Name: tagName}); err != nil {
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
	if code := status.Code(err); code != codes.Aborted {
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
	if code := status.Code(err); code != codes.Aborted {
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

// TestDeleteTag_ReleasesExternalSnapshot verifies the delete
// collects the external snapshot the tag owns before dropping the row that
// names it, and that a failure to collect leaves the row intact so a retry can
// finish the job.
func TestDeleteTag_ReleasesExternalSnapshot(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actor := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-1")
	tag := storetest.MustCreateTag(t, ctx, persistence, newTestTag(t, "v1", actor))
	tagRef := resources.TagRefFromTag(tag)

	objects := objectstoretest.New()
	uri, err := resources.ParseSnapshotURI(tag.GetStatus().GetSnapshot().GetSnapshotUri())
	if err != nil {
		t.Fatalf("ParseSnapshotURI: %v", err)
	}
	objects.PutSnapshot(t, uri, "manifest.json", "memory.zst")
	svc := &RPCService{impl: newServiceImpl(persistence, nil), objectStore: objects}

	// A delete that cannot reach object storage must not drop the row: it is
	// the only handle left on the snapshot.
	objects.OnDelete = func(string, string) error { return errObjectStore }
	req := &ateapipb.DeleteTagRequest{Tag: tagRef.ToObjectRef()}
	if _, err := svc.DeleteTag(ctx, req); !errors.Is(err, errObjectStore) {
		t.Fatalf("DeleteTag = %v, want an error wrapping %v", err, errObjectStore)
	}
	if _, err := persistence.GetTag(ctx, tagRef); err != nil {
		t.Fatalf("GetTag after the failure: %v", err)
	}

	// Simulates a retried deletion. Now, the object deletion succeeds,
	// so we can remove the row from the DB.
	objects.OnDelete = nil
	if _, err := svc.DeleteTag(ctx, req); err != nil {
		t.Fatalf("retried DeleteTag: %v", err)
	}
	if got := objects.Snapshot(t, uri); len(got) != 0 {
		t.Errorf("the tag's external snapshot still holds %v, want it collected", got)
	}
	if _, err := persistence.GetTag(ctx, tagRef); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetTag after the delete = %v, want ErrNotFound", err)
	}
}

// TestDeleteTag_ReleasesPendingSnapshot verifies that deleting a
// tag whose create never finished collects what that create stranded. The
// pending row names the prefix the copy was writing into, and it is the only
// handle left on those objects.
func TestDeleteTag_ReleasesPendingSnapshot(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actor := newTestSuspendedActor(t, ctx, persistence, testAtespace, "actor-1")
	tag := storetest.MustCreateTag(t, ctx, persistence, newPendingTestTag(t, "v1", actor))
	tagRef := resources.TagRefFromTag(tag)

	objects := objectstoretest.New()
	uri, err := resources.ParseSnapshotURI(tag.GetStatus().GetInProgressSnapshotUri())
	if err != nil {
		t.Fatalf("ParseSnapshotURI: %v", err)
	}
	// What a copy that died halfway through left behind.
	objects.PutSnapshot(t, uri, "manifest.json")
	svc := &RPCService{impl: newServiceImpl(persistence, nil), objectStore: objects}

	if _, err := svc.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: tagRef.ToObjectRef()}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	if got := objects.Snapshot(t, uri); len(got) != 0 {
		t.Errorf("the pending tag's stranded objects are still %v, want them collected", got)
	}
	if _, err := persistence.GetTag(ctx, tagRef); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetTag after the delete = %v, want ErrNotFound", err)
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
	if code := status.Code(err); code != codes.FailedPrecondition {
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
