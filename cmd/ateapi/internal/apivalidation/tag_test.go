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

package apivalidation

import (
	"context"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateCreateTagRequest(t *testing.T) {
	ctx := context.Background()
	validTag := func(opts ...func(*ateapi.Tag)) *ateapi.Tag {
		tag := &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: "ns1", Name: "id1"},
		}
		for _, opt := range opts {
			opt(tag)
		}
		return tag
	}
	tests := []struct {
		name      string
		req       *ateapi.CreateTagRequest
		wantError field.ErrorList
	}{
		{
			name:      "valid",
			req:       &ateapi.CreateTagRequest{Tag: validTag()},
			wantError: nil,
		},
		{
			// Status is server-owned and scrubbed before this runs, but a
			// client that echoes back a tag it read is not to be tripped up by
			// one either.
			name: "valid with status",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) {
					tag.Status = &ateapi.TagStatus{
						Snapshot:         validSnapshot(),
						ActorTemplateUid: someActorUID,
					}
				}),
			},
			wantError: nil,
		},
		{
			// The tag has to name the atespace it lands in, like every other
			// create: the source Actor's atespace is checked against it, not
			// used as a default.
			name: "empty tag.metadata.atespace",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Atespace = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata", "atespace"), "")},
		},
		{
			// Malformed, so it is both rejected on its own terms and unable to
			// match the source actor's.
			name: "invalid tag.metadata.atespace",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Atespace = "NS1" }),
			},
			wantError: field.ErrorList{
				field.Invalid(field.NewPath("tag", "metadata", "atespace"), nil, ""),
				field.Invalid(field.NewPath("tag", "metadata", "atespace"), nil, "").WithOrigin("format=k8s-short-name"),
			},
		},
		{
			name: "missing tag.source_actor",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.SourceActor = nil }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "source_actor"), "")},
		},
		{
			// Only source_actor is reported: tag.metadata.atespace cannot be
			// held against a source that has none to compare it to.
			name: "missing tag.source_actor.atespace",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.SourceActor.Atespace = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "source_actor", "atespace"), "")},
		},
		{
			// A malformed source atespace is also one the tag's own atespace
			// cannot match, so both are reported.
			name: "invalid tag.source_actor.atespace",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.SourceActor.Atespace = "NS1" }),
			},
			wantError: field.ErrorList{
				field.Invalid(field.NewPath("tag", "metadata", "atespace"), nil, ""),
				field.Invalid(field.NewPath("tag", "source_actor", "atespace"), nil, "").WithOrigin("format=k8s-short-name"),
			},
		},
		{
			name: "missing tag.source_actor.name",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.SourceActor.Name = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "source_actor", "name"), "")},
		},
		{
			name: "invalid tag.source_actor.name",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.SourceActor.Name = "ID1" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "source_actor", "name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "missing tag",
			req:       &ateapi.CreateTagRequest{},
			wantError: field.ErrorList{field.Required(field.NewPath("tag"), "")},
		},
		{
			name: "missing tag.metadata",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata = nil }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata"), "")},
		},
		{
			name: "missing tag.metadata.name",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Name = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata", "name"), "")},
		},
		{
			name: "invalid tag.metadata.name",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Name = "TAG1" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			// A tag somewhere else than its source Actor could not be resolved
			// back to it.
			name: "tag.metadata.atespace is not the source actor's",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Atespace = "ns2" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "atespace"), nil, "")},
		},
		{
			name: "unset tag.scope",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) {
					tag.Scope = ateapi.TagScope_TAG_SCOPE_UNSPECIFIED
				}),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "scope"), "")},
		},
		{
			name: "tag.scope above the enum",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Scope = ateapi.TagScope(7) }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "scope"), nil, "").WithOrigin("maximum")},
		},
		{
			name: "negative tag.scope",
			req: &ateapi.CreateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Scope = ateapi.TagScope(-1) }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "scope"), nil, "").WithOrigin("minimum")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateCreateTagRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateUpdateTagRequest(t *testing.T) {
	ctx := context.Background()
	// validUID is a well-formed uid to pass validation.
	const validUID = "2a5f8c1e-9b3d-4f7a-8e6c-1d0b4a7f2e93"
	validTag := func(opts ...func(*ateapi.Tag)) *ateapi.Tag {
		tag := &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: validUID, Version: 7},
			Scope:       ateapi.TagScope_TAG_SCOPE_PUBLISHED,
			SourceActor: &ateapi.ObjectRef{Atespace: "ns1", Name: "id1"},
		}
		for _, opt := range opts {
			opt(tag)
		}
		return tag
	}
	tests := []struct {
		name      string
		req       *ateapi.UpdateTagRequest
		wantError field.ErrorList
	}{
		{
			name:      "valid",
			req:       &ateapi.UpdateTagRequest{Tag: validTag()},
			wantError: nil,
		},
		{
			name:      "missing tag",
			req:       &ateapi.UpdateTagRequest{},
			wantError: field.ErrorList{field.Required(field.NewPath("tag"), "")},
		},
		{
			name: "missing tag.metadata",
			req: &ateapi.UpdateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata = nil }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata"), "")},
		},
		{
			name: "missing tag.metadata.atespace",
			req: &ateapi.UpdateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Atespace = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata", "atespace"), "")},
		},
		{
			name: "invalid tag.metadata.atespace",
			req: &ateapi.UpdateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Atespace = "NS1" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name: "missing tag.metadata.name",
			req: &ateapi.UpdateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Name = "" }),
			},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "metadata", "name"), "")},
		},
		{
			name: "invalid tag.metadata.name",
			req: &ateapi.UpdateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Name = "TAG1" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			// The preconditions are the store's to insist on, not the schema's:
			// an update carrying neither is rejected as a blind write once it
			// reaches the store. See TestUpdateTag_BlindWrite.
			name: "missing tag.metadata.uid precondition",
			req: &ateapi.UpdateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Uid = "" }),
			},
			wantError: nil,
		},
		{
			name: "invalid tag.metadata.uid precondition",
			req: &ateapi.UpdateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Uid = "not-a-uuid" }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "uid"), nil, "").WithOrigin("format=k8s-uuid")},
		},
		{
			name: "missing tag.metadata.version precondition",
			req: &ateapi.UpdateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Version = 0 }),
			},
			wantError: nil,
		},
		{
			name: "negative tag.metadata.version precondition",
			req: &ateapi.UpdateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) { tag.Metadata.Version = -1 }),
			},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "metadata", "version"), nil, "").WithOrigin("minimum")},
		},
		{
			// Nothing about the tag body is held against the request: it is
			// checked against the stored tag instead, so a source or a scope
			// the request gets wrong is caught there.
			name: "tag body is not checked against the request",
			req: &ateapi.UpdateTagRequest{
				Tag: validTag(func(tag *ateapi.Tag) {
					tag.SourceActor = nil
					tag.Scope = ateapi.TagScope_TAG_SCOPE_UNSPECIFIED
				}),
			},
			wantError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateUpdateTagRequest(ctx, tt.req), tt.wantError)
		})
	}
}

// TestValidateTagUpdateResult covers the second half of an update:
// the merged tag the server is about to write, checked against the one it
// replaces. This is where everything the request could not be held to lands.
func TestValidateTagUpdateResult(t *testing.T) {
	ctx := context.Background()
	tagPath := field.NewPath("tag")
	storedTag := func(opts ...func(*ateapi.Tag)) *ateapi.Tag {
		tag := &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: "ns1", Name: "tag1", Uid: "2a5f8c1e-9b3d-4f7a-8e6c-1d0b4a7f2e93", Version: 7},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: "ns1", Name: "id1"},
		}
		for _, opt := range opts {
			opt(tag)
		}
		return tag
	}
	tests := []struct {
		name      string
		newVal    *ateapi.Tag
		wantError field.ErrorList
	}{
		{
			name:      "unchanged",
			newVal:    storedTag(),
			wantError: nil,
		},
		{
			name: "publishing is allowed",
			newVal: storedTag(func(tag *ateapi.Tag) {
				tag.Scope = ateapi.TagScope_TAG_SCOPE_PUBLISHED
			}),
			wantError: nil,
		},
		{
			name: "unset tag.scope",
			newVal: storedTag(func(tag *ateapi.Tag) {
				tag.Scope = ateapi.TagScope_TAG_SCOPE_UNSPECIFIED
			}),
			wantError: field.ErrorList{field.Required(tagPath.Child("scope"), "")},
		},
		{
			name:      "tag.scope above the enum",
			newVal:    storedTag(func(tag *ateapi.Tag) { tag.Scope = ateapi.TagScope(7) }),
			wantError: field.ErrorList{field.Invalid(tagPath.Child("scope"), nil, "").WithOrigin("maximum")},
		},
		{
			name:      "negative tag.scope",
			newVal:    storedTag(func(tag *ateapi.Tag) { tag.Scope = ateapi.TagScope(-1) }),
			wantError: field.ErrorList{field.Invalid(tagPath.Child("scope"), nil, "").WithOrigin("minimum")},
		},
		{
			// A tag never moves between snapshots, so it never moves between
			// sources either.
			name: "repointed tag.source_actor",
			newVal: storedTag(func(tag *ateapi.Tag) {
				tag.SourceActor = &ateapi.ObjectRef{Atespace: "ns1", Name: "id2"}
			}),
			wantError: field.ErrorList{field.Invalid(tagPath.Child("source_actor"), nil, "").WithOrigin("immutable")},
		},
		{
			// An update is a whole-object replace, so a client that drops the
			// source it read back would clear it.
			name:   "missing tag.source_actor",
			newVal: storedTag(func(tag *ateapi.Tag) { tag.SourceActor = nil }),
			wantError: field.ErrorList{
				field.Invalid(tagPath.Child("source_actor"), nil, "").WithOrigin("immutable"),
				field.Required(tagPath.Child("source_actor"), ""),
			},
		},
		{
			// The tag is addressed through its atespace and name; moving it
			// would strand every reference to it.
			name:      "renamed tag",
			newVal:    storedTag(func(tag *ateapi.Tag) { tag.Metadata.Name = "tag2" }),
			wantError: field.ErrorList{field.Invalid(tagPath.Child("metadata", "name"), nil, "").WithOrigin("immutable")},
		},
		{
			name:      "tag moved to another atespace",
			newVal:    storedTag(func(tag *ateapi.Tag) { tag.Metadata.Atespace = "ns2" }),
			wantError: field.ErrorList{field.Invalid(tagPath.Child("metadata", "atespace"), nil, "").WithOrigin("immutable")},
		},
		{
			name:   "cleared tag.metadata.atespace",
			newVal: storedTag(func(tag *ateapi.Tag) { tag.Metadata.Atespace = "" }),
			wantError: field.ErrorList{
				field.Required(tagPath.Child("metadata", "atespace"), ""),
				field.Invalid(tagPath.Child("metadata", "atespace"), nil, "").WithOrigin("immutable"),
			},
		},
		{
			name:      "missing tag.metadata",
			newVal:    storedTag(func(tag *ateapi.Tag) { tag.Metadata = nil }),
			wantError: field.ErrorList{field.Required(tagPath.Child("metadata"), "")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateTagUpdate(ctx, tagPath, tt.newVal, storedTag()), tt.wantError)
		})
	}
}

func TestValidateListTagsRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapi.ListTagsRequest
		wantError field.ErrorList
	}{
		{
			name:      "valid, atespace scoped",
			req:       &ateapi.ListTagsRequest{Atespace: "ns1"},
			wantError: nil,
		},
		{
			// Empty atespace means "all atespaces"
			// (kubectl ate get tags -A).
			name:      "valid, empty atespace means all atespaces",
			req:       &ateapi.ListTagsRequest{},
			wantError: nil,
		},
		{
			name:      "invalid atespace",
			req:       &ateapi.ListTagsRequest{Atespace: "NS1"},
			wantError: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "valid, positive page_size",
			req:       &ateapi.ListTagsRequest{Atespace: "ns1", Options: &ateapi.ListOptions{PageSize: 10}},
			wantError: nil,
		},
		{
			name:      "negative page_size",
			req:       &ateapi.ListTagsRequest{Atespace: "ns1", Options: &ateapi.ListOptions{PageSize: -1}},
			wantError: field.ErrorList{field.Invalid(field.NewPath("options", "page_size"), nil, "").WithOrigin("minimum")},
		},
		{
			name:      "valid page_token",
			req:       &ateapi.ListTagsRequest{Atespace: "ns1", Options: &ateapi.ListOptions{PageToken: strings.Repeat("x", 256)}},
			wantError: nil,
		},
		{
			name:      "too-large page_token",
			req:       &ateapi.ListTagsRequest{Atespace: "ns1", Options: &ateapi.ListOptions{PageToken: strings.Repeat("x", 257)}},
			wantError: field.ErrorList{field.TooLongCharacters(field.NewPath("options", "page_token"), "", 256).WithOrigin("maxLength")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateListTagsRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateGetTagRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapi.GetTagRequest
		wantError field.ErrorList
	}{
		{
			name:      "valid",
			req:       &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Atespace: "ns1", Name: "tag1"}},
			wantError: nil,
		},
		{
			name:      "missing tag",
			req:       &ateapi.GetTagRequest{},
			wantError: field.ErrorList{field.Required(field.NewPath("tag"), "")},
		},
		{
			name:      "missing tag.atespace",
			req:       &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Name: "tag1"}},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "atespace"), "")},
		},
		{
			name:      "invalid tag.atespace",
			req:       &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Atespace: "NS1", Name: "tag1"}},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "missing tag.name",
			req:       &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Atespace: "ns1"}},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "name"), "")},
		},
		{
			name:      "invalid tag.name",
			req:       &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Atespace: "ns1", Name: "TAG1"}},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateGetTagRequest(ctx, tt.req), tt.wantError)
		})
	}
}

func TestValidateDeleteTagRequest(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		req       *ateapi.DeleteTagRequest
		wantError field.ErrorList
	}{
		{
			name:      "valid",
			req:       &ateapi.DeleteTagRequest{Tag: &ateapi.ObjectRef{Atespace: "ns1", Name: "tag1"}},
			wantError: nil,
		},
		{
			name:      "missing tag",
			req:       &ateapi.DeleteTagRequest{},
			wantError: field.ErrorList{field.Required(field.NewPath("tag"), "")},
		},
		{
			name:      "missing tag.atespace",
			req:       &ateapi.DeleteTagRequest{Tag: &ateapi.ObjectRef{Name: "tag1"}},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "atespace"), "")},
		},
		{
			name:      "invalid tag.atespace",
			req:       &ateapi.DeleteTagRequest{Tag: &ateapi.ObjectRef{Atespace: "NS1", Name: "tag1"}},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name:      "missing tag.name",
			req:       &ateapi.DeleteTagRequest{Tag: &ateapi.ObjectRef{Atespace: "ns1"}},
			wantError: field.ErrorList{field.Required(field.NewPath("tag", "name"), "")},
		},
		{
			name:      "invalid tag.name",
			req:       &ateapi.DeleteTagRequest{Tag: &ateapi.ObjectRef{Atespace: "ns1", Name: "TAG1"}},
			wantError: field.ErrorList{field.Invalid(field.NewPath("tag", "name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateDeleteTagRequest(ctx, tt.req), tt.wantError)
		})
	}
}
