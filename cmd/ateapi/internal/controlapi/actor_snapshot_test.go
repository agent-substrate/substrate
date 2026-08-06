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
	"testing"

	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"k8s.io/apimachinery/pkg/util/validation/field"

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
			// The zero value is ATESPACE, so leaving scope unset unpublishes the tag.
			name: "unset tag.scope",
			req: &ateapipb.UpdateActorSnapshotTagRequest{
				Tag: &ateapipb.ActorSnapshotTag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tag1"},
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
