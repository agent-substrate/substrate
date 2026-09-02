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

package validation

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/validation/field"
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
			assertValidateErr(t, ValidateUpdateActorSnapshotTagRequest(tt.req), tt.wantError)
		})
	}
}
