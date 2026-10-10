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

package lint

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

func TestExpectFields(t *testing.T) {
	ref := model.Field{Name: "gadget", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}
	name := model.Field{Name: "name", TypeDisplay: "string"}
	opts := model.Field{Name: "options", TypeKind: "message", TypeFullName: "ateapi.DeleteOptions"}
	specs := [][]fieldSpec{
		required("gadget", message("ateapi.ObjectRef")),
		required("name", scalar("string")),
		optionalOfType(message("ateapi.DeleteOptions")),
	}

	tests := []struct {
		name   string
		fields []model.Field
		want   []Finding
	}{
		{
			name:   "exactly the expected fields",
			fields: []model.Field{ref, name, opts},
		},
		{
			name:   "optional field absent",
			fields: []model.Field{ref, name},
		},
		{
			name:   "required field missing",
			fields: []model.Field{ref},
			want: []Finding{
				{Subject: "s", Message: `request has no "name" string field`},
			},
		},
		{
			name:   "named field of the wrong type",
			fields: []model.Field{ref, {Name: "name", TypeDisplay: "int64"}},
			want: []Finding{
				{Subject: "s", Message: `request field "name" is int64, want string`},
			},
		},
		{
			name:   "field of a required type, misnamed",
			fields: []model.Field{{Name: "parent", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}, name},
			want: []Finding{
				{Subject: "s", Message: `request field "parent" is ateapi.ObjectRef, want it named "gadget"`},
			},
		},
		{
			name:   "field of a filled named spec's type is stray",
			fields: []model.Field{ref, name, {Name: "other", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}},
			want: []Finding{
				{Subject: "s", Message: `request field "other" is not allowed - want only "gadget" ateapi.ObjectRef, "name" string, and optional ateapi.DeleteOptions`},
			},
		},
		{
			name:   "stray field",
			fields: []model.Field{ref, name, {Name: "dry_run", TypeDisplay: "bool"}},
			want: []Finding{
				{Subject: "s", Message: `request field "dry_run" is not allowed - want only "gadget" ateapi.ObjectRef, "name" string, and optional ateapi.DeleteOptions`},
			},
		},
		{
			name:   "optional field of a type twice",
			fields: []model.Field{ref, name, opts, opts},
			want: []Finding{
				{Subject: "s", Message: `request has 2 ateapi.DeleteOptions fields, want at most 1`},
			},
		},
		{
			name:   "named field twice",
			fields: []model.Field{ref, ref, name},
			want: []Finding{
				{Subject: "s", Message: `request has 2 "gadget" ateapi.ObjectRef fields, want at most 1`},
			},
		},
		{
			name:   "repeated field where a singular message is expected",
			fields: []model.Field{{Name: "gadget", Repeated: true, TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}, name},
			want: []Finding{
				{Subject: "s", Message: `request field "gadget" is repeated ateapi.ObjectRef, want ateapi.ObjectRef`},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := expectFields("s", "request", model.Message{Fields: tt.fields}, specs...)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("expectFields() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
