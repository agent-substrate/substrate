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

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// testResource stands in for a generic message with two mutable fields and one
// the client may not touch.
type testResource struct {
	Mutable   string
	Other     string
	Immutable string
}

var resourceMutableFields = mutableFields[*testResource]{
	"mutable": func(dst, src *testResource) { dst.Mutable = src.Mutable },
	"other":   func(dst, src *testResource) { dst.Other = src.Other },
}

func TestApplyUpdateMask(t *testing.T) {
	tests := []struct {
		name  string
		src   *testResource
		dst   *testResource
		paths []string
		want  *testResource
	}{
		{
			name:  "sets a masked field",
			src:   &testResource{Mutable: "new"},
			dst:   &testResource{},
			paths: []string{"mutable"},
			want:  &testResource{Mutable: "new"},
		},
		{
			name:  "overwrites a masked field",
			src:   &testResource{Mutable: "new"},
			dst:   &testResource{Mutable: "old"},
			paths: []string{"mutable"},
			want:  &testResource{Mutable: "new"},
		},
		{
			name:  "clears a masked field left unset on src",
			src:   &testResource{},
			dst:   &testResource{Mutable: "old"},
			paths: []string{"mutable"},
			want:  &testResource{},
		},
		{
			name:  "ignores fields set on src but absent from the mask",
			src:   &testResource{Mutable: "new", Other: "ignored"},
			dst:   &testResource{Mutable: "old", Other: "keep"},
			paths: []string{"mutable"},
			want:  &testResource{Mutable: "new", Other: "keep"},
		},
		{
			name:  "applies every masked field",
			src:   &testResource{Mutable: "new-mutable", Other: "new-other"},
			dst:   &testResource{Mutable: "old", Other: "old"},
			paths: []string{"mutable", "other"},
			want:  &testResource{Mutable: "new-mutable", Other: "new-other"},
		},
		{
			name:  "skips a path outside the mutable set",
			src:   &testResource{Immutable: "ignored"},
			dst:   &testResource{Immutable: "keep"},
			paths: []string{"immutable-path-is-ignored"},
			want:  &testResource{Immutable: "keep"},
		},
		{
			name: "leaves dst untouched for a nil mask",
			src:  &testResource{Mutable: "new"},
			dst:  &testResource{Mutable: "old"},
			want: &testResource{Mutable: "old"},
		},
		{
			name:  "applies a repeated path once per occurrence",
			src:   &testResource{Mutable: "new"},
			dst:   &testResource{},
			paths: []string{"mutable", "mutable"},
			want:  &testResource{Mutable: "new"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mask *fieldmaskpb.FieldMask
			if tt.paths != nil {
				mask = &fieldmaskpb.FieldMask{Paths: tt.paths}
			}
			// applyUpdateMask writes to dst in place, so we make a copy
			// of the inputs to check for unwanted mutations to src.
			src := *tt.src

			applyUpdateMask(tt.dst, tt.src, mask, resourceMutableFields)
			if diff := cmp.Diff(tt.want, tt.dst); diff != "" {
				t.Errorf("applyUpdateMask(%+v, %+v, %v) dst mismatch (-want +got):\n%s", *tt.dst, src, tt.paths, diff)
			}
			if diff := cmp.Diff(&src, tt.src); diff != "" {
				t.Errorf("applyUpdateMask(%+v, %+v, %v) mutated src (-want +got):\n%s", *tt.dst, src, tt.paths, diff)
			}
		})
	}
}

func TestValidateUpdateMask(t *testing.T) {
	// Errors are always reported against the request's update_mask field.
	fieldPath := field.NewPath("update_mask")
	supported := []string{"mutable", "other"}

	tests := []struct {
		name string
		mask *fieldmaskpb.FieldMask
		want field.ErrorList
	}{
		{
			name: "nil mask",
			want: field.ErrorList{field.Required(fieldPath, "")},
		},
		{
			name: "empty mask",
			mask: &fieldmaskpb.FieldMask{},
			want: field.ErrorList{field.Required(fieldPath, "")},
		},
		{
			name: "single mutable path",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"mutable"}},
		},
		{
			name: "every mutable path",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"mutable", "other"}},
		},
		{
			name: "wildcard",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"*"}},
			want: field.ErrorList{field.NotSupported(fieldPath, "*", supported)},
		},
		{
			name: "path outside the mutable set",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"immutable"}},
			want: field.ErrorList{field.NotSupported(fieldPath, "immutable", supported)},
		},
		{
			name: "nested path under a mutable field",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"mutable.nested"}},
			want: field.ErrorList{field.NotSupported(fieldPath, "mutable.nested", supported)},
		},
		{
			name: "empty path",
			mask: &fieldmaskpb.FieldMask{Paths: []string{""}},
			want: field.ErrorList{field.NotSupported(fieldPath, "", supported)},
		},
		{
			name: "reports every unsupported path",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"immutable", "mutable", "unknown"}},
			want: field.ErrorList{
				field.NotSupported(fieldPath, "immutable", supported),
				field.NotSupported(fieldPath, "unknown", supported),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateMask(tt.mask, resourceMutableFields), tt.want)
		})
	}
}
