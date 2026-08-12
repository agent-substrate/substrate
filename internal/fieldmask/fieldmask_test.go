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

package fieldmask_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/agent-substrate/substrate/internal/fieldmask"
	"github.com/agent-substrate/substrate/internal/fieldmask/fieldmasktestpb"
)

func TestApply(t *testing.T) {
	tests := []struct {
		name  string
		src   *fieldmasktestpb.TestMessage
		dst   *fieldmasktestpb.TestMessage
		paths []string
		want  *fieldmasktestpb.TestMessage
	}{
		{
			name:  "sets a plain scalar field",
			src:   &fieldmasktestpb.TestMessage{Name: "new"},
			dst:   &fieldmasktestpb.TestMessage{Name: "old"},
			paths: []string{"name"},
			want:  &fieldmasktestpb.TestMessage{Name: "new"},
		},
		{
			name:  "sets a plain scalar field to its zero value when unset on src",
			src:   &fieldmasktestpb.TestMessage{},
			dst:   &fieldmasktestpb.TestMessage{Count: 7},
			paths: []string{"count"},
			want:  &fieldmasktestpb.TestMessage{},
		},
		{
			name:  "sets an optional scalar field",
			src:   &fieldmasktestpb.TestMessage{OptionalName: proto.String("new")},
			dst:   &fieldmasktestpb.TestMessage{},
			paths: []string{"optional_name"},
			want:  &fieldmasktestpb.TestMessage{OptionalName: proto.String("new")},
		},
		{
			name:  "clears an optional scalar field left unset on src",
			src:   &fieldmasktestpb.TestMessage{},
			dst:   &fieldmasktestpb.TestMessage{OptionalCount: proto.Int32(7)},
			paths: []string{"optional_count"},
			want:  &fieldmasktestpb.TestMessage{},
		},
		{
			name:  "sets an optional scalar field explicitly to its zero value, staying present rather than clearing",
			src:   &fieldmasktestpb.TestMessage{OptionalCount: proto.Int32(0)},
			dst:   &fieldmasktestpb.TestMessage{OptionalCount: proto.Int32(7)},
			paths: []string{"optional_count"},
			want:  &fieldmasktestpb.TestMessage{OptionalCount: proto.Int32(0)},
		},
		{
			name:  "sets a bool field",
			src:   &fieldmasktestpb.TestMessage{Active: true},
			dst:   &fieldmasktestpb.TestMessage{},
			paths: []string{"active"},
			want:  &fieldmasktestpb.TestMessage{Active: true},
		},
		{
			name:  "sets a bytes field",
			src:   &fieldmasktestpb.TestMessage{Payload: []byte("data")},
			dst:   &fieldmasktestpb.TestMessage{},
			paths: []string{"payload"},
			want:  &fieldmasktestpb.TestMessage{Payload: []byte("data")},
		},
		{
			name:  "sets an enum field",
			src:   &fieldmasktestpb.TestMessage{Kind: fieldmasktestpb.TestEnum_TEST_ENUM_SECOND},
			dst:   &fieldmasktestpb.TestMessage{Kind: fieldmasktestpb.TestEnum_TEST_ENUM_FIRST},
			paths: []string{"kind"},
			want:  &fieldmasktestpb.TestMessage{Kind: fieldmasktestpb.TestEnum_TEST_ENUM_SECOND},
		},
		{
			name:  "overwrites a masked nested message field as a whole",
			src:   &fieldmasktestpb.TestMessage{Nested: &fieldmasktestpb.Nested{Value: "new"}},
			dst:   &fieldmasktestpb.TestMessage{Nested: &fieldmasktestpb.Nested{Value: "old", Inner: &fieldmasktestpb.Inner{Number: 1}}},
			paths: []string{"nested"},
			want:  &fieldmasktestpb.TestMessage{Nested: &fieldmasktestpb.Nested{Value: "new"}},
		},
		{
			name:  "clears a masked message field left unset on src",
			src:   &fieldmasktestpb.TestMessage{},
			dst:   &fieldmasktestpb.TestMessage{Nested: &fieldmasktestpb.Nested{Value: "old"}},
			paths: []string{"nested"},
			want:  &fieldmasktestpb.TestMessage{},
		},
		{
			name: "initializes every nil intermediate message, two levels deep, to set a leaf field",
			src: &fieldmasktestpb.TestMessage{
				Nested: &fieldmasktestpb.Nested{Inner: &fieldmasktestpb.Inner{Number: 42}},
			},
			dst:   &fieldmasktestpb.TestMessage{},
			paths: []string{"nested.inner.number"},
			want: &fieldmasktestpb.TestMessage{
				Nested: &fieldmasktestpb.Nested{Inner: &fieldmasktestpb.Inner{Number: 42}},
			},
		},
		{
			name: "clears a leaf field of a nested message left unset on src, sibling untouched",
			src: &fieldmasktestpb.TestMessage{
				Nested: &fieldmasktestpb.Nested{Value: "keep", Inner: &fieldmasktestpb.Inner{}},
			},
			dst: &fieldmasktestpb.TestMessage{
				Nested: &fieldmasktestpb.Nested{Value: "keep", Inner: &fieldmasktestpb.Inner{Number: 42}},
			},
			paths: []string{"nested.inner.number"},
			want: &fieldmasktestpb.TestMessage{
				Nested: &fieldmasktestpb.Nested{Value: "keep", Inner: &fieldmasktestpb.Inner{}},
			},
		},
		{
			name: "sets a leaf field of a nested message, sibling untouched",
			src: &fieldmasktestpb.TestMessage{
				Nested: &fieldmasktestpb.Nested{Value: "ignored", Inner: &fieldmasktestpb.Inner{Number: 42}},
			},
			dst: &fieldmasktestpb.TestMessage{
				Nested: &fieldmasktestpb.Nested{Value: "keep", Inner: &fieldmasktestpb.Inner{Number: 1}},
			},
			paths: []string{"nested.inner.number"},
			want: &fieldmasktestpb.TestMessage{
				Nested: &fieldmasktestpb.Nested{Value: "keep", Inner: &fieldmasktestpb.Inner{Number: 42}},
			},
		},
		{
			name:  "replaces a repeated scalar field as a whole",
			src:   &fieldmasktestpb.TestMessage{Tags: []string{"a", "b"}},
			dst:   &fieldmasktestpb.TestMessage{Tags: []string{"old"}},
			paths: []string{"tags"},
			want:  &fieldmasktestpb.TestMessage{Tags: []string{"a", "b"}},
		},
		{
			name:  "clears a repeated scalar field left empty on src",
			src:   &fieldmasktestpb.TestMessage{},
			dst:   &fieldmasktestpb.TestMessage{Tags: []string{"old"}},
			paths: []string{"tags"},
			want:  &fieldmasktestpb.TestMessage{},
		},
		{
			name: "replaces a repeated message field as a whole",
			src: &fieldmasktestpb.TestMessage{
				Items: []*fieldmasktestpb.Nested{{Value: "new"}},
			},
			dst: &fieldmasktestpb.TestMessage{
				Items: []*fieldmasktestpb.Nested{{Value: "old-1"}, {Value: "old-2"}},
			},
			paths: []string{"items"},
			want: &fieldmasktestpb.TestMessage{
				Items: []*fieldmasktestpb.Nested{{Value: "new"}},
			},
		},
		{
			name:  "replaces a string map field as a whole",
			src:   &fieldmasktestpb.TestMessage{Labels: map[string]string{"a": "1"}},
			dst:   &fieldmasktestpb.TestMessage{Labels: map[string]string{"b": "2"}},
			paths: []string{"labels"},
			want:  &fieldmasktestpb.TestMessage{Labels: map[string]string{"a": "1"}},
		},
		{
			name:  "clears a map field left empty on src",
			src:   &fieldmasktestpb.TestMessage{},
			dst:   &fieldmasktestpb.TestMessage{Labels: map[string]string{"b": "2"}},
			paths: []string{"labels"},
			want:  &fieldmasktestpb.TestMessage{},
		},
		{
			name: "replaces a message-valued map field as a whole",
			src: &fieldmasktestpb.TestMessage{
				NamedItems: map[string]*fieldmasktestpb.Nested{"a": {Value: "new"}},
			},
			dst: &fieldmasktestpb.TestMessage{
				NamedItems: map[string]*fieldmasktestpb.Nested{"b": {Value: "old"}},
			},
			paths: []string{"named_items"},
			want: &fieldmasktestpb.TestMessage{
				NamedItems: map[string]*fieldmasktestpb.Nested{"a": {Value: "new"}},
			},
		},
		{
			name: "sets one member of a oneof, clearing the previously-set sibling member",
			src: &fieldmasktestpb.TestMessage{
				Choice: &fieldmasktestpb.TestMessage_ChoiceText{ChoiceText: "new"},
			},
			dst: &fieldmasktestpb.TestMessage{
				Choice: &fieldmasktestpb.TestMessage_ChoiceNested{ChoiceNested: &fieldmasktestpb.Nested{Value: "old"}},
			},
			paths: []string{"choice_text"},
			want: &fieldmasktestpb.TestMessage{
				Choice: &fieldmasktestpb.TestMessage_ChoiceText{ChoiceText: "new"},
			},
		},
		{
			name: "clears a oneof member left unset on src",
			src:  &fieldmasktestpb.TestMessage{},
			dst: &fieldmasktestpb.TestMessage{
				Choice: &fieldmasktestpb.TestMessage_ChoiceText{ChoiceText: "old"},
			},
			paths: []string{"choice_text"},
			want:  &fieldmasktestpb.TestMessage{},
		},
		{
			name: "ignores fields set on src but absent from the mask",
			src: &fieldmasktestpb.TestMessage{
				Name:  "new",
				Count: 99,
			},
			dst: &fieldmasktestpb.TestMessage{
				Name:  "old",
				Count: 1,
			},
			paths: []string{"name"},
			want: &fieldmasktestpb.TestMessage{
				Name:  "new",
				Count: 1,
			},
		},
		{
			name: "applies every masked field",
			src: &fieldmasktestpb.TestMessage{
				Name:  "new",
				Count: 99,
			},
			dst:   &fieldmasktestpb.TestMessage{},
			paths: []string{"name", "count"},
			want: &fieldmasktestpb.TestMessage{
				Name:  "new",
				Count: 99,
			},
		},
		{
			name:  "skips a path that names no real field",
			src:   &fieldmasktestpb.TestMessage{Name: "ignored"},
			dst:   &fieldmasktestpb.TestMessage{},
			paths: []string{"totally-unknown-field"},
			want:  &fieldmasktestpb.TestMessage{},
		},
		{
			name: "leaves dst untouched for a nil mask",
			src:  &fieldmasktestpb.TestMessage{Name: "new"},
			dst:  &fieldmasktestpb.TestMessage{Name: "old"},
			want: &fieldmasktestpb.TestMessage{Name: "old"},
		},
		{
			name:  "applies a repeated path once per occurrence",
			src:   &fieldmasktestpb.TestMessage{Name: "new"},
			dst:   &fieldmasktestpb.TestMessage{},
			paths: []string{"name", "name"},
			want:  &fieldmasktestpb.TestMessage{Name: "new"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mask *fieldmaskpb.FieldMask
			if tt.paths != nil {
				mask = &fieldmaskpb.FieldMask{Paths: tt.paths}
			}
			// Apply writes to dst in place, so we make a copy of the input
			// to check for unwanted mutations to src.
			src := proto.Clone(tt.src)

			fieldmask.Apply(tt.dst, tt.src, mask)
			if diff := cmp.Diff(tt.want, tt.dst, protocmp.Transform()); diff != "" {
				t.Errorf("Apply(%v, %v, %v) dst mismatch (-want +got):\n%s", tt.dst, tt.src, tt.paths, diff)
			}
			if diff := cmp.Diff(src, tt.src, protocmp.Transform()); diff != "" {
				t.Errorf("Apply(%v, %v, %v) mutated src (-want +got):\n%s", tt.dst, tt.src, tt.paths, diff)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	fieldPath := field.NewPath("update_mask")

	tests := []struct {
		name   string
		fields fieldmask.MutableFields
		mask   *fieldmaskpb.FieldMask
		want   field.ErrorList
	}{
		{
			name:   "nil mask",
			fields: fieldmask.NewMutableFields("name"),
			want:   field.ErrorList{field.Required(fieldPath, "")},
		},
		{
			name:   "empty mask",
			fields: fieldmask.NewMutableFields("name"),
			mask:   &fieldmaskpb.FieldMask{},
			want:   field.ErrorList{field.Required(fieldPath, "")},
		},
		{
			name:   "single mutable path",
			fields: fieldmask.NewMutableFields("name", "nested"),
			mask:   &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		},
		{
			name:   "every mutable top-level path",
			fields: fieldmask.NewMutableFields("name", "nested"),
			mask:   &fieldmaskpb.FieldMask{Paths: []string{"name", "nested"}},
		},
		{
			name:   "wildcard",
			fields: fieldmask.NewMutableFields("name"),
			mask:   &fieldmaskpb.FieldMask{Paths: []string{"*"}},
			want:   field.ErrorList{field.NotSupported(fieldPath, "*", []string{"name"})},
		},
		{
			name:   "path outside the mutable set",
			fields: fieldmask.NewMutableFields("name"),
			mask:   &fieldmaskpb.FieldMask{Paths: []string{"count"}},
			want:   field.ErrorList{field.NotSupported(fieldPath, "count", []string{"name"})},
		},
		{
			name:   "message-typed field mutable as a whole does not grant its own subfields",
			fields: fieldmask.NewMutableFields("nested"),
			mask:   &fieldmaskpb.FieldMask{Paths: []string{"nested.value"}},
			want:   field.ErrorList{field.NotSupported(fieldPath, "nested.value", []string{"nested"})},
		},
		{
			name:   "exactly-listed nested leaf path is allowed even though its container is not listed",
			fields: fieldmask.NewMutableFields("nested.inner.number"),
			mask:   &fieldmaskpb.FieldMask{Paths: []string{"nested.inner.number"}},
		},
		{
			name:   "sibling leaf path not exactly listed",
			fields: fieldmask.NewMutableFields("nested.inner.number"),
			mask:   &fieldmaskpb.FieldMask{Paths: []string{"nested.value"}},
			want:   field.ErrorList{field.NotSupported(fieldPath, "nested.value", []string{"nested.inner.number"})},
		},
		{
			name:   "empty path",
			fields: fieldmask.NewMutableFields("name"),
			mask:   &fieldmaskpb.FieldMask{Paths: []string{""}},
			want:   field.ErrorList{field.NotSupported(fieldPath, "", []string{"name"})},
		},
		{
			name:   "reports every unsupported path",
			fields: fieldmask.NewMutableFields("name"),
			mask:   &fieldmaskpb.FieldMask{Paths: []string{"count", "name", "unknown"}},
			want: field.ErrorList{
				field.NotSupported(fieldPath, "count", []string{"name"}),
				field.NotSupported(fieldPath, "unknown", []string{"name"}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, fieldmask.Validate(tt.mask, tt.fields, fieldPath), tt.want)
		})
	}
}

func assertValidateErr(t *testing.T, got field.ErrorList, want field.ErrorList) {
	t.Helper()
	field.ErrorMatcher{}.ByType().ByField().ByValue().Test(t, want, got)
}
