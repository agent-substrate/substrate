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
	"math"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validResourceMetadata(mutate ...func(*ateapipb.ResourceMetadata)) *ateapipb.ResourceMetadata {
	// This is valid with as many fields populated as possible.
	rm := &ateapipb.ResourceMetadata{
		Atespace:   "as",
		Name:       "nm",
		Uid:        "01234567-89ab-cdef-0123-456789abcdef",
		Version:    93,
		CreateTime: &timestamppb.Timestamp{Seconds: 867},
		UpdateTime: &timestamppb.Timestamp{Seconds: 5309},
	}
	for _, m := range mutate {
		m(rm)
	}
	return rm
}

func TestValidateResourceMetadataCreate(t *testing.T) {
	valid := validResourceMetadata

	// Focus this test on fields other than atespace and name.
	tests := []struct {
		name string
		obj  *ateapipb.ResourceMetadata
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid atespace: empty",
		obj:  valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "" }),
	}, {
		name: "missing name",
		obj:  valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		name: "unspecified uid",
		obj:  valid(func(rm *ateapipb.ResourceMetadata) { rm.Uid = "" }),
		want: nil,
	}, {
		name: "invalid uid: close but not valid",
		obj:  valid(func(rm *ateapipb.ResourceMetadata) { rm.Uid = "aaaaaaaa-bbbbcccc-dddd-eeeeeeeeeeee" }),
		want: field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid uid: not even close",
		obj:  valid(func(rm *ateapipb.ResourceMetadata) { rm.Uid = "not a uid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "unspecified version",
		obj:  valid(func(rm *ateapipb.ResourceMetadata) { rm.Version = 0 }),
		want: nil,
	}, {
		name: "valid version: large",
		obj:  valid(func(rm *ateapipb.ResourceMetadata) { rm.Version = math.MaxInt64 }),
	}, {
		name: "invalid version: negative",
		obj:  valid(func(rm *ateapipb.ResourceMetadata) { rm.Version = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("version"), nil, "").WithOrigin("minimum")},
	}, {
		name: "unspecified createTime",
		obj:  valid(func(rm *ateapipb.ResourceMetadata) { rm.CreateTime = nil }),
		want: nil,
	}, {
		name: "unspecified updateTime",
		obj:  valid(func(rm *ateapipb.ResourceMetadata) { rm.UpdateTime = nil }),
		want: nil,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_ResourceMetadata(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateResourceMetadataUpdate(t *testing.T) {
	valid := validResourceMetadata

	// Focus this test on fields other than atespace and name.
	tests := []struct {
		name   string
		oldObj *ateapipb.ResourceMetadata // should always be valid
		newObj *ateapipb.ResourceMetadata
		want   field.ErrorList
	}{{
		name:   "valid",
		oldObj: valid(),
		newObj: valid(),
	}, {
		name:   "atespace: empty -> non-empty",
		oldObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "" }),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "present" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "atespace: non-empty -> empty",
		oldObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "present" }),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "atespace: changed",
		oldObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "value-1" }),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "value-2" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "name: unset",
		oldObj: valid(),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "" }),
		want: field.ErrorList{
			field.Required(field.NewPath("name"), ""),
			field.Invalid(field.NewPath("name"), nil, "").WithOrigin("immutable"),
		},
	}, {
		name:   "name: changed",
		oldObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "value-1" }),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "value-2" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "uid: unset",
		oldObj: valid(),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Uid = "" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "uid: changed to valid",
		oldObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" }),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Uid = "11111111-2222-3333-4444-555555555555" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "uid: changed to invalid",
		oldObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" }),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Uid = "not a uid" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "version: unset",
		oldObj: valid(),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Version = 0 }),
		want:   field.ErrorList{field.Invalid(field.NewPath("version"), nil, "").WithOrigin("update")},
	}, {
		name:   "version: changed to valid",
		oldObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Version = 123 }),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Version = 456 }),
		want:   nil,
	}, {
		name:   "version: changed non-monotonically",
		oldObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Version = 456 }),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Version = 123 }),
		want: field.ErrorList{
			field.Invalid(field.NewPath("version"), nil, "").WithOrigin("monotonic"),
		},
	}, {
		name:   "version: changed to invalid",
		oldObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Version = 456 }),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.Version = -1 }),
		want: field.ErrorList{
			field.Invalid(field.NewPath("version"), nil, "").WithOrigin("monotonic"),
			field.Invalid(field.NewPath("version"), nil, "").WithOrigin("minimum"),
		},
	}, {
		name:   "create_time: unset",
		oldObj: valid(),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.CreateTime = nil }),
		want:   field.ErrorList{field.Invalid(field.NewPath("create_time"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "create_time: changed",
		oldObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.CreateTime.Seconds = 123 }),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.CreateTime.Seconds = 456 }),
		want:   field.ErrorList{field.Invalid(field.NewPath("create_time"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "update_time: unset",
		oldObj: valid(),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.UpdateTime = nil }),
		want:   field.ErrorList{field.Invalid(field.NewPath("update_time"), nil, "").WithOrigin("update")},
	}, {
		name:   "update_time: changed to valid",
		oldObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.UpdateTime.Seconds = 123 }),
		newObj: valid(func(rm *ateapipb.ResourceMetadata) { rm.UpdateTime.Seconds = 456 }),
		want:   nil,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Update}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_ResourceMetadata(context.Background(), op, nil, tt.newObj, tt.oldObj))
		})
	}
}

func TestValidateResourceMetadataNameAndAtespaceFormat(t *testing.T) {
	valid := validResourceMetadata

	// Focus this test on exhaustive testing of the name and atespace fields.
	tests := []struct {
		name string
		obj  *ateapipb.ResourceMetadata
		want field.ErrorList
	}{{
		"valid",
		valid(),
		nil,
	}, {
		"valid atespace: alphabetic",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "myatespace" }),
		nil,
	}, {
		"valid atespace: dashes",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my-ate-space" }),
		nil,
	}, {
		"valid atespace: repeat dashes",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my---ate---space" }),
		nil,
	}, {
		"valid atespace: alphanumeric",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my-123-atespace" }),
		nil,
	}, {
		"valid atespace: leading numeric",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "123-atespace" }),
		nil,
	}, {
		"valid atespace: trailing numeric",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my-123" }),
		nil,
	}, {
		"valid atespace: fully numeric",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "123" }),
		nil,
	}, {
		"valid atespace: long",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = strings.Repeat("x", 63) }),
		nil,
	}, {
		"invalid atespace: uppercase",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "MYATESPACE" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: leading dash",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "-atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: trailing dash",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my-" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: dots",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my.atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: underscores",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my_atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: bang",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my!atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: at",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my@atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: pound",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my#atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: dollar",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my$atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: percent",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my%%atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: caret",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my^atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: ampersand",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my&atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: star",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = "my*atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: too long",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Atespace = strings.Repeat("x", 64) }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"valid name: alphabetic",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "myname" }),
		nil,
	}, {
		"valid name: dashes",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my-na-me" }),
		nil,
	}, {
		"valid name: repeat dashes",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my---na---me" }),
		nil,
	}, {
		"valid name: alphanumeric",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my-123-name" }),
		nil,
	}, {
		"invalid name: leading numeric",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "123-name" }),
		nil,
	}, {
		"invalid name: trailing numeric",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my-123" }),
		nil,
	}, {
		"invalid name: fully numeric",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "123" }),
		nil,
	}, {
		"valid name: long",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = strings.Repeat("x", 63) }),
		nil,
	}, {
		"invalid name: uppercase",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "MYNAME" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: leading dash",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "-name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: trailing dash",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my-" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: dots",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my.name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: underscores",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my_name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: bang",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my!name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: at",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my@name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: pound",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my#name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: dollar",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my$name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: percent",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my%%name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: caret",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my^name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: ampersand",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my&name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: star",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = "my*name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: too long",
		valid(func(rm *ateapipb.ResourceMetadata) { rm.Name = strings.Repeat("x", 64) }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := proto.CloneOf(tt.obj) // avoid internal mutations
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_ResourceMetadata(context.Background(), op, nil, obj, nil))
		})
	}
}

func TestValidateObjectRef(t *testing.T) {
	valid := func(mutate func(*ateapipb.ObjectRef)) *ateapipb.ObjectRef {
		r := &ateapipb.ObjectRef{
			Atespace: "as",
			Name:     "nm",
		}
		if mutate != nil {
			mutate(r)
		}
		return r
	}

	tests := []struct {
		name string
		ref  *ateapipb.ObjectRef
		want field.ErrorList
	}{{
		"valid",
		valid(nil),
		nil,
	}, {
		"valid atespace: empty",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "" }),
		nil,
	}, {
		"valid atespace: alphabetic",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "myatespace" }),
		nil,
	}, {
		"valid atespace: dashes",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my-ate-space" }),
		nil,
	}, {
		"valid atespace: repeat dashes",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my---ate---space" }),
		nil,
	}, {
		"valid atespace: alphanumeric",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my-123-atespace" }),
		nil,
	}, {
		"valid atespace: leading numeric",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "123-atespace" }),
		nil,
	}, {
		"valid atespace: trailing numeric",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my-123" }),
		nil,
	}, {
		"valid atespace: fully numeric",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "123" }),
		nil,
	}, {
		"valid atespace: long",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = strings.Repeat("x", 63) }),
		nil,
	}, {
		"invalid atespace: uppercase",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "MYATESPACE" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: leading dash",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "-atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: trailing dash",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my-" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: dots",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my.atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: underscores",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my_atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: bang",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my!atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: at",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my@atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: pound",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my#atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: dollar",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my$atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: percent",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my%%atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: caret",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my^atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: ampersand",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my&atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: star",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = "my*atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: too long",
		valid(func(r *ateapipb.ObjectRef) { r.Atespace = strings.Repeat("x", 64) }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing name",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		"valid name: alphabetic",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "myname" }),
		nil,
	}, {
		"valid name: dashes",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my-na-me" }),
		nil,
	}, {
		"valid name: repeat dashes",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my---na---me" }),
		nil,
	}, {
		"valid name: alphanumeric",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my-123-name" }),
		nil,
	}, {
		"invalid name: leading numeric",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "123-name" }),
		nil,
	}, {
		"invalid name: trailing numeric",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my-123" }),
		nil,
	}, {
		"invalid name: fully numeric",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "123" }),
		nil,
	}, {
		"valid name: long",
		valid(func(r *ateapipb.ObjectRef) { r.Name = strings.Repeat("x", 63) }),
		nil,
	}, {
		"invalid name: uppercase",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "MYNAME" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: leading dash",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "-name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: trailing dash",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my-" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: dots",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my.name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: underscores",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my_name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: bang",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my!name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: at",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my@name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: pound",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my#name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: dollar",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my$name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: percent",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my%%name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: caret",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my^name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: ampersand",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my&name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: star",
		valid(func(r *ateapipb.ObjectRef) { r.Name = "my*name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: too long",
		valid(func(r *ateapipb.ObjectRef) { r.Name = strings.Repeat("x", 64) }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_ObjectRef(context.Background(), op, nil, tt.ref, nil))
		})
	}
}
