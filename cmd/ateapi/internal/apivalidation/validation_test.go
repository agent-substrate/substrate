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
	"fmt"
	"math"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validResourceMetadata(mutate ...func(*ateapi.ResourceMetadata)) *ateapi.ResourceMetadata {
	// This is valid with as many fields populated as possible.
	rm := &ateapi.ResourceMetadata{
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
		obj  *ateapi.ResourceMetadata
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid atespace: empty",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "" }),
	}, {
		name: "missing name",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		name: "unspecified uid",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.Uid = "" }),
		want: nil,
	}, {
		name: "invalid uid: close but not valid",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.Uid = "aaaaaaaa-bbbbcccc-dddd-eeeeeeeeeeee" }),
		want: field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid uid: not even close",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.Uid = "not a uid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "unspecified version",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.Version = 0 }),
		want: nil,
	}, {
		name: "valid version: large",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.Version = math.MaxInt64 }),
	}, {
		name: "invalid version: negative",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.Version = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("version"), nil, "").WithOrigin("minimum")},
	}, {
		name: "unspecified createTime",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.CreateTime = nil }),
		want: nil,
	}, {
		name: "unspecified updateTime",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.UpdateTime = nil }),
		want: nil,
	}, {
		name: "invalid createTime: seconds out of range",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.CreateTime = &timestamppb.Timestamp{Seconds: 253402300800} }),
		want: field.ErrorList{field.Invalid(field.NewPath("create_time"), nil, "")},
	}, {
		name: "invalid updateTime: negative nanos",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.UpdateTime = &timestamppb.Timestamp{Seconds: 5309, Nanos: -1} }),
		want: field.ErrorList{field.Invalid(field.NewPath("update_time"), nil, "")},
	}, {
		name: "invalid updateTime: precedes createTime",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.UpdateTime = &timestamppb.Timestamp{Seconds: 866} }),
		want: field.ErrorList{field.Invalid(field.NewPath("update_time"), nil, "")},
	}, {
		name: "valid updateTime: equals createTime",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.UpdateTime = &timestamppb.Timestamp{Seconds: 867} }),
		want: nil,
	}, {
		name: "valid updateTime: set without createTime",
		obj:  valid(func(rm *ateapi.ResourceMetadata) { rm.CreateTime = nil }),
		want: nil,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
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
		oldObj *ateapi.ResourceMetadata // should always be valid
		newObj *ateapi.ResourceMetadata
		want   field.ErrorList
	}{{
		name:   "valid",
		oldObj: valid(),
		newObj: valid(),
	}, {
		name:   "atespace: empty -> non-empty",
		oldObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "" }),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "present" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "atespace: non-empty -> empty",
		oldObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "present" }),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "atespace: changed",
		oldObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "value-1" }),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "value-2" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "name: unset",
		oldObj: valid(),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "" }),
		want: field.ErrorList{
			field.Required(field.NewPath("name"), ""),
			field.Invalid(field.NewPath("name"), nil, "").WithOrigin("immutable"),
		},
	}, {
		name:   "name: changed",
		oldObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "value-1" }),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "value-2" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "uid: unset",
		oldObj: valid(),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Uid = "" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "uid: changed to valid",
		oldObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" }),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Uid = "11111111-2222-3333-4444-555555555555" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "uid: changed to invalid",
		oldObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" }),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Uid = "not a uid" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "version: unset",
		oldObj: valid(),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Version = 0 }),
		want:   field.ErrorList{field.Invalid(field.NewPath("version"), nil, "").WithOrigin("update")},
	}, {
		name:   "version: changed to valid",
		oldObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Version = 123 }),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Version = 456 }),
		want:   nil,
	}, {
		name:   "version: changed non-monotonically",
		oldObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Version = 456 }),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Version = 123 }),
		want: field.ErrorList{
			field.Invalid(field.NewPath("version"), nil, "").WithOrigin("monotonic"),
		},
	}, {
		name:   "version: changed to invalid",
		oldObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Version = 456 }),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.Version = -1 }),
		want: field.ErrorList{
			field.Invalid(field.NewPath("version"), nil, "").WithOrigin("monotonic"),
			field.Invalid(field.NewPath("version"), nil, "").WithOrigin("minimum"),
		},
	}, {
		name:   "create_time: unset",
		oldObj: valid(),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.CreateTime = nil }),
		want:   field.ErrorList{field.Invalid(field.NewPath("create_time"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "create_time: changed",
		oldObj: valid(func(rm *ateapi.ResourceMetadata) { rm.CreateTime.Seconds = 123 }),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.CreateTime.Seconds = 456 }),
		want:   field.ErrorList{field.Invalid(field.NewPath("create_time"), nil, "").WithOrigin("immutable")},
	}, {
		name:   "update_time: unset",
		oldObj: valid(),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.UpdateTime = nil }),
		want:   field.ErrorList{field.Invalid(field.NewPath("update_time"), nil, "").WithOrigin("update")},
	}, {
		name:   "update_time: changed to valid",
		oldObj: valid(func(rm *ateapi.ResourceMetadata) { rm.UpdateTime.Seconds = 1000 }),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.UpdateTime.Seconds = 6000 }),
		want:   nil,
	}, {
		name:   "update_time: changed to precede create_time",
		oldObj: valid(),
		newObj: valid(func(rm *ateapi.ResourceMetadata) { rm.UpdateTime.Seconds = 866 }),
		want:   field.ErrorList{field.Invalid(field.NewPath("update_time"), nil, "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeUpdateOp(context.Background())
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
		obj  *ateapi.ResourceMetadata
		want field.ErrorList
	}{{
		"valid",
		valid(),
		nil,
	}, {
		"valid atespace: alphabetic",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "myatespace" }),
		nil,
	}, {
		"valid atespace: dashes",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my-ate-space" }),
		nil,
	}, {
		"valid atespace: repeat dashes",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my---ate---space" }),
		nil,
	}, {
		"valid atespace: alphanumeric",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my-123-atespace" }),
		nil,
	}, {
		"valid atespace: leading numeric",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "123-atespace" }),
		nil,
	}, {
		"valid atespace: trailing numeric",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my-123" }),
		nil,
	}, {
		"valid atespace: fully numeric",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "123" }),
		nil,
	}, {
		"valid atespace: long",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = strings.Repeat("x", 63) }),
		nil,
	}, {
		"invalid atespace: uppercase",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "MYATESPACE" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: leading dash",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "-atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: trailing dash",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my-" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: dots",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my.atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: underscores",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my_atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: bang",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my!atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: at",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my@atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: pound",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my#atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: dollar",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my$atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: percent",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my%%atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: caret",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my^atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: ampersand",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my&atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: star",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = "my*atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: too long",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Atespace = strings.Repeat("x", 64) }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"valid name: alphabetic",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "myname" }),
		nil,
	}, {
		"valid name: dashes",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my-na-me" }),
		nil,
	}, {
		"valid name: repeat dashes",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my---na---me" }),
		nil,
	}, {
		"valid name: alphanumeric",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my-123-name" }),
		nil,
	}, {
		"invalid name: leading numeric",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "123-name" }),
		nil,
	}, {
		"invalid name: trailing numeric",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my-123" }),
		nil,
	}, {
		"invalid name: fully numeric",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "123" }),
		nil,
	}, {
		"valid name: long",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = strings.Repeat("x", 63) }),
		nil,
	}, {
		"invalid name: uppercase",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "MYNAME" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: leading dash",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "-name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: trailing dash",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my-" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: dots",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my.name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: underscores",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my_name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: bang",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my!name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: at",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my@name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: pound",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my#name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: dollar",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my$name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: percent",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my%%name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: caret",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my^name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: ampersand",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my&name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: star",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = "my*name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: too long",
		valid(func(rm *ateapi.ResourceMetadata) { rm.Name = strings.Repeat("x", 64) }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := proto.CloneOf(tt.obj) // avoid internal mutations
			op := MakeCreateOp(context.Background())
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_ResourceMetadata(context.Background(), op, nil, obj, nil))
		})
	}
}

func TestValidateObjectRef(t *testing.T) {
	valid := func(mutate func(*ateapi.ObjectRef)) *ateapi.ObjectRef {
		r := &ateapi.ObjectRef{
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
		ref  *ateapi.ObjectRef
		want field.ErrorList
	}{{
		"valid",
		valid(nil),
		nil,
	}, {
		"valid atespace: empty",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "" }),
		nil,
	}, {
		"valid atespace: alphabetic",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "myatespace" }),
		nil,
	}, {
		"valid atespace: dashes",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my-ate-space" }),
		nil,
	}, {
		"valid atespace: repeat dashes",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my---ate---space" }),
		nil,
	}, {
		"valid atespace: alphanumeric",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my-123-atespace" }),
		nil,
	}, {
		"valid atespace: leading numeric",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "123-atespace" }),
		nil,
	}, {
		"valid atespace: trailing numeric",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my-123" }),
		nil,
	}, {
		"valid atespace: fully numeric",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "123" }),
		nil,
	}, {
		"valid atespace: long",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = strings.Repeat("x", 63) }),
		nil,
	}, {
		"invalid atespace: uppercase",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "MYATESPACE" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: leading dash",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "-atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: trailing dash",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my-" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: dots",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my.atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: underscores",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my_atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: bang",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my!atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: at",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my@atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: pound",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my#atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: dollar",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my$atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: percent",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my%%atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: caret",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my^atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: ampersand",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my&atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: star",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = "my*atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: too long",
		valid(func(r *ateapi.ObjectRef) { r.Atespace = strings.Repeat("x", 64) }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing name",
		valid(func(r *ateapi.ObjectRef) { r.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		"valid name: alphabetic",
		valid(func(r *ateapi.ObjectRef) { r.Name = "myname" }),
		nil,
	}, {
		"valid name: dashes",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my-na-me" }),
		nil,
	}, {
		"valid name: repeat dashes",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my---na---me" }),
		nil,
	}, {
		"valid name: alphanumeric",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my-123-name" }),
		nil,
	}, {
		"invalid name: leading numeric",
		valid(func(r *ateapi.ObjectRef) { r.Name = "123-name" }),
		nil,
	}, {
		"invalid name: trailing numeric",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my-123" }),
		nil,
	}, {
		"invalid name: fully numeric",
		valid(func(r *ateapi.ObjectRef) { r.Name = "123" }),
		nil,
	}, {
		"valid name: long",
		valid(func(r *ateapi.ObjectRef) { r.Name = strings.Repeat("x", 63) }),
		nil,
	}, {
		"invalid name: uppercase",
		valid(func(r *ateapi.ObjectRef) { r.Name = "MYNAME" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: leading dash",
		valid(func(r *ateapi.ObjectRef) { r.Name = "-name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: trailing dash",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my-" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: dots",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my.name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: underscores",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my_name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: bang",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my!name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: at",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my@name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: pound",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my#name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: dollar",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my$name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: percent",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my%%name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: caret",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my^name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: ampersand",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my&name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: star",
		valid(func(r *ateapi.ObjectRef) { r.Name = "my*name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: too long",
		valid(func(r *ateapi.ObjectRef) { r.Name = strings.Repeat("x", 64) }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_ObjectRef(context.Background(), op, nil, tt.ref, nil))
		})
	}
}

func validSystemInfoVolumeSource(mutate ...func(*ateapi.SystemInfoVolumeSource)) *ateapi.SystemInfoVolumeSource {
	// This is valid with as many fields populated as possible.
	s := &ateapi.SystemInfoVolumeSource{
		DataSources: []*ateapi.SystemInfoDataSource{{
			ActorMetadata: &ateapi.ActorMetadataDataSource{
				Items: []*ateapi.ActorMetadataItem{{
					Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_NAME,
					Path:  "actor-name",
				}, {
					Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_UID,
					Path:  "actor-uid",
				}},
			},
		}, {
			TrustBundle: &ateapi.TrustBundleDataSource{
				Names: []string{"egress-mitm.ate.dev"},
				Path:  "trust-bundle.pem",
			},
		}},
	}
	for _, m := range mutate {
		m(s)
	}
	return s
}

func TestValidateSystemInfoVolumeSource(t *testing.T) {
	valid := validSystemInfoVolumeSource
	dsPath := field.NewPath("data_sources")
	itemsPath := dsPath.Index(0).Child("actor_metadata", "items")

	tests := []struct {
		name string
		obj  *ateapi.SystemInfoVolumeSource
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: no data sources",
		obj:  valid(func(s *ateapi.SystemInfoVolumeSource) { s.DataSources = nil }),
	}, {
		name: "too many data sources",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			for len(s.DataSources) <= 8 {
				s.DataSources = append(s.DataSources, &ateapi.SystemInfoDataSource{
					TrustBundle: &ateapi.TrustBundleDataSource{Names: []string{"egress-mitm.ate.dev"}, Path: "tb.pem"},
				})
			}
		}),
		want: field.ErrorList{field.TooMany(dsPath, 9, 8).WithOrigin("maxItems")},
	}, {
		name: "no union member set",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata = nil
		}),
		want: field.ErrorList{field.Invalid(dsPath.Index(0), nil, "").WithOrigin("union")},
	}, {
		name: "both union members set",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[0].TrustBundle = &ateapi.TrustBundleDataSource{
				Names: []string{"egress-mitm.ate.dev"},
				Path:  "tb2.pem",
			}
		}),
		want: field.ErrorList{field.Invalid(dsPath.Index(0), nil, "").WithOrigin("union")},
	}, {
		name: "empty items",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items = nil
		}),
		want: field.ErrorList{field.Required(itemsPath, "")},
	}, {
		name: "duplicate projected field",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[1].Field = ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_NAME
		}),
		want: field.ErrorList{field.Duplicate(itemsPath.Index(1), nil)},
	}, {
		name: "unspecified item field",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[0].Field = ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(itemsPath.Index(0).Child("field"), "")},
	}, {
		name: "item field outside the enum",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[0].Field = ateapi.ActorMetadataField(99)
		}),
		want: field.ErrorList{field.Invalid(itemsPath.Index(0).Child("field"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative item field",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[0].Field = ateapi.ActorMetadataField(-1)
		}),
		want: field.ErrorList{field.Invalid(itemsPath.Index(0).Child("field"), nil, "").WithOrigin("minimum")},
	}, {
		name: "empty item path",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[0].Path = ""
		}),
		want: field.ErrorList{field.Required(itemsPath.Index(0).Child("path"), "")},
	}, {
		name: "item path too long",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[0].Path = strings.Repeat("p", 256)
		}),
		want: field.ErrorList{field.TooLong(itemsPath.Index(0).Child("path"), nil, 255).WithOrigin("maxLength")},
	}, {
		name: "item path traversal",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[0].Path = "../../traversal-escape"
		}),
		want: field.ErrorList{field.Invalid(itemsPath.Index(0).Child("path"), nil, "")},
	}, {
		name: "trust bundle path traversal",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[1].TrustBundle.Path = "../escape"
		}),
		want: field.ErrorList{field.Invalid(dsPath.Index(1).Child("trust_bundle", "path"), nil, "")},
	}, {
		name: "duplicate path within items",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[1].Path = "actor-name"
		}),
		want: field.ErrorList{field.Duplicate(itemsPath.Index(1).Child("path"), nil)},
	}, {
		name: "duplicate path across data sources",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources[1].TrustBundle.Path = "actor-uid"
		}),
		want: field.ErrorList{field.Duplicate(dsPath.Index(1).Child("trust_bundle", "path"), nil)},
	}, {
		name: "second actor_metadata entry",
		obj: valid(func(s *ateapi.SystemInfoVolumeSource) {
			s.DataSources = append(s.DataSources, &ateapi.SystemInfoDataSource{
				ActorMetadata: &ateapi.ActorMetadataDataSource{
					Items: []*ateapi.ActorMetadataItem{{
						Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_NAME,
						Path:  "actor-name-2",
					}},
				},
			})
		}),
		want: field.ErrorList{field.Forbidden(dsPath.Index(2).Child("actor_metadata"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_SystemInfoVolumeSource(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateTrustBundleDataSource(t *testing.T) {
	valid := func(mutate ...func(*ateapi.TrustBundleDataSource)) *ateapi.TrustBundleDataSource {
		tb := &ateapi.TrustBundleDataSource{
			Names: []string{"egress-mitm.ate.dev"},
			Path:  "trust-bundle.pem",
		}
		for _, m := range mutate {
			m(tb)
		}
		return tb
	}

	tests := []struct {
		name string
		obj  *ateapi.TrustBundleDataSource
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: multiple names",
		obj: valid(func(tb *ateapi.TrustBundleDataSource) {
			tb.Names = []string{"egress-mitm.ate.dev", "system-roots.ate.dev"}
		}),
	}, {
		name: "no names",
		obj:  valid(func(tb *ateapi.TrustBundleDataSource) { tb.Names = nil }),
		want: field.ErrorList{field.Required(field.NewPath("names"), "")},
	}, {
		name: "too many names",
		obj: valid(func(tb *ateapi.TrustBundleDataSource) {
			tb.Names = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}
		}),
		want: field.ErrorList{field.TooMany(field.NewPath("names"), 9, 8).WithOrigin("maxItems")},
	}, {
		name: "empty name",
		obj:  valid(func(tb *ateapi.TrustBundleDataSource) { tb.Names = []string{""} }),
		want: field.ErrorList{field.TooShort(field.NewPath("names").Index(0), "", 1).WithOrigin("minLength")},
	}, {
		name: "name too long",
		obj:  valid(func(tb *ateapi.TrustBundleDataSource) { tb.Names = []string{strings.Repeat("n", 254)} }),
		want: field.ErrorList{field.TooLong(field.NewPath("names").Index(0), nil, 253).WithOrigin("maxLength")},
	}, {
		name: "empty path",
		obj:  valid(func(tb *ateapi.TrustBundleDataSource) { tb.Path = "" }),
		want: field.ErrorList{field.Required(field.NewPath("path"), "")},
	}, {
		name: "path too long",
		obj:  valid(func(tb *ateapi.TrustBundleDataSource) { tb.Path = strings.Repeat("p", 256) }),
		want: field.ErrorList{field.TooLong(field.NewPath("path"), nil, 255).WithOrigin("maxLength")},
	}, {
		name: "path traversal",
		obj:  valid(func(tb *ateapi.TrustBundleDataSource) { tb.Path = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("path"), nil, "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_TrustBundleDataSource(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func validExternalVolume(mutate ...func(*ateapi.ExternalVolumeStatus)) *ateapi.ExternalVolumeStatus {
	v := &ateapi.ExternalVolumeStatus{
		Name:            "my-vol",
		StorageVolumeId: "valid-storage-id",
		VolumeType:      "mock",
	}
	for _, m := range mutate {
		m(v)
	}
	return v
}

func TestValidateExternalVolume(t *testing.T) {
	valid := validExternalVolume

	tests := []struct {
		name string
		obj  *ateapi.ExternalVolumeStatus
		want field.ErrorList
	}{{
		name: "valid external volume",
		obj:  valid(),
	}, {
		name: "missing volume name",
		obj:  valid(func(v *ateapi.ExternalVolumeStatus) { v.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		name: "invalid volume name",
		obj:  valid(func(v *ateapi.ExternalVolumeStatus) { v.Name = "NOT A VOLUME" }),
		want: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "valid external volume with empty storage volume id",
		obj:  valid(func(v *ateapi.ExternalVolumeStatus) { v.StorageVolumeId = "" }),
	}, {
		name: "invalid storage volume id with null U+0000",
		obj:  valid(func(v *ateapi.ExternalVolumeStatus) { v.StorageVolumeId = "vol\x00id" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "storage volume id too long",
		obj:  valid(func(v *ateapi.ExternalVolumeStatus) { v.StorageVolumeId = strings.Repeat("x", 257) }),
		want: field.ErrorList{field.TooLong(field.NewPath("storage_volume_id"), nil, 256).WithOrigin("maxLength")},
	}, {
		name: "missing volume type",
		obj:  valid(func(v *ateapi.ExternalVolumeStatus) { v.VolumeType = "" }),
		want: field.ErrorList{field.Required(field.NewPath("volume_type"), "")},
	}, {
		name: "invalid volume type with uppercase",
		obj:  valid(func(v *ateapi.ExternalVolumeStatus) { v.VolumeType = "MockPlugin" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
	}, {
		name: "valid volume type with 253 characters",
		obj:  valid(func(v *ateapi.ExternalVolumeStatus) { v.VolumeType = strings.Repeat("a", 253) }),
	}, {
		name: "invalid volume type exceeding 253 characters",
		obj:  valid(func(v *ateapi.ExternalVolumeStatus) { v.VolumeType = strings.Repeat("a", 254) }),
		want: field.ErrorList{
			field.Invalid(field.NewPath("volume_type"), nil, ""),
			field.TooLong(field.NewPath("volume_type"), nil, 253).WithOrigin("maxLength"),
		},
	}, {
		name: "valid volume with substrate.io prefixed volume type",
		obj:  valid(func(v *ateapi.ExternalVolumeStatus) { v.VolumeType = "substrate.io/mock" }),
	}, {
		name: "storage volume id at the bound",
		obj:  valid(func(v *ateapi.ExternalVolumeStatus) { v.StorageVolumeId = strings.Repeat("x", 256) }),
	}, {
		name: "volume_context with many entries, as a CSI driver may return",
		obj: valid(func(v *ateapi.ExternalVolumeStatus) {
			ctxMap := make(map[string]string, 100)
			for i := range 100 {
				ctxMap[fmt.Sprintf("key-%03d", i)] = "v"
			}
			v.VolumeContext = ctxMap
		}),
	}, {
		name: "volume_context with one long value, as a CSI driver may return",
		obj: valid(func(v *ateapi.ExternalVolumeStatus) {
			v.VolumeContext = map[string]string{"attachment": strings.Repeat("v", 4000)}
		}),
	}, {
		name: "volume_context over the size bound",
		obj: valid(func(v *ateapi.ExternalVolumeStatus) {
			v.VolumeContext = map[string]string{"attachment": strings.Repeat("v", 40*1024)}
		}),
		want: field.ErrorList{field.TooLong(field.NewPath("volume_context"), nil, 40*1024)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
			assertValidateErr(t, Validate_ExternalVolumeStatus(context.Background(), op, nil, tt.obj, nil), tt.want)
		})
	}
}

func TestValidateExternalVolume_Update(t *testing.T) {
	valid := validExternalVolume

	tests := []struct {
		name   string
		oldObj *ateapi.ExternalVolumeStatus
		newObj *ateapi.ExternalVolumeStatus
		want   field.ErrorList
	}{{
		name:   "unchanged volume is valid",
		oldObj: valid(),
		newObj: valid(),
	}, {
		name:   "name changed is invalid",
		oldObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.Name = "vol1" }),
		newObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.Name = "vol2" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("update")},
	}, {
		name:   "storage_volume_id transition from empty to non-empty is valid",
		oldObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.StorageVolumeId = "" }),
		newObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.StorageVolumeId = "vol-id-1" }),
	}, {
		name:   "storage_volume_id changed once set is invalid",
		oldObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.StorageVolumeId = "vol-id-1" }),
		newObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.StorageVolumeId = "vol-id-2" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "").WithOrigin("update")},
	}, {
		name:   "storage_volume_id unset once set is invalid",
		oldObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.StorageVolumeId = "vol-id-1" }),
		newObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.StorageVolumeId = "" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "").WithOrigin("update")},
	}, {
		name:   "volume_type changed is invalid",
		oldObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.VolumeType = "mock" }),
		newObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.VolumeType = "pd.csi.storage.gke.io" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "").WithOrigin("update")},
	}, {
		name:   "volume_context changed is valid",
		oldObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.VolumeContext = nil }),
		newObj: valid(func(v *ateapi.ExternalVolumeStatus) { v.VolumeContext = map[string]string{"foo": "bar"} }),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeUpdateOp(context.Background())
			assertValidateErr(t, Validate_ExternalVolumeStatus(context.Background(), op, nil, tt.newObj, tt.oldObj), tt.want)
		})
	}
}

func TestValidateDeleteOptions(t *testing.T) {
	valid := func(mutate ...func(*ateapi.DeleteOptions)) *ateapi.DeleteOptions {
		tb := &ateapi.DeleteOptions{}
		for _, m := range mutate {
			m(tb)
		}
		return tb
	}

	tests := []struct {
		name string
		obj  *ateapi.DeleteOptions
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(), // all optional fields
	}, {
		name: "valid version",
		obj:  valid(func(do *ateapi.DeleteOptions) { do.Version = 1 }),
		want: nil,
	}, {
		name: "invalid version",
		obj:  valid(func(do *ateapi.DeleteOptions) { do.Version = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("version"), nil, "").WithOrigin("minimum")},
	}, {
		name: "valid uid",
		obj:  valid(func(do *ateapi.DeleteOptions) { do.Uid = "11111111-2222-3333-4444-555555555555" }),
		want: nil,
	}, {
		name: "invalid uid",
		obj:  valid(func(do *ateapi.DeleteOptions) { do.Uid = "not a uid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_DeleteOptions(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func validObjectSnapshot(mutate ...func(*ateapi.ObjectSnapshot)) *ateapi.ObjectSnapshot {
	s := &ateapi.ObjectSnapshot{
		SnapshotUri: "gs://private/atespaces/as/actors/" + someActorUID + "/snapshots/snap-1",
	}
	for _, m := range mutate {
		m(s)
	}
	return s
}

func validSnapshotStorage(mutate ...func(*ateapi.SnapshotStorage)) *ateapi.SnapshotStorage {
	ss := &ateapi.SnapshotStorage{
		Status: ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
		Object: validObjectSnapshot(),
	}
	for _, m := range mutate {
		m(ss)
	}
	return ss
}

func validSnapshot(mutate ...func(*ateapi.Snapshot)) *ateapi.Snapshot {
	s := &ateapi.Snapshot{
		Uuid:            "snap-1",
		Generation:      1,
		Owner:           ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
		Fidelity:        ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Locality:        "node-1",
		DurableSnapshot: validSnapshotStorage(),
	}
	for _, m := range mutate {
		m(s)
	}
	return s
}

// badSnapshot violates Snapshot's rules at once, so a caller can assert that a
// containing type reaches every field of it.
func badSnapshot(mutate ...func(*ateapi.Snapshot)) *ateapi.Snapshot {
	breakIt := func(s *ateapi.Snapshot) {
		s.Fidelity = ateapi.SnapshotFidelity(4)
		s.DurableSnapshot = &ateapi.SnapshotStorage{
			Status: ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
			Object: &ateapi.ObjectSnapshot{SnapshotUri: ""},
		}
	}
	return validSnapshot(append([]func(*ateapi.Snapshot){breakIt}, mutate...)...)
}

func TestValidateSnapshot(t *testing.T) {
	valid := validSnapshot
	uuidPath := field.NewPath("uuid")
	genPath := field.NewPath("generation")
	ownerPath := field.NewPath("owner")
	fidelityPath := field.NewPath("fidelity")
	localityPath := field.NewPath("locality")
	durableSnapshotPath := field.NewPath("durable_snapshot")
	statusPath := durableSnapshotPath.Child("status")
	objectPath := durableSnapshotPath.Child("object")
	uriPath := objectPath.Child("snapshot_uri")

	tests := []struct {
		name string
		obj  *ateapi.Snapshot
		want field.ErrorList
	}{
		{
			name: "valid",
			obj:  valid(),
		},
		{
			name: "valid optional fields unset",
			obj: valid(func(s *ateapi.Snapshot) {
				s.Locality = ""
			}),
		},
		{
			name: "missing uuid",
			obj:  valid(func(s *ateapi.Snapshot) { s.Uuid = "" }),
			want: field.ErrorList{field.Required(uuidPath, "")},
		},
		{
			name: "invalid uuid",
			obj:  valid(func(s *ateapi.Snapshot) { s.Uuid = "SNAP 1" }),
			want: field.ErrorList{field.Invalid(uuidPath, nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name: "invalid locality",
			obj:  valid(func(s *ateapi.Snapshot) { s.Locality = "NODE 1" }),
			want: field.ErrorList{field.Invalid(localityPath, nil, "").WithOrigin("format=k8s-long-name")},
		},
		{
			name: "missing generation",
			obj:  valid(func(s *ateapi.Snapshot) { s.Generation = 0 }),
			want: field.ErrorList{field.Required(genPath, "")},
		},
		{
			name: "negative generation",
			obj:  valid(func(s *ateapi.Snapshot) { s.Generation = -1 }),
			want: field.ErrorList{field.Invalid(genPath, nil, "").WithOrigin("minimum")},
		},
		{
			name: "missing owner",
			obj:  valid(func(s *ateapi.Snapshot) { s.Owner = ateapi.SnapshotOwner_SNAPSHOT_OWNER_UNSPECIFIED }),
			want: field.ErrorList{field.Required(ownerPath, "")},
		},
		{
			name: "owner above the enum",
			obj:  valid(func(s *ateapi.Snapshot) { s.Owner = ateapi.SnapshotOwner(3) }),
			want: field.ErrorList{field.Invalid(ownerPath, nil, "").WithOrigin("maximum")},
		},
		{
			name: "valid fidelity: volumes",
			obj: valid(func(s *ateapi.Snapshot) {
				s.Fidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
			}),
		},
		{
			name: "missing fidelity",
			obj: valid(func(s *ateapi.Snapshot) {
				s.Fidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
			}),
			want: field.ErrorList{field.Required(fidelityPath, "")},
		},
		{
			name: "valid without durable_snapshot",
			obj:  valid(func(s *ateapi.Snapshot) { s.DurableSnapshot = nil }),
		},
		{
			name: "missing durable_snapshot.status",
			obj: valid(func(s *ateapi.Snapshot) {
				s.DurableSnapshot.Status = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED
			}),
			want: field.ErrorList{field.Required(statusPath, "")},
		},
		{
			name: "negative durable_snapshot.status",
			obj: valid(func(s *ateapi.Snapshot) {
				s.DurableSnapshot.Status = ateapi.SnapshotStorageStatus(-1)
			}),
			want: field.ErrorList{field.Invalid(statusPath, nil, "").WithOrigin("minimum")},
		},
		{
			name: "durable_snapshot.status above the enum",
			obj: valid(func(s *ateapi.Snapshot) {
				s.DurableSnapshot.Status = ateapi.SnapshotStorageStatus(3)
			}),
			want: field.ErrorList{field.Invalid(statusPath, nil, "").WithOrigin("maximum")},
		},
		{
			name: "missing durable_snapshot.object",
			obj: valid(func(s *ateapi.Snapshot) {
				s.DurableSnapshot.Object = nil
			}),
			want: field.ErrorList{field.Required(objectPath, "")},
		},
		{
			name: "missing snapshot_uri",
			obj:  valid(func(s *ateapi.Snapshot) { s.DurableSnapshot.Object.SnapshotUri = "" }),
			want: field.ErrorList{field.Required(uriPath, "")},
		},
		{
			name: "snapshot_uri too long",
			obj: valid(func(s *ateapi.Snapshot) {
				s.DurableSnapshot.Object.SnapshotUri = "gs://" + strings.Repeat("x", 2044)
			}),
			want: field.ErrorList{field.TooLong(uriPath, nil, 2048).WithOrigin("maxLength")},
		},
		{
			name: "fidelity above the enum",
			obj:  valid(func(s *ateapi.Snapshot) { s.Fidelity = ateapi.SnapshotFidelity(4) }),
			want: field.ErrorList{field.Invalid(fidelityPath, nil, "").WithOrigin("maximum")},
		},
		{
			name: "negative fidelity",
			obj:  valid(func(s *ateapi.Snapshot) { s.Fidelity = ateapi.SnapshotFidelity(-1) }),
			want: field.ErrorList{field.Invalid(fidelityPath, nil, "").WithOrigin("minimum")},
		},
		{
			name: "every field invalid",
			obj:  badSnapshot(),
			want: field.ErrorList{
				field.Required(uriPath, ""),
				field.Invalid(fidelityPath, nil, "").WithOrigin("maximum"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
			assertValidateErr(t, Validate_Snapshot(context.Background(), op, nil, tt.obj, nil), tt.want)
		})
	}
}

func TestValidateSnapshotUpdate(t *testing.T) {
	valid := validSnapshot
	fidelityPath := field.NewPath("fidelity")
	uriPath := field.NewPath("durable_snapshot", "object", "snapshot_uri")

	tests := []struct {
		name   string
		oldObj *ateapi.Snapshot
		newObj *ateapi.Snapshot
		want   field.ErrorList
	}{
		{
			name:   "unchanged",
			oldObj: valid(),
			newObj: valid(),
		},
		{
			// Each field is only revalidated when it changes, so a row written
			// before these rules existed does not block updates to the rest of
			// the object.
			name:   "unchanged invalid fields are not revalidated",
			oldObj: badSnapshot(),
			newObj: badSnapshot(),
		},
		{
			name:   "fidelity changed to a valid value",
			oldObj: valid(),
			newObj: valid(func(s *ateapi.Snapshot) {
				s.Fidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
			}),
		},
		{
			name:   "fidelity cleared",
			oldObj: valid(),
			newObj: valid(func(s *ateapi.Snapshot) {
				s.Fidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
			}),
			want: field.ErrorList{field.Required(fidelityPath, "")},
		},
		{
			name:   "fidelity changed to a value outside the enum",
			oldObj: valid(),
			newObj: valid(func(s *ateapi.Snapshot) { s.Fidelity = ateapi.SnapshotFidelity(4) }),
			want:   field.ErrorList{field.Invalid(fidelityPath, nil, "").WithOrigin("maximum")},
		},
		{
			name:   "snapshot_uri cleared",
			oldObj: valid(),
			newObj: valid(func(s *ateapi.Snapshot) { s.DurableSnapshot.Object.SnapshotUri = "" }),
			want:   field.ErrorList{field.Required(uriPath, "")},
		},
		{
			// The other side of the ratchet: a row that predates these rules
			// can still be repaired, one field at a time.
			name:   "fidelity repaired",
			oldObj: badSnapshot(),
			newObj: badSnapshot(func(s *ateapi.Snapshot) {
				s.Fidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
			}),
		},
		{
			name:   "snapshot_uri changed to a value that is too long",
			oldObj: valid(),
			newObj: valid(func(s *ateapi.Snapshot) {
				s.DurableSnapshot.Object.SnapshotUri = "gs://" + strings.Repeat("x", 2044)
			}),
			want: field.ErrorList{field.TooLong(uriPath, nil, 2048).WithOrigin("maxLength")},
		},
		{
			name:   "snapshot_uri repaired",
			oldObj: badSnapshot(),
			newObj: badSnapshot(func(s *ateapi.Snapshot) { s.DurableSnapshot.Object.SnapshotUri = validObjectSnapshot().SnapshotUri }),
		},
		{
			name:   "every field repaired",
			oldObj: badSnapshot(),
			newObj: valid(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeUpdateOp(context.Background())
			assertValidateErr(t, Validate_Snapshot(context.Background(), op, nil, tt.newObj, tt.oldObj), tt.want)
		})
	}
}

// TestValidateNestedSnapshot checks that every type that
// holds Snapshot has to descend into it, and report under
// the holder's own path.
func TestValidateNestedSnapshot(t *testing.T) {
	tests := []struct {
		name string
		// path is where the offending Snapshot sits in the holder.
		path     *field.Path
		validate func(ctx context.Context) field.ErrorList
	}{
		{
			name: "actor.status.snapshots",
			path: field.NewPath("status", "snapshots").Index(0),
			validate: func(ctx context.Context) field.ErrorList {
				// The live path: the server validates the Actor it is about to
				// write, as an update against the stored one.
				op := MakeUpdateOp(context.Background())
				oldVal := validActor(withActorStatus())
				newVal := validActor(withActorStatus(func(s *ateapi.ActorStatus) {
					s.Snapshots = []*ateapi.Snapshot{badSnapshot()}
				}))
				return Validate_Actor(ctx, op, nil, newVal, oldVal)
			},
		},
		{
			name: "tag.status.snapshot",
			path: field.NewPath("status", "snapshot"),
			validate: func(ctx context.Context) field.ErrorList {
				op := MakeCreateOp(context.Background())
				obj := validTag(func(tag *ateapi.Tag) {
					tag.Status.Snapshot = badSnapshot()
				})
				return Validate_Tag(ctx, op, nil, obj, nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := field.ErrorList{
				field.Required(tt.path.Child("durable_snapshot", "object", "snapshot_uri"), ""),
				field.Invalid(tt.path.Child("fidelity"), nil, "").WithOrigin("maximum"),
			}
			assertValidateErr(t, tt.validate(context.Background()), want)
		})
	}
}

func validTag(mutate ...func(*ateapi.Tag)) *ateapi.Tag {
	tag := &ateapi.Tag{
		Metadata: validResourceMetadata(),
		Status: &ateapi.TagStatus{
			Snapshot:         validSnapshot(func(s *ateapi.Snapshot) { s.Owner = ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG }),
			ActorTemplateUid: someActorUID,
		},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: &ateapi.ObjectRef{Atespace: "as", Name: "nm"},
	}
	for _, m := range mutate {
		m(tag)
	}
	return tag
}

func TestValidateTag(t *testing.T) {
	valid := validTag
	metadataPath := field.NewPath("metadata")
	scopePath := field.NewPath("scope")
	sourceActorPath := field.NewPath("source_actor")
	statusPath := field.NewPath("status")

	tests := []struct {
		name string
		obj  *ateapi.Tag
		want field.ErrorList
	}{
		{
			name: "valid",
			obj:  valid(),
		},
		{
			name: "valid scope: published",
			obj: valid(func(tag *ateapi.Tag) {
				tag.Scope = ateapi.TagScope_TAG_SCOPE_PUBLISHED
			}),
		},
		{
			name: "missing status",
			obj:  valid(func(tag *ateapi.Tag) { tag.Status = nil }),
		},
		{
			name: "missing metadata",
			obj:  valid(func(tag *ateapi.Tag) { tag.Metadata = nil }),
			want: field.ErrorList{field.Required(metadataPath, "")},
		},
		{
			// A tag is addressed through the atespace that owns it, so unlike a
			// global-scoped resource it always has to name one.
			name: "missing metadata.atespace",
			obj:  valid(func(tag *ateapi.Tag) { tag.Metadata.Atespace = "" }),
			want: field.ErrorList{field.Required(metadataPath.Child("atespace"), "")},
		},
		{
			name: "invalid nested metadata.name",
			obj:  valid(func(tag *ateapi.Tag) { tag.Metadata.Name = "" }),
			want: field.ErrorList{field.Required(metadataPath.Child("name"), "")},
		},
		{
			name: "unspecified scope",
			obj: valid(func(tag *ateapi.Tag) {
				tag.Scope = ateapi.TagScope_TAG_SCOPE_UNSPECIFIED
			}),
			want: field.ErrorList{field.Required(scopePath, "")},
		},
		{
			name: "scope above the enum",
			obj:  valid(func(tag *ateapi.Tag) { tag.Scope = ateapi.TagScope(3) }),
			want: field.ErrorList{field.Invalid(scopePath, nil, "").WithOrigin("maximum")},
		},
		{
			name: "negative scope",
			obj:  valid(func(tag *ateapi.Tag) { tag.Scope = ateapi.TagScope(-1) }),
			want: field.ErrorList{field.Invalid(scopePath, nil, "").WithOrigin("minimum")},
		},
		{
			name: "missing source_actor",
			obj:  valid(func(tag *ateapi.Tag) { tag.SourceActor = nil }),
			want: field.ErrorList{field.Required(sourceActorPath, "")},
		},
		{
			name: "missing source_actor.atespace",
			obj:  valid(func(tag *ateapi.Tag) { tag.SourceActor.Atespace = "" }),
			want: field.ErrorList{field.Required(sourceActorPath.Child("atespace"), "")},
		},
		{
			name: "missing status.actor_template_uid",
			obj:  valid(func(tag *ateapi.Tag) { tag.Status.ActorTemplateUid = "" }),
			want: field.ErrorList{field.Required(statusPath.Child("actor_template_uid"), "")},
		},
		{
			name: "malformed status.actor_template_uid",
			obj:  valid(func(tag *ateapi.Tag) { tag.Status.ActorTemplateUid = "not-a-uuid" }),
			want: field.ErrorList{field.Invalid(statusPath.Child("actor_template_uid"), nil, "").WithOrigin("format=k8s-uuid")},
		},
		{
			// A reserved tag has no snapshot until the tag workflow records it.
			name: "missing status.snapshot",
			obj:  valid(func(tag *ateapi.Tag) { tag.Status.Snapshot = nil }),
		},
		{
			name: "missing status.snapshot.durable_snapshot",
			obj:  valid(func(tag *ateapi.Tag) { tag.Status.Snapshot.DurableSnapshot = nil }),
			want: field.ErrorList{field.Required(statusPath.Child("snapshot", "durable_snapshot"), "")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
			assertValidateErr(t, Validate_Tag(context.Background(), op, nil, tt.obj, nil), tt.want)
		})
	}
}

func TestValidateTagUpdate(t *testing.T) {
	valid := validTag

	tests := []struct {
		name   string
		oldObj *ateapi.Tag // should always be valid
		newObj *ateapi.Tag
		want   field.ErrorList
	}{
		{
			name:   "unchanged",
			oldObj: valid(),
			newObj: valid(),
		},
		{
			// The tag is addressed through its atespace; moving it would strand
			// every reference to it.
			name:   "metadata.atespace changed",
			oldObj: valid(),
			newObj: valid(func(tag *ateapi.Tag) { tag.Metadata.Atespace = "other" }),
			want:   field.ErrorList{field.Invalid(field.NewPath("metadata", "atespace"), nil, "").WithOrigin("immutable")},
		},
		{
			name:   "source_actor changed",
			oldObj: valid(),
			newObj: valid(func(tag *ateapi.Tag) { tag.SourceActor.Name = "other" }),
			want:   field.ErrorList{field.Invalid(field.NewPath("source_actor"), nil, "").WithOrigin("immutable")},
		},
		{
			name:   "scope changed",
			oldObj: valid(),
			newObj: valid(func(tag *ateapi.Tag) {
				tag.Scope = ateapi.TagScope_TAG_SCOPE_PUBLISHED
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeUpdateOp(context.Background())
			assertValidateErr(t, Validate_Tag(context.Background(), op, nil, tt.newObj, tt.oldObj), tt.want)
		})
	}
}

// TestValidateTagRequestPayloads covers the generated rules on the
// tag requests. The RPCs call these directly (see
// validateCreateTagRequest), so these guard the schema against
// drift; tag_test.go covers what the handlers make of it.
func TestValidateTagRequestPayloads(t *testing.T) {
	validTag := validTag
	validRef := func() *ateapi.ObjectRef { return &ateapi.ObjectRef{Atespace: "as", Name: "nm"} }
	tagPath := field.NewPath("tag")

	tests := []struct {
		name     string
		validate func(ctx context.Context, op operation.Operation) field.ErrorList
		want     field.ErrorList
	}{
		{
			name: "create: valid",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.CreateTagRequest{Tag: validTag()}
				return Validate_CreateTagRequest(ctx, op, nil, req, nil)
			},
		},
		{
			name: "create: missing tag",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.CreateTagRequest{}
				return Validate_CreateTagRequest(ctx, op, nil, req, nil)
			},
			want: field.ErrorList{field.Required(tagPath, "")},
		},
		{
			name: "create: invalid nested snapshot",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				tag := validTag()
				tag.Status.Snapshot = badSnapshot()
				req := &ateapi.CreateTagRequest{Tag: tag}
				return Validate_CreateTagRequest(ctx, op, nil, req, nil)
			},
			want: field.ErrorList{
				field.Required(tagPath.Child("status", "snapshot", "durable_snapshot", "object", "snapshot_uri"), ""),
				field.Invalid(tagPath.Child("status", "snapshot", "fidelity"), nil, "").WithOrigin("maximum"),
			},
		},
		{
			name: "update: valid",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.UpdateTagRequest{Tag: validTag()}
				return Validate_UpdateTagRequest(ctx, op, nil, req, nil)
			},
		},
		{
			name: "update: missing tag",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.UpdateTagRequest{}
				return Validate_UpdateTagRequest(ctx, op, nil, req, nil)
			},
			want: field.ErrorList{field.Required(tagPath, "")},
		},
		{
			// The tag body is opaque to the request: an update is validated
			// again as a whole object, against the tag it replaces.
			name: "update: nested snapshot is not descended into",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				tag := validTag()
				tag.Status.Snapshot = badSnapshot()
				req := &ateapi.UpdateTagRequest{Tag: tag}
				return Validate_UpdateTagRequest(ctx, op, nil, req, nil)
			},
		},
		{
			name: "get: valid",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.GetTagRequest{Tag: validRef()}
				return Validate_GetTagRequest(ctx, op, nil, req, nil)
			},
		},
		{
			name: "get: missing tag",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.GetTagRequest{}
				return Validate_GetTagRequest(ctx, op, nil, req, nil)
			},
			want: field.ErrorList{field.Required(tagPath, "")},
		},
		{
			// A tag is addressed through the atespace that owns it, so a ref
			// naming only a name does not name a tag.
			name: "get: missing tag.atespace",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Name: "nm"}}
				return Validate_GetTagRequest(ctx, op, nil, req, nil)
			},
			want: field.ErrorList{field.Required(tagPath.Child("atespace"), "")},
		},
		{
			name: "get: invalid tag.name",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Atespace: "as", Name: "NM"}}
				return Validate_GetTagRequest(ctx, op, nil, req, nil)
			},
			want: field.ErrorList{field.Invalid(tagPath.Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name: "delete: valid",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.DeleteTagRequest{Tag: validRef()}
				return Validate_DeleteTagRequest(ctx, op, nil, req, nil)
			},
		},
		{
			name: "delete: missing tag",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.DeleteTagRequest{}
				return Validate_DeleteTagRequest(ctx, op, nil, req, nil)
			},
			want: field.ErrorList{field.Required(tagPath, "")},
		},
		{
			name: "delete: missing tag.atespace",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.DeleteTagRequest{Tag: &ateapi.ObjectRef{Name: "nm"}}
				return Validate_DeleteTagRequest(ctx, op, nil, req, nil)
			},
			want: field.ErrorList{field.Required(tagPath.Child("atespace"), "")},
		},
		{
			// Listing is the one read that may leave the atespace out: that is
			// how a client asks for every atespace at once.
			name: "list: empty request",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.ListTagsRequest{}
				return Validate_ListTagsRequest(ctx, op, nil, req, nil)
			},
		},
		{
			name: "list: invalid atespace",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.ListTagsRequest{Atespace: "AS"}
				return Validate_ListTagsRequest(ctx, op, nil, req, nil)
			},
			want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
		},
		{
			name: "list: negative page_size",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.ListTagsRequest{Options: &ateapi.ListOptions{PageSize: -1}}
				return Validate_ListTagsRequest(ctx, op, nil, req, nil)
			},
			want: field.ErrorList{field.Invalid(field.NewPath("options", "page_size"), nil, "").WithOrigin("minimum")},
		},
		{
			name: "list: over-long page_token",
			validate: func(ctx context.Context, op operation.Operation) field.ErrorList {
				req := &ateapi.ListTagsRequest{Options: &ateapi.ListOptions{PageToken: strings.Repeat("t", 257)}}
				return Validate_ListTagsRequest(ctx, op, nil, req, nil)
			},
			want: field.ErrorList{field.TooLong(field.NewPath("options", "page_token"), nil, 256).WithOrigin("maxLength")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
			assertValidateErr(t, tt.validate(context.Background(), op), tt.want)
		})
	}
}

func validGoldenSnapshotStatus(mutate ...func(*ateapi.GoldenSnapshotStatus)) *ateapi.GoldenSnapshotStatus {
	s := &ateapi.GoldenSnapshotStatus{
		GoldenTag:    &ateapi.ObjectRef{Atespace: "team-a", Name: "01234567-89ab-cdef-0123-456789abcdef"},
		SnapshotTime: &timestamppb.Timestamp{Seconds: 867},
	}
	for _, m := range mutate {
		m(s)
	}
	return s
}

func TestValidateGoldenSnapshotStatus(t *testing.T) {
	valid := validGoldenSnapshotStatus
	msgPath := field.NewPath("error_message")

	tests := []struct {
		name string
		obj  *ateapi.GoldenSnapshotStatus
		want field.ErrorList
	}{
		{
			name: "valid",
			obj:  valid(),
		},
		{
			name: "valid empty",
			obj:  &ateapi.GoldenSnapshotStatus{},
		},
		{
			name: "valid failed",
			obj: &ateapi.GoldenSnapshotStatus{
				SnapshotTime: &timestamppb.Timestamp{Seconds: 867},
				ErrorMessage: "GoldenActorCrashed: golden actor crashed before its snapshot was taken",
			},
		},
		{
			name: "error_message at the bound",
			obj: valid(func(s *ateapi.GoldenSnapshotStatus) {
				s.ErrorMessage = strings.Repeat("x", 4096)
			}),
		},
		{
			name: "error_message too long",
			obj: valid(func(s *ateapi.GoldenSnapshotStatus) {
				s.ErrorMessage = strings.Repeat("x", 4097)
			}),
			want: field.ErrorList{field.TooLong(msgPath, nil, 4096).WithOrigin("maxLength")},
		},
		{
			name: "golden_tag missing atespace",
			obj: valid(func(s *ateapi.GoldenSnapshotStatus) {
				s.GoldenTag.Atespace = ""
			}),
			want: field.ErrorList{field.Required(field.NewPath("golden_tag", "atespace"), "")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeCreateOp(context.Background())
			assertValidateErr(t, Validate_GoldenSnapshotStatus(context.Background(), op, nil, tt.obj, nil), tt.want)
		})
	}
}

func TestValidateGoldenSnapshotStatusUpdate(t *testing.T) {
	valid := validGoldenSnapshotStatus

	tests := []struct {
		name   string
		oldObj *ateapi.GoldenSnapshotStatus // should always be valid
		newObj *ateapi.GoldenSnapshotStatus
		want   field.ErrorList
	}{
		{
			name:   "unchanged",
			oldObj: valid(),
			newObj: valid(),
		},
		{
			// The reconciler restarts the warmup clock on every resume, so the
			// deadline must stay mutable.
			name:   "snapshot_time changed",
			oldObj: valid(),
			newObj: valid(func(s *ateapi.GoldenSnapshotStatus) {
				s.SnapshotTime = &timestamppb.Timestamp{Seconds: 5309}
			}),
		},
		{
			name:   "snapshot_time set",
			oldObj: &ateapi.GoldenSnapshotStatus{},
			newObj: valid(),
		},
		{
			name:   "error_message set",
			oldObj: valid(),
			newObj: valid(func(s *ateapi.GoldenSnapshotStatus) {
				s.ErrorMessage = "GoldenActorCrashed: golden actor crashed before its snapshot was taken"
			}),
		},
		{
			name:   "error_message set too long",
			oldObj: valid(),
			newObj: valid(func(s *ateapi.GoldenSnapshotStatus) {
				s.ErrorMessage = strings.Repeat("x", 4097)
			}),
			want: field.ErrorList{field.TooLong(field.NewPath("error_message"), nil, 4096).WithOrigin("maxLength")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := MakeUpdateOp(context.Background())
			assertValidateErr(t, Validate_GoldenSnapshotStatus(context.Background(), op, nil, tt.newObj, tt.oldObj), tt.want)
		})
	}
}
