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
	"fmt"
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

func validSystemInfoVolumeSource(mutate ...func(*ateapipb.SystemInfoVolumeSource)) *ateapipb.SystemInfoVolumeSource {
	// This is valid with as many fields populated as possible.
	s := &ateapipb.SystemInfoVolumeSource{
		DataSources: []*ateapipb.SystemInfoDataSource{{
			ActorMetadata: &ateapipb.ActorMetadataDataSource{
				Items: []*ateapipb.ActorMetadataItem{{
					Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME,
					Path:  "actor-name",
				}, {
					Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_UID,
					Path:  "actor-uid",
				}},
			},
		}, {
			TrustBundle: &ateapipb.TrustBundleDataSource{
				Name: "egress-mitm.ate.dev",
				Path: "trust-bundle.pem",
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
		obj  *ateapipb.SystemInfoVolumeSource
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: no data sources",
		obj:  valid(func(s *ateapipb.SystemInfoVolumeSource) { s.DataSources = nil }),
	}, {
		name: "too many data sources",
		obj: valid(func(s *ateapipb.SystemInfoVolumeSource) {
			for len(s.DataSources) <= 8 {
				s.DataSources = append(s.DataSources, &ateapipb.SystemInfoDataSource{
					TrustBundle: &ateapipb.TrustBundleDataSource{Name: "egress-mitm.ate.dev", Path: "tb.pem"},
				})
			}
		}),
		want: field.ErrorList{field.TooMany(dsPath, 9, 8).WithOrigin("maxItems")},
	}, {
		name: "no union member set",
		obj: valid(func(s *ateapipb.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata = nil
		}),
		want: field.ErrorList{field.Invalid(dsPath.Index(0), nil, "").WithOrigin("union")},
	}, {
		name: "both union members set",
		obj: valid(func(s *ateapipb.SystemInfoVolumeSource) {
			s.DataSources[0].TrustBundle = &ateapipb.TrustBundleDataSource{
				Name: "egress-mitm.ate.dev",
				Path: "tb2.pem",
			}
		}),
		want: field.ErrorList{field.Invalid(dsPath.Index(0), nil, "").WithOrigin("union")},
	}, {
		name: "empty items",
		obj: valid(func(s *ateapipb.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items = nil
		}),
		want: field.ErrorList{field.Required(itemsPath, "")},
	}, {
		name: "duplicate projected field",
		obj: valid(func(s *ateapipb.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[1].Field = ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME
		}),
		want: field.ErrorList{field.Duplicate(itemsPath.Index(1), nil)},
	}, {
		name: "unspecified item field",
		obj: valid(func(s *ateapipb.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[0].Field = ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(itemsPath.Index(0).Child("field"), "")},
	}, {
		name: "item field outside the enum",
		obj: valid(func(s *ateapipb.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[0].Field = ateapipb.ActorMetadataField(99)
		}),
		want: field.ErrorList{field.Invalid(itemsPath.Index(0).Child("field"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative item field",
		obj: valid(func(s *ateapipb.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[0].Field = ateapipb.ActorMetadataField(-1)
		}),
		want: field.ErrorList{field.Invalid(itemsPath.Index(0).Child("field"), nil, "").WithOrigin("minimum")},
	}, {
		name: "empty item path",
		obj: valid(func(s *ateapipb.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[0].Path = ""
		}),
		want: field.ErrorList{field.Required(itemsPath.Index(0).Child("path"), "")},
	}, {
		name: "item path too long",
		obj: valid(func(s *ateapipb.SystemInfoVolumeSource) {
			s.DataSources[0].ActorMetadata.Items[0].Path = strings.Repeat("p", 256)
		}),
		want: field.ErrorList{field.TooLong(itemsPath.Index(0).Child("path"), nil, 255).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_SystemInfoVolumeSource(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateTrustBundleDataSource(t *testing.T) {
	valid := func(mutate ...func(*ateapipb.TrustBundleDataSource)) *ateapipb.TrustBundleDataSource {
		tb := &ateapipb.TrustBundleDataSource{
			Name: "egress-mitm.ate.dev",
			Path: "trust-bundle.pem",
		}
		for _, m := range mutate {
			m(tb)
		}
		return tb
	}

	tests := []struct {
		name string
		obj  *ateapipb.TrustBundleDataSource
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "empty name",
		obj:  valid(func(tb *ateapipb.TrustBundleDataSource) { tb.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		name: "name too long",
		obj:  valid(func(tb *ateapipb.TrustBundleDataSource) { tb.Name = strings.Repeat("n", 254) }),
		want: field.ErrorList{field.TooLong(field.NewPath("name"), nil, 253).WithOrigin("maxLength")},
	}, {
		name: "empty path",
		obj:  valid(func(tb *ateapipb.TrustBundleDataSource) { tb.Path = "" }),
		want: field.ErrorList{field.Required(field.NewPath("path"), "")},
	}, {
		name: "path too long",
		obj:  valid(func(tb *ateapipb.TrustBundleDataSource) { tb.Path = strings.Repeat("p", 256) }),
		want: field.ErrorList{field.TooLong(field.NewPath("path"), nil, 255).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_TrustBundleDataSource(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func validExternalVolume(mutate ...func(*ateapipb.ExternalVolume)) *ateapipb.ExternalVolume {
	v := &ateapipb.ExternalVolume{
		VolumeName:      "my-vol",
		StorageVolumeId: "valid-storage-id",
		VolumeType:      "mock",
		Status:          ateapipb.ExternalVolume_STATUS_CREATED,
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
		obj  *ateapipb.ExternalVolume
		want field.ErrorList
	}{{
		name: "valid external volume",
		obj:  valid(),
	}, {
		name: "missing volume name",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("volume_name"), "")},
	}, {
		name: "invalid volume name",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeName = "NOT A VOLUME" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "valid external volume with empty storage volume id",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "" }),
	}, {
		name: "invalid storage volume id with null U+0000",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol\x00id" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "invalid storage volume id with unit separator U+001F",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol\x1fid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "invalid storage volume id with DEL U+007F",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol\x7fid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "invalid storage volume id with C1 control U+0080",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol\u0080id" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "invalid storage volume id with C1 control U+009F",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol\u009fid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "storage volume id too long",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = strings.Repeat("x", 257) }),
		want: field.ErrorList{field.TooLong(field.NewPath("storage_volume_id"), nil, 256).WithOrigin("maxLength")},
	}, {
		name: "valid csi volume type",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "pd.csi.storage.gke.io" }),
	}, {
		name: "missing volume type",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "" }),
		want: field.ErrorList{field.Required(field.NewPath("volume_type"), "")},
	}, {
		name: "invalid volume type with uppercase",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "MockPlugin" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
	}, {
		name: "valid volume type with 253 characters",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = strings.Repeat("a", 253) }),
	}, {
		name: "invalid volume type exceeding 253 characters",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = strings.Repeat("a", 254) }),
		want: field.ErrorList{
			field.Invalid(field.NewPath("volume_type"), nil, ""),
			field.TooLong(field.NewPath("volume_type"), nil, 253).WithOrigin("maxLength"),
		},
	}, {
		name: "valid volume with substrate.io prefixed volume type",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "substrate.io/mock" }),
	}, {
		name: "invalid volume type with empty plugin after substrate.io prefix",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "substrate.io/" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
	}, {
		name: "invalid volume type with invalid plugin name after substrate.io prefix",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "substrate.io/Mock_Plugin" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
	}, {
		name: "invalid volume type with non-substrate prefix",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "other.io/mock" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
	}, {
		name: "negative status",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.Status = ateapipb.ExternalVolume_Status(-1) }),
		want: field.ErrorList{field.Invalid(field.NewPath("status"), nil, "").WithOrigin("minimum")},
	}, {
		name: "status outside the enum",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.Status = ateapipb.ExternalVolume_Status(4) }),
		want: field.ErrorList{field.Invalid(field.NewPath("status"), nil, "").WithOrigin("maximum")},
	}, {
		name: "storage volume id at the bound",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = strings.Repeat("x", 256) }),
	}, {
		name: "too many volume_context entries",
		obj: valid(func(v *ateapipb.ExternalVolume) {
			ctxMap := make(map[string]string, 33)
			for i := 0; i < 33; i++ {
				ctxMap[fmt.Sprintf("key-%d", i)] = "v"
			}
			v.VolumeContext = ctxMap
		}),
		want: field.ErrorList{field.TooMany(field.NewPath("volume_context"), 33, 32).WithOrigin("maxProperties")},
	}, {
		name: "volume_context key too long",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeContext = map[string]string{strings.Repeat("k", 129): "v"} }),
		want: field.ErrorList{field.TooLong(field.NewPath("volume_context"), nil, 128).WithOrigin("maxLength")},
	}, {
		name: "volume_context value too long",
		obj: valid(func(v *ateapipb.ExternalVolume) {
			v.VolumeContext = map[string]string{"attachment": strings.Repeat("v", 257)}
		}),
		want: field.ErrorList{field.TooLong(field.NewPath("volume_context").Key("attachment"), nil, 256).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			assertValidateErr(t, Validate_ExternalVolume(context.Background(), op, nil, tt.obj, nil), tt.want)
		})
	}
}

func TestValidateExternalVolume_Update(t *testing.T) {
	valid := validExternalVolume

	tests := []struct {
		name   string
		oldObj *ateapipb.ExternalVolume
		newObj *ateapipb.ExternalVolume
		want   field.ErrorList
	}{{
		name:   "unchanged volume is valid",
		oldObj: valid(),
		newObj: valid(),
	}, {
		name:   "volume_name changed is invalid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) { v.VolumeName = "vol1" }),
		newObj: valid(func(v *ateapipb.ExternalVolume) { v.VolumeName = "vol2" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("volume_name"), nil, "").WithOrigin("update")},
	}, {
		name:   "storage_volume_id transition from empty to non-empty is valid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "" }),
		newObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol-id-1" }),
	}, {
		name:   "storage_volume_id changed once set is invalid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol-id-1" }),
		newObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol-id-2" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "").WithOrigin("update")},
	}, {
		name:   "storage_volume_id unset once set is invalid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol-id-1" }),
		newObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "").WithOrigin("update")},
	}, {
		name:   "volume_type changed is invalid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "mock" }),
		newObj: valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "pd.csi.storage.gke.io" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "").WithOrigin("update")},
	}, {
		name: "status and volume_context changed is valid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) {
			v.Status = ateapipb.ExternalVolume_STATUS_PENDING
			v.VolumeContext = nil
		}),
		newObj: valid(func(v *ateapipb.ExternalVolume) {
			v.Status = ateapipb.ExternalVolume_STATUS_CREATED
			v.VolumeContext = map[string]string{"foo": "bar"}
		}),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Update}
			assertValidateErr(t, Validate_ExternalVolume(context.Background(), op, nil, tt.newObj, tt.oldObj), tt.want)
		})
	}
}

func TestValidateDeleteOptions(t *testing.T) {
	valid := func(mutate ...func(*ateapipb.DeleteOptions)) *ateapipb.DeleteOptions {
		tb := &ateapipb.DeleteOptions{}
		for _, m := range mutate {
			m(tb)
		}
		return tb
	}

	tests := []struct {
		name string
		obj  *ateapipb.DeleteOptions
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(), // all optional fields
	}, {
		name: "valid version",
		obj:  valid(func(do *ateapipb.DeleteOptions) { do.Version = 1 }),
		want: nil,
	}, {
		name: "invalid version",
		obj:  valid(func(do *ateapipb.DeleteOptions) { do.Version = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("version"), nil, "").WithOrigin("minimum")},
	}, {
		name: "valid uid",
		obj:  valid(func(do *ateapipb.DeleteOptions) { do.Uid = "11111111-2222-3333-4444-555555555555" }),
		want: nil,
	}, {
		name: "invalid uid",
		obj:  valid(func(do *ateapipb.DeleteOptions) { do.Uid = "not a uid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_DeleteOptions(context.Background(), op, nil, tt.obj, nil))
		})
	}
}
