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

package ateapipb

import (
	"context"
	"math"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateResourceMetadata(t *testing.T) {
	valid := func(mutate func(*ResourceMetadata)) *ResourceMetadata {
		m := &ResourceMetadata{
			Atespace:   "as",
			Name:       "nm",
			Uid:        "01234567-89ab-cdef-0123-456789abcdef",
			Version:    93,
			CreateTime: &timestamppb.Timestamp{Seconds: 867},
			UpdateTime: &timestamppb.Timestamp{Seconds: 5309},
		}
		if mutate != nil {
			mutate(m)
		}
		return m
	}

	tests := []struct {
		name string
		req  *ResourceMetadata
		want field.ErrorList
	}{{
		"valid",
		valid(nil),
		nil,
	}, {
		"valid atespace: empty",
		valid(func(m *ResourceMetadata) { m.Atespace = "" }),
		nil,
	}, {
		"valid atespace: alphabetic",
		valid(func(m *ResourceMetadata) { m.Atespace = "myatespace" }),
		nil,
	}, {
		"valid atespace: dashes",
		valid(func(m *ResourceMetadata) { m.Atespace = "my-ate-space" }),
		nil,
	}, {
		"valid atespace: repeat dashes",
		valid(func(m *ResourceMetadata) { m.Atespace = "my---ate---space" }),
		nil,
	}, {
		"valid atespace: alphanumeric",
		valid(func(m *ResourceMetadata) { m.Atespace = "my-123-atespace" }),
		nil,
	}, {
		"valid atespace: leading numeric",
		valid(func(m *ResourceMetadata) { m.Atespace = "123-atespace" }),
		nil,
	}, {
		"valid atespace: trailing numeric",
		valid(func(m *ResourceMetadata) { m.Atespace = "my-123" }),
		nil,
	}, {
		"valid atespace: fully numeric",
		valid(func(m *ResourceMetadata) { m.Atespace = "123" }),
		nil,
	}, {
		"valid atespace: long",
		valid(func(m *ResourceMetadata) { m.Atespace = strings.Repeat("x", 63) }),
		nil,
	}, {
		"invalid atespace: uppercase",
		valid(func(m *ResourceMetadata) { m.Atespace = "MYATESPACE" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: leading dash",
		valid(func(m *ResourceMetadata) { m.Atespace = "-atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: trailing dash",
		valid(func(m *ResourceMetadata) { m.Atespace = "my-" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: dots",
		valid(func(m *ResourceMetadata) { m.Atespace = "my.atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: underscores",
		valid(func(m *ResourceMetadata) { m.Atespace = "my_atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: bang",
		valid(func(m *ResourceMetadata) { m.Atespace = "my!atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: at",
		valid(func(m *ResourceMetadata) { m.Atespace = "my@atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: pound",
		valid(func(m *ResourceMetadata) { m.Atespace = "my#atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: dollar",
		valid(func(m *ResourceMetadata) { m.Atespace = "my$atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: percent",
		valid(func(m *ResourceMetadata) { m.Atespace = "my%%atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: caret",
		valid(func(m *ResourceMetadata) { m.Atespace = "my^atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: ampersand",
		valid(func(m *ResourceMetadata) { m.Atespace = "my&atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: star",
		valid(func(m *ResourceMetadata) { m.Atespace = "my*atespace" }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid atespace: too long",
		valid(func(m *ResourceMetadata) { m.Atespace = strings.Repeat("x", 64) }),
		field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing name",
		valid(func(m *ResourceMetadata) { m.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		"valid name: alphabetic",
		valid(func(m *ResourceMetadata) { m.Name = "myname" }),
		nil,
	}, {
		"valid name: dashes",
		valid(func(m *ResourceMetadata) { m.Name = "my-na-me" }),
		nil,
	}, {
		"valid name: repeat dashes",
		valid(func(m *ResourceMetadata) { m.Name = "my---na---me" }),
		nil,
	}, {
		"valid name: alphanumeric",
		valid(func(m *ResourceMetadata) { m.Name = "my-123-name" }),
		nil,
	}, {
		"invalid name: leading numeric",
		valid(func(m *ResourceMetadata) { m.Name = "123-name" }),
		nil,
	}, {
		"invalid name: trailing numeric",
		valid(func(m *ResourceMetadata) { m.Name = "my-123" }),
		nil,
	}, {
		"invalid name: fully numeric",
		valid(func(m *ResourceMetadata) { m.Name = "123" }),
		nil,
	}, {
		"valid name: long",
		valid(func(m *ResourceMetadata) { m.Name = strings.Repeat("x", 63) }),
		nil,
	}, {
		"invalid name: uppercase",
		valid(func(m *ResourceMetadata) { m.Name = "MYNAME" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: leading dash",
		valid(func(m *ResourceMetadata) { m.Name = "-name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: trailing dash",
		valid(func(m *ResourceMetadata) { m.Name = "my-" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: dots",
		valid(func(m *ResourceMetadata) { m.Name = "my.name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: underscores",
		valid(func(m *ResourceMetadata) { m.Name = "my_name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: bang",
		valid(func(m *ResourceMetadata) { m.Name = "my!name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: at",
		valid(func(m *ResourceMetadata) { m.Name = "my@name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: pound",
		valid(func(m *ResourceMetadata) { m.Name = "my#name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: dollar",
		valid(func(m *ResourceMetadata) { m.Name = "my$name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: percent",
		valid(func(m *ResourceMetadata) { m.Name = "my%%name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: caret",
		valid(func(m *ResourceMetadata) { m.Name = "my^name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: ampersand",
		valid(func(m *ResourceMetadata) { m.Name = "my&name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: star",
		valid(func(m *ResourceMetadata) { m.Name = "my*name" }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid name: too long",
		valid(func(m *ResourceMetadata) { m.Name = strings.Repeat("x", 64) }),
		field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"unspecified uid",
		valid(func(m *ResourceMetadata) { m.Uid = "" }),
		nil,
	}, {
		"invalid uid: close but not valid",
		valid(func(m *ResourceMetadata) { m.Uid = "aaaaaaaa-bbbbcccc-dddd-eeeeeeeeeeee" }),
		field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		"invalid uid: not even close",
		valid(func(m *ResourceMetadata) { m.Uid = "not a uid" }),
		field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		"unspecified version",
		valid(func(m *ResourceMetadata) { m.Version = 0 }),
		nil,
	}, {
		"valid version: large",
		valid(func(m *ResourceMetadata) { m.Version = math.MaxInt64 }),
		nil,
	}, {
		"invalid version: negative",
		valid(func(m *ResourceMetadata) { m.Version = -1 }),
		field.ErrorList{field.Invalid(field.NewPath("version"), nil, "").WithOrigin("minimum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_ResourceMetadata(context.Background(), op, nil, tt.req, nil))
		})
	}
}
