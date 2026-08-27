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

package ateinterceptors

import (
	"context"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// RejectUnknownFieldsUnaryInterceptor refuses any request carrying a field
// this binary has no descriptor for, at any level of nesting.
func RejectUnknownFieldsUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if msg, ok := req.(proto.Message); ok {
		if errs := findUnknownFields(msg); len(errs) > 0 {
			return nil, status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
		}
	}
	return handler(ctx, req)
}

// findUnknownFields reports an error for every unknown field in m, at every
// level of nesting. An unknown field on m itself is reported at "request".
func findUnknownFields(m proto.Message) field.ErrorList {
	r := m.ProtoReflect()
	if !r.IsValid() {
		return nil
	}

	var errs field.ErrorList
	if err := protorange.Range(r, func(p protopath.Values) error {
		msg, ok := p.Index(-1).Value.Interface().(protoreflect.Message)
		if !ok || len(msg.GetUnknown()) == 0 {
			return nil
		}
		path := toFieldPath(p.Path)
		if path == nil {
			// Use "request" for the root message which has no field path.
			path = field.NewPath("request")
		}
		detail := "unknown field"
		if num, _, n := protowire.ConsumeTag(msg.GetUnknown()); n > 0 {
			detail = "unknown field with protobuf tag " + strconv.Itoa(int(num))
		}
		errs = append(errs, field.Invalid(path, field.OmitValueType{}, detail))
		return nil
	}); err != nil {
		errs = append(errs, field.InternalError(field.NewPath("request"), err))
	}
	return errs
}

// toFieldPath renders a protopath as a field.Path. The root step renders as
// nil.
func toFieldPath(p protopath.Path) *field.Path {
	var out *field.Path
	for _, step := range p {
		switch step.Kind() {
		case protopath.FieldAccessStep:
			out = out.Child(step.FieldDescriptor().TextName())
		case protopath.ListIndexStep:
			out = out.Index(step.ListIndex())
		case protopath.MapIndexStep:
			out = out.Key(step.MapIndex().String())
		}
	}
	return out
}
