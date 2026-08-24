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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func toGRPCStatusError(errs field.ErrorList) error {
	return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
}

// validateNoUnknownFields reports an error for every unknown field in m, at
// every level of nesting.
//
// A client newer than this binary can set fields this binary has no descriptor
// for. protobuf keeps those bytes on the parsed message, so an Update — which
// replaces the whole object — would persist them. The server cannot validate
// such a field.
func validateNoUnknownFields(m proto.Message, fldPath *field.Path) field.ErrorList {
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
		// Report the path of the unknwown field
		errs = append(errs, field.Invalid(toFieldPath(p.Path, fldPath), field.OmitValueType{}, ""))
		return nil
	}); err != nil {
		errs = append(errs, field.InternalError(fldPath, err))
	}
	return errs
}

// toFieldPath renders a protopath as a field.Path rooted at root.
func toFieldPath(p protopath.Path, root *field.Path) *field.Path {
	out := root
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
