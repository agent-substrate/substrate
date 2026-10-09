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

// Package protoshapetest describes the field set of a proto message, so tests
// of the benchmark fake data plane can pin the messages the fakes exchange
// with ate-api-server to the fields each test expects. A field added to one of
// them fails the test, and whoever adds it decides what the fake should do
// with it.
package protoshapetest

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Fields returns one line per field of m, in declaration order: its number,
// name, cardinality, type and oneof, e.g. "9 egress_gateway optional
// atelet.EgressGateway". A message with no fields gives nil.
func Fields(m proto.Message) []string {
	var out []string
	fields := m.ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		s := fmt.Sprintf("%d %s", f.Number(), f.Name())
		if f.IsList() {
			s += " repeated"
		}
		if f.HasOptionalKeyword() {
			s += " optional"
		}
		s += " " + typeName(f)
		if o := f.ContainingOneof(); o != nil && !o.IsSynthetic() {
			s += " oneof " + string(o.Name())
		}
		out = append(out, s)
	}
	return out
}

func typeName(f protoreflect.FieldDescriptor) string {
	switch {
	case f.IsMap():
		return fmt.Sprintf("map<%s, %s>", typeName(f.MapKey()), typeName(f.MapValue()))
	case f.Message() != nil:
		return string(f.Message().FullName())
	case f.Enum() != nil:
		return string(f.Enum().FullName())
	}
	return f.Kind().String()
}
