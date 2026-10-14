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

// Package removedfields lists the fields the API removed that a client built
// against an older API may still send, and accepts them for a transition: the
// field stays among its message's unknown fields, nothing reads it, and it is
// stored and returned as it was sent, so a client that compares what it reads
// back with what it wrote sees no difference.
package removedfields

import (
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Field is a removed field. It is matched by number and wire type, so a later
// field that reuses the number with another type is never mistaken for it.
type Field struct {
	Name     protoreflect.Name
	Number   protowire.Number
	WireType protowire.Type
}

var fields = map[protoreflect.FullName][]Field{
	// on_pause: a pause captures the template's snapshot scope, so the value
	// has nothing left to set.
	"ateapi.SnapshotConfig": {{Name: "on_pause", Number: 1, WireType: protowire.VarintType}},
}

// Lookup returns the removed field of the message msg with this number and
// wire type.
func Lookup(msg protoreflect.FullName, num protowire.Number, typ protowire.Type) (Field, bool) {
	for _, f := range fields[msg] {
		if f.Number == num && f.WireType == typ {
			return f, true
		}
	}
	return Field{}, false
}

// DiscardUnknown drops the unknown fields of m and of every message it holds,
// as proto.UnmarshalOptions.DiscardUnknown does at decode time, except the
// removed fields.
func DiscardUnknown(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList():
			if fd.Message() != nil {
				for l, i := v.List(), 0; i < l.Len(); i++ {
					DiscardUnknown(l.Get(i).Message())
				}
			}
		case fd.IsMap():
			if fd.MapValue().Message() != nil {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					DiscardUnknown(mv.Message())
					return true
				})
			}
		case fd.Message() != nil:
			DiscardUnknown(v.Message())
		}
		return true
	})
	if b := m.GetUnknown(); b != nil {
		m.SetUnknown(keepRemoved(m.Descriptor().FullName(), b))
	}
}

// keepRemoved returns the removed fields of the message msg among its unknown
// fields b, or nil when there is none.
func keepRemoved(msg protoreflect.FullName, b protoreflect.RawFields) protoreflect.RawFields {
	if len(fields[msg]) == 0 {
		return nil
	}
	var kept protoreflect.RawFields
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			break
		}
		vn := protowire.ConsumeFieldValue(num, typ, b[n:])
		if vn < 0 {
			break
		}
		if _, ok := Lookup(msg, num, typ); ok {
			kept = append(kept, b[:n+vn]...)
		}
		b = b[n+vn:]
	}
	return kept
}
