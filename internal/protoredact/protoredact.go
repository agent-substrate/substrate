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

// Package protoredact masks the fields of a protobuf message that are marked
// [debug_redact = true] in the schema, so the message can be logged without
// exposing the secrets it carries. The label is set next to the field in the
// .proto file; this package is the reader for it, since the Go protobuf
// runtime does not act on the option itself.
package protoredact

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Placeholder replaces the value of a singular string field marked
// debug_redact. Names and structure are kept so a log line still shows which
// fields were set.
const Placeholder = "[REDACTED]"

// ForLog returns a value safe to log. A proto message is cloned and every
// field carrying the debug_redact option is masked in the copy; any other
// value is returned unchanged. The original message is never modified.
func ForLog(v any) any {
	msg, ok := v.(proto.Message)
	if !ok {
		return v
	}
	return Clone(msg)
}

// Clone returns a deep copy of m with every debug_redact field masked. m is
// not modified.
//
// TODO: memoize, per message descriptor, whether the type can reach a
// debug_redact field at all. Most of our RPC messages cannot (Actor,
// ListActorsResponse, Worker, ...), and for those the clone and the walk are
// pure overhead: they could be returned as-is after a cache lookup. The same
// answer lets Redact skip sub-messages whose type has no redacted fields
// instead of traversing them. Benchmarks for both are linked from #1743.
func Clone(m proto.Message) proto.Message {
	clone := proto.Clone(m)
	Redact(clone.ProtoReflect())
	return clone
}

// IsRedacted reports whether fd carries [debug_redact = true].
func IsRedacted(fd protoreflect.FieldDescriptor) bool {
	opts, ok := fd.Options().(*descriptorpb.FieldOptions)
	return ok && opts.GetDebugRedact()
}

// Redact masks, in place, every populated field of msg that carries the
// debug_redact option, recursing through nested messages, lists and map
// values. Singular string fields are replaced with Placeholder; any other
// kind (bytes, repeated, map, message, ...) is cleared, because no honest
// placeholder exists for them.
func Redact(msg protoreflect.Message) {
	msg.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if IsRedacted(fd) {
			if fd.Kind() == protoreflect.StringKind && !fd.IsList() && !fd.IsMap() {
				msg.Set(fd, protoreflect.ValueOfString(Placeholder))
			} else {
				msg.Clear(fd)
			}
			return true
		}
		switch {
		case fd.IsMap():
			// A map field reports MessageKind (its entry type) but its value
			// is a Map, so this case must come before the message case.
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				value.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					Redact(mv.Message())
					return true
				})
			}
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
				list := value.List()
				for i := 0; i < list.Len(); i++ {
					Redact(list.Get(i).Message())
				}
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			Redact(value.Message())
		}
		return true
	})
}
