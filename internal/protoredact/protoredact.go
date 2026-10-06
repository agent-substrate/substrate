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
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Placeholder replaces the value of a singular string field marked
// debug_redact. Names and structure are kept so a log line still shows which
// fields were set.
const Placeholder = "[REDACTED]"

// NeedsRedaction reports whether logging m as it is would expose a
// debug_redact field: m is a valid message, its type can reach such a field,
// and at least one is populated. A nil message, a type that cannot reach a
// labeled field (Actor, ListActorsResponse, Worker, ...) and a message whose
// labeled fields are all unset (an ActorTemplate with no env) all answer
// false. The type check is memoized; the populated walk stops at the first
// hit and visits only sub-messages whose type can reach a labeled field.
func NeedsRedaction(m proto.Message) bool {
	if isNil(m) {
		return false
	}
	mr := m.ProtoReflect()
	return mr.IsValid() && reachesDebugRedact(mr.Descriptor()) && hasPopulatedDebugRedact(mr)
}

// Redacted returns m with every debug_redact field masked, without modifying
// m. When NeedsRedaction(m) is false m itself is returned: there is nothing
// to mask, so nothing to copy, and callers must treat the result as
// read-only. Otherwise a deep copy is made and masked.
func Redacted(m proto.Message) proto.Message {
	if !NeedsRedaction(m) {
		return m
	}
	clone := proto.Clone(m)
	redact(clone.ProtoReflect())
	return clone
}

// isNil reports whether m is a nil interface or a typed nil pointer. A typed
// nil generated message answers ProtoReflect with a usable value, but other
// implementations (dynamicpb) return the nil receiver itself, which faults
// on the first method call.
func isNil(m proto.Message) bool {
	if m == nil {
		return true
	}
	rv := reflect.ValueOf(m)
	return rv.Kind() == reflect.Pointer && rv.IsNil()
}

// reachable memoizes reachesDebugRedact per message descriptor. The answer
// depends only on the schema, which is fixed for the life of the process, so
// it is computed once per type.
var reachable sync.Map // protoreflect.MessageDescriptor -> bool

// reachesDebugRedact reports whether a message of type md can carry a
// debug_redact field, directly or through any nested message, list element
// or map value, however deep.
func reachesDebugRedact(md protoreflect.MessageDescriptor) bool {
	if v, ok := reachable.Load(md); ok {
		return v.(bool)
	}
	r := walkReachesDebugRedact(md, map[protoreflect.MessageDescriptor]bool{})
	reachable.Store(md, r)
	return r
}

// walkReachesDebugRedact is the uncached walk. seen guards recursive schemas
// (a message type that contains itself).
func walkReachesDebugRedact(md protoreflect.MessageDescriptor, seen map[protoreflect.MessageDescriptor]bool) bool {
	if seen[md] {
		return false
	}
	seen[md] = true
	// Extension fields are not listed by Fields(). A type that admits
	// extensions may carry a labeled one, so it counts as reaching a secret
	// and the populated walk, which does visit extensions, decides.
	if md.ExtensionRanges().Len() > 0 {
		return true
	}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if hasDebugRedact(fd) {
			return true
		}
		// fd.Message() is the nested type for message and group fields and
		// the entry type for maps, whose value field is checked in turn.
		if sub := fd.Message(); sub != nil && walkReachesDebugRedact(sub, seen) {
			return true
		}
	}
	return false
}

// hasDebugRedact reports whether fd carries [debug_redact = true].
func hasDebugRedact(fd protoreflect.FieldDescriptor) bool {
	opts, ok := fd.Options().(*descriptorpb.FieldOptions)
	return ok && opts.GetDebugRedact()
}

// hasPopulatedDebugRedact reports whether msg has a populated debug_redact
// field anywhere below it, stopping at the first one found. It walks the
// declared fields with Has and Get rather than Range, so the common outcome,
// a labeled type with nothing set, costs no closure allocations; Range is
// used only for a type that admits extensions, which Fields() does not list.
func hasPopulatedDebugRedact(msg protoreflect.Message) bool {
	md := msg.Descriptor()
	if md.ExtensionRanges().Len() > 0 {
		found := false
		msg.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			found = populatedFieldHasDebugRedact(fd, value)
			return !found
		})
		return found
	}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if hasDebugRedact(fd) {
			if msg.Has(fd) {
				return true
			}
			continue
		}
		if sub := fd.Message(); sub != nil && reachesDebugRedact(sub) && msg.Has(fd) && populatedFieldHasDebugRedact(fd, msg.Get(fd)) {
			return true
		}
	}
	return false
}

// populatedFieldHasDebugRedact is hasPopulatedDebugRedact for one populated
// field: the field itself is labeled, or a message it holds has a populated
// labeled field below it.
func populatedFieldHasDebugRedact(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
	if hasDebugRedact(fd) {
		return true
	}
	if sub := fd.Message(); sub == nil || !reachesDebugRedact(sub) {
		return false
	}
	switch {
	case fd.IsMap():
		// A map field reports MessageKind (its entry type) but its value is
		// a Map, so this case must come before the plain message case.
		found := false
		value.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
			found = hasPopulatedDebugRedact(mv.Message())
			return !found
		})
		return found
	case fd.IsList():
		list := value.List()
		for i := 0; i < list.Len(); i++ {
			if hasPopulatedDebugRedact(list.Get(i).Message()) {
				return true
			}
		}
		return false
	default:
		return hasPopulatedDebugRedact(value.Message())
	}
}

// redact masks, in place, every populated field of msg that carries the
// debug_redact option, recursing through nested messages, lists and map
// values. Singular string fields are replaced with Placeholder; any other
// kind (bytes, repeated, map, message, ...) is cleared, because no honest
// placeholder exists for them. Sub-messages whose type cannot carry a
// debug_redact field are not visited. Range, not Fields(), so that
// extension fields are covered.
func redact(msg protoreflect.Message) {
	msg.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if hasDebugRedact(fd) {
			if fd.Kind() == protoreflect.StringKind && !fd.IsList() {
				msg.Set(fd, protoreflect.ValueOfString(Placeholder))
			} else {
				msg.Clear(fd)
			}
			return true
		}
		if sub := fd.Message(); sub == nil || !reachesDebugRedact(sub) {
			return true
		}
		switch {
		case fd.IsMap():
			value.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				redact(mv.Message())
				return true
			})
		case fd.IsList():
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				redact(list.Get(i).Message())
			}
		default:
			redact(value.Message())
		}
		return true
	})
}
