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

// Package logredact keeps credentials out of log output.
//
// A protobuf field that can carry a credential is labeled in its .proto file
// with the standard `debug_redact` option. This package reads that label:
// Handler clears such fields from every message a record carries, and Sanitize
// does the same for one value. Redaction follows the schema, so labeling a new
// field is all it takes to keep it out of logs.
package logredact

import (
	"context"
	"log/slog"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// protoMessageType is the interface type a protobuf message implements.
var protoMessageType = reflect.TypeOf((*proto.Message)(nil)).Elem()

// Handler wraps an slog.Handler and removes `debug_redact` protobuf fields from
// every record before the wrapped handler sees it.
//
// A message is recognized whether it is logged directly or as an element of a
// slice or array, a value of a map (map keys are left alone), or a nested
// group's attribute. A custom struct that merely holds a protobuf message in
// one of its fields is not walked: log the message itself instead.
type Handler struct {
	inner slog.Handler
}

var _ slog.Handler = (*Handler)(nil)

// NewHandler returns a handler that redacts protobuf fields marked with the
// `debug_redact` option before passing each record to inner.
func NewHandler(inner slog.Handler) *Handler {
	return &Handler{inner: inner}
}

// Enabled reports whether inner is enabled for lvl.
func (h *Handler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.inner.Enabled(ctx, lvl)
}

// Handle passes rec to inner with sensitive protobuf fields cleared. A record
// that carries nothing redactable is forwarded untouched.
func (h *Handler) Handle(ctx context.Context, rec slog.Record) error {
	if recordNeedsRedaction(rec) {
		out := slog.NewRecord(rec.Time, rec.Level, rec.Message, rec.PC)
		rec.Attrs(func(a slog.Attr) bool {
			sa, _ := sanitizeAttr(a)
			out.AddAttrs(sa)
			return true
		})
		rec = out
	}
	return h.inner.Handle(ctx, rec)
}

// WithAttrs returns a handler whose records carry attrs, redacted as if they
// had been logged with the record.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		out[i], _ = sanitizeAttr(a)
	}
	return &Handler{inner: h.inner.WithAttrs(out)}
}

// WithGroup returns a handler that qualifies every attribute with name.
func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{inner: h.inner.WithGroup(name)}
}

// Sanitize returns v with `debug_redact` protobuf fields removed. A message is
// copied before it is changed, so the caller's value is never mutated, and v
// itself is returned when it can carry nothing redactable.
func Sanitize(v any) any {
	out, _ := sanitizeAny(v)
	return out
}

// recordNeedsRedaction reports whether any attribute could hold a redactable
// message. It never resolves a LogValuer, so Handle knows to run the full path
// when one appears instead of calling it twice.
func recordNeedsRedaction(rec slog.Record) bool {
	needs := false
	rec.Attrs(func(a slog.Attr) bool {
		if attrNeedsRedaction(a) {
			needs = true
			return false
		}
		return true
	})
	return needs
}

func attrNeedsRedaction(a slog.Attr) bool {
	switch a.Value.Kind() {
	case slog.KindGroup:
		for _, child := range a.Value.Group() {
			if attrNeedsRedaction(child) {
				return true
			}
		}
		return false
	case slog.KindAny:
		return anyNeedsRedaction(a.Value.Any())
	case slog.KindLogValuer:
		return true
	default:
		return false
	}
}

func anyNeedsRedaction(v any) bool {
	if _, ok := v.(proto.Message); ok {
		return true
	}
	switch reflect.ValueOf(v).Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return mayHoldProto(reflect.TypeOf(v))
	default:
		return false
	}
}

// sanitizeAttr returns a with its value resolved and any sensitive protobuf
// fields cleared. The second result reports whether the returned attribute
// differs from what was passed in.
func sanitizeAttr(a slog.Attr) (slog.Attr, bool) {
	val := a.Value.Resolve()
	// A resolved LogValuer must be handed to the next handler even when the
	// resolved value needed no redaction.
	resolved := a.Value.Kind() == slog.KindLogValuer

	if val.Kind() == slog.KindGroup {
		group, changed := sanitizeGroup(val.Group())
		if changed {
			a.Value = slog.GroupValue(group...)
			return a, true
		}
		if resolved {
			a.Value = val
			return a, true
		}
		return a, false
	}

	if v, changed := sanitizeAny(val.Any()); changed {
		a.Value = slog.AnyValue(v)
		return a, true
	}
	if resolved {
		a.Value = val
		return a, true
	}
	return a, false
}

func sanitizeGroup(attrs []slog.Attr) ([]slog.Attr, bool) {
	out := make([]slog.Attr, len(attrs))
	changed := false
	for i, a := range attrs {
		sa, c := sanitizeAttr(a)
		out[i] = sa
		changed = changed || c
	}
	return out, changed
}

// sanitizeAny returns v with sensitive protobuf fields cleared, and reports
// whether the returned value differs from v.
func sanitizeAny(v any) (any, bool) {
	switch x := v.(type) {
	case nil:
		return nil, false
	case proto.Message:
		return sanitizeMessage(x)
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		return sanitizeSequence(rv)
	case reflect.Map:
		return sanitizeMap(rv)
	default:
		return v, false
	}
}

func sanitizeSequence(rv reflect.Value) (any, bool) {
	if !mayHoldProto(rv.Type().Elem()) {
		return rv.Interface(), false
	}

	// Copy before touching an element, so a container that needs redaction never
	// shares backing storage with the value the caller logged.
	var out reflect.Value
	if rv.Kind() == reflect.Slice {
		out = reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
		reflect.Copy(out, rv)
	} else {
		out = reflect.New(rv.Type()).Elem()
		out.Set(rv)
	}

	changed := false
	for i := 0; i < rv.Len(); i++ {
		ev, c := sanitizeAny(rv.Index(i).Interface())
		if !c {
			continue
		}
		out.Index(i).Set(reflect.ValueOf(ev))
		changed = true
	}
	if !changed {
		return rv.Interface(), false
	}
	return out.Interface(), true
}

func sanitizeMap(rv reflect.Value) (any, bool) {
	if !mayHoldProto(rv.Type().Elem()) {
		return rv.Interface(), false
	}

	var out reflect.Value
	iter := rv.MapRange()
	for iter.Next() {
		ev, c := sanitizeAny(iter.Value().Interface())
		if !c {
			continue
		}
		if !out.IsValid() {
			out = reflect.MakeMapWithSize(rv.Type(), rv.Len())
			existing := rv.MapRange()
			for existing.Next() {
				out.SetMapIndex(existing.Key(), existing.Value())
			}
		}
		out.SetMapIndex(iter.Key(), reflect.ValueOf(ev))
	}
	if !out.IsValid() {
		return rv.Interface(), false
	}
	return out.Interface(), true
}

func sanitizeMessage(m proto.Message) (any, bool) {
	if m == nil || !m.ProtoReflect().IsValid() {
		return m, false
	}
	msg := m.ProtoReflect()
	if !mayContainRedactedField(msg.Descriptor()) || !walkRedacted(msg, false) {
		return m, false
	}
	clone := proto.Clone(m)
	walkRedacted(clone.ProtoReflect(), true)
	return clone, true
}

// mayHoldProto reports whether a value of type t is, or can contain, a protobuf
// message. It mirrors what sanitizeAny walks: pointers to containers are not
// walked, so they are not claimed here either.
func mayHoldProto(t reflect.Type) bool {
	if t.Implements(protoMessageType) {
		return true
	}
	switch t.Kind() {
	case reflect.Interface:
		return true
	case reflect.Slice, reflect.Array, reflect.Map:
		return mayHoldProto(t.Elem())
	default:
		return false
	}
}

// redactedFieldCache maps a message descriptor to whether it can reach a
// `debug_redact` field, so the message graph is walked once per type rather
// than once per log call.
var redactedFieldCache sync.Map // protoreflect.MessageDescriptor -> bool

// mayContainRedactedField reports whether a message of this type, at any depth,
// has a field marked `debug_redact`.
func mayContainRedactedField(md protoreflect.MessageDescriptor) bool {
	if cached, ok := redactedFieldCache.Load(md); ok {
		return cached.(bool)
	}
	// Record true before descending: a type that reaches itself through a cycle
	// then terminates with the conservative answer.
	redactedFieldCache.Store(md, true)
	found := scanForRedactedField(md)
	redactedFieldCache.Store(md, found)
	return found
}

func scanForRedactedField(md protoreflect.MessageDescriptor) bool {
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fieldIsRedacted(fd) {
			return true
		}
		switch {
		case fd.IsMap():
			if mv := fd.MapValue(); isMessage(mv) && mayContainRedactedField(mv.Message()) {
				return true
			}
		case isMessage(fd):
			if mayContainRedactedField(fd.Message()) {
				return true
			}
		}
	}
	return false
}

// walkRedacted reports whether msg carries a populated field marked
// `debug_redact`, at any depth. It descends into nested messages, repeated
// message fields, and message values of maps. With clear set it also clears
// those fields, which is only safe on a copy of the caller's message; without
// it the walk stops at the first field it finds.
func walkRedacted(msg protoreflect.Message, clear bool) bool {
	found := false
	msg.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if fieldIsRedacted(fd) {
			found = true
			if !clear {
				return false
			}
			msg.Clear(fd)
			// Keep going: a sibling field may be redacted too.
			return true
		}
		switch {
		case fd.IsMap():
			if mv := fd.MapValue(); isMessage(mv) {
				value.Map().Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool {
					found = walkRedacted(v.Message(), clear) || found
					return clear || !found
				})
			}
		case fd.IsList():
			if isMessage(fd) {
				list := value.List()
				for i := 0; i < list.Len(); i++ {
					found = walkRedacted(list.Get(i).Message(), clear) || found
					if !clear && found {
						break
					}
				}
			}
		case isMessage(fd):
			found = walkRedacted(value.Message(), clear) || found
		}
		return clear || !found
	})
	return found
}

func isMessage(fd protoreflect.FieldDescriptor) bool {
	return fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind
}

func fieldIsRedacted(fd protoreflect.FieldDescriptor) bool {
	opts, ok := fd.Options().(*descriptorpb.FieldOptions)
	return ok && opts.GetDebugRedact()
}
