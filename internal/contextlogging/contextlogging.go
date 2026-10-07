// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package contextlogging

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/protoredact"
)

type ContextHandler struct {
	internal slog.Handler
}

func NewHandler(internal slog.Handler) *ContextHandler {
	return &ContextHandler{
		internal: internal,
	}
}

func (h *ContextHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.internal.Enabled(ctx, lvl)
}

// Handle redacts protobuf attributes and records the active span on the log
// record.
//
// Redaction: an attribute whose value is a proto.Message with a populated
// debug_redact field is replaced by a copy with those fields masked (see
// protoredact). The message may be the attribute's own value, a member of a
// group at any depth, or what a LogValuer resolves to; attributes bound with
// With are handled once by WithAttrs. Doing this here rather than at each
// call site means any slog call in a process that installs this handler is
// covered, not only the gRPC interceptor.
//
// Not covered: a message reached only through a slice, map, or non-proto
// struct (slog.Any("env", []*EnvVar{...})), a message formatted into a string
// before the call (fmt.Sprintf("%v", msg)), a message packed inside
// google.protobuf.Any, and the OTLP copy of an actor event, which
// internal/actorevent builds from the attributes itself. Log such a value as
// a direct attribute instead.
//
// Cost: the record is copied only when an attribute changes. A message that
// needs masking is one such change; a LogValuer is the other, since it is
// resolved here once and the record keeps the resolved value rather than
// having the terminal handler compute it again. A record with neither, which
// is nearly all of them, passes through after a scan that allocates nothing.
//
// Trace correlation: the span is recorded under the field names the OTel spec
// fixes for non-OTLP log formats, so a collector can lift them onto the log
// record's own trace fields. Gated on the whole span context being valid: a
// trace ID without a span ID names a request but not the operation within it.
func (h *ContextHandler) Handle(ctx context.Context, rec slog.Record) error {
	rec = redactRecord(rec)
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		rec.AddAttrs(
			slog.String(ateattr.LogTraceIDField, sc.TraceID().String()),
			slog.String(ateattr.LogSpanIDField, sc.SpanID().String()),
			slog.String(ateattr.LogTraceFlagsField, fmt.Sprintf("%02x", byte(sc.TraceFlags()))),
		)
	}

	return h.internal.Handle(ctx, rec)
}

// WithAttrs redacts the pre-bound attributes once, when they are bound, since
// Handle never sees them again.
func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{internal: h.internal.WithAttrs(redactAttrs(attrs))}
}

func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{internal: h.internal.WithGroup(name)}
}

// redactRecord returns rec with every attribute that redactValue changes
// replaced. A record where nothing changes is returned as it is. slog.Record
// cannot be edited in place, so a copy is started at the first changed
// attribute, with the ones before it replayed unchanged.
func redactRecord(rec slog.Record) slog.Record {
	var out slog.Record
	copying := false
	i := 0
	rec.Attrs(func(a slog.Attr) bool {
		v, changed := redactValue(a.Value)
		if changed && !copying {
			copying = true
			out = slog.NewRecord(rec.Time, rec.Level, rec.Message, rec.PC)
			replayed := 0
			rec.Attrs(func(before slog.Attr) bool {
				if replayed == i {
					return false
				}
				out.AddAttrs(before)
				replayed++
				return true
			})
		}
		if copying {
			out.AddAttrs(slog.Attr{Key: a.Key, Value: v})
		}
		i++
		return true
	})
	if !copying {
		return rec
	}
	return out
}

// redactAttrs is redactRecord for a plain attribute slice (WithAttrs). The
// input slice is returned as it is when nothing changes and is never
// modified.
func redactAttrs(attrs []slog.Attr) []slog.Attr {
	var out []slog.Attr
	for i, a := range attrs {
		v, changed := redactValue(a.Value)
		if changed && out == nil {
			out = make([]slog.Attr, i, len(attrs))
			copy(out, attrs[:i])
		}
		if out != nil {
			out = append(out, slog.Attr{Key: a.Key, Value: v})
		}
	}
	if out == nil {
		return attrs
	}
	return out
}

// redactValue returns v with every proto.Message that needs redaction
// replaced by its redacted copy, and whether anything changed. A LogValuer
// is resolved and counts as changed, so LogValue runs once per line and the
// record keeps its result. Other kinds are returned as they are with nothing
// allocated.
func redactValue(v slog.Value) (slog.Value, bool) {
	switch v.Kind() {
	case slog.KindLogValuer:
		r, _ := redactValue(v.Resolve())
		return r, true
	case slog.KindAny:
		if m, ok := v.Any().(proto.Message); ok {
			if r, changed := protoredact.Redacted(m); changed {
				return slog.AnyValue(r), true
			}
		}
	case slog.KindGroup:
		g := v.Group()
		var out []slog.Attr
		for i, a := range g {
			nv, changed := redactValue(a.Value)
			if changed && out == nil {
				out = make([]slog.Attr, i, len(g))
				copy(out, g[:i])
			}
			if out != nil {
				out = append(out, slog.Attr{Key: a.Key, Value: nv})
			}
		}
		if out != nil {
			return slog.GroupValue(out...), true
		}
	}
	return v, false
}
