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

	"github.com/agent-substrate/substrate/internal/ateattr"
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

// Handle records the active span on the log record under the field names the OTel
// spec fixes for non-OTLP log formats, so a collector can lift them onto the log
// record's own trace fields. Gated on the whole span context being valid: a trace
// ID without a span ID names a request but not the operation within it.
func (h *ContextHandler) Handle(ctx context.Context, rec slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		rec.AddAttrs(
			slog.String(ateattr.LogTraceIDField, sc.TraceID().String()),
			slog.String(ateattr.LogSpanIDField, sc.SpanID().String()),
			slog.String(ateattr.LogTraceFlagsField, fmt.Sprintf("%02x", byte(sc.TraceFlags()))),
		)
	}

	return h.internal.Handle(ctx, rec)
}

func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{internal: h.internal.WithAttrs(attrs)}
}

func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{internal: h.internal.WithGroup(name)}
}
