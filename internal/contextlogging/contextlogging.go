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
	"log/slog"

	"go.opentelemetry.io/otel/trace"
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

func (h *ContextHandler) Handle(ctx context.Context, rec slog.Record) error {
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.HasTraceID() {
		traceID := spanContext.TraceID()
		rec.AddAttrs(slog.String("ate.dev/trace-id", traceID.String()))
	}

	return h.internal.Handle(ctx, rec)
}

func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{internal: h.internal.WithAttrs(attrs)}
}

func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{internal: h.internal.WithGroup(name)}
}
