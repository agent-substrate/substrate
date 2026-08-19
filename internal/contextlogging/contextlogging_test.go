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
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

const (
	testTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID  = "00f067aa0ba902b7"
)

func spanContext(t *testing.T, flags trace.TraceFlags) trace.SpanContext {
	t.Helper()
	traceID, err := trace.TraceIDFromHex(testTraceID)
	if err != nil {
		t.Fatalf("TraceIDFromHex(%q): %v", testTraceID, err)
	}
	spanID, err := trace.SpanIDFromHex(testSpanID)
	if err != nil {
		t.Fatalf("SpanIDFromHex(%q): %v", testSpanID, err)
	}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: flags})
}

func TestHandleTraceCorrelation(t *testing.T) {
	tests := []struct {
		name           string
		ctx            func(t *testing.T) context.Context
		wantTraceID    string
		wantSpanID     string
		wantTraceFlags string
	}{
		{
			name: "no span in context",
			ctx:  func(*testing.T) context.Context { return context.Background() },
		},
		{
			name: "invalid span context contributes nothing",
			ctx: func(*testing.T) context.Context {
				return trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{}))
			},
		},
		{
			name: "sampled span",
			ctx: func(t *testing.T) context.Context {
				return trace.ContextWithSpanContext(context.Background(), spanContext(t, trace.FlagsSampled))
			},
			wantTraceID:    testTraceID,
			wantSpanID:     testSpanID,
			wantTraceFlags: "01",
		},
		{
			// An unsampled record still says which request it belongs to; the flags
			// are what tells a reader why the trace is not in the backend.
			name: "unsampled span still correlates",
			ctx: func(t *testing.T) context.Context {
				return trace.ContextWithSpanContext(context.Background(), spanContext(t, 0))
			},
			wantTraceID:    testTraceID,
			wantSpanID:     testSpanID,
			wantTraceFlags: "00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))
			logger.InfoContext(tt.ctx(t), "something happened")

			var rec map[string]any
			if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
				t.Fatalf("failed to parse log record %q: %v", buf.String(), err)
			}

			for field, want := range map[string]string{
				ateattr.LogTraceIDField:    tt.wantTraceID,
				ateattr.LogSpanIDField:     tt.wantSpanID,
				ateattr.LogTraceFlagsField: tt.wantTraceFlags,
			} {
				got, present := rec[field]
				if want == "" {
					if present {
						t.Errorf("%s = %v, want absent", field, got)
					}
					continue
				}
				if got != want {
					t.Errorf("%s = %v, want %q", field, got, want)
				}
			}
		})
	}
}

// TestHandleUngroupedKeepsTraceFieldsTopLevel pins the placement the spec
// requires: a collector only lifts these onto the log record from the top level.
func TestHandleUngroupedKeepsTraceFieldsTopLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext(t, trace.FlagsSampled))
	logger.With(slog.String("component", "atelet")).InfoContext(ctx, "something happened", slog.String("id", "abc"))

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("failed to parse log record %q: %v", buf.String(), err)
	}
	if rec[ateattr.LogTraceIDField] != testTraceID {
		t.Errorf("%s = %v, want %q at the top level, got record %v", ateattr.LogTraceIDField, rec[ateattr.LogTraceIDField], testTraceID, rec)
	}
}
