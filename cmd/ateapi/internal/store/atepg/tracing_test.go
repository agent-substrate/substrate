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

package atepg

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// withRecordingTracer installs a recording global tracer provider for the
// test and returns the recorder; the previous provider is restored on cleanup.
func withRecordingTracer(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return sr
}

func TestQueryTracerJoinsParentTrace(t *testing.T) {
	sr := withRecordingTracer(t)
	ctx, parent := otel.Tracer("test").Start(context.Background(), "parent")

	qt := queryTracer{}
	qctx := qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT id FROM actors WHERE name = $1"})
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{})
	parent.End()

	var dbSpan sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == "db.SELECT" {
			dbSpan = s
		}
	}
	if dbSpan == nil {
		t.Fatalf("no db.SELECT span recorded; got %d spans", len(sr.Ended()))
	}
	if got, want := dbSpan.Parent().SpanID(), parent.SpanContext().SpanID(); got != want {
		t.Errorf("db span parent = %s, want the RPC span %s", got, want)
	}
	if dbSpan.SpanKind() != trace.SpanKindClient {
		t.Errorf("db span kind = %v, want client", dbSpan.SpanKind())
	}
	var gotQueryText string
	for _, a := range dbSpan.Attributes() {
		if string(a.Key) == "db.query.text" {
			gotQueryText = a.Value.AsString()
		}
	}
	if gotQueryText != "SELECT id FROM actors WHERE name = $1" {
		t.Errorf("db.query.text = %q", gotQueryText)
	}
}

func TestQueryTracerSkipsWithoutParent(t *testing.T) {
	sr := withRecordingTracer(t)

	qt := queryTracer{}
	ctx := context.Background()
	qctx := qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	if qctx != ctx {
		t.Error("TraceQueryStart without a parent span must return the context unchanged")
	}
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{})

	if n := len(sr.Ended()); n != 0 {
		t.Errorf("background statement recorded %d spans, want 0", n)
	}
}

func TestQueryTracerErrorStatus(t *testing.T) {
	sr := withRecordingTracer(t)
	ctx, parent := otel.Tracer("test").Start(context.Background(), "parent")
	qt := queryTracer{}

	// A real failure marks the span.
	qctx := qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "UPDATE actors SET state = $1"})
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{Err: errors.New("deadlock detected")})

	// ErrNoRows is an expected lookup outcome and must not mark the span.
	qctx = qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT id FROM actors"})
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{Err: pgx.ErrNoRows})
	parent.End()

	var updateStatus, selectStatus codes.Code
	for _, s := range sr.Ended() {
		switch s.Name() {
		case "db.UPDATE":
			updateStatus = s.Status().Code
		case "db.SELECT":
			selectStatus = s.Status().Code
		}
	}
	if updateStatus != codes.Error {
		t.Errorf("failed statement status = %v, want Error", updateStatus)
	}
	if selectStatus == codes.Error {
		t.Error("ErrNoRows must not set Error status")
	}
}

func TestQuerySpanName(t *testing.T) {
	for sql, want := range map[string]string{
		"SELECT 1":                   "db.SELECT",
		"  insert into t values($1)": "db.INSERT",
		"WITH cte AS (SELECT 1) SELECT * FROM cte": "db.WITH",
		"": "db.query",
	} {
		if got := querySpanName(sql); got != want {
			t.Errorf("querySpanName(%q) = %q, want %q", sql, got, want)
		}
	}
}
