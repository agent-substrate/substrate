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
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// newTestQueryTracer builds a queryTracer against a local recording provider,
// never touching the global one, so these tests stay parallel-safe.
func newTestQueryTracer(t *testing.T) (*queryTracer, *sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	cc, err := pgx.ParseConfig("postgres://ate@db.example.internal:5433/atedb?sslmode=disable")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	return newQueryTracer(tp, cc), tp, sr
}

func spanAttrs(s sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	m := make(map[attribute.Key]attribute.Value, len(s.Attributes()))
	for _, kv := range s.Attributes() {
		m[kv.Key] = kv.Value
	}
	return m
}

func findSpan(t *testing.T, sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range sr.Ended() {
		if s.Name() == name {
			return s
		}
	}
	var names []string
	for _, s := range sr.Ended() {
		names = append(names, s.Name())
	}
	t.Fatalf("no %q span recorded; got %q", name, names)
	return nil
}

func TestQueryTracerJoinsParentTrace(t *testing.T) {
	t.Parallel()
	qt, tp, sr := newTestQueryTracer(t)
	ctx, parent := tp.Tracer("test").Start(context.Background(), "parent")

	const sql = "SELECT id FROM actors WHERE name = $1"
	qctx := qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: sql})
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{})
	parent.End()

	dbSpan := findSpan(t, sr, "SELECT actors")
	if got, want := dbSpan.Parent().SpanID(), parent.SpanContext().SpanID(); got != want {
		t.Errorf("db span parent = %s, want the RPC span %s", got, want)
	}
	if dbSpan.SpanKind() != trace.SpanKindClient {
		t.Errorf("db span kind = %v, want client", dbSpan.SpanKind())
	}
	if dbSpan.Status().Code != codes.Unset {
		t.Errorf("status = %v, want Unset", dbSpan.Status().Code)
	}
	attrs := spanAttrs(dbSpan)
	for key, want := range map[attribute.Key]attribute.Value{
		semconv.DBSystemNameKey:     semconv.DBSystemNamePostgreSQL.Value,
		semconv.ServerAddressKey:    attribute.StringValue("db.example.internal"),
		semconv.ServerPortKey:       attribute.IntValue(5433),
		semconv.DBNamespaceKey:      attribute.StringValue("atedb"),
		semconv.DBQueryTextKey:      attribute.StringValue(sql),
		semconv.DBOperationNameKey:  attribute.StringValue("SELECT"),
		semconv.DBCollectionNameKey: attribute.StringValue("actors"),
		semconv.DBQuerySummaryKey:   attribute.StringValue("SELECT actors"),
	} {
		if got, ok := attrs[key]; !ok {
			t.Errorf("missing %s", key)
		} else if got != want {
			t.Errorf("%s = %v, want %v", key, got.String(), want.String())
		}
	}
	for _, key := range []attribute.Key{semconv.ErrorTypeKey, semconv.DBResponseStatusCodeKey} {
		if _, ok := attrs[key]; ok {
			t.Errorf("successful statement must not carry %s", key)
		}
	}
}

func TestQueryTracerSkipsWithoutSampledParent(t *testing.T) {
	t.Parallel()
	qt, _, sr := newTestQueryTracer(t)

	// No span at all: background work.
	ctx := context.Background()
	qctx := qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	if qctx != ctx {
		t.Error("TraceQueryStart without a parent span must return the context unchanged")
	}
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{})

	// A parent that was not sampled: its children could never be sampled
	// either, and ending the statement must leave the parent untouched.
	unsampled := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()), sdktrace.WithSpanProcessor(sr))
	ctx, parent := unsampled.Tracer("test").Start(context.Background(), "parent")
	if parent.SpanContext().IsSampled() {
		t.Fatal("test setup: parent must not be sampled")
	}
	qctx = qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	if qctx != ctx {
		t.Error("TraceQueryStart under an unsampled parent must return the context unchanged")
	}
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{})
	if n := len(sr.Ended()); n != 0 {
		t.Errorf("unsampled statements recorded %d spans, want 0", n)
	}
}

// recordOnlySampler records every span without sampling it, the one
// decision that yields a parent that is recording yet not sampled.
type recordOnlySampler struct{}

func (recordOnlySampler) ShouldSample(sdktrace.SamplingParameters) sdktrace.SamplingResult {
	return sdktrace.SamplingResult{Decision: sdktrace.RecordOnly}
}

func (recordOnlySampler) Description() string { return "RecordOnly" }

func TestQueryTracerLeavesRecordOnlyParentOpen(t *testing.T) {
	t.Parallel()
	qt, _, sr := newTestQueryTracer(t)
	recordOnly := sdktrace.NewTracerProvider(sdktrace.WithSampler(recordOnlySampler{}), sdktrace.WithSpanProcessor(sr))
	ctx, parent := recordOnly.Tracer("test").Start(context.Background(), "parent")
	if parent.SpanContext().IsSampled() || !parent.IsRecording() {
		t.Fatal("test setup: parent must be recording but not sampled")
	}

	// The statement is skipped because the parent is not sampled. Ending it
	// must not touch the parent, which is the span the context now carries.
	qctx := qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{})

	if !parent.IsRecording() {
		t.Error("TraceQueryEnd ended the parent span it did not start")
	}
	if n := len(sr.Ended()); n != 0 {
		t.Errorf("recorded %d ended spans before the parent finished, want 0", n)
	}
}

func TestQueryTracerErrorStatus(t *testing.T) {
	t.Parallel()
	qt, tp, sr := newTestQueryTracer(t)
	ctx, parent := tp.Tracer("test").Start(context.Background(), "parent")

	// A PostgreSQL error carries its SQLSTATE as status code and error type.
	pgErr := &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}
	qctx := qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "UPDATE actors SET state = $1"})
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{Err: pgErr})

	// Any other failure is typed by its Go type.
	qctx = qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "DELETE FROM workers WHERE name = $1"})
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{Err: context.DeadlineExceeded})
	parent.End()

	update := findSpan(t, sr, "UPDATE actors")
	if update.Status().Code != codes.Error {
		t.Errorf("failed statement status = %v, want Error", update.Status().Code)
	}
	attrs := spanAttrs(update)
	if got := attrs[semconv.DBResponseStatusCodeKey].AsString(); got != "40P01" {
		t.Errorf("db.response.status_code = %q, want 40P01", got)
	}
	if got := attrs[semconv.ErrorTypeKey].AsString(); got != "40P01" {
		t.Errorf("error.type = %q, want 40P01", got)
	}
	if len(update.Events()) == 0 {
		t.Error("failed statement recorded no exception event")
	}

	del := findSpan(t, sr, "DELETE workers")
	if del.Status().Code != codes.Error {
		t.Errorf("failed statement status = %v, want Error", del.Status().Code)
	}
	attrs = spanAttrs(del)
	if _, ok := attrs[semconv.DBResponseStatusCodeKey]; ok {
		t.Error("non-PostgreSQL error must not carry db.response.status_code")
	}
	if got := attrs[semconv.ErrorTypeKey].AsString(); got == "" || got == "40P01" {
		t.Errorf("error.type = %q, want the Go error type", got)
	}
}

func TestQuerySummary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		sql, operation, collection string
	}{
		// Statements as the store writes them.
		{"SELECT proto FROM actors WHERE atespace = $1 AND name = $2", "SELECT", "actors"},
		{"SELECT proto FROM workers WHERE name = $1 FOR UPDATE", "SELECT", "workers"},
		{"\n\t\tSELECT atespace, name, proto\n\t\tFROM actor_templates\n\t\tWHERE atespace = $1\n", "SELECT", "actor_templates"},
		{"INSERT INTO actors (atespace, name, uid, version, proto) VALUES ($1, $2, $3, $4, $5)", "INSERT", "actors"},
		{"UPDATE actor_templates SET version = $1, proto = $2 WHERE atespace = $3", "UPDATE", "actor_templates"},
		{"DELETE FROM actor_templates AS t WHERE t.atespace = $1 RETURNING t.proto", "DELETE", "actor_templates"},
		{"DELETE FROM leases WHERE expires_at <= clock_timestamp()", "DELETE", "leases"},
		{"INSERT INTO leases (key, token, expires_at) VALUES ($1, $2, clock_timestamp()) ON CONFLICT (key) DO UPDATE SET token = $2", "INSERT", "leases"},
		{"INSERT INTO worker_outbox_trim (xid) SELECT xid FROM worker_outbox_default ORDER BY xid DESC LIMIT 1", "INSERT", "worker_outbox_trim"},
		{"SELECT proto FROM actors WHERE name IN (SELECT name FROM actor_templates WHERE atespace = $1)", "SELECT", "actors"},
		{"LOCK TABLE worker_outbox IN ACCESS EXCLUSIVE MODE", "LOCK", "worker_outbox"},
		// No single table.
		{"SELECT clock_timestamp()", "SELECT", ""},
		{"SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "SELECT", ""},
		{"SELECT child.relname FROM pg_inherits i JOIN pg_class child ON child.oid = i.inhrelid", "SELECT", ""},
		{"SELECT count(*) FROM (SELECT 1) AS sub", "SELECT", ""},
		{"SELECT EXISTS(SELECT 1 FROM worker_outbox_default)", "SELECT", ""},
		{"SELECT (SELECT xid FROM worker_outbox_trim) >= $1::xid8", "SELECT", ""},
		{"CREATE SCHEMA IF NOT EXISTS \"ate\"", "CREATE", ""},
		{"DROP TABLE worker_outbox_p1", "DROP", ""},
		// Transaction control as pgx issues it.
		{"begin", "begin", ""},
		{"begin isolation level serializable", "begin", ""},
		{"commit", "commit", ""},
		{"rollback", "rollback", ""},
		{"", "", ""},
	} {
		op, coll := querySummary(tc.sql)
		if op != tc.operation || coll != tc.collection {
			t.Errorf("querySummary(%q) = (%q, %q), want (%q, %q)", tc.sql, op, coll, tc.operation, tc.collection)
		}
	}
}

func TestPoolConfigTracerSharedWithWatchPool(t *testing.T) {
	t.Parallel()
	cfg, err := poolConfig("postgres://postgres@localhost:5432/atepg?sslmode=disable")
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	qt, ok := cfg.ConnConfig.Tracer.(*queryTracer)
	if !ok {
		t.Fatalf("pool tracer = %T, want *queryTracer", cfg.ConnConfig.Tracer)
	}
	watch := cfg.Copy()
	if watch.ConnConfig.Tracer != qt {
		t.Errorf("watch pool tracer = %v, want the same *queryTracer as the main pool", watch.ConnConfig.Tracer)
	}
}

// TestQueryTracerAgainstPostgreSQL drives real statements through a traced
// pool, so the span names, transaction control spans, lookup misses and
// server errors are what pgx actually delivers rather than what the unit
// tests feed the tracer by hand.
func TestQueryTracerAgainstPostgreSQL(t *testing.T) {
	requirePool(t)
	ctx := context.Background()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(ctx) })

	cfg, err := poolConfig(containerDSN)
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	cfg.ConnConfig.Tracer = newQueryTracer(tp, cfg.ConnConfig)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("opening traced pool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx, parent := tp.Tracer("test").Start(ctx, "rpc")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	var relname string
	if err := tx.QueryRow(ctx, "SELECT relname FROM pg_class WHERE false").Scan(&relname); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("lookup miss err = %v, want ErrNoRows", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := pool.Exec(ctx, "SELECT 1 FROM no_such_table"); err == nil {
		t.Fatal("querying a missing table succeeded")
	}
	parent.End()

	var names []string
	for _, s := range sr.Ended() {
		if s.Name() == "rpc" {
			continue
		}
		names = append(names, s.Name())
		if got, want := s.Parent().SpanID(), parent.SpanContext().SpanID(); got != want {
			t.Errorf("%s parent = %s, want the rpc span %s", s.Name(), got, want)
		}
	}
	want := []string{"begin", "SELECT pg_class", "commit", "SELECT no_such_table"}
	if !slices.Equal(names, want) {
		t.Fatalf("span names = %q, want %q", names, want)
	}

	miss := findSpan(t, sr, "SELECT pg_class")
	if miss.Status().Code != codes.Unset {
		t.Errorf("lookup miss status = %v, want Unset: pgx must not hand ErrNoRows to the tracer", miss.Status().Code)
	}
	failed := findSpan(t, sr, "SELECT no_such_table")
	if failed.Status().Code != codes.Error {
		t.Errorf("missing table status = %v, want Error", failed.Status().Code)
	}
	if got := spanAttrs(failed)[semconv.DBResponseStatusCodeKey].AsString(); got != "42P01" {
		t.Errorf("db.response.status_code = %q, want 42P01 (undefined_table)", got)
	}
}
