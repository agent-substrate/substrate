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
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// queryTracer is a pgx QueryTracer that opens one client span per statement,
// so an RPC trace shows where its time went inside PostgreSQL. Statements are
// parameterized ($1, $2, ...), so db.query.text carries no argument values.
//
// Spans follow the OpenTelemetry database semantic conventions: the span name
// is the query summary ("SELECT actors", "UPDATE workers", "commit"), and the
// connection-level attributes are computed once from the pool configuration.
type queryTracer struct {
	tracer trace.Tracer
	// attrs holds db.system.name, server.address, server.port and
	// db.namespace, which are the same for every statement on the pool.
	attrs []attribute.KeyValue
}

var _ pgx.QueryTracer = (*queryTracer)(nil)

// newQueryTracer builds the tracer for pools opened from cc. The tracer is
// resolved once here rather than per statement.
func newQueryTracer(tp trace.TracerProvider, cc *pgx.ConnConfig) *queryTracer {
	return &queryTracer{
		tracer: tp.Tracer("atepg"),
		attrs: []attribute.KeyValue{
			semconv.DBSystemNamePostgreSQL,
			semconv.ServerAddress(cc.Host),
			semconv.ServerPort(int(cc.Port)),
			semconv.DBNamespace(cc.Database),
		},
	}
}

// querySpanKey marks a context whose statement span was opened by
// TraceQueryStart, so TraceQueryEnd never ends a span it did not start.
type querySpanKey struct{}

func (t *queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	// Join sampled traces only. Statements issued from background work
	// (outbox polling, lease maintenance) carry no span, and opening a root
	// span for each would flood the backend with single-span traces. An
	// unsampled parent is skipped too: every sampler serverboot installs is
	// ParentBased, so its children could never be sampled, and skipping
	// them saves the non-recording span and context wrap per statement.
	if !trace.SpanContextFromContext(ctx).IsSampled() {
		return ctx
	}
	operation, collection := querySummary(data.SQL)
	attrs := make([]attribute.KeyValue, 0, len(t.attrs)+4)
	attrs = append(attrs, t.attrs...)
	attrs = append(attrs, semconv.DBQueryText(data.SQL))
	name := "postgresql"
	if operation != "" {
		name = operation
		attrs = append(attrs, semconv.DBOperationName(operation))
		if collection != "" {
			name += " " + collection
			attrs = append(attrs, semconv.DBCollectionName(collection))
		}
		attrs = append(attrs, semconv.DBQuerySummary(name))
	}
	ctx, span := t.tracer.Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...))
	return context.WithValue(ctx, querySpanKey{}, span)
}

func (*queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span, ok := ctx.Value(querySpanKey{}).(trace.Span)
	if !ok {
		return
	}
	// pgx.ErrNoRows never arrives here: pgx synthesizes it for the caller
	// after the rows are closed, so a lookup miss ends the span cleanly.
	if data.Err != nil {
		span.RecordError(data.Err)
		span.SetStatus(codes.Error, data.Err.Error())
		if code := pgErrCode(data.Err); code != "" {
			// SQLSTATE is both the server's status code and the most
			// useful low-cardinality error class.
			span.SetAttributes(semconv.DBResponseStatusCode(code), semconv.ErrorTypeKey.String(code))
		} else {
			span.SetAttributes(semconv.ErrorTypeKey.String(fmt.Sprintf("%T", data.Err)))
		}
	}
	span.End()
}

// querySummary extracts the db.operation.name and db.collection.name of a
// statement: the leading keyword, and the single table it acts on. Both are
// returned as written, without case normalization, as the conventions ask.
//
// The store issues one hand-written statement per call, so a keyword scan is
// enough: the table follows INTO for INSERT, UPDATE for UPDATE, TABLE for
// LOCK, and the first FROM for SELECT and DELETE. A statement that reads
// several tables (any JOIN) or none (SELECT clock_timestamp()) has no
// collection, and DDL and transaction control report only their keyword.
func querySummary(sql string) (operation, collection string) {
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return "", ""
	}
	operation = fields[0]
	var marker string
	switch strings.ToUpper(operation) {
	case "SELECT", "DELETE":
		marker = "FROM"
	case "INSERT":
		marker = "INTO"
	case "LOCK":
		marker = "TABLE"
	case "UPDATE":
		return operation, collectionToken(fields, 1)
	default:
		return operation, ""
	}
	at := -1
	for i, f := range fields[1:] {
		switch {
		case strings.EqualFold(f, "JOIN"):
			return operation, ""
		case at < 0 && strings.EqualFold(f, marker):
			at = i + 2
		}
	}
	if at < 0 {
		return operation, ""
	}
	return operation, collectionToken(fields, at)
}

// collectionToken returns fields[i] stripped of surrounding punctuation, or
// "" when it is absent or is not an identifier (a subquery, for instance).
func collectionToken(fields []string, i int) string {
	if i >= len(fields) {
		return ""
	}
	tok := strings.Trim(fields[i], "(),;")
	if tok == "" || strings.EqualFold(tok, "SELECT") {
		return ""
	}
	return tok
}
