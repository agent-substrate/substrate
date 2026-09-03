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
	"strings"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// queryTracer is a pgx QueryTracer that opens one client span per statement,
// so an RPC trace shows where its time went inside PostgreSQL. Statements are
// parameterized ($1, $2, ...), so db.query.text carries no argument values.
type queryTracer struct{}

var _ pgx.QueryTracer = queryTracer{}

func (queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	// Join existing traces only. Statements issued from background work
	// (outbox polling, lease maintenance) carry no span, and opening a root
	// span for each would flood the backend with single-span traces.
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx
	}
	ctx, _ = otel.Tracer("atepg").Start(ctx, querySpanName(data.SQL),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.query.text", data.SQL),
		))
	return ctx
}

func (queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	// pgx.ErrNoRows is an expected lookup outcome (the store maps it to
	// NotFound), not a query failure.
	if data.Err != nil && !errors.Is(data.Err, pgx.ErrNoRows) {
		span.RecordError(data.Err)
		span.SetStatus(codes.Error, data.Err.Error())
	}
	span.End()
}

// querySpanName is "db." plus the statement's leading keyword ("db.SELECT",
// "db.INSERT", ...): stable low-cardinality names that group by statement
// kind, with the full text in db.query.text.
func querySpanName(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return "db.query"
	}
	return "db." + strings.ToUpper(fields[0])
}
