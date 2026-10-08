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
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	connectionCountMetric = "db.client.connection.count"
	connectionMaxMetric   = "db.client.connection.max"
	connectionWaitMetric  = "db.client.connection.wait_time"
)

// Acquisition buckets cover sub-millisecond reuse through prolonged contention.
var connectionWaitBuckets = []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60}

// Instruments holds PostgreSQL pool metrics. A nil *Instruments disables them.
type Instruments struct {
	meter metric.Meter
	count metric.Int64ObservableUpDownCounter
	max   metric.Int64ObservableUpDownCounter
	wait  metric.Float64Histogram
}

func NewInstruments(meter metric.Meter) (*Instruments, error) {
	count, err := meter.Int64ObservableUpDownCounter(connectionCountMetric,
		metric.WithUnit("{connection}"),
		metric.WithDescription("Number of used and idle PostgreSQL connections by pool."))
	if err != nil {
		return nil, fmt.Errorf("create %s instrument: %w", connectionCountMetric, err)
	}
	max, err := meter.Int64ObservableUpDownCounter(connectionMaxMetric,
		metric.WithUnit("{connection}"),
		metric.WithDescription("Maximum number of open PostgreSQL connections allowed by pool."))
	if err != nil {
		return nil, fmt.Errorf("create %s instrument: %w", connectionMaxMetric, err)
	}
	wait, err := meter.Float64Histogram(connectionWaitMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Time to acquire a PostgreSQL connection, including unsuccessful attempts, by pool and outcome."),
		metric.WithExplicitBucketBoundaries(connectionWaitBuckets...))
	if err != nil {
		return nil, fmt.Errorf("create %s instrument: %w", connectionWaitMetric, err)
	}
	return &Instruments{meter: meter, count: count, max: max, wait: wait}, nil
}

func (i *Instruments) configurePool(cfg *pgxpool.Config, name string) {
	if i == nil {
		return
	}
	tracer := &poolMetricsTracer{instruments: i, name: name}
	if cfg.ConnConfig.Tracer == nil {
		cfg.ConnConfig.Tracer = tracer
	} else {
		cfg.ConnConfig.Tracer = multitracer.New(cfg.ConnConfig.Tracer, tracer)
	}
}

type namedPool struct {
	name string
	pool *pgxpool.Pool
}

func (i *Instruments) registerPools(pools ...namedPool) (metric.Registration, error) {
	if i == nil {
		return nil, nil
	}
	registration, err := i.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		for _, pool := range pools {
			stats := pool.pool.Stat()
			poolAttr := ateattr.DBConnectionPoolNameKey.String(pool.name)
			observer.ObserveInt64(i.count, int64(stats.AcquiredConns()), metric.WithAttributes(
				poolAttr, ateattr.DBConnectionStateKey.String(ateattr.DBConnectionStateUsed)))
			observer.ObserveInt64(i.count, int64(stats.IdleConns()), metric.WithAttributes(
				poolAttr, ateattr.DBConnectionStateKey.String(ateattr.DBConnectionStateIdle)))
			observer.ObserveInt64(i.max, int64(stats.MaxConns()), metric.WithAttributes(poolAttr))
		}
		return nil
	}, i.count, i.max)
	if err != nil {
		return nil, fmt.Errorf("register PostgreSQL pool metrics callback: %w", err)
	}
	return registration, nil
}

func (i *Instruments) recordAcquire(ctx context.Context, name string, duration time.Duration, err error) {
	if i == nil {
		return
	}
	outcome := ateattr.StoreConnectionAcquireSuccess
	switch {
	case errors.Is(err, context.Canceled):
		outcome = ateattr.StoreConnectionAcquireCancelled
	case errors.Is(err, context.DeadlineExceeded):
		outcome = ateattr.StoreConnectionAcquireTimeout
	case err != nil:
		outcome = ateattr.StoreConnectionAcquireError
	}
	attrs := []attribute.KeyValue{
		ateattr.DBConnectionPoolNameKey.String(name),
		ateattr.StoreConnectionAcquireOutcomeKey.String(outcome),
	}
	if outcome == ateattr.StoreConnectionAcquireError {
		attrs = append(attrs, ateattr.ErrorTypeKey.String("_OTHER"))
	}
	i.wait.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

type poolMetricsTracer struct {
	instruments *Instruments
	name        string
}

type acquireStartKey struct{}

var _ pgxpool.AcquireTracer = (*poolMetricsTracer)(nil)

// pgx requires a QueryTracer to install pool acquisition hooks. These methods
// do not trace queries or create spans.
func (*poolMetricsTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (*poolMetricsTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (*poolMetricsTracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	return context.WithValue(ctx, acquireStartKey{}, time.Now())
}

func (t *poolMetricsTracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	start := ctx.Value(acquireStartKey{}).(time.Time)
	t.instruments.recordAcquire(ctx, t.name, time.Since(start), data.Err)
}
