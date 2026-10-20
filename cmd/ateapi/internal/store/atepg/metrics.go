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
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
	"go.ate.dev/substrate/internal/ateattr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/semconv/v1.40.0/dbconv"
)

const (
	connectionCountMetric   = "db.client.connection.count"
	connectionMaxMetric     = "db.client.connection.max"
	connectionWaitMetric    = "db.client.connection.wait_time"
	connectionPendingMetric = "db.client.connection.pending_requests"
)

// Acquisition buckets cover sub-millisecond reuse through prolonged contention.
var connectionWaitBuckets = []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60}

// Instruments holds PostgreSQL pool metrics. A nil *Instruments disables them.
type Instruments struct {
	meter   metric.Meter
	count   metric.Int64ObservableUpDownCounter
	max     metric.Int64ObservableUpDownCounter
	wait    metric.Float64Histogram
	pending metric.Int64UpDownCounter
}

func NewInstruments(meter metric.Meter) (*Instruments, error) {
	count, err := meter.Int64ObservableUpDownCounter(connectionCountMetric,
		metric.WithUnit("{connection}"),
		metric.WithDescription(dbconv.ClientConnectionCount{}.Description()))
	if err != nil {
		return nil, fmt.Errorf("create %s instrument: %w", connectionCountMetric, err)
	}
	max, err := meter.Int64ObservableUpDownCounter(connectionMaxMetric,
		metric.WithUnit("{connection}"),
		metric.WithDescription(dbconv.ClientConnectionMax{}.Description()))
	if err != nil {
		return nil, fmt.Errorf("create %s instrument: %w", connectionMaxMetric, err)
	}
	wait, err := dbconv.NewClientConnectionWaitTime(meter,
		metric.WithExplicitBucketBoundaries(connectionWaitBuckets...))
	if err != nil {
		return nil, fmt.Errorf("create %s instrument: %w", connectionWaitMetric, err)
	}
	pending, err := dbconv.NewClientConnectionPendingRequests(meter)
	if err != nil {
		return nil, fmt.Errorf("create %s instrument: %w", connectionPendingMetric, err)
	}
	return &Instruments{meter: meter, count: count, max: max, wait: wait.Float64Histogram, pending: pending.Int64UpDownCounter}, nil
}

func (i *Instruments) configurePool(cfg *pgxpool.Config, name string) {
	if i == nil {
		return
	}
	poolAttr := ateattr.DBConnectionPoolNameKey.String(name)
	// Precompute acquisition labels once per pool to avoid rebuilding attribute
	// sets on the hot path. Only failed acquisitions include error.type.
	acquireOption := func(outcome, errorType string) metric.MeasurementOption {
		attrs := []attribute.KeyValue{
			poolAttr,
			ateattr.StoreConnectionAcquireOutcomeKey.String(outcome),
		}
		if errorType != "" {
			attrs = append(attrs, ateattr.ErrorTypeKey.String(errorType))
		}
		return metric.WithAttributeSet(attribute.NewSet(attrs...))
	}
	tracer := &poolMetricsTracer{
		instruments: i,
		poolOption:  metric.WithAttributeSet(attribute.NewSet(poolAttr)),

		successOption:   acquireOption(ateattr.StoreConnectionAcquireSuccess, ""),
		cancelledOption: acquireOption(ateattr.StoreConnectionAcquireCancelled, ""),
		timeoutOption:   acquireOption(ateattr.StoreConnectionAcquireTimeout, ""),

		poolClosedOption: acquireOption(
			ateattr.StoreConnectionAcquireError,
			ateattr.StoreConnectionAcquirePoolClosed,
		),
		connectErrorOption: acquireOption(
			ateattr.StoreConnectionAcquireError,
			ateattr.StoreConnectionAcquireConnectError,
		),
		otherErrorOption: acquireOption(
			ateattr.StoreConnectionAcquireError,
			ateattr.StoreConnectionAcquireOtherError,
		),
	}
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

func (t *poolMetricsTracer) recordAcquire(ctx context.Context, duration time.Duration, err error) {
	option := t.successOption
	if err != nil {
		// Check pool and connection errors before cancellation or timeout.
		// A pgconn ConnectError can wrap DeadlineExceeded from its own connection
		// timeout while the caller's context is still active. Classify that as
		// connect_error, not a caller acquisition timeout.
		switch acquireErrorType(err) {
		case ateattr.StoreConnectionAcquirePoolClosed:
			option = t.poolClosedOption
		case ateattr.StoreConnectionAcquireConnectError:
			option = t.connectErrorOption
		default:
			switch {
			case errors.Is(err, context.Canceled):
				option = t.cancelledOption
			case errors.Is(err, context.DeadlineExceeded):
				option = t.timeoutOption
			default:
				option = t.otherErrorOption
			}
		}
	}
	t.instruments.wait.Record(ctx, duration.Seconds(), option)
}

func acquireErrorType(err error) string {
	if errors.Is(err, puddle.ErrClosedPool) {
		return ateattr.StoreConnectionAcquirePoolClosed
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return ateattr.StoreConnectionAcquireConnectError
	}
	return ateattr.StoreConnectionAcquireOtherError
}

type poolMetricsTracer struct {
	instruments         *Instruments
	missingStartWarning sync.Once

	poolOption         metric.MeasurementOption
	successOption      metric.MeasurementOption
	cancelledOption    metric.MeasurementOption
	timeoutOption      metric.MeasurementOption
	poolClosedOption   metric.MeasurementOption
	connectErrorOption metric.MeasurementOption
	otherErrorOption   metric.MeasurementOption
}

type acquireStartKey struct{}

var _ pgxpool.AcquireTracer = (*poolMetricsTracer)(nil)

// pgx requires a QueryTracer to install pool acquisition hooks. These methods
// do not trace queries or create spans.
func (*poolMetricsTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (*poolMetricsTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *poolMetricsTracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	t.instruments.pending.Add(ctx, 1, t.poolOption)
	return context.WithValue(ctx, acquireStartKey{}, time.Now())
}

func (t *poolMetricsTracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	t.instruments.pending.Add(ctx, -1, t.poolOption)
	start, ok := ctx.Value(acquireStartKey{}).(time.Time)
	if !ok {
		t.missingStartWarning.Do(func() {
			slog.WarnContext(ctx, "PostgreSQL acquisition metric missing start time")
		})
		return
	}
	t.recordAcquire(ctx, time.Since(start), data.Err)
}
