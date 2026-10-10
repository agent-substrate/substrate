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
	"reflect"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func newPoolMetrics(t *testing.T) (*Instruments, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown metric provider: %v", err)
		}
	})
	instruments, err := NewInstruments(provider.Meter("ateapi"))
	if err != nil {
		t.Fatal(err)
	}
	return instruments, reader
}

func collectPoolMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	result := make(map[string]metricdata.Metrics)
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			result[m.Name] = m
		}
	}
	return result
}

func assertPoolCounts(t *testing.T, reader *sdkmetric.ManualReader, pools map[string][3]int64) {
	t.Helper()
	metrics := collectPoolMetrics(t, reader)
	for _, name := range []string{connectionCountMetric, connectionMaxMetric} {
		m, found := metrics[name]
		if !found && len(pools) == 0 {
			continue
		}
		if !found || m.Unit != "{connection}" {
			t.Fatalf("%s missing or wrong unit: %+v", name, m)
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		if !ok || sum.IsMonotonic {
			t.Fatalf("%s is not an UpDownCounter: %T", name, m.Data)
		}
		want := make(map[attribute.Set]int64)
		for pool, counts := range pools {
			poolAttr := ateattr.DBConnectionPoolNameKey.String(pool)
			if name == connectionMaxMetric {
				want[attribute.NewSet(poolAttr)] = counts[2]
			} else {
				want[attribute.NewSet(poolAttr, ateattr.DBConnectionStateKey.String(ateattr.DBConnectionStateUsed))] = counts[0]
				want[attribute.NewSet(poolAttr, ateattr.DBConnectionStateKey.String(ateattr.DBConnectionStateIdle))] = counts[1]
			}
		}
		got := make(map[attribute.Set]int64)
		for _, point := range sum.DataPoints {
			got[point.Attributes] = point.Value
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s series = %v, want %v", name, got, want)
		}
	}
}

func TestPoolMetricsZeroCountsAndUnregister(t *testing.T) {
	instruments, reader := newPoolMetrics(t)
	var pools []namedPool
	want := make(map[string][3]int64)
	for _, tc := range []struct {
		name string
		max  int32
	}{
		{ateattr.DBConnectionPoolMain, 7},
		{ateattr.DBConnectionPoolWatch, watchPoolMaxConns},
		{ateattr.DBConnectionPoolOwner, ownerPoolMaxConns},
	} {
		cfg, err := pgxpool.ParseConfig("postgres://unused@127.0.0.1:1/test?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		cfg.MaxConns = tc.max
		pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		pools = append(pools, namedPool{tc.name, pool})
		want[tc.name] = [3]int64{0, 0, int64(tc.max)}
	}
	registration, err := instruments.registerPools(pools...)
	if err != nil {
		t.Fatal(err)
	}
	assertPoolCounts(t, reader, want)
	if err := registration.Unregister(); err != nil {
		t.Fatal(err)
	}
	assertPoolCounts(t, reader, nil)
}

func TestPoolMetricsAcquireOutcomesUnsampled(t *testing.T) {
	instruments, reader := newPoolMetrics(t)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown trace provider: %v", err)
		}
	})
	ctx, span := provider.Tracer("test").Start(t.Context(), "unsampled")
	defer span.End()
	if span.SpanContext().IsSampled() {
		t.Fatal("test span is sampled")
	}
	for _, tc := range []struct {
		err error
	}{
		{nil},
		{fmt.Errorf("wrapped: %w", context.Canceled)},
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded)},
		{errors.New("unbounded error message")},
		{errors.New("a different error message")},
	} {
		instruments.recordAcquire(ctx, ateattr.DBConnectionPoolMain, 2*time.Millisecond, tc.err)
	}
	m := collectPoolMetrics(t, reader)[connectionWaitMetric]
	histogram, ok := m.Data.(metricdata.Histogram[float64])
	if m.Unit != "s" || !ok {
		t.Fatalf("wrong wait histogram: %+v", m)
	}
	want := map[attribute.Set]uint64{}
	for _, outcome := range []string{
		ateattr.StoreConnectionAcquireSuccess, ateattr.StoreConnectionAcquireCancelled,
		ateattr.StoreConnectionAcquireTimeout, ateattr.StoreConnectionAcquireError,
	} {
		attrs := []attribute.KeyValue{
			ateattr.DBConnectionPoolNameKey.String(ateattr.DBConnectionPoolMain),
			ateattr.StoreConnectionAcquireOutcomeKey.String(outcome),
		}
		count := uint64(1)
		if outcome == ateattr.StoreConnectionAcquireError {
			attrs = append(attrs, ateattr.ErrorTypeKey.String("_OTHER"))
			count = 2
		}
		want[attribute.NewSet(attrs...)] = count
	}
	got := make(map[attribute.Set]uint64)
	for _, point := range histogram.DataPoints {
		got[point.Attributes] = point.Count
		if point.Sum != float64(point.Count)*0.002 || !reflect.DeepEqual(point.Bounds, connectionWaitBuckets) {
			t.Errorf("wrong duration or buckets: %+v", point)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("acquire series = %v, want %v", got, want)
	}
}

func TestPoolMetricsNilSafe(t *testing.T) {
	var instruments *Instruments
	cfg, err := pgxpool.ParseConfig("postgres://unused@localhost/test")
	if err != nil {
		t.Fatal(err)
	}
	instruments.configurePool(cfg, ateattr.DBConnectionPoolMain)
	if cfg.ConnConfig.Tracer != nil {
		t.Fatal("nil instruments installed a tracer")
	}
	registration, err := instruments.registerPools()
	if registration != nil || err != nil {
		t.Fatalf("nil instruments registration = %v, %v", registration, err)
	}
	instruments.recordAcquire(t.Context(), ateattr.DBConnectionPoolMain, time.Second, nil)
}

type queryProbe struct {
	started bool
	ended   bool
}

type queryProbeKey struct{}

func (q *queryProbe) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	q.started = true
	return context.WithValue(ctx, queryProbeKey{}, true)
}

func (q *queryProbe) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	q.ended = ctx.Value(queryProbeKey{}) == true
}

func TestPoolMetricsPreservesQueryTracer(t *testing.T) {
	instruments, reader := newPoolMetrics(t)
	cfg, err := pgxpool.ParseConfig("postgres://unused@localhost/test")
	if err != nil {
		t.Fatal(err)
	}
	probe := &queryProbe{}
	cfg.ConnConfig.Tracer = probe
	instruments.configurePool(cfg, ateattr.DBConnectionPoolMain)
	queryCtx := cfg.ConnConfig.Tracer.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{})
	cfg.ConnConfig.Tracer.TraceQueryEnd(queryCtx, nil, pgx.TraceQueryEndData{})
	if !probe.started || !probe.ended {
		t.Fatal("query tracer was not preserved")
	}
	tracer := cfg.ConnConfig.Tracer.(pgxpool.AcquireTracer)
	ctx := tracer.TraceAcquireStart(t.Context(), nil, pgxpool.TraceAcquireStartData{})
	tracer.TraceAcquireEnd(ctx, nil, pgxpool.TraceAcquireEndData{})
	if _, ok := collectPoolMetrics(t, reader)[connectionWaitMetric]; !ok {
		t.Fatal("composed tracer did not record acquisition")
	}
}

func TestPoolMetricsContention(t *testing.T) {
	requirePool(t)
	instruments, reader := newPoolMetrics(t)
	cfg, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	instruments.configurePool(cfg, ateattr.DBConnectionPoolMain)
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	registration, err := instruments.registerPools(namedPool{ateattr.DBConnectionPoolMain, pool})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := registration.Unregister(); err != nil {
			t.Errorf("unregister: %v", err)
		}
	})
	conn, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	assertPoolCounts(t, reader, map[string][3]int64{ateattr.DBConnectionPoolMain: {1, 0, 1}})

	const timeout = 25 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	if _, err := pool.Exec(ctx, "SELECT 1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("saturated Exec error = %v", err)
	}
	ctx, cancelAcquire := context.WithCancel(t.Context())
	cancelAcquire()
	if _, err := pool.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquisition error = %v", err)
	}
	acquired := make(chan error, 1)
	go func() {
		_, err := pool.Exec(t.Context(), "SELECT 1")
		acquired <- err
	}()
	select {
	case err := <-acquired:
		t.Fatalf("acquisition completed while pool was saturated: %v", err)
	case <-time.After(timeout):
	}
	conn.Release()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquisition did not complete after release")
	}
	assertPoolCounts(t, reader, map[string][3]int64{ateattr.DBConnectionPoolMain: {0, 1, 1}})

	// These paths acquire implicitly; time holding or querying a connection
	// must not count as another acquisition.
	rows, err := pool.Query(t.Context(), "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	var value int
	if err := pool.QueryRow(t.Context(), "SELECT 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	if _, err := pool.Acquire(t.Context()); err == nil {
		t.Fatal("closed pool acquisition succeeded")
	}
	histogram := collectPoolMetrics(t, reader)[connectionWaitMetric].Data.(metricdata.Histogram[float64])
	got := make(map[string]uint64)
	for _, point := range histogram.DataPoints {
		outcome, _ := point.Attributes.Value(ateattr.StoreConnectionAcquireOutcomeKey)
		got[outcome.AsString()] = point.Count
		if outcome.AsString() == ateattr.StoreConnectionAcquireTimeout && point.Sum < timeout.Seconds() {
			t.Errorf("timeout acquisition duration = %g, want >= %g", point.Sum, timeout.Seconds())
		}
	}
	want := map[string]uint64{
		ateattr.StoreConnectionAcquireSuccess: 5, ateattr.StoreConnectionAcquireCancelled: 1,
		ateattr.StoreConnectionAcquireTimeout: 1, ateattr.StoreConnectionAcquireError: 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("acquisition counts = %v, want %v", got, want)
	}
}

func TestPoolMetricsConnectAndClose(t *testing.T) {
	requirePool(t)
	instruments, reader := newPoolMetrics(t)
	cfg := testConnectConfig("pool_metrics")
	cfg.PoolMaxConns = 7
	cfg.Instruments = instruments
	p, err := Connect(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Pool().Close)
	t.Cleanup(p.Close)
	for _, pool := range []namedPool{
		{ateattr.DBConnectionPoolMain, p.pool},
		{ateattr.DBConnectionPoolWatch, p.watchPool},
		{ateattr.DBConnectionPoolOwner, p.ownerPool},
	} {
		if _, err := pool.pool.Exec(t.Context(), "SELECT 1"); err != nil {
			t.Fatal(err)
		}
	}
	want := make(map[string][3]int64)
	for _, pool := range []namedPool{
		{ateattr.DBConnectionPoolMain, p.pool},
		{ateattr.DBConnectionPoolWatch, p.watchPool},
		{ateattr.DBConnectionPoolOwner, p.ownerPool},
	} {
		stats := pool.pool.Stat()
		want[pool.name] = [3]int64{int64(stats.AcquiredConns()), int64(stats.IdleConns()), int64(stats.MaxConns())}
	}
	assertPoolCounts(t, reader, want)
	histogram := collectPoolMetrics(t, reader)[connectionWaitMetric].Data.(metricdata.Histogram[float64])
	seen := make(map[string]bool)
	for _, point := range histogram.DataPoints {
		pool, _ := point.Attributes.Value(ateattr.DBConnectionPoolNameKey)
		seen[pool.AsString()] = true
	}
	if len(seen) != 3 {
		t.Errorf("acquisition pools = %v, want main, watch, owner", seen)
	}
	p.Close()
	assertPoolCounts(t, reader, nil)
	if err := p.pool.Ping(t.Context()); err != nil {
		t.Errorf("Persistence.Close closed caller-owned main pool: %v", err)
	}
}

type failingPoolMeter struct {
	metric.Meter
	fail string
}

var errPoolMetric = errors.New("instrument failure")

func (m failingPoolMeter) Int64ObservableUpDownCounter(name string, opts ...metric.Int64ObservableUpDownCounterOption) (metric.Int64ObservableUpDownCounter, error) {
	if name == m.fail {
		return nil, errPoolMetric
	}
	return m.Meter.Int64ObservableUpDownCounter(name, opts...)
}

func (m failingPoolMeter) Float64Histogram(name string, opts ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if name == m.fail {
		return nil, errPoolMetric
	}
	return m.Meter.Float64Histogram(name, opts...)
}

func (m failingPoolMeter) Int64UpDownCounter(name string, opts ...metric.Int64UpDownCounterOption) (metric.Int64UpDownCounter, error) {
	if name == m.fail {
		return nil, errPoolMetric
	}
	return m.Meter.Int64UpDownCounter(name, opts...)
}

func (m failingPoolMeter) RegisterCallback(callback metric.Callback, instruments ...metric.Observable) (metric.Registration, error) {
	if m.fail == "callback" {
		return nil, errPoolMetric
	}
	return m.Meter.RegisterCallback(callback, instruments...)
}

func TestPoolMetricsInstrumentErrors(t *testing.T) {
	for _, name := range []string{connectionCountMetric, connectionMaxMetric, connectionWaitMetric, connectionPendingMetric} {
		t.Run(name, func(t *testing.T) {
			instruments, _ := newPoolMetrics(t)
			_, err := NewInstruments(failingPoolMeter{Meter: instruments.meter, fail: name})
			if !errors.Is(err, errPoolMetric) {
				t.Fatalf("constructor error = %v", err)
			}
		})
	}
	instruments, _ := newPoolMetrics(t)
	instruments.meter = failingPoolMeter{Meter: instruments.meter, fail: "callback"}
	if _, err := instruments.registerPools(); !errors.Is(err, errPoolMetric) {
		t.Fatalf("registration error = %v", err)
	}
}

func TestPoolMetricsFailedConnectDoesNotRegister(t *testing.T) {
	requirePool(t)
	instruments, reader := newPoolMetrics(t)
	cfg := testConnectConfig("pool_metrics_failed")
	cfg.Instruments = instruments
	cfg.OwnerDSN = "postgres://invalid@127.0.0.1:1/test?sslmode=disable&connect_timeout=1"
	for range 2 {
		if _, err := Connect(t.Context(), cfg); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Connect error = %v, want unavailable", err)
		}
		assertPoolCounts(t, reader, nil)
	}
}

func TestPoolMetricsConnectRegistrationFailure(t *testing.T) {
	requirePool(t)
	instruments, reader := newPoolMetrics(t)
	meter := instruments.meter
	instruments.meter = failingPoolMeter{Meter: meter, fail: "callback"}
	cfg := testConnectConfig("pool_metrics_registration_failure")
	cfg.Instruments = instruments
	if _, err := Connect(t.Context(), cfg); !errors.Is(err, errPoolMetric) {
		t.Fatalf("Connect error = %v, want registration failure", err)
	}
	assertPoolCounts(t, reader, nil)
	// Retrying with the same instruments must not retain the failed attempt's
	// maintenance loop, pools, or callback registration.
	instruments.meter = meter
	p, err := Connect(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.pool.Close()
	p.Close()
	assertPoolCounts(t, reader, nil)
}

func assertPendingAcquires(t *testing.T, reader *sdkmetric.ManualReader, want map[string]int64) {
	t.Helper()
	m := collectPoolMetrics(t, reader)[connectionPendingMetric]
	sum, ok := m.Data.(metricdata.Sum[int64])
	if m.Unit != "{request}" || !ok || sum.IsMonotonic {
		t.Fatalf("wrong pending request instrument: %+v", m)
	}
	got := make(map[string]int64)
	for _, point := range sum.DataPoints {
		if point.Attributes.Len() != 1 {
			t.Fatalf("unexpected pending attributes: %v", point.Attributes)
		}
		pool, _ := point.Attributes.Value(ateattr.DBConnectionPoolNameKey)
		got[pool.AsString()] = point.Value
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pending requests = %v, want %v", got, want)
	}
}

func TestPoolMetricsPendingRequests(t *testing.T) {
	instruments, reader := newPoolMetrics(t)
	tracer := &poolMetricsTracer{instruments: instruments, name: ateattr.DBConnectionPoolMain}
	for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, errors.New("acquire failure")} {
		first := tracer.TraceAcquireStart(t.Context(), nil, pgxpool.TraceAcquireStartData{})
		second := tracer.TraceAcquireStart(t.Context(), nil, pgxpool.TraceAcquireStartData{})
		assertPendingAcquires(t, reader, map[string]int64{ateattr.DBConnectionPoolMain: 2})
		tracer.TraceAcquireEnd(first, nil, pgxpool.TraceAcquireEndData{Err: err})
		assertPendingAcquires(t, reader, map[string]int64{ateattr.DBConnectionPoolMain: 1})
		tracer.TraceAcquireEnd(second, nil, pgxpool.TraceAcquireEndData{Err: err})
		assertPendingAcquires(t, reader, map[string]int64{ateattr.DBConnectionPoolMain: 0})
	}
}
