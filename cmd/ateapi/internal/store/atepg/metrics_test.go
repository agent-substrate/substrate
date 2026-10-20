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
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
	"go.ate.dev/substrate/internal/ateattr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/semconv/v1.40.0/dbconv"
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
		{ateattr.DBConnectionPoolProbe, probePoolMaxConns},
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
	metrics := collectPoolMetrics(t, reader)
	for name, description := range map[string]string{
		connectionCountMetric: dbconv.ClientConnectionCount{}.Description(),
		connectionMaxMetric:   dbconv.ClientConnectionMax{}.Description(),
	} {
		if got := metrics[name].Description; got != description {
			t.Errorf("%s description = %q, want %q", name, got, description)
		}
	}
	if err := registration.Unregister(); err != nil {
		t.Fatal(err)
	}
	assertPoolCounts(t, reader, nil)
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
}

func newPoolMetricsTracer(t *testing.T, instruments *Instruments, name string) *poolMetricsTracer {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://unused@localhost/test")
	if err != nil {
		t.Fatal(err)
	}
	instruments.configurePool(cfg, name)
	return cfg.ConnConfig.Tracer.(*poolMetricsTracer)
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
	started := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	if _, err := pool.Exec(ctx, "SELECT 1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("saturated Exec error = %v", err)
	}
	elapsed := time.Since(started)
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
	if _, err := pool.Acquire(t.Context()); !errors.Is(err, puddle.ErrClosedPool) {
		t.Fatalf("closed pool acquisition error = %v, want ErrClosedPool", err)
	}
	histogram := collectPoolMetrics(t, reader)[connectionWaitMetric].Data.(metricdata.Histogram[float64])
	type acquisitionResult struct {
		outcome   string
		errorType string
	}
	got := make(map[acquisitionResult]uint64)
	for _, point := range histogram.DataPoints {
		outcome, _ := point.Attributes.Value(ateattr.StoreConnectionAcquireOutcomeKey)
		errorType, _ := point.Attributes.Value(ateattr.ErrorTypeKey)
		got[acquisitionResult{outcome.AsString(), errorType.AsString()}] += point.Count
		if outcome.AsString() == ateattr.StoreConnectionAcquireTimeout &&
			!(point.Sum > 0 && point.Sum <= elapsed.Seconds()) {
			t.Errorf("timeout acquisition duration = %g, want > 0 and <= %g", point.Sum, elapsed.Seconds())
		}
	}
	want := map[acquisitionResult]uint64{
		{ateattr.StoreConnectionAcquireSuccess, ""}:                                     5,
		{ateattr.StoreConnectionAcquireCancelled, ""}:                                   1,
		{ateattr.StoreConnectionAcquireTimeout, ""}:                                     1,
		{ateattr.StoreConnectionAcquireError, ateattr.StoreConnectionAcquirePoolClosed}: 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("acquisition counts = %v, want %v", got, want)
	}
}

func TestPoolMetricsPreservesQueryTracing(t *testing.T) {
	requirePool(t)
	instruments, reader := newPoolMetrics(t)
	qt, tp, sr := newTestQueryTracer(t)
	cfg, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = qt
	instruments.configurePool(cfg, ateattr.DBConnectionPoolMain)
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ctx, parent := tp.Tracer("test").Start(t.Context(), "parent")
	if _, err := pool.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	parent.End()
	span := findSpan(t, sr, "SELECT")
	if span.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Error("query span lost its parent")
	}
	histogram := collectPoolMetrics(t, reader)[connectionWaitMetric].Data.(metricdata.Histogram[float64])
	if len(histogram.DataPoints) != 1 || histogram.DataPoints[0].Count != 1 {
		t.Fatalf("acquisition histogram = %+v, want one sample", histogram)
	}
	assertPendingAcquires(t, reader, map[string]int64{ateattr.DBConnectionPoolMain: 0})
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
	acquisitionCounts := func() map[attribute.Set]uint64 {
		counts := make(map[attribute.Set]uint64)
		histogram := collectPoolMetrics(t, reader)[connectionWaitMetric].Data.(metricdata.Histogram[float64])
		for _, point := range histogram.DataPoints {
			counts[point.Attributes] = point.Count
		}
		return counts
	}
	for _, pool := range []namedPool{
		{ateattr.DBConnectionPoolMain, p.pool},
		{ateattr.DBConnectionPoolWatch, p.watchPool},
		{ateattr.DBConnectionPoolOwner, p.ownerPool},
		{ateattr.DBConnectionPoolProbe, p.probePool},
	} {
		want := acquisitionCounts()
		want[attribute.NewSet(
			ateattr.DBConnectionPoolNameKey.String(pool.name),
			ateattr.StoreConnectionAcquireOutcomeKey.String(ateattr.StoreConnectionAcquireSuccess),
		)]++
		if _, err := pool.pool.Exec(t.Context(), "SELECT 1"); err != nil {
			t.Fatal(err)
		}
		if got := acquisitionCounts(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s acquisition counts = %v, want %v", pool.name, got, want)
		}
	}
	want := make(map[string][3]int64)
	for _, pool := range []namedPool{
		{ateattr.DBConnectionPoolMain, p.pool},
		{ateattr.DBConnectionPoolWatch, p.watchPool},
		{ateattr.DBConnectionPoolOwner, p.ownerPool},
		{ateattr.DBConnectionPoolProbe, p.probePool},
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
	if len(seen) != 4 {
		t.Errorf("acquisition pools = %v, want main, watch, owner, probe", seen)
	}
	p.Close()
	assertPoolCounts(t, reader, nil)
	if err := p.pool.Ping(t.Context()); err != nil {
		t.Errorf("Persistence.Close closed caller-owned main pool: %v", err)
	}
}

type failingPoolMeter struct {
	metric.Meter
}

var errPoolMetric = errors.New("instrument failure")

func (failingPoolMeter) RegisterCallback(metric.Callback, ...metric.Observable) (metric.Registration, error) {
	return nil, errPoolMetric
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
	instruments.meter = failingPoolMeter{Meter: meter}
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

func TestPoolMetricsAcquireOutcomes(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://unused@127.0.0.1:1/test?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	conn, connectErr := pool.Acquire(t.Context())
	if connectErr == nil {
		conn.Release()
		t.Fatal("expected connection establishment failure")
	}
	var typedConnectErr *pgconn.ConnectError
	if !errors.As(connectErr, &typedConnectErr) {
		t.Fatalf("acquisition error is not a ConnectError: %v", connectErr)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name      string
		ctx       context.Context
		err       error
		outcome   string
		errorType string
	}{
		{"success", t.Context(), nil, ateattr.StoreConnectionAcquireSuccess, ""},
		{"closed pool", t.Context(), puddle.ErrClosedPool, ateattr.StoreConnectionAcquireError, ateattr.StoreConnectionAcquirePoolClosed},
		{"wrapped closed pool", t.Context(), fmt.Errorf("acquire: %w", puddle.ErrClosedPool), ateattr.StoreConnectionAcquireError, ateattr.StoreConnectionAcquirePoolClosed},
		{"connect", t.Context(), connectErr, ateattr.StoreConnectionAcquireError, ateattr.StoreConnectionAcquireConnectError},
		{"wrapped connect", t.Context(), fmt.Errorf("acquire: %w", connectErr), ateattr.StoreConnectionAcquireError, ateattr.StoreConnectionAcquireConnectError},
		{"connect with cancelled context", cancelled, connectErr, ateattr.StoreConnectionAcquireError, ateattr.StoreConnectionAcquireConnectError},
		{"unknown", t.Context(), errors.New("unbounded details"), ateattr.StoreConnectionAcquireError, ateattr.StoreConnectionAcquireOtherError},
		{"another unknown", t.Context(), errors.New("different details"), ateattr.StoreConnectionAcquireError, ateattr.StoreConnectionAcquireOtherError},
		{"wrapped cancellation", cancelled, fmt.Errorf("acquire: %w", context.Canceled), ateattr.StoreConnectionAcquireCancelled, ""},
		{"wrapped deadline", t.Context(), fmt.Errorf("acquire: %w", context.DeadlineExceeded), ateattr.StoreConnectionAcquireTimeout, ""},
		{"connect joined with cancellation", cancelled, errors.Join(connectErr, context.Canceled), ateattr.StoreConnectionAcquireError, ateattr.StoreConnectionAcquireConnectError},
		{"success with cancelled context", cancelled, nil, ateattr.StoreConnectionAcquireSuccess, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instruments, reader := newPoolMetrics(t)
			tracer := newPoolMetricsTracer(t, instruments, ateattr.DBConnectionPoolMain)
			tracer.recordAcquire(tc.ctx, 2*time.Millisecond, tc.err)
			m := collectPoolMetrics(t, reader)[connectionWaitMetric]
			if m.Description != (dbconv.ClientConnectionWaitTime{}).Description() {
				t.Errorf("wait_time description = %q, want upstream description", m.Description)
			}
			histogram, ok := m.Data.(metricdata.Histogram[float64])
			if m.Unit != "s" || !ok || len(histogram.DataPoints) != 1 {
				t.Fatalf("wrong acquisition histogram: %+v", m)
			}
			point := histogram.DataPoints[0]
			attrs := []attribute.KeyValue{
				ateattr.DBConnectionPoolNameKey.String(ateattr.DBConnectionPoolMain),
				ateattr.StoreConnectionAcquireOutcomeKey.String(tc.outcome),
			}
			if tc.errorType != "" {
				attrs = append(attrs, ateattr.ErrorTypeKey.String(tc.errorType))
			}
			want := attribute.NewSet(attrs...)
			if !point.Attributes.Equals(&want) || point.Count != 1 || point.Sum != 0.002 || !reflect.DeepEqual(point.Bounds, connectionWaitBuckets) {
				t.Fatalf("acquisition point = %+v, want labels %v, count 1 and duration 0.002", point, want)
			}
		})
	}
}

func TestPoolMetricsInternalConnectTimeout(t *testing.T) {
	instruments, reader := newPoolMetrics(t)
	cfg, err := pgxpool.ParseConfig("postgres://unused@127.0.0.1:1/test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.ConnectTimeout = time.Millisecond
	cfg.ConnConfig.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	instruments.configurePool(cfg, ateattr.DBConnectionPoolMain)
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	conn, err := pool.Acquire(t.Context())
	if err == nil {
		conn.Release()
		t.Fatal("expected connection timeout")
	}
	var connectErr *pgconn.ConnectError
	if !errors.As(err, &connectErr) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquisition error = %v, want ConnectError wrapping DeadlineExceeded", err)
	}
	if err := t.Context().Err(); err != nil {
		t.Fatalf("caller context ended: %v", err)
	}
	histogram := collectPoolMetrics(t, reader)[connectionWaitMetric].Data.(metricdata.Histogram[float64])
	want := attribute.NewSet(
		ateattr.DBConnectionPoolNameKey.String(ateattr.DBConnectionPoolMain),
		ateattr.StoreConnectionAcquireOutcomeKey.String(ateattr.StoreConnectionAcquireError),
		ateattr.ErrorTypeKey.String(ateattr.StoreConnectionAcquireConnectError),
	)
	if len(histogram.DataPoints) != 1 {
		t.Fatalf("acquisition histogram = %+v, want one series", histogram)
	}
	point := histogram.DataPoints[0]
	if !point.Attributes.Equals(&want) || point.Count != 1 || point.Sum <= 0 {
		t.Fatalf("acquisition point = %+v, want labels %v, count 1 and positive duration", point, want)
	}
	assertPendingAcquires(t, reader, map[string]int64{ateattr.DBConnectionPoolMain: 0})
}

func TestPoolMetricsCallerCancellationDuringConnect(t *testing.T) {
	instruments, reader := newPoolMetrics(t)
	cfg, err := pgxpool.ParseConfig("postgres://unused@127.0.0.1:1/test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	dialStarted := make(chan struct{})
	cfg.ConnConfig.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(dialStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	instruments.configurePool(cfg, ateattr.DBConnectionPoolMain)
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		select {
		case <-dialStarted:
			cancel()
		case <-ctx.Done():
		}
	}()
	conn, err := pool.Acquire(ctx)
	if err == nil {
		conn.Release()
		t.Fatal("expected caller cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("acquisition error = %v, want caller cancellation", err)
	}
	histogram := collectPoolMetrics(t, reader)[connectionWaitMetric].Data.(metricdata.Histogram[float64])
	want := attribute.NewSet(
		ateattr.DBConnectionPoolNameKey.String(ateattr.DBConnectionPoolMain),
		ateattr.StoreConnectionAcquireOutcomeKey.String(ateattr.StoreConnectionAcquireCancelled),
	)
	if len(histogram.DataPoints) != 1 || !histogram.DataPoints[0].Attributes.Equals(&want) ||
		histogram.DataPoints[0].Count != 1 {
		t.Fatalf("acquisition histogram = %+v, want one cancelled sample without error.type", histogram)
	}
	assertPendingAcquires(t, reader, map[string]int64{ateattr.DBConnectionPoolMain: 0})
}

func assertPendingAcquires(t *testing.T, reader *sdkmetric.ManualReader, want map[string]int64) {
	t.Helper()
	m := collectPoolMetrics(t, reader)[connectionPendingMetric]
	sum, ok := m.Data.(metricdata.Sum[int64])
	if m.Unit != "{request}" || !ok || sum.IsMonotonic {
		t.Fatalf("wrong pending request instrument: %+v", m)
	}
	if m.Description != (dbconv.ClientConnectionPendingRequests{}).Description() {
		t.Errorf("pending_requests description = %q, want upstream description", m.Description)
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
	tracer := newPoolMetricsTracer(t, instruments, ateattr.DBConnectionPoolMain)
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

func TestPoolMetricsAcquireEndMissingStart(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  context.Context
	}{
		{"missing", t.Context()},
		{"wrong type", context.WithValue(t.Context(), acquireStartKey{}, "invalid")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instruments, reader := newPoolMetrics(t)
			tracer := newPoolMetricsTracer(t, instruments, ateattr.DBConnectionPoolMain)
			for range 3 {
				tracer.TraceAcquireStart(t.Context(), nil, pgxpool.TraceAcquireStartData{})
				// Simulate another tracer replacing the acquisition context.
				tracer.TraceAcquireEnd(tc.ctx, nil, pgxpool.TraceAcquireEndData{})
				assertPendingAcquires(t, reader, map[string]int64{ateattr.DBConnectionPoolMain: 0})
			}
			if m, found := collectPoolMetrics(t, reader)[connectionWaitMetric]; found {
				t.Fatalf("recorded duration without a start time: %+v", m)
			}
			ctx := tracer.TraceAcquireStart(t.Context(), nil, pgxpool.TraceAcquireStartData{})
			tracer.TraceAcquireEnd(ctx, nil, pgxpool.TraceAcquireEndData{})
			assertPendingAcquires(t, reader, map[string]int64{ateattr.DBConnectionPoolMain: 0})
			histogram := collectPoolMetrics(t, reader)[connectionWaitMetric].Data.(metricdata.Histogram[float64])
			if len(histogram.DataPoints) != 1 || histogram.DataPoints[0].Count != 1 {
				t.Fatalf("subsequent acquisition not recorded correctly: %+v", histogram)
			}
		})
	}
}

func BenchmarkPoolMetricsAcquire(b *testing.B) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"cancelled", context.Canceled},
		{"timeout", context.DeadlineExceeded},
		{"pool_closed", puddle.ErrClosedPool},
		{"connect_error", &pgconn.ConnectError{}},
		{"other", errors.New("acquire failure")},
	} {
		b.Run(tc.name, func(b *testing.B) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			b.Cleanup(func() {
				if err := provider.Shutdown(context.Background()); err != nil {
					b.Error(err)
				}
			})
			instruments, err := NewInstruments(provider.Meter("ateapi"))
			if err != nil {
				b.Fatal(err)
			}
			cfg, err := pgxpool.ParseConfig("postgres://unused@localhost/test")
			if err != nil {
				b.Fatal(err)
			}
			instruments.configurePool(cfg, ateattr.DBConnectionPoolMain)
			tracer := cfg.ConnConfig.Tracer.(pgxpool.AcquireTracer)
			ctx := b.Context()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				acquireCtx := tracer.TraceAcquireStart(ctx, nil, pgxpool.TraceAcquireStartData{})
				tracer.TraceAcquireEnd(acquireCtx, nil, pgxpool.TraceAcquireEndData{Err: tc.err})
			}
		})
	}
}
