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
	"io"
	"math"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	promclient "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel"
)

// This exercises production store and exporter wiring, not a deployed ateapi.
func TestPoolMetricsOTLPCollector(t *testing.T) {
	requirePool(t)
	const config = `
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
processors:
  batch:
    timeout: 100ms
exporters:
  prometheus:
    endpoint: 0.0.0.0:8889
  debug:
    verbosity: detailed
service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [prometheus, debug]
`
	collector, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "otel/opentelemetry-collector-contrib:0.157.0@sha256:f2f01157055a9b2aab9df7118e1f1c9abf345e99b23bc7a2bc791db374a7d0f6",
			Cmd:   []string{"--config=/etc/otelcol/pool-metrics.yaml"},
			Files: []testcontainers.ContainerFile{{
				Reader: strings.NewReader(config), ContainerFilePath: "/etc/otelcol/pool-metrics.yaml", FileMode: 0o644,
			}},
			ExposedPorts: []string{"4317/tcp", "8889/tcp"},
			HostConfigModifier: func(cfg *container.HostConfig) {
				cfg.PortBindings = network.PortMap{
					network.MustParsePort("4317/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1")}},
					network.MustParsePort("8889/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1")}},
				}
			},
			WaitingFor: wait.ForHTTP("/metrics").WithPort("8889/tcp").WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	if collector != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			logs, logErr := collector.Logs(ctx)
			if logErr == nil {
				data, readErr := io.ReadAll(logs)
				logs.Close()
				if readErr == nil {
					t.Logf("COLLECTOR LOGS\n%s", data)
				}
			}
			if err := collector.Terminate(ctx); err != nil {
				t.Errorf("terminate collector: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	host, err := collector.Host(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	grpcPort, err := collector.MappedPort(t.Context(), "4317/tcp")
	if err != nil {
		t.Fatal(err)
	}
	scrapePort, err := collector.MappedPort(t.Context(), "8889/tcp")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := fmt.Sprintf("http://%s:%s", host, grpcPort.Port())
	scrapeURL := fmt.Sprintf("http://%s:%s/metrics", host, scrapePort.Port())
	t.Logf("collector ID=%s OTLP=%s scrape=%s PostgreSQL ID=%s", collector.GetContainerID(), endpoint, scrapeURL, containerPG.GetContainerID())

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)
	t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "cumulative")
	// Flush explicitly so assertions do not depend on the periodic interval.
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "3600000")
	previousProvider := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(previousProvider) })
	previousRegisterer, previousGatherer := promclient.DefaultRegisterer, promclient.DefaultGatherer
	registry := promclient.NewRegistry()
	promclient.DefaultRegisterer, promclient.DefaultGatherer = registry, registry
	t.Cleanup(func() {
		promclient.DefaultRegisterer, promclient.DefaultGatherer = previousRegisterer, previousGatherer
	})
	provider, err := serverboot.InitMetrics(t.Context(), "ateapi")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := provider.Shutdown(ctx); err != nil {
			t.Errorf("shutdown metrics: %v", err)
		}
	})
	instruments, err := NewInstruments(provider.Meter("ateapi"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConnectConfig("pool_metrics_otlp")
	cfg.PoolMaxConns = 1
	cfg.Instruments = instruments
	p, err := Connect(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.pool.Close)
	t.Cleanup(p.Close)
	pools := []namedPool{{"main", p.pool}, {"watch", p.watchPool}, {"owner", p.ownerPool}}
	maxima := map[string]int64{"main": 1, "watch": watchPoolMaxConns, "owner": ownerPoolMaxConns}
	var held []*pgxpool.Conn
	defer func() {
		for _, conn := range held {
			conn.Release()
		}
	}()
	for _, pool := range pools {
		for range maxima[pool.name] {
			conn, err := pool.pool.Acquire(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, conn)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
		_, err := pool.pool.Acquire(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s saturated acquisition: %v", pool.name, err)
		}
		ctx, cancel = context.WithCancel(t.Context())
		cancel()
		if _, err := pool.pool.Acquire(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("%s cancelled acquisition: %v", pool.name, err)
		}
	}

	client := &http.Client{Timeout: 5 * time.Second}
	exportAndScrape := func(phase string, check func(map[string]*dto.MetricFamily) error) map[string]*dto.MetricFamily {
		t.Helper()
		if err := provider.ForceFlush(t.Context()); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		var checkErr error
		for {
			resp, err := client.Get(scrapeURL)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusOK {
				t.Fatalf("scrape status=%d error=%v", resp.StatusCode, err)
			}
			parser := expfmt.NewTextParser(model.LegacyValidation)
			families, err := parser.TextToMetricFamilies(strings.NewReader(string(data)))
			if err != nil {
				t.Fatal(err)
			}
			checkErr = check(families)
			if checkErr == nil {
				t.Logf("SCRAPE %s\n%s", phase, data)
				return families
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %v\n%s", phase, checkErr, data)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	saturatedMetrics := exportAndScrape("saturated", func(families map[string]*dto.MetricFamily) error {
		return checkExportedPoolMetrics(families, maxima, true, false)
	})

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		conn, err := p.pool.Acquire(t.Context())
		if err == nil {
			conn.Release()
		}
		done <- err
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("contended acquisition completed before release: %v", err)
	case <-time.After(60 * time.Millisecond):
	}
	for _, conn := range held {
		conn.Release()
	}
	held = nil
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("contended acquisition did not complete after release")
	}
	exportAndScrape("released", func(families map[string]*dto.MetricFamily) error {
		if err := checkExportedPoolMetrics(families, maxima, false, false); err != nil {
			return err
		}
		findSuccess := func(families map[string]*dto.MetricFamily) *dto.Histogram {
			for _, series := range families["db_client_connection_wait_time_seconds"].Metric {
				var main, success bool
				for _, label := range series.Label {
					main = main || (label.GetName() == "db_client_connection_pool_name" && label.GetValue() == "main")
					success = success || (label.GetName() == "ate_store_connection_acquire_outcome" && label.GetValue() == "success")
				}
				if main && success {
					return series.Histogram
				}
			}
			return nil
		}
		before, after := findSuccess(saturatedMetrics), findSuccess(families)
		if before == nil || after == nil || after.GetSampleCount() != before.GetSampleCount()+1 ||
			after.GetSampleSum()-before.GetSampleSum() < 0.05 ||
			after.Bucket[7].GetCumulativeCount() != before.Bucket[7].GetCumulativeCount() {
			return fmt.Errorf("contended acquisition did not export one successful observation over 50ms")
		}
		return nil
	})
	p.pool.Close()
	if _, err := p.pool.Acquire(t.Context()); err == nil {
		t.Fatal("closed pool acquisition succeeded")
	}
	exportAndScrape("closed-main-error", func(families map[string]*dto.MetricFamily) error {
		return checkExportedPoolMetrics(families, maxima, false, true)
	})
}

func checkExportedPoolMetrics(families map[string]*dto.MetricFamily, maxima map[string]int64, saturated, closed bool) error {
	for _, name := range []string{"db_client_connection_count", "db_client_connection_max", "db_client_connection_wait_time_seconds"} {
		family := families[name]
		if family == nil {
			return fmt.Errorf("missing exported family %s", name)
		}
		seen := make(map[string]bool)
		for _, series := range family.Metric {
			labels := make(map[string]string)
			for _, label := range series.Label {
				switch label.GetName() {
				case "db_client_connection_pool_name", "db_client_connection_state", "ate_store_connection_acquire_outcome", "error_type":
					labels[label.GetName()] = label.GetValue()
				case "job", "instance", "otel_scope_name", "otel_scope_version", "otel_scope_schema_url":
				default:
					return fmt.Errorf("%s unexpected label %s", name, label.GetName())
				}
			}
			pool := labels["db_client_connection_pool_name"]
			maximum, ok := maxima[pool]
			if !ok {
				return fmt.Errorf("%s unbounded pool %q", name, pool)
			}
			state, outcome := labels["db_client_connection_state"], labels["ate_store_connection_acquire_outcome"]
			key := pool + "/" + state + "/" + outcome
			if seen[key] {
				return fmt.Errorf("%s duplicate series %s", name, key)
			}
			seen[key] = true
			switch name {
			case "db_client_connection_count", "db_client_connection_max":
				if outcome != "" || labels["error_type"] != "" || series.Gauge == nil {
					return fmt.Errorf("%s unexpected attributes/type", name)
				}
				want := maximum
				if name == "db_client_connection_count" {
					switch state {
					case "used":
						if !saturated {
							want = 0
						}
					case "idle":
						if saturated || (closed && pool == "main") {
							want = 0
						}
					default:
						return fmt.Errorf("unbounded connection state %q", state)
					}
				} else if state != "" {
					return fmt.Errorf("capacity has a state label")
				}
				if series.Gauge.GetValue() != float64(want) {
					return fmt.Errorf("%s %s = %g, want %d", name, key, series.Gauge.GetValue(), want)
				}
			case "db_client_connection_wait_time_seconds":
				if state != "" || series.Histogram == nil {
					return fmt.Errorf("wait_time unexpected attributes/type")
				}
				h := series.Histogram
				switch outcome {
				case "success":
					if h.GetSampleCount() < uint64(maximum) {
						return fmt.Errorf("%s success count too low", pool)
					}
				case "timeout", "cancelled":
					if h.GetSampleCount() != 1 {
						return fmt.Errorf("%s %s count = %d, want 1", pool, outcome, h.GetSampleCount())
					}
					if outcome == "timeout" && (h.GetSampleSum() < 0.04 || h.GetSampleSum() > 5) {
						return fmt.Errorf("%s timeout duration not seconds: %g", pool, h.GetSampleSum())
					}
				case "error":
					if !closed || pool != "main" || labels["error_type"] != "_OTHER" || h.GetSampleCount() != 1 {
						return fmt.Errorf("unexpected error series %v", labels)
					}
				default:
					return fmt.Errorf("unbounded acquisition outcome %q", outcome)
				}
				if outcome != "error" && labels["error_type"] != "" {
					return fmt.Errorf("non-error series carries error_type")
				}
				buckets := h.Bucket
				if len(buckets) != len(connectionWaitBuckets)+1 {
					return fmt.Errorf("%s bucket count = %d", key, len(buckets))
				}
				var count uint64
				for n, bucket := range buckets {
					bound := math.Inf(1)
					if n < len(connectionWaitBuckets) {
						bound = connectionWaitBuckets[n]
					}
					if bucket.GetUpperBound() != bound || bucket.GetCumulativeCount() < count {
						return fmt.Errorf("%s malformed cumulative buckets", key)
					}
					count = bucket.GetCumulativeCount()
				}
				if count != h.GetSampleCount() {
					return fmt.Errorf("%s +Inf does not match count", key)
				}
				if outcome == "timeout" && (buckets[6].GetCumulativeCount() != 0 || buckets[10].GetCumulativeCount() != 1) {
					return fmt.Errorf("%s timeout landed outside seconds buckets", key)
				}
			}
		}
		wantSeries := 3
		switch name {
		case "db_client_connection_count":
			wantSeries = 6
		case "db_client_connection_wait_time_seconds":
			wantSeries = 9
			if closed {
				wantSeries++
			}
		}
		if len(seen) != wantSeries {
			return fmt.Errorf("%s series count = %d, want %d", name, len(seen), wantSeries)
		}
	}
	return nil
}
