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

// Package serverboot collects the startup boilerplate shared by the
// long-running substrate server binaries (ateapi, atelet, ateom-gvisor):
// slog wiring, OTel tracer + meter providers, a Prometheus + /readyz
// HTTP surface, and a couple of small helpers for startup fail-fast.
package serverboot

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// InitLogger sets the global slog logger to a JSON handler wrapped in
// contextlogging.NewHandler. Call once at process start.
func InitLogger() {
	slog.SetDefault(slog.New(contextlogging.NewHandler(slog.NewJSONHandler(os.Stdout, nil))))
}

// TracingOptions configures InitTracing.
type TracingOptions struct {
	// ServiceName is required; populates resource.semconv ServiceName.
	ServiceName string
	// Sampler is required. ateapi typically uses ParentBased(AlwaysSample);
	// atelet/ateom-gvisor use ParentBased(NeverSample).
	Sampler sdktrace.Sampler
	// NoExporter skips the OTLP exporter. Used by binaries with no
	// network egress (ateom-gvisor, after eth0 moves into the gvisor
	// netns).
	NoExporter bool
}

// InitTracing registers a global TracerProvider with the given options
// and the TraceContext text-map propagator.
func InitTracing(ctx context.Context, opts TracingOptions) (*sdktrace.TracerProvider, error) {
	if opts.ServiceName == "" {
		return nil, fmt.Errorf("TracingOptions.ServiceName is required")
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(opts.ServiceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("create tracer resource: %w", err)
	}

	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(opts.Sampler),
	}
	if !opts.NoExporter {
		exporter, err := otlptracegrpc.New(ctx,
			// GKE managed traces doesn't support validating the TLS certs of the collector.
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("create OTLP exporter: %w", err)
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exporter))
	}

	tp := sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}

// InitMetrics registers a global MeterProvider with both a Prometheus
// reader (exposed via StartMetricsServer's /metrics handler) and an
// OTLP periodic reader.
func InitMetrics(ctx context.Context, serviceName string) (*sdkmetric.MeterProvider, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("serviceName is required")
	}
	promExporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("create Prometheus metric exporter: %w", err)
	}
	otlpExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("create metric resource: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(otlpExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	return mp, nil
}

// Fatal logs msg + err and exits with status 1. For startup-time
// fail-fast where there's no recovery path.
func Fatal(ctx context.Context, msg string, err error) {
	slog.ErrorContext(ctx, msg, slog.Any("err", err))
	os.Exit(1)
}

// ShutdownProvider invokes the OTel provider's Shutdown and logs any
// error. Designed to be deferred from main():
//
//	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)
func ShutdownProvider(name string, shutdown func(context.Context) error) {
	if err := shutdown(context.Background()); err != nil {
		slog.Error("Failed to shutdown "+name, slog.Any("err", err))
	}
}

// MetricsServerOptions configures StartMetricsServer.
type MetricsServerOptions struct {
	// Addr is the TCP listen address (e.g. ":9090").
	Addr string
	// EnableReadyz adds a /readyz handler that returns 200 OK. Some
	// binaries (ateapi) want it for Kubernetes readiness probes; others
	// (atelet) historically didn't surface one.
	EnableReadyz bool
}

// StartMetricsServer runs an HTTP server exposing /metrics (Prometheus)
// and optionally /readyz. Blocks until http.ListenAndServe returns;
// designed to be `go`-launched.
func StartMetricsServer(ctx context.Context, opts MetricsServerOptions) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if opts.EnableReadyz {
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
	}
	slog.InfoContext(ctx, fmt.Sprintf("Starting Prometheus metrics server on %s", opts.Addr))
	if err := http.ListenAndServe(opts.Addr, mux); err != nil {
		slog.Error("Failed to start prometheus metrics server", slog.Any("err", err))
	}
}
