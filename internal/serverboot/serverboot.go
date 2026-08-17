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
// long-running substrate server binaries (ateapi, atelet, ateom-gvisor,
// ateom-microvm): slog wiring, OTel tracer + meter providers, a Prometheus +
// /readyz HTTP surface, and a couple of small helpers for startup fail-fast.
package serverboot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"google.golang.org/grpc"
)

// InitLogger sets the global slog logger to a JSON handler wrapped in
// contextlogging.NewHandler, writing to os.Stdout. Call once at process start.
func InitLogger() {
	InitLoggerWithWriter(os.Stdout)
}

// InitLoggerWithWriter is InitLogger with an explicit destination. Use it to share
// one synchronized writer between the runtime logger and a separate writer (e.g.
// ateom's actor-log forwarder) so their lines don't interleave.
func InitLoggerWithWriter(w io.Writer) {
	slog.SetDefault(slog.New(contextlogging.NewHandler(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: &logLevel}))))
}

// logLevel is the dynamic minimum level behind the serverboot loggers.
// A LevelVar so SetLogLevel works before or after InitLogger.
var logLevel slog.LevelVar

// SetLogLevel sets the minimum level of the serverboot loggers from a flag
// value: "debug", "info", "warn", or "error" (case-insensitive). Empty
// means unset and leaves the current level unchanged, so configs that
// never populate the field keep the default.
func SetLogLevel(level string) error {
	if level == "" {
		return nil
	}
	if err := logLevel.UnmarshalText([]byte(level)); err != nil {
		return fmt.Errorf("invalid log level %q (want debug, info, warn, or error): %w", level, err)
	}
	return nil
}

// LogLevel exposes the level behind the serverboot loggers, for binaries
// that build their own handler but should still honor --log-level. A
// Leveler (not the LevelVar) so SetLogLevel stays the only mutation path.
func LogLevel() slog.Leveler { return &logLevel }

// serviceInstanceID is generated once so the tracer and meter resources share it.
var serviceInstanceID = uuid.NewString()

// newResource builds the resource shared by the tracer and meter providers.
// WithFromEnv is last so OTEL_* env vars override the defaults.
func newResource(ctx context.Context, serviceName string, extraAttrs ...attribute.KeyValue) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(serviceName),
		semconv.ServiceInstanceID(serviceInstanceID),
	}
	attrs = append(attrs, extraAttrs...)
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		// Must track the schema version the SDK's own detectors emit, else the
		// merge drops the schema URL with ErrSchemaURLConflict (tolerated below).
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
	)
	if errors.Is(err, resource.ErrPartialResource) || errors.Is(err, resource.ErrSchemaURLConflict) {
		slog.WarnContext(ctx, "partial telemetry resource", slog.Any("err", err))
	} else if err != nil {
		return nil, err
	}
	return res, nil
}

// relayAttrKey records which OTLP export path a signal took. It only makes
// sense for the components that have a relay to take or miss — the ateoms —
// so relayAttrs leaves it off everything else rather than labeling, say,
// atecontroller "direct" for a relay it was never offered.
//
// The name is spelled here rather than taken from internal/ateattr on purpose:
// serverboot is the bottom of the dependency graph (it imports one other
// agent-substrate package) and every binary's main links it, while ateattr
// pulls in pkg/api/v1alpha1, ateapipb, internal/resources and ateletpb. Move it
// to ateattr if that cost ever drops, or if a second non-ate.* consumer appears.
const relayAttrKey = "ate.otlp.relay"

// relayAttrs describes the export path taken by a component that could have
// used the relay. relayCapable false means the component never had one, and
// gets no attribute at all; true means it did, and conn says whether it got it.
//
// The distinction matters because a nil conn on a relay-capable component is
// exactly the degraded case worth alerting on: the ateom asked for the relay,
// could not dial it, and is now exporting over the worker pod's own network.
// Collapsing that into the same "no attribute" bucket as atecontroller would
// hide it.
func relayAttrs(relayCapable bool, conn *grpc.ClientConn) []attribute.KeyValue {
	if !relayCapable {
		return nil
	}
	status := "direct"
	if conn != nil {
		status = "relay"
	}
	return []attribute.KeyValue{attribute.String(relayAttrKey, status)}
}

// TracingOptions configures InitTracing.
type TracingOptions struct {
	// ServiceName is required; populates resource.semconv ServiceName.
	ServiceName string
	// Sampling is required. Build it with ResolveTraceSampling so
	// OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG override the component
	// default.
	Sampling TraceSampling
	// ExporterConn, when non-nil, is the connection the OTLP exporter sends
	// over, instead of dialing OTEL_EXPORTER_OTLP_ENDPOINT itself. ateom passes
	// the unix socket to atelet's relay (internal/otlprelay) so a worker pod
	// exports without a network path of its own; nil keeps the direct dial.
	//
	// The caller owns the connection: the exporter's Shutdown does not close a
	// connection it did not create.
	ExporterConn *grpc.ClientConn
	// RelayCapable marks a component that is meant to export through the relay,
	// whether or not it managed to (see relayAttrs). Only the ateoms set it. It
	// is separate from ExporterConn because a nil conn on its own cannot tell
	// "the ateom tried and fell back" from "this component never had a relay".
	RelayCapable bool
}

// InitTracing registers a global TracerProvider with the given options
// and the TraceContext text-map propagator.
func InitTracing(ctx context.Context, opts TracingOptions) (*sdktrace.TracerProvider, error) {
	if opts.ServiceName == "" {
		return nil, fmt.Errorf("TracingOptions.ServiceName is required")
	}
	if opts.Sampling.sampler == nil {
		return nil, fmt.Errorf("TracingOptions.Sampling is required")
	}
	res, err := newResource(ctx, opts.ServiceName, relayAttrs(opts.RelayCapable, opts.ExporterConn)...)
	if err != nil {
		return nil, fmt.Errorf("create tracer resource: %w", err)
	}

	// The SDK's default handler writes to stderr, bypassing the JSON logs.
	// Registered before NewTracerProvider so its env parsing complaints land
	// in slog too.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		slog.Warn("OpenTelemetry SDK error", slog.Any("err", err))
	}))

	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(opts.Sampling.Sampler()),
	}
	expOpts := []otlptracegrpc.Option{
		// GKE managed traces doesn't support validating the TLS certs of the collector.
		otlptracegrpc.WithInsecure(),
	}
	if opts.ExporterConn != nil {
		// WithGRPCConn takes precedence over endpoint/credential options, so
		// WithInsecure above is inert on this path.
		expOpts = append(expOpts, otlptracegrpc.WithGRPCConn(opts.ExporterConn))
	}
	exporter, err := otlptracegrpc.New(ctx, expOpts...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}
	tpOpts = append(tpOpts, sdktrace.WithBatcher(exporter))

	tp := sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	slog.InfoContext(ctx, "Tracing initialized", slog.String("sampler", opts.Sampling.Sampler().Description()))
	return tp, nil
}

// InitMetrics registers a global MeterProvider with both a Prometheus
// reader (exposed via StartMetricsServer's /metrics handler) and an
// OTLP periodic reader. No Producer option, unlike InitMetricsPushOnly: a bridged
// registry would be served twice, here and on its own endpoint.
func InitMetrics(ctx context.Context, serviceName string) (*sdkmetric.MeterProvider, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("serviceName is required")
	}
	promExporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("create Prometheus metric exporter: %w", err)
	}
	return newMeterProvider(ctx, serviceName, false, nil, nil, promExporter)
}

// InitMetricsPushOnly is InitMetrics without the Prometheus reader, for binaries
// that run no metrics HTTP server of their own (ateom, atecontroller): a pull
// reader would collect into a registry nothing serves. producers put metrics
// recorded outside the OTel SDK on the same push path; atecontroller bridges
// controller-runtime's registry that way.
func InitMetricsPushOnly(ctx context.Context, serviceName string, producers ...sdkmetric.Producer) (*sdkmetric.MeterProvider, error) {
	return newMeterProvider(ctx, serviceName, false, nil, producers)
}

// InitMetricsPushOnlyVia is InitMetricsPushOnly with an explicit exporter
// connection: the metrics counterpart of TracingOptions.ExporterConn. ateom
// passes atelet's relay socket (internal/otlprelay) so the worker pod needs no
// network path of its own; a nil conn keeps the direct dial to
// OTEL_EXPORTER_OTLP_ENDPOINT.
//
// The caller owns the connection: the meter provider's Shutdown does not close
// a connection it did not create.
//
// Calling this at all marks the component relay-capable, so its metrics carry
// the relay attribute (see relayAttrs) either way — "relay" with a conn,
// "direct" without one. It is the metrics counterpart of
// TracingOptions.RelayCapable, implied rather than a parameter because only a
// caller that has a relay to pass reaches for this function in the first place.
func InitMetricsPushOnlyVia(ctx context.Context, serviceName string, conn *grpc.ClientConn, producers ...sdkmetric.Producer) (*sdkmetric.MeterProvider, error) {
	return newMeterProvider(ctx, serviceName, true, conn, producers)
}

func newMeterProvider(ctx context.Context, serviceName string, relayCapable bool, conn *grpc.ClientConn, producers []sdkmetric.Producer, extraReaders ...sdkmetric.Reader) (*sdkmetric.MeterProvider, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("serviceName is required")
	}
	expOpts := []otlpmetricgrpc.Option{
		// GKE managed metrics doesn't support validating the TLS certs of the collector.
		otlpmetricgrpc.WithInsecure(),
	}
	if conn != nil {
		// WithGRPCConn takes precedence over endpoint/credential options, so
		// WithInsecure above is inert on this path.
		expOpts = append(expOpts, otlpmetricgrpc.WithGRPCConn(conn))
	}
	otlpExporter, err := otlpmetricgrpc.New(ctx, expOpts...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}
	res, err := newResource(ctx, serviceName, relayAttrs(relayCapable, conn)...)
	if err != nil {
		return nil, fmt.Errorf("create metric resource: %w", err)
	}
	readerOpts := make([]sdkmetric.PeriodicReaderOption, 0, len(producers))
	for _, p := range producers {
		readerOpts = append(readerOpts, sdkmetric.WithProducer(p))
	}
	opts := []sdkmetric.Option{
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(otlpExporter, readerOpts...)),
	}
	for _, r := range extraReaders {
		opts = append(opts, sdkmetric.WithReader(r))
	}
	mp := sdkmetric.NewMeterProvider(opts...)
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

// Readiness is a predicate for process readiness. The zero value
// reports ready. Calling MarkNotReady flips it permanently to not ready,
// which causes /readyz to start returning "Service Unavailable" (503).
type Readiness struct {
	notReady atomic.Bool
}

// MarkNotReady makes /readyz return 503 from now on.
func (r *Readiness) MarkNotReady() { r.notReady.Store(true) }

// Ready reports whether /readyz returns 200.
func (r *Readiness) Ready() bool { return !r.notReady.Load() }

// MetricsServerOptions configures StartMetricsServer.
type MetricsServerOptions struct {
	// Addr is the TCP listen address (e.g. ":9090").
	Addr string
	// Readiness, if non-nil, enables a /readyz handler: 200 while
	// Ready, 503 after MarkNotReady. A zero-value Readiness never
	// flips, giving a static 200 for binaries with no drain sequence.
	// Nil serves no /readyz at all.
	Readiness *Readiness
	// EnableHealthz adds an always-200 /healthz for liveness probes,
	// which must keep succeeding while a draining server fails /readyz.
	EnableHealthz bool
}

// StartMetricsServer runs an HTTP server exposing /metrics (Prometheus)
// and optionally /readyz and /healthz. Blocks until http.ListenAndServe
// returns; designed to be `go`-launched.
func StartMetricsServer(ctx context.Context, opts MetricsServerOptions) {
	slog.InfoContext(ctx, "Starting metrics HTTP server", slog.String("addr", opts.Addr))
	if err := http.ListenAndServe(opts.Addr, metricsMux(opts)); err != nil {
		slog.Error("Failed to start prometheus metrics server", slog.Any("err", err))
	}
}

// metricsMux builds the handler for StartMetricsServer; split out so
// tests can exercise the endpoints without binding a port.
func metricsMux(opts MetricsServerOptions) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if opts.Readiness != nil {
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
			if !opts.Readiness.Ready() {
				http.Error(w, "draining", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
	}
	if opts.EnableHealthz {
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
	}
	return mux
}
