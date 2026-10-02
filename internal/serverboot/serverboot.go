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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/google/uuid"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	prombridge "go.opentelemetry.io/contrib/bridges/prometheus"
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
	return []attribute.KeyValue{ateattr.OTLPRelayKey.String(status)}
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

const metricsExporterEnv = "OTEL_METRICS_EXPORTER"

// metricsPushEnabled applies OTEL_METRICS_EXPORTER: otlp, the default, or none,
// which drops the OTLP reader for a component whose metrics are scraped
// instead. An unrecognized value keeps the OTLP export and logs, the same way
// ResolveLogsExporter treats OTEL_LOGS_EXPORTER.
func metricsPushEnabled(ctx context.Context) bool {
	switch value := strings.ToLower(strings.TrimSpace(os.Getenv(metricsExporterEnv))); value {
	case "", "otlp":
		return true
	case "none":
		return false
	default:
		slog.WarnContext(ctx, "Unsupported metrics exporter, keeping the OTLP export",
			slog.String("env", metricsExporterEnv),
			slog.String("exporter", value))
		return true
	}
}

// InitMetrics registers a global MeterProvider with both a Prometheus
// reader (exposed via StartMetricsServer's /metrics handler) and an
// OTLP periodic reader, the latter unless OTEL_METRICS_EXPORTER is "none".
// The Prometheus reader registers on the default registry.
func InitMetrics(ctx context.Context, serviceName string) (*sdkmetric.MeterProvider, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("serviceName is required")
	}
	promReader, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("create Prometheus metric exporter: %w", err)
	}
	return newMeterProvider(ctx, serviceName, metricsPushEnabled(ctx), false, nil, nil, promReader)
}

// InitMetricsBridged is for a binary whose scraped endpoint serves a registry
// it does not own (atecontroller: controller-runtime's), so every metric
// reaches the backend exactly once on whichever path is active. With the OTLP
// export, reg is bridged onto the push and the OTel instruments stay off it,
// since the bridge would otherwise push them a second time. With
// OTEL_METRICS_EXPORTER=none nothing is pushed and the OTel instruments
// register on reg, so its endpoint serves both.
//
// wrapProducer, when non-nil, wraps the bridge producer on the OTLP path, for
// a rewrite the push backend needs but the scraped endpoint does not.
func InitMetricsBridged(ctx context.Context, serviceName string, reg interface {
	promclient.Registerer
	promclient.Gatherer
}, wrapProducer func(sdkmetric.Producer) sdkmetric.Producer) (*sdkmetric.MeterProvider, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("serviceName is required")
	}
	if metricsPushEnabled(ctx) {
		producer := prombridge.NewMetricProducer(prombridge.WithGatherer(reg))
		if wrapProducer != nil {
			producer = wrapProducer(producer)
		}
		return newMeterProvider(ctx, serviceName, true, false, nil, []sdkmetric.Producer{producer})
	}
	promReader, err := prometheus.New(prometheus.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("create Prometheus metric exporter: %w", err)
	}
	return newMeterProvider(ctx, serviceName, false, false, nil, nil, promReader)
}

// InitMetricsPushOnlyVia is InitMetrics without the Prometheus reader, for a
// binary that runs no metrics HTTP server of its own (ateom): a pull reader
// would collect into a registry nothing serves. conn is the metrics counterpart
// of TracingOptions.ExporterConn: ateom passes atelet's relay socket
// (internal/otlprelay) so the worker pod needs no network path of its own; a
// nil conn keeps the direct dial to OTEL_EXPORTER_OTLP_ENDPOINT.
//
// The caller owns the connection: the meter provider's Shutdown does not close
// a connection it did not create.
//
// Calling this at all marks the component relay-capable, so its metrics carry
// the relay attribute (see relayAttrs) either way — "relay" with a conn,
// "direct" without one. It is the metrics counterpart of
// TracingOptions.RelayCapable, implied rather than a parameter because only a
// caller that has a relay to pass reaches for this function in the first place.
func InitMetricsPushOnlyVia(ctx context.Context, serviceName string, conn *grpc.ClientConn) (*sdkmetric.MeterProvider, error) {
	return newMeterProvider(ctx, serviceName, metricsPushEnabled(ctx), true, conn, nil)
}

func newMeterProvider(ctx context.Context, serviceName string, push, relayCapable bool, conn *grpc.ClientConn, producers []sdkmetric.Producer, extraReaders ...sdkmetric.Reader) (*sdkmetric.MeterProvider, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("serviceName is required")
	}
	res, err := newResource(ctx, serviceName, relayAttrs(relayCapable, conn)...)
	if err != nil {
		return nil, fmt.Errorf("create metric resource: %w", err)
	}
	opts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	if push {
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
		readerOpts := make([]sdkmetric.PeriodicReaderOption, 0, len(producers))
		for _, p := range producers {
			readerOpts = append(readerOpts, sdkmetric.WithProducer(p))
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(otlpExporter, readerOpts...)))
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
func Fatal(ctx context.Context, msg string, err error, additionalArgs ...any) {
	args := []any{slog.Any("err", err)}
	args = append(args, additionalArgs...)
	slog.ErrorContext(ctx, msg, args...)
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

// checkTimeout bounds the dependency checks of one /readyz request. It sits
// under the kubelet's default 1s probe timeout so a hung dependency gets a 503
// rather than a timed-out probe.
const checkTimeout = 500 * time.Millisecond

// Readiness is a predicate for process readiness. The zero value
// reports ready. Calling MarkNotReady flips it permanently to not ready,
// which causes /readyz to start returning "Service Unavailable" (503).
// Checks added with AddCheck make /readyz return 503 only while they fail.
type Readiness struct {
	notReady atomic.Bool

	mu     sync.RWMutex
	checks []*readinessCheck
}

type readinessCheck struct {
	name    string
	fn      func(context.Context) error
	failing atomic.Bool
}

// MarkNotReady makes /readyz return 503 from now on.
func (r *Readiness) MarkNotReady() { r.notReady.Store(true) }

// Ready reports whether MarkNotReady has not been called. It does not run the
// checks added with AddCheck.
func (r *Readiness) Ready() bool { return !r.notReady.Load() }

// AddCheck registers a dependency check that /readyz runs on every request.
// While it returns an error, or does not return within checkTimeout, /readyz
// returns 503, and it returns 200 again once the check passes. Liveness is
// unaffected. Check errors are logged when a check starts and stops failing,
// and never sent to the client.
func (r *Readiness) AddCheck(name string, check func(context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks = append(r.checks, &readinessCheck{name: name, fn: check})
}

// notReadyReason returns why /readyz should answer 503, or "" if it should
// answer 200. Draining wins over the checks, which are not run.
func (r *Readiness) notReadyReason(ctx context.Context) string {
	if !r.Ready() {
		return "draining"
	}
	r.mu.RLock()
	checks := slices.Clone(r.checks)
	r.mu.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	for _, c := range checks {
		if err := c.fn(ctx); err != nil {
			if !c.failing.Swap(true) {
				slog.WarnContext(ctx, "Readiness check failing", slog.String("check", c.name), slog.Any("err", err))
			}
			return c.name + " check failed"
		}
		if c.failing.Swap(false) {
			slog.InfoContext(ctx, "Readiness check recovered", slog.String("check", c.name))
		}
	}
	return ""
}

// MetricsServerOptions configures StartMetricsServer.
type MetricsServerOptions struct {
	// Addr is the TCP listen address (e.g. ":9090").
	Addr string
	// Readiness, if non-nil, enables a /readyz handler: 200 while
	// Ready and every check passes, 503 after MarkNotReady or while a
	// check fails. A zero-value Readiness never flips, giving a static
	// 200 for binaries with no drain sequence. Nil serves no /readyz at all.
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

// StartReadinessServer runs an HTTP server exposing only /readyz. Blocks until
// http.ListenAndServe returns; designed to be `go`-launched. A serve failure
// exits the process: a worker whose readiness endpoint cannot come up never
// turns Ready and never registers, so dying loudly lets the kubelet restart it
// instead of leaving a pod that looks alive but can never receive work.
func StartReadinessServer(ctx context.Context, addr string, readiness *Readiness) {
	slog.InfoContext(ctx, "Starting readiness HTTP server", slog.String("addr", addr))
	if err := http.ListenAndServe(addr, readinessMux(readiness)); err != nil {
		slog.Error("Readiness HTTP server failed", slog.Any("err", err))
		os.Exit(1)
	}
}

// metricsMux builds the handler for StartMetricsServer; split out so
// tests can exercise the endpoints without binding a port.
func metricsMux(opts MetricsServerOptions) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if opts.Readiness != nil {
		mux.Handle("/readyz", readinessHandler(opts.Readiness))
	}
	if opts.EnableHealthz {
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
	}
	return mux
}

// readinessMux builds the handler for StartReadinessServer.
func readinessMux(readiness *Readiness) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/readyz", readinessHandler(readiness))
	return mux
}

func readinessHandler(readiness *Readiness) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reason := readiness.notReadyReason(r.Context()); reason != "" {
			http.Error(w, reason, http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}
