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

// Command actor-telemetry is a reference demo for actor telemetry continuity
// across suspend/resume. It emits OpenTelemetry metrics and trace spans over
// OTLP on every request, then demonstrates the mitigation for the telemetry
// loss described in agent-substrate/substrate#503: when the OTel push interval
// (e.g. 60s) is longer than the active-execution window before idle suspension
// (e.g. 30s), telemetry generated during the active phase stays buffered in the
// SDK's in-memory queues (PeriodicReader accumulators, BatchSpanProcessor ring
// buffer) and is lost when the actor is suspended with onPause: Data (the
// process is killed and RAM is discarded).
//
// The mitigation an actor author can apply TODAY, before the PreSuspend
// lifecycle hook (#450) exists, is to ForceFlush both providers on the
// self-suspend path, immediately before invoking the suspend. This demo models
// that path: an idle watcher detects that the actor has gone quiet and calls
// selfSuspend, which invokes flushTelemetry before it would call the Substrate
// suspend API. When #450 lands, flushTelemetry does not change; only its
// call-site moves, from selfSuspend into a PreSuspend handler.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

const serviceName = "ate-demo-actor-telemetry"

// templateName is a bounded, per-template identifier used as a metric
// attribute. It is deliberately NOT the per-actor name: actor identity on a
// metric datapoint is unbounded cardinality, and this project targets billions
// of actors, so a per-actor label would explode the time-series database. Actor
// identity belongs on spans (events), not on metrics (series). See #761.
const templateName = "ate-demo-actor-telemetry"

// serviceInstanceID is generated once so the tracer and meter resources share
// it, mirroring internal/serverboot.
var serviceInstanceID = uuid.NewString()

var (
	pushInterval = pflag.Duration("push-interval", 60*time.Second,
		"OTLP push/export interval for metrics and spans. Intentionally longer "+
			"than the active window before idle suspension, to reproduce #503.")
	idleFlush = pflag.Duration("idle-flush", 25*time.Second,
		"Duration of request inactivity after which the demo enters the "+
			"self-suspend path and flushes both providers, simulating a "+
			"PreSuspend hook (#450). Set to 0 to disable the flush and reproduce "+
			"the telemetry loss.")

	ready atomic.Bool
)

// flushTelemetry force-flushes the OTel MeterProvider and TracerProvider,
// exporting everything still buffered in the SDK's in-memory queues. This is
// the seam for #503: it is the single place telemetry is drained before an
// actor loses its process memory.
//
// Today it is called from the self-suspend path (selfSuspend), immediately
// before the suspend would be invoked. When the PreSuspend lifecycle hook
// (#450) lands, this function does not change; only its call-site moves, into
// the PreSuspend handler, so the flush also covers Substrate-initiated pauses
// (idle timeout, on-demand) that the application cannot observe today.
func flushTelemetry(ctx context.Context, mp *sdkmetric.MeterProvider, tp *sdktrace.TracerProvider) {
	slog.InfoContext(ctx, "flushTelemetry: exporting buffered telemetry")
	fctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := mp.ForceFlush(fctx); err != nil {
		slog.WarnContext(ctx, "meter ForceFlush failed", slog.Any("err", err))
	}
	if err := tp.ForceFlush(fctx); err != nil {
		slog.WarnContext(ctx, "tracer ForceFlush failed", slog.Any("err", err))
	}
	slog.InfoContext(ctx, "flushTelemetry: complete")
}

// idleFlusher fires onIdle() once the actor has been idle (no signal on
// activity) for timeout. Every send on activity resets the timer. It returns
// when ctx is done. A timeout <= 0 disables the trigger (the watcher only
// drains activity), which is how the demo reproduces the buffered-telemetry
// loss: nothing flushes before the suspend.
func idleFlusher(ctx context.Context, timeout time.Duration, activity <-chan struct{}, onIdle func(context.Context)) {
	if timeout <= 0 {
		for {
			select {
			case <-ctx.Done():
				return
			case <-activity:
			}
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-activity:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(timeout)
		case <-timer.C:
			onIdle(ctx)
			timer.Reset(timeout)
		}
	}
}

// newResource builds the OTel resource shared by the meter and tracer
// providers, mirroring internal/serverboot. WithFromEnv is last so OTEL_*
// env vars (e.g. resource attributes injected by the actor runtime) win.
func newResource(ctx context.Context) (*resource.Resource, error) {
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			// service.instance.id distinguishes this process instance. It is
			// generated once at startup and shared by the meter and tracer
			// resources, mirroring internal/serverboot. Note: like everything
			// built in main(), it is captured in the golden snapshot and is
			// therefore identical across actors of a template; genuine
			// per-actor identity is read fresh from the identity file and put
			// on spans (see actorName), not baked into the resource.
			semconv.ServiceInstanceID(serviceInstanceID),
		),
		resource.WithFromEnv(),
	)
	if errors.Is(err, resource.ErrPartialResource) || errors.Is(err, resource.ErrSchemaURLConflict) {
		slog.WarnContext(ctx, "partial telemetry resource", slog.Any("err", err))
	} else if err != nil {
		return nil, err
	}
	return res, nil
}

// identityFile is the per-actor identity file Substrate bind-mounts read-only
// into every actor at /run/ate/actor-id. It holds the raw actor name with no
// trailing newline. See docs/api-guide.md ("Actor Identity"). It is a var only
// so tests can redirect it; production never reassigns it.
var identityFile = "/run/ate/actor-id"

// actorName reads the actor's own name fresh from the bind-mounted identity
// file on every call. Reading fresh is required, not an optimization we skip:
//
//   - os.Hostname() returns "runsc" for every actor (the hostname is hardcoded
//     in the OCI spec, cmd/atelet/oci.go), so it can never identify an actor.
//   - ATE_ACTOR_NAME is not set by the runtime, and any value captured at
//     process start (env var, or a read cached in a package var) is frozen at
//     the golden actor's name once the process is checkpointed into the
//     snapshot, so it would be identical for every actor of the template.
//
// The bind mount exists precisely so a fresh read after a resume reports the
// correct per-actor name. Returns "unknown" if the file cannot be read.
func actorName() string {
	b, err := os.ReadFile(identityFile)
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(b))
}

// initMeterProvider builds a MeterProvider whose PeriodicReader exports over
// OTLP every push-interval. The long interval is the whole point: it is what
// leaves telemetry buffered when a suspend arrives before the timer fires.
func initMeterProvider(ctx context.Context, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	// No WithInsecure(): it would override the SDK's env-driven TLS config
	// (OTEL_EXPORTER_OTLP_* and the http:// vs https:// endpoint scheme) and
	// silently force plaintext everywhere. See #741. The demo's endpoint is
	// plaintext http://, which the SDK already infers from the scheme.
	exp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp,
			sdkmetric.WithInterval(*pushInterval),
		)),
	)
	otel.SetMeterProvider(mp)
	return mp, nil
}

// initTracerProvider builds a TracerProvider whose BatchSpanProcessor flushes
// over OTLP every push-interval, for the same reason as the meter above.
func initTracerProvider(ctx context.Context, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	// No WithInsecure(): see initMeterProvider and #741.
	exp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(*pushInterval)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}

func main() {
	pflag.Parse()
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	res, err := newResource(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "build resource", slog.Any("err", err))
		os.Exit(1)
	}
	mp, err := initMeterProvider(ctx, res)
	if err != nil {
		slog.ErrorContext(ctx, "init metrics", slog.Any("err", err))
		os.Exit(1)
	}
	tp, err := initTracerProvider(ctx, res)
	if err != nil {
		slog.ErrorContext(ctx, "init tracing", slog.Any("err", err))
		os.Exit(1)
	}

	meter := mp.Meter(serviceName)
	requests, err := meter.Int64Counter(
		"demo.actor.requests",
		metric.WithDescription("Requests handled by the actor-telemetry demo, by template."),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		slog.ErrorContext(ctx, "create counter", slog.Any("err", err))
		os.Exit(1)
	}
	tracer := tp.Tracer(serviceName)

	// selfSuspend is the application self-suspend path (#503 mitigation 4): on
	// going idle, flush buffered telemetry BEFORE invoking the suspend, so a
	// process kill in onPause: Data mode cannot discard it. The suspend API
	// call is a no-op log here since the demo does not drive its own lifecycle;
	// the point is the ordering, flush then suspend. When #450 lands the flush
	// moves into a PreSuspend handler and this path just returns.
	selfSuspend := func(ctx context.Context) {
		flushTelemetry(ctx, mp, tp)
		slog.InfoContext(ctx, "self-suspend: telemetry flushed, ready for suspend")
	}

	// activity signals the idle watcher on each request; buffer of 1 keeps the
	// hot path non-blocking.
	activity := make(chan struct{}, 1)
	go idleFlusher(ctx, *idleFlush, activity, selfSuspend)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "handle-request")
		defer span.End()

		// Read the actor's identity fresh for this request (see actorName).
		actor := actorName()

		// Metric: label with the bounded template name only. Per-actor labels
		// are unbounded cardinality at this project's scale (see templateName).
		// Span/metric attributes use the dotted ate.* namespace (see
		// internal/ateattr), not the ate.dev/ slash form used for k8s labels.
		requests.Add(ctx, 1, metric.WithAttributes(
			attribute.String("ate.actor.template.name", templateName),
		))
		// Span: an event, not a series, so per-actor identity is safe and
		// useful here for correlating a trace to the actor that served it.
		span.SetAttributes(attribute.String("ate.actor.name", actor))

		select {
		case activity <- struct{}{}:
		default:
		}

		msg := fmt.Sprintf("handled request for actor %s | telemetry buffered, will flush after %s idle\n", actor, *idleFlush)
		slog.InfoContext(ctx, "handled request",
			slog.String("trace_id", span.SpanContext().TraceID().String()),
			slog.String("actor", actor),
		)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(msg))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	go func() {
		slog.InfoContext(ctx, "starting actor-telemetry server on port 80",
			slog.Duration("push_interval", *pushInterval),
			slog.Duration("idle_flush", *idleFlush),
		)
		// otelhttp.NewHandler extracts the incoming trace context (propagated
		// by the router over W3C traceparent) and starts a server span, so the
		// per-request span started below joins the router's trace instead of
		// starting a detached root. See #427. Matches cmd/atenet router and
		// cmd/benchmarking/glutton.
		if err := http.ListenAndServe(":80", otelhttp.NewHandler(mux, "/")); err != nil {
			slog.ErrorContext(ctx, "server error", slog.Any("err", err))
			os.Exit(1)
		}
	}()

	ready.Store(true)
	slog.InfoContext(ctx, "readyz now reports OK")

	select {}
}
