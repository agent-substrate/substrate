// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serverboot

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/internal/actorevent"
)

const logsExporterEnv = "OTEL_LOGS_EXPORTER"

// logsExporters are the names OTEL_LOGS_EXPORTER accepts. console is the
// actor events' stdout form, the component's own JSON log line.
var logsExporters = []string{ExporterOTLP, ExporterConsole}

// ResolveLogsExporter reads OTEL_LOGS_EXPORTER with the shared exporter rules.
// The default is none rather than the spec's otlp, so turning it on is one
// ConfigMap key per environment.
func ResolveLogsExporter(ctx context.Context) Exporters {
	return resolveExportersEnv(ctx, logsExporterEnv, logsExporters, Exporters{})
}

// LoggingOptions configures InitLogging.
type LoggingOptions struct {
	// ServiceName is required; populates resource.semconv ServiceName.
	ServiceName string
	// Exporter is required; the empty set is none. Build it with
	// ResolveLogsExporter.
	Exporter Exporters
	// ExporterConn and RelayCapable are the logs counterpart of the same fields
	// on TracingOptions: ateom passes its relay connection and marks itself
	// relay-capable so the resource carries ate.otlp.relay.
	ExporterConn *grpc.ClientConn
	RelayCapable bool
}

// InitLogging registers a global LoggerProvider for the records components emit
// through internal/actorevent, and sets whether those records also go to stdout
// (console).
//
// It returns (nil, nil) when the exporter does not include otlp, or when the
// component has no place to push to (see hasExportTarget), and leaves the OTel
// global alone: emitting then costs one Enabled check, and the records go to
// stdout. Guard the shutdown
// on a nil provider, unlike InitTracing which always returns one.
//
// The processor batches. These records sit on the actor resume and suspend
// path, and exporting inside the emit call would put a blocking gRPC round trip
// there, serialized across every emitting goroutine, so an unreachable
// collector would surface as control-plane latency. The cost is that an
// ungraceful exit loses the records still queued, unless console also wrote
// them to stdout.
func InitLogging(ctx context.Context, opts LoggingOptions) (*sdklog.LoggerProvider, error) {
	if opts.ServiceName == "" {
		return nil, fmt.Errorf("LoggingOptions.ServiceName is required")
	}
	if opts.Exporter == nil {
		return nil, fmt.Errorf("LoggingOptions.Exporter is required")
	}
	actorevent.SetConsole(opts.Exporter.Has(ExporterConsole))
	if !opts.Exporter.Has(ExporterOTLP) {
		slog.InfoContext(ctx, "OTLP logs export disabled", slog.String("exporter", opts.Exporter.String()))
		return nil, nil
	}
	if !hasExportTarget(ctx, "logs", opts.RelayCapable, opts.ExporterConn, logsEndpointEnv) {
		return nil, nil
	}

	lp, err := newLoggerProvider(ctx, opts)
	if err != nil {
		return nil, err
	}
	global.SetLoggerProvider(lp)
	slog.InfoContext(ctx, "Logging initialized", slog.String("exporter", opts.Exporter.String()))
	return lp, nil
}

// testLogProcessors are added to every provider newLoggerProvider builds, so a
// test can read the records, and their resource, that the provider emits.
var testLogProcessors []sdklog.Processor

// newLoggerProvider builds the provider InitLogging installs.
func newLoggerProvider(ctx context.Context, opts LoggingOptions) (*sdklog.LoggerProvider, error) {
	res, err := newResource(ctx, opts.ServiceName, relayAttrs(opts.RelayCapable, opts.ExporterConn)...)
	if err != nil {
		return nil, fmt.Errorf("create logger resource: %w", err)
	}

	// Matches the trace and metric exporters: GKE managed telemetry does not
	// support validating the TLS certs of the collector.
	expOpts := []otlploggrpc.Option{otlploggrpc.WithInsecure()}
	if opts.ExporterConn != nil {
		// WithGRPCConn takes precedence over endpoint/credential options, so
		// WithInsecure above is inert on this path.
		expOpts = append(expOpts, otlploggrpc.WithGRPCConn(opts.ExporterConn))
	}
	exporter, err := otlploggrpc.New(ctx, expOpts...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP log exporter: %w", err)
	}

	popts := []sdklog.LoggerProviderOption{
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	}
	for _, p := range testLogProcessors {
		popts = append(popts, sdklog.WithProcessor(p))
	}
	return sdklog.NewLoggerProvider(popts...), nil
}
