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
	"os"
	"strings"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

const logsExporterEnv = "OTEL_LOGS_EXPORTER"

// LogsExporter says where log records go. Only the records a component emits
// through the OTel logs API are affected; slog to stdout is always on.
type LogsExporter string

const (
	// LogsExporterNone drops the records. It is the component default, so an
	// environment that has not opted in changes nothing.
	LogsExporterNone LogsExporter = "none"
	// LogsExporterOTLP sends to OTEL_EXPORTER_OTLP_ENDPOINT.
	LogsExporterOTLP LogsExporter = "otlp"
)

// ResolveLogsExporter applies OTEL_LOGS_EXPORTER on top of the component
// default. An unrecognized value keeps the default and logs, rather than
// failing startup over a telemetry setting.
//
// The default is none rather than the spec's otlp while this rolls out, so
// turning it on is one ConfigMap key per environment.
func ResolveLogsExporter(ctx context.Context, def LogsExporter) LogsExporter {
	value, isSet := os.LookupEnv(logsExporterEnv)
	resolved, err := resolveLogsExporter(value, isSet, def)
	if err != nil {
		slog.WarnContext(ctx, "Invalid logs exporter environment, keeping the component default",
			slog.String("exporter", value),
			slog.String("default", string(def)),
			slog.Any("err", err))
	}
	return resolved
}

// resolveLogsExporter accepts otlp and none. Any error means def was kept.
func resolveLogsExporter(value string, isSet bool, def LogsExporter) (LogsExporter, error) {
	if !isSet {
		return def, nil
	}
	value = strings.ToLower(strings.TrimSpace(value))
	// Treat set-but-empty as unset: templated manifests can render empty env vars.
	if value == "" {
		return def, nil
	}

	switch LogsExporter(value) {
	case LogsExporterNone:
		return LogsExporterNone, nil
	case LogsExporterOTLP:
		return LogsExporterOTLP, nil
	}
	return def, fmt.Errorf("unsupported %s %q", logsExporterEnv, value)
}

// LoggingOptions configures InitLogging.
type LoggingOptions struct {
	// ServiceName is required; populates resource.semconv ServiceName.
	ServiceName string
	// Exporter is required. Build it with ResolveLogsExporter so
	// OTEL_LOGS_EXPORTER overrides the component default.
	Exporter LogsExporter
}

// InitLogging registers a global LoggerProvider for the records components emit
// through internal/actorevent.
//
// It returns (nil, nil) when the exporter is none, and leaves the OTel global
// alone: emitting then costs one Enabled check. Guard the shutdown on a nil
// provider, unlike InitTracing which always returns one.
//
// The processor batches. These records sit on the actor resume and suspend
// path, and exporting inside the emit call would put a blocking gRPC round trip
// there, serialized across every emitting goroutine, so an unreachable
// collector would surface as control-plane latency. The cost is a bounded loss
// window on an ungraceful exit, where the same record is still on stdout.
func InitLogging(ctx context.Context, opts LoggingOptions) (*sdklog.LoggerProvider, error) {
	if opts.ServiceName == "" {
		return nil, fmt.Errorf("LoggingOptions.ServiceName is required")
	}
	if opts.Exporter == "" {
		return nil, fmt.Errorf("LoggingOptions.Exporter is required")
	}
	if opts.Exporter == LogsExporterNone {
		slog.InfoContext(ctx, "Logs exporter disabled", slog.String("exporter", string(opts.Exporter)))
		return nil, nil
	}

	res, err := newResource(ctx, opts.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("create logger resource: %w", err)
	}

	exporter, err := otlploggrpc.New(ctx,
		// Matches the trace and metric exporters: GKE managed telemetry does not
		// support validating the TLS certs of the collector.
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP log exporter: %w", err)
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)
	global.SetLoggerProvider(lp)
	slog.InfoContext(ctx, "Logging initialized", slog.String("exporter", string(opts.Exporter)))
	return lp, nil
}
