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

package main

import "github.com/agent-substrate/substrate/internal/env"

var otelEndpointEnv = env.Var[string]{
	Name:        "OTEL_EXPORTER_OTLP_ENDPOINT",
	Default:     "",
	Description: "Default for --otel-exporter-otlp-endpoint. Empty disables injection of worker telemetry settings; a flag overrides the environment.",
}

var otelMetricExportIntervalEnv = env.Var[string]{
	Name:    "OTEL_METRIC_EXPORT_INTERVAL",
	Default: "",
	Description: `Metric export interval in milliseconds, used as the default for --otel-metric-export-interval.
An explicit flag overrides this value. Forwarded unchanged to workers when an OTLP endpoint is set;
validation is left to the worker SDK. Empty uses the SDK default of 60000 ms.`,
}

var otelMetricExportTimeoutEnv = env.Var[string]{
	Name:    "OTEL_METRIC_EXPORT_TIMEOUT",
	Default: "",
	Description: `Per-export timeout in milliseconds, used as the default for --otel-metric-export-timeout.
An explicit flag overrides this value. Forwarded unchanged to workers when an OTLP endpoint is set;
validation is left to the worker SDK. Empty uses the SDK default of 30000 ms.`,
}

var otelTracesSamplerEnv = env.Var[string]{
	Name:        "OTEL_TRACES_SAMPLER",
	Default:     "",
	Description: "Default for --otel-traces-sampler, forwarded to workers when an OTLP endpoint is set. Empty keeps the worker sampling default.",
}

var otelTracesSamplerArgEnv = env.Var[string]{
	Name:        "OTEL_TRACES_SAMPLER_ARG",
	Default:     "",
	Description: "Default for --otel-traces-sampler-arg, forwarded to workers only when an OTLP endpoint and sampler are set.",
}
