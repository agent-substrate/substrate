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

package otlprelay

import "github.com/agent-substrate/substrate/internal/env"

const (
	// endpointEnv and its signal-specific overrides are the standard OTLP
	// exporter variables. The relay resolves them itself because it dials the
	// collector directly rather than through an OTel SDK exporter.
	endpointEnv        = "OTEL_EXPORTER_OTLP_ENDPOINT"
	tracesEndpointEnv  = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	metricsEndpointEnv = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
	logsEndpointEnv    = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"

	// compressionEnv and its signal-specific overrides configure upstream
	// gRPC compression (gzip or none).
	compressionEnv        = "OTEL_EXPORTER_OTLP_COMPRESSION"
	tracesCompressionEnv  = "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION"
	metricsCompressionEnv = "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION"
	logsCompressionEnv    = "OTEL_EXPORTER_OTLP_LOGS_COMPRESSION"

	// headersEnv and its signal-specific overrides carry the headers the
	// collector expects (an API key, a tenant id). Unlike the endpoint and the
	// compression, these are per-call metadata rather than per-connection, so
	// traces, metrics, and logs may legitimately differ and are resolved separately.
	headersEnv        = "OTEL_EXPORTER_OTLP_HEADERS"
	tracesHeadersEnv  = "OTEL_EXPORTER_OTLP_TRACES_HEADERS"
	metricsHeadersEnv = "OTEL_EXPORTER_OTLP_METRICS_HEADERS"
	logsHeadersEnv    = "OTEL_EXPORTER_OTLP_LOGS_HEADERS"
)

var endpointVar = env.Var[string]{
	Name:    endpointEnv,
	Default: "",
	Description: `Collector address for the OTLP relay, as a hostname, host:port, or http:// URL.
The default port is 4317. HTTPS and other URL schemes are rejected; the relay uses plaintext gRPC.
Nonempty signal-specific endpoints override this value. Resolved nonempty trace, metric, and log
settings must match exactly. The relay is disabled when all endpoint settings are empty.`,
}

var tracesEndpointVar = env.Var[string]{
	Name:    tracesEndpointEnv,
	Default: "",
	Description: `Collector address for OTLP relay traces, as a hostname, host:port, or http:// URL.
The default port is 4317; HTTPS and other URL schemes are rejected. Nonempty values override
OTEL_EXPORTER_OTLP_ENDPOINT; empty values inherit it. Resolved nonempty trace, metric, and log
settings must match exactly because all signals share one connection.`,
}

var metricsEndpointVar = env.Var[string]{
	Name:    metricsEndpointEnv,
	Default: "",
	Description: `Collector address for OTLP relay metrics, as a hostname, host:port, or http:// URL.
The default port is 4317; HTTPS and other URL schemes are rejected. Nonempty values override
OTEL_EXPORTER_OTLP_ENDPOINT; empty values inherit it. Resolved nonempty trace, metric, and log
settings must match exactly because all signals share one connection.`,
}

var logsEndpointVar = env.Var[string]{
	Name:    logsEndpointEnv,
	Default: "",
	Description: `Collector address for OTLP relay logs, as a hostname, host:port, or http:// URL.
The default port is 4317; HTTPS and other URL schemes are rejected. Nonempty values override
OTEL_EXPORTER_OTLP_ENDPOINT; empty values inherit it. Resolved nonempty trace, metric, and log
settings must match exactly because all signals share one connection.`,
}

var compressionVar = env.Var[string]{
	Name:    compressionEnv,
	Default: "",
	Description: `Compression for the OTLP relay. Accepted values are gzip and none, with surrounding
whitespace ignored; names are case-sensitive. Nonempty signal-specific settings override this value.
Resolved nonempty trace, metric, and log settings must match exactly. If all settings are empty,
compression is disabled. Unsupported or conflicting values prevent the relay from starting.`,
}

var tracesCompressionVar = env.Var[string]{
	Name:    tracesCompressionEnv,
	Default: "",
	Description: `Compression for OTLP relay traces: gzip or none, case-sensitive, with surrounding whitespace ignored.
Nonempty values override OTEL_EXPORTER_OTLP_COMPRESSION; empty values inherit it.
Resolved nonempty trace, metric, and log settings must match exactly. Unsupported or conflicting values
prevent the relay from starting.`,
}

var metricsCompressionVar = env.Var[string]{
	Name:    metricsCompressionEnv,
	Default: "",
	Description: `Compression for OTLP relay metrics: gzip or none, case-sensitive, with surrounding whitespace ignored.
Nonempty values override OTEL_EXPORTER_OTLP_COMPRESSION; empty values inherit it.
Resolved nonempty trace, metric, and log settings must match exactly. Unsupported or conflicting values
prevent the relay from starting.`,
}

var logsCompressionVar = env.Var[string]{
	Name:    logsCompressionEnv,
	Default: "",
	Description: `Compression for OTLP relay logs: gzip or none, case-sensitive, with surrounding whitespace ignored.
Nonempty values override OTEL_EXPORTER_OTLP_COMPRESSION; empty values inherit it.
Resolved nonempty trace, metric, and log settings must match exactly. Unsupported or conflicting values
prevent the relay from starting.`,
}

var headersVar = env.Var[string]{
	Name:    headersEnv,
	Default: "",
	Description: `Headers sent by the OTLP relay, as comma-separated key=value pairs with percent-encoded values.
Header names are case-insensitive. For example, x-team=platform,x-label=hello%20world sets two headers.
Nonempty signal-specific headers replace this entire list rather than merging with it.
Empty means no headers. Malformed entries prevent the relay from starting. Values may contain secrets.`,
}

var tracesHeadersVar = env.Var[string]{
	Name:    tracesHeadersEnv,
	Default: "",
	Description: `Headers sent with OTLP relay traces, as comma-separated key=value pairs with percent-encoded values.
Nonempty values replace OTEL_EXPORTER_OTLP_HEADERS entirely; empty values inherit it.
Header names are case-insensitive. Malformed entries prevent the relay from starting.
Values may contain secrets.`,
}

var metricsHeadersVar = env.Var[string]{
	Name:    metricsHeadersEnv,
	Default: "",
	Description: `Headers sent with OTLP relay metrics, as comma-separated key=value pairs with percent-encoded values.
Nonempty values replace OTEL_EXPORTER_OTLP_HEADERS entirely; empty values inherit it.
Header names are case-insensitive. Malformed entries prevent the relay from starting.
Values may contain secrets.`,
}

var logsHeadersVar = env.Var[string]{
	Name:    logsHeadersEnv,
	Default: "",
	Description: `Headers sent with OTLP relay logs, as comma-separated key=value pairs with percent-encoded values.
Nonempty values replace OTEL_EXPORTER_OTLP_HEADERS entirely; empty values inherit it.
Header names are case-insensitive. Malformed entries prevent the relay from starting.
Values may contain secrets.`,
}
