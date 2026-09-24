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
	"testing"

	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

func TestResolveLogsExporter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		isSet   bool
		def     LogsExporter
		want    LogsExporter
		wantErr bool
	}{
		{
			name: "unset keeps the default",
			def:  LogsExporterNone,
			want: LogsExporterNone,
		},
		{
			name:  "set but empty reads as unset",
			value: "",
			isSet: true,
			def:   LogsExporterNone,
			want:  LogsExporterNone,
		},
		{
			name:  "whitespace only reads as unset",
			value: "   ",
			isSet: true,
			def:   LogsExporterOTLP,
			want:  LogsExporterOTLP,
		},
		{
			name:  "otlp",
			value: "otlp",
			isSet: true,
			def:   LogsExporterNone,
			want:  LogsExporterOTLP,
		},
		{
			name:  "none overrides an otlp default",
			value: "none",
			isSet: true,
			def:   LogsExporterOTLP,
			want:  LogsExporterNone,
		},
		{
			name:  "mixed case and padding",
			value: "  OTLP\t",
			isSet: true,
			def:   LogsExporterNone,
			want:  LogsExporterOTLP,
		},
		{
			name:    "unsupported value keeps the default",
			value:   "console",
			isSet:   true,
			def:     LogsExporterNone,
			want:    LogsExporterNone,
			wantErr: true,
		},
		{
			name:    "unsupported value keeps a non-default default",
			value:   "otlp/http",
			isSet:   true,
			def:     LogsExporterOTLP,
			want:    LogsExporterOTLP,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveLogsExporter(tt.value, tt.isSet, tt.def)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolveLogsExporter(%q, %v, %q) error = %v, wantErr %v", tt.value, tt.isSet, tt.def, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("resolveLogsExporter(%q, %v, %q) = %q, want %q", tt.value, tt.isSet, tt.def, got, tt.want)
			}
		})
	}
}

func TestResolveLogsExporterReadsEnv(t *testing.T) {
	ctx := context.Background()

	t.Setenv(logsExporterEnv, "otlp")
	if got := ResolveLogsExporter(ctx, LogsExporterNone); got != LogsExporterOTLP {
		t.Errorf("ResolveLogsExporter() = %q, want %q", got, LogsExporterOTLP)
	}

	t.Setenv(logsExporterEnv, "not_an_exporter")
	if got := ResolveLogsExporter(ctx, LogsExporterNone); got != LogsExporterNone {
		t.Errorf("ResolveLogsExporter() on an invalid value = %q, want the default %q", got, LogsExporterNone)
	}
}

func TestInitLoggingDisabled(t *testing.T) {
	// global.SetLoggerProvider has no undo, so assert on the provider this
	// returns rather than on the global. A nil provider is what proves
	// InitLogging left the global alone.
	before := global.GetLoggerProvider()

	lp, err := InitLogging(context.Background(), LoggingOptions{
		ServiceName: "test",
		Exporter:    LogsExporterNone,
	})
	if err != nil {
		t.Fatalf("InitLogging() error = %v", err)
	}
	if lp != nil {
		t.Errorf("InitLogging() with the exporter disabled = %v, want nil", lp)
	}
	if got := global.GetLoggerProvider(); got != before {
		t.Errorf("InitLogging() with the exporter disabled replaced the global provider")
	}
}

// The log path's half of the relay decision, asserted on the resource an
// emitted record carries rather than on what relayAttrs returns, so dropping
// the attrs in newLoggerProvider fails here. TestRelayAttrs pins the values.
func TestLoggerProviderRelayAttribute(t *testing.T) {
	for _, tc := range []struct {
		name         string
		relayCapable bool
		conn         *grpc.ClientConn
		want         string // "" means absent
	}{
		{name: "relay", relayCapable: true, conn: lazyConn(t), want: "relay"},
		{name: "direct", relayCapable: true, want: "direct"},
		{name: "not relay capable", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := emittedResource(t, tc.relayCapable, tc.conn)[string(ateattr.OTLPRelayKey)]
			if ok != (tc.want != "") || got != tc.want {
				t.Errorf("%s = %q (present %t), want %q", string(ateattr.OTLPRelayKey), got, ok, tc.want)
			}
		})
	}
}

// emittedResource builds the provider the way InitLogging does, emits one
// record through a capturing processor, and returns that record's resource.
func emittedResource(t *testing.T, relayCapable bool, conn *grpc.ClientConn) map[string]string {
	t.Helper()
	proc := &captureProcessor{}
	lp, err := newLoggerProvider(context.Background(), LoggingOptions{
		ServiceName:  "ateom-gvisor",
		Exporter:     LogsExporterOTLP,
		ExporterConn: conn,
		RelayCapable: relayCapable,
	}, proc)
	if err != nil {
		t.Fatalf("newLoggerProvider: %v", err)
	}
	t.Cleanup(func() {
		// A cancelled context skips the OTLP flush; no collector listens here.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = lp.Shutdown(ctx)
	})
	lp.Logger("test").Emit(context.Background(), otellog.Record{})
	if proc.res == nil {
		t.Fatal("no record reached the processor")
	}
	return resourceAttrs(proc.res)
}

type captureProcessor struct{ res *resource.Resource }

func (c *captureProcessor) OnEmit(_ context.Context, r *sdklog.Record) error {
	c.res = r.Resource()
	return nil
}
func (c *captureProcessor) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (c *captureProcessor) Shutdown(context.Context) error                         { return nil }
func (c *captureProcessor) ForceFlush(context.Context) error                       { return nil }

func TestInitLoggingRequiresOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts LoggingOptions
	}{
		{
			name: "no service name",
			opts: LoggingOptions{Exporter: LogsExporterOTLP},
		},
		{
			name: "no exporter",
			opts: LoggingOptions{ServiceName: "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := InitLogging(context.Background(), tt.opts); err == nil {
				t.Errorf("InitLogging(%+v) error = nil, want an error", tt.opts)
			}
		})
	}
}
