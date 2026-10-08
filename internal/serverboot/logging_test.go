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
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/internal/actorevent"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestResolveLogsExporter pins the logs names on top of the shared rules: otlp
// and console, a default of none, and no other signal's names.
func TestResolveLogsExporter(t *testing.T) {
	ctx := context.Background()
	for value, want := range map[string]string{
		"":                "none",
		"otlp":            "otlp",
		"otlp,console":    "console,otlp",
		"console":         "console",
		"prometheus":      "none",
		"otlp,prometheus": "otlp",
	} {
		t.Setenv(logsExporterEnv, value)
		got := ResolveLogsExporter(ctx)
		// InitLogging rejects a nil set, so nil here would stop every component.
		if got == nil {
			t.Fatalf("ResolveLogsExporter() with %q = nil, want a set", value)
		}
		if got.String() != want {
			t.Errorf("ResolveLogsExporter() with %q = %q, want %q", value, got, want)
		}
	}
}

func TestInitLoggingDisabled(t *testing.T) {
	// global.SetLoggerProvider has no undo, so assert on the provider this
	// returns rather than on the global. A nil provider is what proves
	// InitLogging left the global alone.
	before := global.GetLoggerProvider()

	lp, err := InitLogging(context.Background(), LoggingOptions{
		ServiceName: "test",
		Exporter:    Exporters{},
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
	proc := withCaptureProcessor(t)
	lp, err := newLoggerProvider(context.Background(), LoggingOptions{
		ServiceName:  "ateworker-gvisor",
		Exporter:     Exporters{ExporterOTLP: true},
		ExporterConn: conn,
		RelayCapable: relayCapable,
	})
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
			opts: LoggingOptions{Exporter: Exporters{ExporterOTLP: true}},
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

// TestInitLoggingSetsConsole pins that InitLogging, which every component
// calls, decides whether an actor event also goes to stdout beside its OTLP
// event, so no binary has to set it on its own.
func TestInitLoggingSetsConsole(t *testing.T) {
	t.Cleanup(func() { actorevent.SetConsole(false) })
	for _, tc := range []struct {
		exporter   Exporters
		wantStdout int
	}{
		{Exporters{ExporterConsole: true}, 1},
		{Exporters{}, 0},
	} {
		if _, err := InitLogging(context.Background(), LoggingOptions{ServiceName: "test", Exporter: tc.exporter}); err != nil {
			t.Fatalf("InitLogging(%v) error = %v", tc.exporter, err)
		}
		stdout := &countHandler{}
		lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(&captureProcessor{}))
		actorevent.NewEmitterTo(lp, stdout).Log(context.Background(), actorevent.StateChanged, nil)
		_ = lp.Shutdown(context.Background())
		if stdout.n != tc.wantStdout {
			t.Errorf("after InitLogging(%v), an emitted event wrote %d stdout records, want %d", tc.exporter, stdout.n, tc.wantStdout)
		}
	}
}

// TestInitLoggingOTLPAndConsole pins kind's setting: otlp,console installs the
// provider as the global, built from the caller's options, and also keeps each
// actor event on stdout.
func TestInitLoggingOTLPAndConsole(t *testing.T) {
	t.Cleanup(func() { actorevent.SetConsole(false) })
	proc := withCaptureProcessor(t)
	lp, err := InitLogging(context.Background(), LoggingOptions{
		ServiceName:  "test",
		Exporter:     Exporters{ExporterOTLP: true, ExporterConsole: true},
		ExporterConn: lazyConn(t),
		RelayCapable: true,
	})
	if err != nil {
		t.Fatalf("InitLogging() error = %v", err)
	}
	if lp == nil {
		t.Fatal("InitLogging() with otlp,console returned no provider")
	}
	if global.GetLoggerProvider() != lp {
		t.Error("InitLogging() with otlp,console did not install its provider as the global")
	}
	t.Cleanup(func() {
		// A cancelled context skips the OTLP flush; no collector listens here.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = lp.Shutdown(ctx)
	})
	stdout := &countHandler{}
	actorevent.NewEmitterTo(lp, stdout).Log(context.Background(), actorevent.StateChanged, nil)
	if stdout.n != 1 {
		t.Errorf("with otlp,console an emitted event wrote %d stdout records, want 1", stdout.n)
	}
	if proc.res == nil {
		t.Fatal("no record reached the provider")
	}
	attrs := resourceAttrs(proc.res)
	if got := attrs[string(ateattr.OTLPRelayKey)]; got != "relay" {
		t.Errorf("%s = %q, want relay: the caller's relay options did not reach the provider", ateattr.OTLPRelayKey, got)
	}
	if got := attrs["service.name"]; got != "test" {
		t.Errorf("service.name = %q, want test", got)
	}
}

// withCaptureProcessor adds a capture processor to the providers the test
// builds, until it ends.
func withCaptureProcessor(t *testing.T) *captureProcessor {
	t.Helper()
	proc := &captureProcessor{}
	testLogProcessors = []sdklog.Processor{proc}
	t.Cleanup(func() { testLogProcessors = nil })
	return proc
}

// countHandler counts the stdout records it is handed.
type countHandler struct{ n int }

func (h *countHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (h *countHandler) Handle(context.Context, slog.Record) error { h.n++; return nil }
func (h *countHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h *countHandler) WithGroup(string) slog.Handler             { return h }

// TestInitLoggingConsoleAlone pins that console without otlp installs no
// provider: it only keeps the stdout record, which no provider means anyway.
func TestInitLoggingConsoleAlone(t *testing.T) {
	t.Cleanup(func() { actorevent.SetConsole(false) })
	lp, err := InitLogging(context.Background(), LoggingOptions{ServiceName: "test", Exporter: Exporters{ExporterConsole: true}})
	if err != nil {
		t.Fatalf("InitLogging() error = %v", err)
	}
	if lp != nil {
		t.Errorf("InitLogging() with console alone = %v, want nil", lp)
	}
}

// TestInitLoggerInstallsRedactingHandler pins the contract the gRPC
// interceptors rely on: the default logger every server gets from InitLogger
// masks debug_redact fields, so bodies can be logged as they are. Not parallel:
// it swaps the process-wide default logger.
func TestInitLoggerInstallsRedactingHandler(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	var buf bytes.Buffer
	InitLoggerWithWriter(&buf)

	if h := slog.Default().Handler(); h == nil {
		t.Fatal("no default handler installed")
	} else if _, ok := h.(*contextlogging.ContextHandler); !ok {
		t.Fatalf("default handler is %T, want *contextlogging.ContextHandler", h)
	}

	const token = "eyJhbGciOiJSUzI1NiJ9.secret-token"
	slog.Info("Handle RPC", slog.Any("resp", &ateapipb.MintActorJWTResponse{ActorJwt: token}))
	got := buf.String()
	if strings.Contains(got, token) {
		t.Fatalf("default logger wrote a debug_redact field: %s", got)
	}
	if !strings.Contains(got, `"actor_jwt":"[REDACTED]"`) {
		t.Fatalf("default logger did not mask actor_jwt: %s", got)
	}
}
