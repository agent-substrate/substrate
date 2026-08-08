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

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// fakeCollector is a stand-in for the real OTLP collector: it records what the
// relay forwards so the tests can assert the payload arrived unchanged.
type fakeCollector struct {
	coltracepb.UnimplementedTraceServiceServer

	mu      sync.Mutex
	traces  []*coltracepb.ExportTraceServiceRequest
	metrics []*colmetricspb.ExportMetricsServiceRequest
	got     chan struct{}
}

func (f *fakeCollector) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	f.mu.Lock()
	f.traces = append(f.traces, req)
	f.mu.Unlock()
	f.got <- struct{}{}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// metricsSink exists because the two OTLP services both declare Export with
// different request types, the same collision the relay itself works around.
type metricsSink struct {
	colmetricspb.UnimplementedMetricsServiceServer
	parent *fakeCollector
}

func (m *metricsSink) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	m.parent.mu.Lock()
	m.parent.metrics = append(m.parent.metrics, req)
	m.parent.mu.Unlock()
	m.parent.got <- struct{}{}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

// startFakeCollector serves the OTLP collector services on a loopback TCP port
// (the shape the relay forwards to) and returns the sink and its host:port.
func startFakeCollector(t *testing.T) (*fakeCollector, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	sink := &fakeCollector{got: make(chan struct{}, 8)}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, sink)
	colmetricspb.RegisterMetricsServiceServer(srv, &metricsSink{parent: sink})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return sink, lis.Addr().String()
}

// startRelay brings up a relay on a socket in a temp dir, wired to collector.
func startRelay(t *testing.T, collector string) string {
	t.Helper()
	t.Setenv(endpointEnv, collector)

	// Short filename: a unix socket path is capped at ~104 bytes and the test
	// temp dir already eats most of that on darwin.
	sock := filepath.Join(t.TempDir(), "r.sock")
	relay, err := NewServer(context.Background(), sock)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if relay == nil {
		t.Fatal("NewServer returned nil with a collector endpoint set")
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- relay.Serve(context.Background()) }()
	t.Cleanup(relay.Stop)

	// Serve creates the socket asynchronously; Dial's existence check needs it.
	waitForSocket(t, sock, serveErr)
	return sock
}

func waitForSocket(t *testing.T, sock string, serveErr <-chan error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-serveErr:
			t.Fatalf("relay.Serve returned early: %v", err)
		default:
		}
		// Closed immediately: a probe connection that never speaks HTTP/2 sits
		// in the server's handshake path until its 120s timeout, and
		// GracefulStop would wait the whole of it.
		if c, err := net.Dial("unix", sock); err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("relay socket %q never became connectable", sock)
}

// TestRelayForwardsTracesVerbatim is the property the whole design rests on:
// what an ateom exports is what the collector sees, including the resource
// attributes that attribute the spans to that ateom rather than to atelet.
func TestRelayForwardsTracesVerbatim(t *testing.T) {
	sink, collector := startFakeCollector(t)
	sock := startRelay(t, collector)

	conn, err := Dial(context.Background(), sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if conn == nil {
		t.Fatal("Dial returned no connection for an existing socket")
	}
	defer conn.Close()

	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{{
					Key:   "service.name",
					Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "ateom-microvm"}},
				}},
			},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					Name:    "RunWorkload",
					TraceId: []byte("0123456789abcdef"),
					SpanId:  []byte("01234567"),
				}},
			}},
		}},
	}
	if _, err := coltracepb.NewTraceServiceClient(conn).Export(context.Background(), req); err != nil {
		t.Fatalf("Export through the relay: %v", err)
	}

	select {
	case <-sink.got:
	case <-time.After(5 * time.Second):
		t.Fatal("collector never received the forwarded trace export")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.traces) != 1 {
		t.Fatalf("collector got %d trace exports, want 1", len(sink.traces))
	}
	if diff := cmp.Diff(req, sink.traces[0], protocmp.Transform()); diff != "" {
		t.Errorf("forwarded request differs from what was sent (-sent +received):\n%s", diff)
	}
}

func TestRelayForwardsMetrics(t *testing.T) {
	sink, collector := startFakeCollector(t)
	sock := startRelay(t, collector)

	conn, err := Dial(context.Background(), sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{Name: "ateom.workload.runs"}},
			}},
		}},
	}
	if _, err := colmetricspb.NewMetricsServiceClient(conn).Export(context.Background(), req); err != nil {
		t.Fatalf("Export through the relay: %v", err)
	}

	select {
	case <-sink.got:
	case <-time.After(5 * time.Second):
		t.Fatal("collector never received the forwarded metric export")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.metrics) != 1 {
		t.Fatalf("collector got %d metric exports, want 1", len(sink.metrics))
	}
	if diff := cmp.Diff(req, sink.metrics[0], protocmp.Transform()); diff != "" {
		t.Errorf("forwarded request differs from what was sent (-sent +received):\n%s", diff)
	}
}

// TestStopRemovesSocket matters for the restart path: a leftover socket makes
// the next atelet's Listen fail with EADDRINUSE, and in the meantime makes
// every ateom on the node believe a relay is there.
func TestStopRemovesSocket(t *testing.T) {
	_, collector := startFakeCollector(t)
	t.Setenv(endpointEnv, collector)

	sock := filepath.Join(t.TempDir(), "r.sock")
	relay, err := NewServer(context.Background(), sock)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- relay.Serve(context.Background()) }()
	waitForSocket(t, sock, serveErr)

	relay.Stop()
	if _, err := os.Stat(sock); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("os.Stat(%q) after Stop = %v, want the socket to be gone", sock, err)
	}
}

// TestServeReplacesStaleSocket covers the atelet-crashed-and-restarted case:
// the socket file survives the process, and Listen would refuse to reuse it.
func TestServeReplacesStaleSocket(t *testing.T) {
	_, collector := startFakeCollector(t)
	t.Setenv(endpointEnv, collector)

	sock := filepath.Join(t.TempDir(), "r.sock")
	if err := os.WriteFile(sock, nil, 0o600); err != nil {
		t.Fatalf("planting a stale socket file: %v", err)
	}

	relay, err := NewServer(context.Background(), sock)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- relay.Serve(context.Background()) }()
	t.Cleanup(relay.Stop)
	waitForSocket(t, sock, serveErr)
}

func TestNewServerDisabled(t *testing.T) {
	t.Setenv(endpointEnv, "otel-collector:4317")
	relay, err := NewServer(context.Background(), "")
	if err != nil {
		t.Fatalf("NewServer with an empty socket path: %v", err)
	}
	if relay != nil {
		t.Error("NewServer with an empty socket path returned a server, want nil (relay disabled)")
	}
}

func TestNewServerWithoutCollector(t *testing.T) {
	t.Setenv(endpointEnv, "")
	t.Setenv(tracesEndpointEnv, "")
	t.Setenv(metricsEndpointEnv, "")
	relay, err := NewServer(context.Background(), filepath.Join(t.TempDir(), "r.sock"))
	if err != nil {
		t.Fatalf("NewServer with no collector configured: %v", err)
	}
	if relay != nil {
		t.Error("NewServer with no collector configured returned a server; it would accept spans and drop them")
	}
}

func TestDialMissingSocketFallsBack(t *testing.T) {
	conn, err := Dial(context.Background(), filepath.Join(t.TempDir(), "absent.sock"))
	if err != nil {
		t.Fatalf("Dial on an absent socket: %v, want the fallback", err)
	}
	if conn != nil {
		conn.Close()
		t.Error("Dial on an absent socket returned a connection, want nil so the caller exports directly")
	}
}

func TestDialEmptyPath(t *testing.T) {
	conn, err := Dial(context.Background(), "")
	if err != nil {
		t.Fatalf("Dial(\"\"): %v", err)
	}
	if conn != nil {
		conn.Close()
		t.Error("Dial(\"\") returned a connection, want nil")
	}
}

func TestNormalizeEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      string
		want    string
		wantErr string
	}{
		{name: "host and port", in: "otel-collector.ate-system.svc:4317", want: "otel-collector.ate-system.svc:4317"},
		{name: "bare host defaults the port", in: "otel-collector", want: "otel-collector:" + otlpDefaultPort},
		{name: "http url", in: "http://otel-collector:4317", want: "otel-collector:4317"},
		{name: "http url without port", in: "http://otel-collector", want: "otel-collector:" + otlpDefaultPort},
		{name: "ipv6 literal", in: "[::1]:4317", want: "[::1]:4317"},
		{name: "ipv6 literal without port", in: "[::1]", want: "[::1]:" + otlpDefaultPort},
		{name: "https rejected", in: "https://otel-collector:4317", wantErr: "https"},
		{name: "unknown scheme rejected", in: "grpc://otel-collector:4317", wantErr: "unsupported scheme"},
		{name: "empty host rejected", in: "http://:4317", wantErr: "names no host"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeEndpoint(tc.in)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("normalizeEndpoint(%q) = %q, want an error containing %q", tc.in, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("normalizeEndpoint(%q) error = %v, want it to mention %q", tc.in, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeEndpoint(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("normalizeEndpoint(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestUpstreamTarget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		generic string
		traces  string
		metrics string
		want    string
		wantErr bool
	}{
		{name: "unset", want: ""},
		{name: "generic only", generic: "collector:4317", want: "collector:4317"},
		{name: "signal specific overrides generic", generic: "generic:4317", traces: "specific:4317", metrics: "specific:4317", want: "specific:4317"},
		{name: "one signal specific", generic: "generic:4317", metrics: "specific:4317", want: "specific:4317"},
		{name: "signal specific alone", traces: "specific:4317", want: "specific:4317"},
		{name: "conflicting signals rejected", traces: "a:4317", metrics: "b:4317", wantErr: true},
		{name: "whitespace trimmed", generic: "  collector:4317  ", want: "collector:4317"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(endpointEnv, tc.generic)
			t.Setenv(tracesEndpointEnv, tc.traces)
			t.Setenv(metricsEndpointEnv, tc.metrics)
			got, err := upstreamTarget()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("upstreamTarget() = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("upstreamTarget(): %v", err)
			}
			if got != tc.want {
				t.Errorf("upstreamTarget() = %q, want %q", got, tc.want)
			}
		})
	}
}
