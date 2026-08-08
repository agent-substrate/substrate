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

// Package otlprelay carries ateom's OTLP telemetry to the collector over a unix
// socket served by atelet, so a worker pod needs no network path of its own to
// export spans and metrics.
//
// Motivation. ateom runs inside the worker pod that hosts the actor, and until
// now exported OTLP straight to the collector over the pod's network (the
// endpoint is injected by atecontroller, see workerpool_apply.go). That has four
// costs the relay removes:
//
//   - Blast radius. The pod runs untrusted agent code. Exporting over the pod
//     network means the pod must be allowed egress to the collector, which is
//     reachable to anything that escapes the sandbox. A unix socket cannot leave
//     the node, so the pod can be denied network egress entirely.
//   - Connection count. Worker pods are heavily oversubscribed, so a node runs
//     many ateoms, each holding its own gRPC connection to the collector. They
//     collapse into atelet's single per-node connection.
//   - Interference. ateom installs a transparent redirect of actor egress to its
//     own atunnel listener; its own outbound traffic has to stay clear of the
//     rules it installs. A unix socket is not IP traffic and cannot be caught.
//   - Shutdown loss. Teardown frees the actor's network and then the pod goes
//     away, which is exactly when the spans describing teardown are still queued
//     in the batch processor. atelet outlives the worker pod.
//
// The relay forwards the OTLP request message verbatim rather than decoding it
// into SDK records and re-exporting. Pass-through keeps each ateom's own
// resource (service.name, service.instance.id, pod attributes) intact, so its
// spans stay attributed to ateom instead of being absorbed into atelet's.
package otlprelay

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

const (
	// endpointEnv and its signal-specific overrides are the standard OTLP
	// exporter variables. The relay resolves them itself because it dials the
	// collector directly rather than through an OTel SDK exporter.
	endpointEnv        = "OTEL_EXPORTER_OTLP_ENDPOINT"
	tracesEndpointEnv  = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	metricsEndpointEnv = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"

	// otlpDefaultPort is the OTLP/gRPC default, used when an endpoint names a
	// host with no port. Matches atenet's normalizeOtlpCollector.
	otlpDefaultPort = "4317"

	// socketMode keeps the relay socket reachable by the ateom pods on the node
	// (which do not necessarily share atelet's uid) while staying off-node by
	// construction. The socket is inside BasePath, a root-owned host directory.
	socketMode = 0o666

	// maxRecvMsgSize bounds a single Export payload. One misbehaving ateom
	// should not be able to make atelet allocate without limit; the OTel SDK's
	// batch processor emits far smaller messages than this.
	maxRecvMsgSize = 16 << 20 // 16 MiB
)

// Server is the atelet half of the relay: an OTLP receiver on a unix socket
// that forwards to the real collector over the node's network.
type Server struct {
	upstream *grpc.ClientConn
	grpc     *grpc.Server
	sockPath string
}

// The two OTLP services both declare a method named Export, with different
// request types, so one type cannot implement both: the embedded Unimplemented
// structs would give Server an ambiguous promoted Export and satisfy neither
// interface. Each service gets its own tiny forwarder instead.

type traceRelay struct {
	coltracepb.UnimplementedTraceServiceServer
	upstream coltracepb.TraceServiceClient
}

// Export forwards a batch of spans to the collector unchanged.
//
// Deliberately not wrapped in a span of atelet's own: the relay must not inject
// itself into the trace it is carrying.
func (t *traceRelay) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	return t.upstream.Export(ctx, req)
}

type metricRelay struct {
	colmetricspb.UnimplementedMetricsServiceServer
	upstream colmetricspb.MetricsServiceClient
}

// Export forwards a batch of metric datapoints to the collector unchanged.
func (m *metricRelay) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	return m.upstream.Export(ctx, req)
}

// NewServer builds a relay that forwards to the collector named by the standard
// OTLP endpoint environment variables. It returns (nil, nil) when sockPath is
// empty (the relay is switched off) or when no endpoint is configured: a relay
// with nowhere to forward to would accept an ateom's spans and drop them, which
// is worse than ateom finding no socket and falling back to a direct export.
func NewServer(ctx context.Context, sockPath string) (*Server, error) {
	if sockPath == "" {
		return nil, nil
	}
	target, err := upstreamTarget()
	if err != nil {
		return nil, err
	}
	if target == "" {
		slog.InfoContext(ctx, "OTLP relay disabled: no collector endpoint configured",
			slog.String("env", endpointEnv))
		return nil, nil
	}

	// Lazy by design: grpc.NewClient does not block on the collector being up,
	// so atelet startup does not depend on the collector's readiness.
	upstream, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("while dialing OTLP collector %q: %w", target, err)
	}

	s := &Server{
		upstream: upstream,
		sockPath: sockPath,
		grpc:     grpc.NewServer(grpc.MaxRecvMsgSize(maxRecvMsgSize)),
	}
	coltracepb.RegisterTraceServiceServer(s.grpc, &traceRelay{upstream: coltracepb.NewTraceServiceClient(upstream)})
	colmetricspb.RegisterMetricsServiceServer(s.grpc, &metricRelay{upstream: colmetricspb.NewMetricsServiceClient(upstream)})
	slog.InfoContext(ctx, "OTLP relay forwarding to collector", slog.String("collector", target))
	return s, nil
}

// Serve listens on the relay socket and blocks until the server stops. Designed
// to be `go`-launched; it returns an error only if the socket cannot be opened
// or serving fails.
func (s *Server) Serve(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(s.sockPath), 0o755); err != nil {
		return fmt.Errorf("while creating the OTLP relay socket directory: %w", err)
	}
	// A socket left behind by a previous atelet would make Listen fail with
	// EADDRINUSE even though nothing holds it.
	if err := os.RemoveAll(s.sockPath); err != nil {
		return fmt.Errorf("while removing a stale OTLP relay socket %q: %w", s.sockPath, err)
	}
	lis, err := net.Listen("unix", s.sockPath)
	if err != nil {
		return fmt.Errorf("while opening the OTLP relay socket %q: %w", s.sockPath, err)
	}
	// net.Listen applies the umask, which on atelet would typically leave the
	// socket group/other-unwritable and unreachable from an ateom running as a
	// different uid. Widen it explicitly.
	if err := os.Chmod(s.sockPath, socketMode); err != nil {
		_ = lis.Close()
		return fmt.Errorf("while setting the OTLP relay socket mode: %w", err)
	}

	slog.InfoContext(ctx, "OTLP relay serving", slog.String("socket", s.sockPath))
	return s.grpc.Serve(lis)
}

// Stop drains the relay and closes the upstream connection.
func (s *Server) Stop() {
	s.grpc.GracefulStop()
	_ = s.upstream.Close()
	_ = os.Remove(s.sockPath)
}

// upstreamTarget resolves the collector address the relay forwards to, from the
// standard OTLP endpoint variables, into the bare host:port grpc.NewClient wants.
//
// The signal-specific variables must agree: the relay carries traces and metrics
// over one connection, so it cannot honor two different collectors. Configuring
// both differently is a misconfiguration rather than something to silently pick
// a winner for.
func upstreamTarget() (string, error) {
	generic := strings.TrimSpace(os.Getenv(endpointEnv))
	traces := strings.TrimSpace(os.Getenv(tracesEndpointEnv))
	metrics := strings.TrimSpace(os.Getenv(metricsEndpointEnv))

	resolved := generic
	for _, specific := range []string{traces, metrics} {
		if specific == "" {
			continue
		}
		if resolved != "" && resolved != generic && specific != resolved {
			return "", fmt.Errorf("%s and %s name different collectors (%q vs %q); the relay carries both signals over one connection",
				tracesEndpointEnv, metricsEndpointEnv, resolved, specific)
		}
		resolved = specific
	}
	if resolved == "" {
		return "", nil
	}
	return normalizeEndpoint(resolved)
}

// normalizeEndpoint accepts both a bare "host:port" and the URL form the OTLP
// environment variables carry, and returns the host:port grpc.NewClient dials.
//
// https is rejected rather than downgraded: the relay dials with insecure
// credentials, so honoring it would ship telemetry in plaintext to an endpoint
// that asked for TLS.
func normalizeEndpoint(addr string) (string, error) {
	hostport := addr
	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil {
			return "", fmt.Errorf("parse OTLP collector endpoint %q: %w", addr, err)
		}
		switch u.Scheme {
		case "http":
		case "https":
			return "", fmt.Errorf("OTLP collector endpoint %q uses https, which the relay does not support: it forwards over an insecure gRPC connection. Point it at an http:// endpoint", addr)
		default:
			return "", fmt.Errorf("OTLP collector endpoint %q has unsupported scheme %q, want http", addr, u.Scheme)
		}
		hostport = u.Host
	}

	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		host = strings.Trim(hostport, "[]")
		port = otlpDefaultPort
	}
	if host == "" {
		return "", fmt.Errorf("OTLP collector endpoint %q names no host", addr)
	}
	return net.JoinHostPort(host, port), nil
}
