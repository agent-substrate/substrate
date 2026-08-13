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
// into SDK records and re-exporting. Verbatim pass-through keeps each ateom's
// own resource (service.name, service.instance.id, pod attributes) intact, so
// its spans stay attributed to ateom instead of being absorbed into atelet's.
// Restricting this pass-through to verified ateom sources ensures that future
// actor telemetry requiring identity rewrites (#761) will be added as an
// explicit rewriting path alongside this forwarder; see ateomServices.
//
// Verbatim applies to the payload, not to the call around it. The request's
// metadata is dropped and replaced with the headers atelet resolves from its own
// OTEL_EXPORTER_OTLP_HEADERS, since the upstream leg is atelet's connection and
// authenticating it is atelet's business; see upstreamContext.
package otlprelay

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"k8s.io/utils/lru"

	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

const (
	// endpointEnv and its signal-specific overrides are the standard OTLP
	// exporter variables. The relay resolves them itself because it dials the
	// collector directly rather than through an OTel SDK exporter.
	endpointEnv        = "OTEL_EXPORTER_OTLP_ENDPOINT"
	tracesEndpointEnv  = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	metricsEndpointEnv = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"

	// compressionEnv and its signal-specific overrides configure upstream
	// gRPC compression (gzip or none).
	compressionEnv        = "OTEL_EXPORTER_OTLP_COMPRESSION"
	tracesCompressionEnv  = "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION"
	metricsCompressionEnv = "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION"

	// headersEnv and its signal-specific overrides carry the headers the
	// collector expects (an API key, a tenant id). Unlike the endpoint and the
	// compression, these are per-call metadata rather than per-connection, so
	// traces and metrics may legitimately differ and are resolved separately.
	headersEnv        = "OTEL_EXPORTER_OTLP_HEADERS"
	tracesHeadersEnv  = "OTEL_EXPORTER_OTLP_TRACES_HEADERS"
	metricsHeadersEnv = "OTEL_EXPORTER_OTLP_METRICS_HEADERS"

	// otlpDefaultPort matches atenet's normalizeOtlpCollector.
	otlpDefaultPort = "4317"

	// socketMode keeps the relay socket private to root: both atelet and the
	// ateom worker pods run as root (runAsUser: 0). The socket lives inside
	// BasePath, a root-owned host directory.
	socketMode = 0o600

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

// ateomServices are the only sources this relay carries, keyed by the
// service.name their resource declares — which the OTEL_* environment can
// override out from under an ateom; see sourceGate for what that looks like.
// Mirrors the serviceName constants in
// cmd/ateom-gvisor and cmd/ateom-microvm, which are package main and cannot be
// imported; TestAteomServicesMatchTheAteomBinaries guards the duplication.
//
// This allowlist is a protocol contract rather than a security boundary:
// service.name is client-provided, so a compromised process could claim an
// ateom name. Its purpose is to prevent accidental misuse (e.g. an actor SDK
// pointed at the socket) and keep the pass-through contract explicit for #761.
// Peer authentication, if needed, would require per-pod sockets or UDS peer
// credentials (SO_PEERCRED) tied to #741.
var ateomServices = map[string]bool{
	"ateom-gvisor":  true,
	"ateom-microvm": true,
}

// sourceGate applies the ateomServices allowlist and reports the first
// rejection of each service.name.
//
// The log matters because of how this failure presents. service.name is
// whatever the resource declares, and resource.WithFromEnv() runs last in
// serverboot.newResource, so OTEL_SERVICE_NAME or an OTEL_RESOURCE_ATTRIBUTES
// entry set on a worker pod overrides the ateom's own name. The relay then
// refuses every export with PermissionDenied, which the OTel SDK does not retry
// — it drops the batch and reports through the SDK error handler. Telemetry from
// that ateom simply stops, with nothing on the collector side to say why. One
// line per distinct name on the node makes it greppable; the ateom itself is
// unaffected, so this is a diagnosability problem rather than an outage.
//
// Dedup is keyed by name with an LRU cache bounded at 256 entries to prevent
// memory growth from arbitrary client-provided service names, while still
// suppressing spam from a rejected exporter that retries repeatedly for the
// pod's whole life.
type sourceGate struct {
	logged *lru.Cache // service.name -> struct{}
}

func newSourceGate() *sourceGate {
	return &sourceGate{
		logged: lru.New(256),
	}
}

// check rejects a missing service.name along with an unrecognized one: an
// unidentified source is the one the relay cannot vouch for.
func (g *sourceGate) check(ctx context.Context, r *resourcepb.Resource) error {
	name := resourceServiceName(r)
	if ateomServices[name] {
		return nil
	}
	if g.logged == nil {
		g.logged = lru.New(256)
	}
	if _, seen := g.logged.Get(name); !seen {
		g.logged.Add(name, struct{}{})
		slog.WarnContext(ctx, "OTLP relay rejected telemetry from an unrecognized source; it is being dropped, not retried. If this is an ateom, check whether OTEL_SERVICE_NAME or OTEL_RESOURCE_ATTRIBUTES on the worker pod is overriding its service.name",
			slog.String("service.name", name),
			slog.Any("allowed", allowedServices()),
			slog.String("note", "logged once per distinct service.name"))
	}
	return status.Errorf(codes.PermissionDenied,
		"the OTLP relay carries ateom telemetry only, got service.name %q; a source whose identity has to be rewritten (#761) must not be forwarded verbatim. If this is an ateom, an OTEL_SERVICE_NAME or OTEL_RESOURCE_ATTRIBUTES override on the worker pod would produce exactly this",
		name)
}

func allowedServices() []string {
	names := make([]string, 0, len(ateomServices))
	for name := range ateomServices {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func resourceServiceName(r *resourcepb.Resource) string {
	for _, attr := range r.GetAttributes() {
		if attr.GetKey() == string(semconv.ServiceNameKey) {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

// upstreamContext builds the metadata for the upstream call from atelet's own
// configuration, dropping whatever the ateom sent.
//
// The upstream leg is atelet's connection to the collector, so its credentials
// belong to atelet: the relay resolves OTEL_EXPORTER_OTLP_HEADERS from its own
// environment, exactly as the SDK exporter it replaces would have. Forwarding
// the client's headers instead would let anything that reached the socket choose
// what atelet presents to the collector — a header set is not telemetry to be
// passed through verbatim the way the resource is, and unlike service.name (see
// ateomServices) it is not merely claimed identity but an actual credential.
//
// Nothing is allow-listed through. atecontroller injects only
// OTEL_EXPORTER_OTLP_ENDPOINT into worker pods (workerpool_apply.go), so no
// ateom has a header to lose today; add an allowlist here, not a blanket
// forward, if one ever needs to reach the collector.
//
// The incoming metadata is dropped by simply not copying it: gRPC never
// propagates incoming metadata to an outgoing call on its own.
func upstreamContext(ctx context.Context, md metadata.MD) context.Context {
	if len(md) == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// parseHeaders reads the W3C-Baggage-shaped list the OTLP headers variables
// carry ("key1=value1,key2=value2", values percent-encoded), as the OTel SDK
// exporters do.
//
// Keys are lower-cased because gRPC metadata keys are case-insensitive and
// metadata.MD is documented to hold them lower-cased; a mixed-case key set here
// would otherwise be invisible to metadata.Get.
func parseHeaders(raw string) (metadata.MD, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	md := metadata.MD{}
	// Errors name the position rather than the offending text: any of these
	// entries may be a credential, and this error reaches a log line.
	for i, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, value, found := strings.Cut(pair, "=")
		if !found {
			return nil, fmt.Errorf("OTLP header %d is not in key=value form", i+1)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			return nil, fmt.Errorf("OTLP header %d has an empty name", i+1)
		}
		// Percent-decoding is what makes a value containing "," or "=" (a base64
		// token, say) expressible in this format at all. PathUnescape preserves '+'
		// as a literal character rather than converting it to a space.
		decoded, err := url.PathUnescape(strings.TrimSpace(value))
		if err != nil {
			// The value is deliberately not in the message: these are credentials.
			return nil, fmt.Errorf("OTLP header %q has a value that is not valid percent-encoding: %w", key, err)
		}
		md.Append(key, decoded)
	}
	return md, nil
}

// upstreamHeaders resolves the headers for one signal. Per the OTLP spec the
// signal-specific variable replaces the generic one rather than merging with
// it, so a component that sets both gets exactly what the SDK would have sent.
func upstreamHeaders(signalEnv string) (metadata.MD, error) {
	env, raw := signalEnv, strings.TrimSpace(os.Getenv(signalEnv))
	if raw == "" {
		env, raw = headersEnv, os.Getenv(headersEnv)
	}
	md, err := parseHeaders(raw)
	if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", env, err)
	}
	return md, nil
}

// The two OTLP services both declare a method named Export, with different
// request types, so one type cannot implement both: the embedded Unimplemented
// structs would give Server an ambiguous promoted Export and satisfy neither
// interface. Each service gets its own tiny forwarder instead.

type traceRelay struct {
	coltracepb.UnimplementedTraceServiceServer
	upstream coltracepb.TraceServiceClient
	// headers atelet presents to the collector; see upstreamContext. Resolved
	// once at construction: they come from atelet's environment, not the call.
	headers metadata.MD
	// gate is shared with metricRelay so a misnamed ateom is reported once, not
	// once per signal.
	gate *sourceGate
}

// Export forwards a batch of spans to the collector unchanged.
//
// Deliberately not wrapped in a span of atelet's own: the relay must not inject
// itself into the trace it is carrying.
//
// A batch is refused whole rather than having the offending resource dropped: a
// partial success the sender reads as success loses telemetry silently.
func (t *traceRelay) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	for _, rs := range req.GetResourceSpans() {
		if err := t.gate.check(ctx, rs.GetResource()); err != nil {
			return nil, err
		}
	}
	return t.upstream.Export(upstreamContext(ctx, t.headers), req)
}

type metricRelay struct {
	colmetricspb.UnimplementedMetricsServiceServer
	upstream colmetricspb.MetricsServiceClient
	headers  metadata.MD
	gate     *sourceGate
}

// Export forwards a batch of metric datapoints to the collector unchanged.
func (m *metricRelay) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	for _, rm := range req.GetResourceMetrics() {
		if err := m.gate.check(ctx, rm.GetResource()); err != nil {
			return nil, err
		}
	}
	return m.upstream.Export(upstreamContext(ctx, m.headers), req)
}

// validateSocketPath rejects a relative path, which gRPC does not resolve:
// "unix://foo/r.sock" parses as authority "foo", path "/r.sock". grpc.NewClient
// being lazy, that wrong target is accepted at startup and fails per export
// afterwards, which is why this errors rather than falling back.
func validateSocketPath(sockPath string) error {
	if !filepath.IsAbs(sockPath) {
		return fmt.Errorf("the OTLP relay socket path %q is relative; it must be absolute, since atelet and ateom would otherwise resolve it against different working directories", sockPath)
	}
	return nil
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
	if err := validateSocketPath(sockPath); err != nil {
		return nil, err
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

	comp, err := upstreamCompression()
	if err != nil {
		return nil, err
	}

	// Resolved before the socket exists: a header set atelet cannot parse would
	// otherwise become a per-export failure against a collector that rejects the
	// unauthenticated calls, which is harder to read than refusing to start the
	// relay. ateom then finds no socket and exports directly.
	traceHeaders, err := upstreamHeaders(tracesHeadersEnv)
	if err != nil {
		return nil, err
	}
	metricHeaders, err := upstreamHeaders(metricsHeadersEnv)
	if err != nil {
		return nil, err
	}

	dialOpts := []grpc.DialOption{
		// Plaintext by design today; TLS support for the upstream leg will be added
		// in tandem with #741.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if comp == "gzip" {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}

	// Lazy by design: grpc.NewClient does not block on the collector being up,
	// so atelet startup does not depend on the collector's readiness.
	upstream, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("while dialing OTLP collector %q: %w", target, err)
	}

	s := &Server{
		upstream: upstream,
		sockPath: sockPath,
		grpc:     grpc.NewServer(grpc.MaxRecvMsgSize(maxRecvMsgSize)),
	}
	gate := newSourceGate()
	coltracepb.RegisterTraceServiceServer(s.grpc, &traceRelay{
		upstream: coltracepb.NewTraceServiceClient(upstream),
		headers:  traceHeaders,
		gate:     gate,
	})
	colmetricspb.RegisterMetricsServiceServer(s.grpc, &metricRelay{
		upstream: colmetricspb.NewMetricsServiceClient(upstream),
		headers:  metricHeaders,
		gate:     gate,
	})
	// Header names only: the values are credentials.
	slog.InfoContext(ctx, "OTLP relay forwarding to collector",
		slog.String("collector", target),
		slog.String("compression", comp),
		slog.Any("traceHeaders", headerNames(traceHeaders)),
		slog.Any("metricHeaders", headerNames(metricHeaders)))
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
	if err := os.Remove(s.sockPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
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

// Stop drains the relay, closes the upstream connection and removes the socket.
func (s *Server) Stop() {
	s.grpc.GracefulStop()
	_ = s.upstream.Close()
	_ = os.Remove(s.sockPath)
}

// headerNames lists the configured header names, sorted, for logging. It exists
// so an operator can confirm the relay picked up the headers without the values
// reaching the node's logs.
func headerNames(md metadata.MD) []string {
	names := make([]string, 0, len(md))
	for k := range md {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// upstreamCompression resolves the compression algorithm (gzip or none) to use
// for upstream export.
func upstreamCompression() (string, error) {
	generic := strings.TrimSpace(os.Getenv(compressionEnv))
	traces := strings.TrimSpace(os.Getenv(tracesCompressionEnv))
	metrics := strings.TrimSpace(os.Getenv(metricsCompressionEnv))

	traceComp := generic
	if traces != "" {
		traceComp = traces
	}
	metricComp := generic
	if metrics != "" {
		metricComp = metrics
	}

	if traceComp != "" && metricComp != "" && traceComp != metricComp {
		return "", fmt.Errorf("signal-specific compression settings conflict (%q for traces vs %q for metrics); the relay carries both signals over one connection",
			traceComp, metricComp)
	}

	resolved := traceComp
	if resolved == "" {
		resolved = metricComp
	}
	switch resolved {
	case "", "none":
		return "none", nil
	case "gzip":
		return "gzip", nil
	default:
		return "", fmt.Errorf("unsupported OTLP compression %q, want gzip or none", resolved)
	}
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

	traceTarget := generic
	if traces != "" {
		traceTarget = traces
	}
	metricTarget := generic
	if metrics != "" {
		metricTarget = metrics
	}

	if traceTarget != "" && metricTarget != "" && traceTarget != metricTarget {
		return "", fmt.Errorf("signal-specific endpoints conflict (%q for traces vs %q for metrics); the relay carries both signals over one connection",
			traceTarget, metricTarget)
	}

	resolved := traceTarget
	if resolved == "" {
		resolved = metricTarget
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
