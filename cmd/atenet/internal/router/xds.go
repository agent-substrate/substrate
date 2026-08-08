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

package router

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/agent-substrate/substrate/internal/atunnel"

	accesslogv3 "github.com/envoyproxy/go-control-plane/envoy/config/accesslog/v3"
	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	mutationrulesv3 "github.com/envoyproxy/go-control-plane/envoy/config/common/mutation_rules/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	tracev3 "github.com/envoyproxy/go-control-plane/envoy/config/trace/v3"
	streamaccesslogv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/stream/v3"
	setfilterstatecommonv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/common/set_filter_state/v3"
	extprocv3filter "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	routerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	setfilterstatev3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/set_filter_state/v3"
	originaldstv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/listener/original_dst/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	internalupstreamv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/internal_upstream/v3"
	rawbufferv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/raw_buffer/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	httpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	clustergrpc "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointgrpc "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenergrpc "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	routegrpc "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"
	secretgrpc "github.com/envoyproxy/go-control-plane/envoy/service/secret/v3"
	matcherv3 "github.com/envoyproxy/go-control-plane/envoy/type/matcher/v3"
	metadatav3 "github.com/envoyproxy/go-control-plane/envoy/type/metadata/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/ingress"
)

const (
	NodeID               = "substrate-envoy-node"
	IngressHTTPListener  = "ingress_http_listener"
	IngressHTTPSListener = "ingress_https_listener"
	RouteName            = "substrate_routes"
	ClusterName          = "ate-cluster"
	OtlpClusterName      = "otel_collector_cluster"
	HTTPSCertSecretName  = "https_serving_cert"

	// httpProtocolOptionsName is the well-known extension key Envoy looks for in
	// a cluster's typed_extension_protocol_options. It must match the message's
	// full proto type name exactly; a typo is silently ignored rather than
	// rejected, so the options simply never take effect.
	httpProtocolOptionsName = "envoy.extensions.upstreams.http.v3.HttpProtocolOptions"

	// OriginalDstClusterName routes actor traffic to the worker's atunnel
	// ingress by the IP:port ext_proc reports in dynamic metadata (see
	// ingress.OriginalDstMetadataKey), while the request :authority stays the
	// actor DNS name so atunnel can identify the active actor.
	OriginalDstClusterName = "actor_original_dst"

	WildcardIP         = "0.0.0.0"
	ConnectUpgradeType = "CONNECT"
	MainInternalName   = "main_internal"

	// dynamicMetadataPortFormat is the %DYNAMIC_METADATA(...)% header-value
	// command operator (see buildRoutes) that derives atunnel.TargetPortHeader
	// from ingress.OriginalDstMetadataKey/ingress.OriginalDstPortKey.
	dynamicMetadataPortFormat = "%DYNAMIC_METADATA(" + ingress.OriginalDstMetadataKey + ":" + ingress.OriginalDstPortKey + ")%"

	// httpExtProcFilterName is envoy.filters.http.ext_proc's own well-known
	// name, used as the HttpFilter.Name in buildHcm.
	httpExtProcFilterName = "envoy.filters.http.ext_proc"
)

// defaultExtProcMessageTimeout is Envoy's per-message ext_proc response timeout
// when request parking is off. With parking on it must cover the park budget,
// otherwise Envoy abandons a parked request (500) long before the router does.
const defaultExtProcMessageTimeout = 5 * time.Second

// defaultExtProcMaxRequests is the circuit-breaker max_requests set on the
// ext_proc cluster: ingress.DefaultParkedRequestMax plus equal fast-path headroom, so a
// full parking lot cannot starve the millisecond-scale header exchanges of
// requests to already-running actors. See buildCluster.
const defaultExtProcMaxRequests = 2048

// defaultRouteTimeout is Envoy's end-to-end route timeout for workload traffic:
// the ceiling on a single request from the ingress listener to the actor's
// response. It bounds the actor's own handling time, not the resume that
// precedes it — parking and the ext_proc timeout cover that part.
//
// The drain sequence also sizes its Envoy-drain window and derived
// drain-timeout from this DEFAULT — deliberately not from the configured
// --route-timeout, so raising the route ceiling for long-running actor turns
// does not silently stretch every shutdown past terminationGracePeriodSeconds.
// Operators who raise --route-timeout and want such turns to survive a drain
// must raise --drain-timeout (and the grace period) explicitly.
const defaultRouteTimeout = 10 * time.Second

// envoyDefaultStreamIdleTimeout is the stream idle timeout Envoy applies when
// the HTTP connection manager does not set one. We never set it, so this is
// what governs today.
//
// It is a distinct limit from the route timeout: the route timeout bounds the
// upstream response time, while this bounds how long the stream may go with no
// encode/decode event at all. A turn that produces no bytes while the actor
// thinks — a non-streaming completion, or a request parked across a resume —
// is idle by this measure even though it is progressing, so without an
// override a route timeout above five minutes would never be reached. See
// routeIdleTimeout.
const envoyDefaultStreamIdleTimeout = 5 * time.Minute

// XdsServer implements an aggregated discovery service server for dynamic Envoy router nodes.
type XdsServer struct {
	xdsPort      int
	extprocPort  int
	extprocAddr  string
	ingressPort  int
	snapshot     cachev3.SnapshotCache
	srv          serverv3.Server
	versionCount int64

	mu sync.Mutex

	httpsPort            int
	connectPlainTextPort int
	connectTLSPort       int
	certPath             string

	// Upstream (actor-facing) mTLS. When upstreamCredentialBundlePath is set, the
	// ORIGINAL_DST actor cluster dials the actor's in-worker atunnel ingress
	// server over mTLS: it presents this podidentity credential bundle as the
	// client cert and validates the atunnel server against upstreamTrustBundlePath.
	upstreamCredentialBundlePath string
	upstreamTrustBundlePath      string
	// upstreamSpiffePrefix, when set, makes the upstream validator accept the
	// atunnel server cert by matching its SPIFFE URI SAN against this prefix
	// (trust-domain match) instead of the actor's ephemeral pod IP. The atunnel
	// cert carries only a spiffe:// URI SAN, so without this Envoy's default
	// SAN check against the dialed IP fails ("verify SAN list").
	upstreamSpiffePrefix string

	otlpHost string
	otlpPort uint32

	// traceRootSamplingPercent mirrors the router's resolved sampling policy
	// into Envoy's RandomSampling. Zero until Run sets it.
	traceRootSamplingPercent float64

	// extProcMessageTimeout bounds how long Envoy waits for the router's ext_proc
	// response. Must be >= the parking budget so parked requests aren't cut short.
	extProcMessageTimeout time.Duration

	// extProcMaxRequests is the circuit-breaker max_requests on the ext_proc
	// cluster — the hard ceiling on concurrent requests held open against the
	// router's processing server, parked requests included. Must be >= the
	// parking lot size (enforced at startup in Run).
	extProcMaxRequests uint32

	// routeTimeout is Envoy's end-to-end timeout on the workload route. Actors
	// that hold a request open for a long turn — an LLM streaming a response,
	// say — need this above the default or Envoy cuts the turn off with a 504.
	routeTimeout time.Duration
}

func NewXdsServer(xdsPort int) *XdsServer {
	cache := cachev3.NewSnapshotCache(true, cachev3.IDHash{}, nil)
	srv := serverv3.NewServer(context.Background(), cache, nil)

	return &XdsServer{
		xdsPort:               xdsPort,
		snapshot:              cache,
		srv:                   srv,
		extprocPort:           50051, // matches default extproc port
		extprocAddr:           "127.0.0.1",
		ingressPort:           8080,
		extProcMessageTimeout: defaultExtProcMessageTimeout,
		extProcMaxRequests:    defaultExtProcMaxRequests,
		routeTimeout:          defaultRouteTimeout,
	}
}

func (x *XdsServer) SetConfig(ingressPort int, extprocPort int, extprocAddr string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.ingressPort = ingressPort
	x.extprocPort = extprocPort
	x.extprocAddr = extprocAddr
}

// TODO: More extensible config setting that doesn't require another lock op
func (x *XdsServer) SetConnectPorts(connectPlainTextPort int, connectTLSPort int) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.connectPlainTextPort = connectPlainTextPort
	x.connectTLSPort = connectTLSPort
}

// SetExtProcMessageTimeout sets how long Envoy waits for the router's ext_proc
// response. Call with (parking budget + margin) when parking is enabled so
// Envoy keeps a parked request open until the router itself decides. A
// non-positive value leaves the default unchanged.
func (x *XdsServer) SetExtProcMessageTimeout(d time.Duration) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if d > 0 {
		x.extProcMessageTimeout = d
	}
}

// SetExtProcMaxRequests sets the circuit-breaker max_requests on the ext_proc
// cluster. Size it to the parking lot plus fast-path headroom (validated in
// Run()); a non-positive value leaves the default unchanged.
func (x *XdsServer) SetExtProcMaxRequests(n int) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if n > 0 {
		x.extProcMaxRequests = uint32(n)
	}
}

// SetRouteTimeout sets Envoy's end-to-end timeout on the workload route. Raise
// it for actors whose turns legitimately run long — a harness relaying an LLM
// completion holds the request open for the whole generation, and at the
// default the client sees a 504 mid-turn. A non-positive value leaves the
// default unchanged.
func (x *XdsServer) SetRouteTimeout(d time.Duration) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if d > 0 {
		x.routeTimeout = d
	}
}

// routeIdleTimeout resolves the route-level idle timeout that accompanies the
// route timeout. Caller must hold x.mu.
//
// Raising --route-timeout on its own would not work: the stream a long turn
// runs on is idle for the whole turn whenever the actor sends nothing until it
// is done, and Envoy would reset it at the five-minute stream idle default
// before the requested timeout was ever reached. The idle timer must therefore
// never be the limit that bites first.
//
// Taking the larger of the two keeps the operator's ceiling honest without
// making the idle timer stricter than it already is: below five minutes the
// route timeout fires first anyway, so this leaves today's behavior alone.
func (x *XdsServer) routeIdleTimeout() time.Duration {
	if x.routeTimeout > envoyDefaultStreamIdleTimeout {
		return x.routeTimeout
	}
	return envoyDefaultStreamIdleTimeout
}

func (x *XdsServer) SetTlsConfig(httpsPort int, certPath string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if httpsPort > 0 && certPath == "" {
		slog.Warn("HTTPS port configured without a certificate path; the HTTPS listener will not be served", slog.Int("port", httpsPort))
	}
	x.httpsPort = httpsPort
	x.certPath = certPath
}

// otlpDefaultPort is the OTLP/gRPC default port, used when the collector
// endpoint names no port.
const otlpDefaultPort = "4317"

// SetUpstreamTls configures actor-facing mTLS on the ORIGINAL_DST actor
// cluster. credentialBundlePath is the router's podidentity credential bundle
// (cert+key concatenated) presented to the actor's atunnel ingress server;
// trustBundlePath is the CA bundle used to validate that server. Empty
// credentialBundlePath leaves the upstream as plaintext.
func (x *XdsServer) SetUpstreamTls(credentialBundlePath, trustBundlePath, spiffePrefix string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.upstreamCredentialBundlePath = credentialBundlePath
	x.upstreamTrustBundlePath = trustBundlePath
	x.upstreamSpiffePrefix = spiffePrefix
}

// SetOtlpCollector enables Envoy-side tracing pointed at the OTLP gRPC
// collector. addr empty disables tracing. See normalizeOtlpCollector for the
// accepted forms.
func (x *XdsServer) SetOtlpCollector(addr string) error {
	if addr == "" {
		x.DisableOtlpCollector()
		return nil
	}
	// normalizeOtlpCollector reads nothing off x, so it runs unlocked.
	host, port, err := normalizeOtlpCollector(addr)
	if err != nil {
		return err
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	x.otlpHost = host
	x.otlpPort = port
	return nil
}

// DisableOtlpCollector turns Envoy-side tracing off. The router's own exporter
// is independent of this and keeps reporting spans.
func (x *XdsServer) DisableOtlpCollector() {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.otlpHost = ""
	x.otlpPort = 0
}

// SetTraceRootSamplingPercent sets the RandomSampling percent Envoy applies to
// requests arriving without a traceparent. Derived from the router's resolved
// OTel sampling policy so the two root decisions cannot drift.
func (x *XdsServer) SetTraceRootSamplingPercent(p float64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.traceRootSamplingPercent = p
}

// normalizeOtlpCollector resolves a collector endpoint to the bare host and
// numeric port an xDS SocketAddress requires (buildOtlpCollectorCluster).
//
// It accepts both a bare "host:port" and the URL form carried by
// OTEL_EXPORTER_OTLP_ENDPOINT, which is where --otlp-collector-address gets
// its default: Envoy's tracer reaches the collector through a named cluster,
// and a cluster endpoint has no room for a scheme or a path. Port defaults to
// otlpDefaultPort when omitted.
//
// https is rejected rather than downgraded — the tracer cluster carries no
// UpstreamTlsContext, so honoring it would mean shipping spans in plaintext to
// an endpoint that asked for TLS. Rejection here only means "Envoy cannot use
// this", not that the router should stop: the same endpoint is usable by the
// router's own exporter, so the caller warns and runs without Envoy-side
// tracing (see setOtlpCollector).
func normalizeOtlpCollector(addr string) (string, uint32, error) {
	hostport := addr
	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil {
			return "", 0, fmt.Errorf("parse OTLP collector endpoint %q: %w", addr, err)
		}
		switch u.Scheme {
		case "http":
		case "https":
			return "", 0, fmt.Errorf("OTLP collector endpoint %q uses https, which Envoy-side tracing does not support: the tracer cluster is plaintext h2c. Point --otlp-collector-address at an http:// endpoint, or pass it empty to disable Envoy-side tracing", addr)
		default:
			return "", 0, fmt.Errorf("OTLP collector endpoint %q has unsupported scheme %q, want http", addr, u.Scheme)
		}
		if p := strings.Trim(u.Path, "/"); p != "" {
			// Envoy's OpenTelemetry tracer derives the gRPC method itself, so a
			// path here cannot be honored. Warn instead of failing: the OTLP
			// spec lets the signal-agnostic env var carry one.
			slog.Warn("Ignoring path in OTLP collector endpoint; Envoy-side tracing addresses the collector by host and port only",
				slog.String("endpoint", addr), slog.String("path", u.Path))
		}
		hostport = u.Host
	}

	host, portStr, err := net.SplitHostPort(hostport)
	if err != nil {
		host = strings.Trim(hostport, "[]")
		portStr = otlpDefaultPort
	}
	if host == "" {
		return "", 0, fmt.Errorf("OTLP collector endpoint %q names no host", addr)
	}
	port, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("parse OTLP collector port from %q: %w", addr, err)
	}
	return host, uint32(port), nil
}

func (x *XdsServer) UpdateSnapshot() error {
	x.mu.Lock()
	defer x.mu.Unlock()

	x.versionCount++
	ver := strconv.FormatInt(x.versionCount, 10)

	// connectEnabled is true when either CONNECT listener (plaintext or TLS) is
	// configured; the main_internal cluster/listener only exist to serve them.
	connectEnabled := x.connectPlainTextPort > 0 || x.connectTLSPort > 0

	// Clusters
	clusters := []types.Resource{
		x.buildCluster(),
		x.buildOriginalDstCluster(),
	}
	if connectEnabled {
		clusters = append(clusters, x.buildMainInternalCluster())
	}
	if x.otlpHost != "" {
		clusters = append(clusters, x.buildOtlpCollectorCluster())
	}

	// Routes
	routes := []types.Resource{
		x.buildRoutes(),
	}

	// Listeners
	listeners := []types.Resource{
		x.buildListener(),
	}
	if connectEnabled {
		listeners = append(listeners, x.buildMainInternalListener())
	}
	if x.connectPlainTextPort > 0 {
		listeners = append(listeners, x.buildConnectTerminateListener())
	}
	var secrets []types.Resource
	needsCert := x.certPath != "" && (x.httpsPort > 0 || x.connectTLSPort > 0)
	if x.httpsPort > 0 && x.certPath != "" {
		listeners = append(listeners, x.buildHttpsListener())
	}
	if x.connectTLSPort > 0 && x.certPath != "" {
		listeners = append(listeners, x.buildConnectTerminateTLSListener())
	}
	if needsCert {
		secrets = append(secrets, x.buildTlsSecret())
	}

	// Snapshot
	snapshot, err := cachev3.NewSnapshot(ver, map[resourcev3.Type][]types.Resource{
		resourcev3.ClusterType:  clusters,
		resourcev3.RouteType:    routes,
		resourcev3.ListenerType: listeners,
		resourcev3.SecretType:   secrets,
	})

	if err != nil {
		return fmt.Errorf("failed to build xDS Snapshot: %w", err)
	}

	if err := snapshot.Consistent(); err != nil {
		return fmt.Errorf("snapshot evaluation failed integrity check: %w", err)
	}

	slog.Info("Deploying updated xDS configuration snapshot", slog.String("version", ver))
	return x.snapshot.SetSnapshot(context.Background(), NodeID, snapshot)
}

func (x *XdsServer) Serve(ctx context.Context, lis net.Listener) error {
	// Ensure a first snapshot is deployed
	if err := x.UpdateSnapshot(); err != nil {
		slog.ErrorContext(ctx, "Warning - initial xDS setup update failed", slog.String("err", err.Error()))
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(grpcServer, x.srv)
	clustergrpc.RegisterClusterDiscoveryServiceServer(grpcServer, x.srv)
	endpointgrpc.RegisterEndpointDiscoveryServiceServer(grpcServer, x.srv)
	listenergrpc.RegisterListenerDiscoveryServiceServer(grpcServer, x.srv)
	routegrpc.RegisterRouteDiscoveryServiceServer(grpcServer, x.srv)
	secretgrpc.RegisterSecretDiscoveryServiceServer(grpcServer, x.srv)

	errChan := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		// Hard stop, deliberately: ADS streams are open-ended, so GracefulStop
		// would block until Envoy disconnects — which during shutdown it only
		// does by dying. xDS clients treat a control-plane disconnect as benign
		// (reconnect with backoff, keep the last delivered config), and the
		// drain sequence only cancels this context after Envoy has drained.
		grpcServer.Stop()
		return nil
	case err := <-errChan:
		return err
	}
}

func (x *XdsServer) buildCluster() *clusterv3.Cluster {
	return &clusterv3.Cluster{
		Name:           ClusterName,
		ConnectTimeout: durationpb.New(250 * time.Millisecond),
		ClusterDiscoveryType: &clusterv3.Cluster_Type{
			Type: clusterv3.Cluster_STATIC,
		},
		LbPolicy: clusterv3.Cluster_ROUND_ROBIN,
		CircuitBreakers: &clusterv3.CircuitBreakers{
			Thresholds: []*clusterv3.CircuitBreakers_Thresholds{{
				Priority:    corev3.RoutingPriority_DEFAULT,
				MaxRequests: wrapperspb.UInt32(x.extProcMaxRequests),
			}},
		},
		LoadAssignment: &endpointv3.ClusterLoadAssignment{
			ClusterName: ClusterName,
			Endpoints: []*endpointv3.LocalityLbEndpoints{
				{
					LbEndpoints: []*endpointv3.LbEndpoint{
						{
							HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
								Endpoint: &endpointv3.Endpoint{
									Address: &corev3.Address{
										Address: &corev3.Address_SocketAddress{
											SocketAddress: &corev3.SocketAddress{
												Address: x.extprocAddr,
												PortSpecifier: &corev3.SocketAddress_PortValue{
													PortValue: uint32(x.extprocPort),
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		TypedExtensionProtocolOptions: map[string]*anypb.Any{
			httpProtocolOptionsName: newAny(&httpv3.HttpProtocolOptions{
				UpstreamProtocolOptions: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_{
					ExplicitHttpConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig{
						ProtocolConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_Http2ProtocolOptions{},
					},
				},
			}),
		},
	}
}

// buildOtlpCollectorCluster builds a STRICT_DNS HTTP/2 cluster that
// targets the OTLP gRPC collector. Required when HCM tracing is enabled
// so Envoy has somewhere to ship spans.
func (x *XdsServer) buildOtlpCollectorCluster() *clusterv3.Cluster {
	return &clusterv3.Cluster{
		Name:           OtlpClusterName,
		ConnectTimeout: durationpb.New(1 * time.Second),
		ClusterDiscoveryType: &clusterv3.Cluster_Type{
			Type: clusterv3.Cluster_STRICT_DNS,
		},
		LbPolicy: clusterv3.Cluster_ROUND_ROBIN,
		LoadAssignment: &endpointv3.ClusterLoadAssignment{
			ClusterName: OtlpClusterName,
			Endpoints: []*endpointv3.LocalityLbEndpoints{
				{
					LbEndpoints: []*endpointv3.LbEndpoint{
						{
							HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
								Endpoint: &endpointv3.Endpoint{
									Address: &corev3.Address{
										Address: &corev3.Address_SocketAddress{
											SocketAddress: &corev3.SocketAddress{
												Address: x.otlpHost,
												PortSpecifier: &corev3.SocketAddress_PortValue{
													PortValue: x.otlpPort,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		TypedExtensionProtocolOptions: map[string]*anypb.Any{
			httpProtocolOptionsName: newAny(&httpv3.HttpProtocolOptions{
				UpstreamProtocolOptions: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_{
					ExplicitHttpConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig{
						ProtocolConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_Http2ProtocolOptions{},
					},
				},
			}),
		},
	}
}

// buildUpstreamTransportSocket returns the actor-facing mTLS transport socket
// for the ORIGINAL_DST actor cluster, or nil when upstream mTLS is not
// configured. The router presents its podidentity credential bundle as the
// client cert and validates the atunnel ingress server against the trust
// bundle. Validation is by the SPIFFE URI SAN prefix (see upstreamSpiffePrefix)
// rather than the dialed pod IP.
func (x *XdsServer) buildUpstreamTransportSocket() *corev3.TransportSocket {
	if x.upstreamCredentialBundlePath == "" {
		return nil
	}

	commonTls := &tlsv3.CommonTlsContext{
		TlsCertificates: []*tlsv3.TlsCertificate{
			{
				CertificateChain: &corev3.DataSource{
					Specifier: &corev3.DataSource_Filename{Filename: x.upstreamCredentialBundlePath},
				},
				PrivateKey: &corev3.DataSource{
					Specifier: &corev3.DataSource_Filename{Filename: x.upstreamCredentialBundlePath},
				},
			},
		},
	}
	if x.upstreamTrustBundlePath != "" {
		validationCtx := &tlsv3.CertificateValidationContext{
			TrustedCa: &corev3.DataSource{
				Specifier: &corev3.DataSource_Filename{Filename: x.upstreamTrustBundlePath},
			},
		}
		// Validate the atunnel server by its SPIFFE URI SAN (trust-domain
		// prefix) rather than the dialed pod IP. Without this, Envoy checks the
		// cert SAN against the ephemeral pod IP, which the SPIFFE-only cert
		// never matches.
		if x.upstreamSpiffePrefix != "" {
			validationCtx.MatchTypedSubjectAltNames = []*tlsv3.SubjectAltNameMatcher{
				{
					SanType: tlsv3.SubjectAltNameMatcher_URI,
					Matcher: &matcherv3.StringMatcher{
						MatchPattern: &matcherv3.StringMatcher_Prefix{Prefix: x.upstreamSpiffePrefix},
					},
				},
			}
		}
		commonTls.ValidationContextType = &tlsv3.CommonTlsContext_ValidationContext{
			ValidationContext: validationCtx,
		}
	}

	upstreamTls := &tlsv3.UpstreamTlsContext{CommonTlsContext: commonTls}
	upstreamTlsAny := newAny(upstreamTls)
	return &corev3.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &corev3.TransportSocket_TypedConfig{
			TypedConfig: upstreamTlsAny,
		},
	}
}

func rawBuffer() *corev3.TransportSocket {
	return &corev3.TransportSocket{
		Name:       "raw_buffer",
		ConfigType: &corev3.TransportSocket_TypedConfig{TypedConfig: newAny(&rawbufferv3.RawBuffer{})},
	}
}

// MainInternalCluster is a simple cluster that sends traffic to the main_internal
// internal listener (used only for CONNECT requests).
func (x *XdsServer) buildMainInternalCluster() *clusterv3.Cluster {
	return &clusterv3.Cluster{
		Name: MainInternalName,
		ClusterDiscoveryType: &clusterv3.Cluster_Type{
			Type: clusterv3.Cluster_STATIC,
		},
		LoadAssignment: &endpointv3.ClusterLoadAssignment{
			ClusterName: MainInternalName,
			Endpoints: []*endpointv3.LocalityLbEndpoints{
				{
					LbEndpoints: []*endpointv3.LbEndpoint{
						{
							HostIdentifier: &endpointv3.LbEndpoint_Endpoint{
								Endpoint: &endpointv3.Endpoint{
									Address: &corev3.Address{
										Address: &corev3.Address_EnvoyInternalAddress{
											EnvoyInternalAddress: &corev3.EnvoyInternalAddress{
												AddressNameSpecifier: &corev3.EnvoyInternalAddress_ServerListenerName{
													ServerListenerName: MainInternalName,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		TransportSocket: &corev3.TransportSocket{
			Name: "internal_upstream",
			ConfigType: &corev3.TransportSocket_TypedConfig{
				TypedConfig: newAny(&internalupstreamv3.InternalUpstreamTransport{
					TransportSocket: rawBuffer(),
					PassthroughMetadata: []*internalupstreamv3.InternalUpstreamTransport_MetadataValueSource{
						{
							// The original_dst LISTENER FILTER's detected destination is
							// genuinely host-associated state (there is no real
							// SO_ORIGINAL_DST on this internal-listener hop), so Host kind
							// is correct here.
							Kind: &metadatav3.MetadataKind{
								Kind: &metadatav3.MetadataKind_Host_{},
							},
							Name: ingress.OriginalDstMetadataKey,
						},
					},
				}),
			},
		},
	}
}

// buildOriginalDstCluster dials the exact worker atunnel address supplied by
// ext_proc in dynamic metadata (see ingress.OriginalDstMetadataKey). It does
// not derive the destination from :authority, so the request keeps the actor
// DNS name as its Host for atunnel to authorize. mTLS to atunnel is applied
// via the shared upstream transport socket (SPIFFE URI validation).
func (x *XdsServer) buildOriginalDstCluster() *clusterv3.Cluster {
	cluster := &clusterv3.Cluster{
		Name:           OriginalDstClusterName,
		ConnectTimeout: durationpb.New(5 * time.Second),
		ClusterDiscoveryType: &clusterv3.Cluster_Type{
			Type: clusterv3.Cluster_ORIGINAL_DST,
		},
		LbPolicy: clusterv3.Cluster_CLUSTER_PROVIDED,
		LbConfig: &clusterv3.Cluster_OriginalDstLbConfig_{
			OriginalDstLbConfig: &clusterv3.Cluster_OriginalDstLbConfig{
				MetadataKey: &metadatav3.MetadataKey{
					Key: ingress.OriginalDstMetadataKey,
					Path: []*metadatav3.MetadataKey_PathSegment{
						{Segment: &metadatav3.MetadataKey_PathSegment_Key{Key: ingress.OriginalDstAddressKey}},
					},
				},
			},
		},
	}

	if ts := x.buildUpstreamTransportSocket(); ts != nil {
		cluster.TransportSocket = ts
		// The atunnel ingress server terminates TLS and reverse-proxies to the
		// actor over HTTP/1.1.
		httpOpts := newAny(&httpv3.HttpProtocolOptions{
			UpstreamProtocolOptions: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_{
				ExplicitHttpConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig{
					ProtocolConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_HttpProtocolOptions{
						HttpProtocolOptions: &corev3.Http1ProtocolOptions{},
					},
				},
			},
		})
		cluster.TypedExtensionProtocolOptions = map[string]*anypb.Any{
			httpProtocolOptionsName: httpOpts,
		}
	}

	return cluster
}

func (x *XdsServer) buildRoutes() *routev3.RouteConfiguration {
	return &routev3.RouteConfiguration{
		Name: RouteName,
		VirtualHosts: []*routev3.VirtualHost{
			{
				Name:    "local_service",
				Domains: []string{"*"},
				Routes: []*routev3.Route{
					{
						Match: &routev3.RouteMatch{
							PathSpecifier: &routev3.RouteMatch_Prefix{
								Prefix: "/",
							},
						},
						Action: &routev3.Route_Route{
							Route: &routev3.RouteAction{
								ClusterSpecifier: &routev3.RouteAction_Cluster{
									Cluster: OriginalDstClusterName,
								},
								// Also serves CONNECT-tunneled traffic re-injected via
								// main_internal: Envoy applies Timeout to the whole tunnel
								// lifetime, so a long-lived tunnel needs --route-timeout raised
								// like a long LLM turn does; routeIdleTimeout keeps the idle
								// timer from cutting it first.
								Timeout:     durationpb.New(x.routeTimeout),
								IdleTimeout: durationpb.New(x.routeIdleTimeout()),
							},
						},
						// atunnel reads the actor's target port from a header, since
						// it can't see Envoy's dynamic metadata; this derives it
						// declaratively from the same metadata ext_proc wrote (see
						// ingress.OriginalDstPortKey).
						RequestHeadersToAdd: []*corev3.HeaderValueOption{
							{
								Header: &corev3.HeaderValue{
									Key:   atunnel.TargetPortHeader,
									Value: dynamicMetadataPortFormat,
								},
								AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
							},
						},
					},
				},
			},
		},
	}
}

func (x *XdsServer) buildMainInternalListener() *listenerv3.Listener {
	return &listenerv3.Listener{
		Name: MainInternalName,
		ListenerSpecifier: &listenerv3.Listener_InternalListener{
			InternalListener: &listenerv3.Listener_InternalListenerConfig{},
		},
		// A single, unconditional HTTP chain: every CONNECT-tunneled protocol
		// this router understands is HTTP, so there is nothing left for a
		// FilterChainMatcher to key off of.
		FilterChains: []*listenerv3.FilterChain{
			{
				Filters: []*listenerv3.Filter{
					{
						Name: "envoy.filters.network.http_connection_manager",
						ConfigType: &listenerv3.Filter_TypedConfig{
							TypedConfig: x.buildHcm("main_internal", false),
						},
					},
				},
			},
		},
		// buildMainInternalCluster's PassthroughMetadata reads this filter's
		// Host-kind filter state and forwards it to the actor connection.
		ListenerFilters: []*listenerv3.ListenerFilter{
			{
				Name: "envoy.filters.listener.original_dst",
				ConfigType: &listenerv3.ListenerFilter_TypedConfig{
					TypedConfig: newAny(&originaldstv3.OriginalDst{}),
				},
			},
		},
	}
}

// authorityFilterStateFilter captures :authority into
// ingress.AuthorityFilterStateKey filter state, so main_internal's HTTP leg
// can read it back across the internal-listener hop (see buildHcm,
// ingress.HandleRequestHeaders). buildConnectTerminateHCM and buildHcm's
// ingress listeners use it; main_internal itself must not, since that would
// capture the tunneled protocol's own, unrelated :authority instead.
func authorityFilterStateFilter() *hcmv3.HttpFilter {
	return &hcmv3.HttpFilter{
		Name: "envoy.filters.http.set_filter_state",
		ConfigType: &hcmv3.HttpFilter_TypedConfig{
			TypedConfig: newAny(&setfilterstatev3.Config{
				OnRequestHeaders: []*setfilterstatecommonv3.FilterStateValue{
					{
						Key: &setfilterstatecommonv3.FilterStateValue_ObjectKey{
							ObjectKey: ingress.AuthorityFilterStateKey,
						},
						// ingress.AuthorityFilterStateKey is a custom (non-well-known)
						// key, so the generic string factory is required.
						FactoryKey: "envoy.string",
						Value: &setfilterstatecommonv3.FilterStateValue_FormatString{
							FormatString: &corev3.SubstitutionFormatString{
								Format: &corev3.SubstitutionFormatString_TextFormatSource{
									TextFormatSource: &corev3.DataSource{
										Specifier: &corev3.DataSource_InlineString{
											InlineString: "%REQ(:AUTHORITY)%",
										},
									},
								},
							},
						},
						SharedWithUpstream: setfilterstatecommonv3.FilterStateValue_ONCE,
					},
				},
			}),
		},
	}
}

func (x *XdsServer) buildConnectTerminateHCM(statPrefix string) *anypb.Any {
	routerAny := newAny(&routerv3.Router{})
	hcm := newAny(&hcmv3.HttpConnectionManager{
		StatPrefix:        statPrefix,
		GenerateRequestId: &wrapperspb.BoolValue{Value: true},
		Tracing:           x.buildTracing(),
		// TODO: Envoy's default access log format is not very useful for CONNECT requests.
		// Need to customize it to surface useful information
		AccessLog: []*accesslogv3.AccessLog{
			{
				Name: "envoy.access_loggers.stdout",
				ConfigType: &accesslogv3.AccessLog_TypedConfig{
					TypedConfig: newAny(&streamaccesslogv3.StdoutAccessLog{}),
				},
			},
		},
		RouteSpecifier: &hcmv3.HttpConnectionManager_RouteConfig{
			RouteConfig: buildConnectRoutes(),
		},
		UpgradeConfigs: []*hcmv3.HttpConnectionManager_UpgradeConfig{
			{
				UpgradeType: ConnectUpgradeType,
			},
		},
		CodecType: hcmv3.HttpConnectionManager_AUTO,
		HttpFilters: []*hcmv3.HttpFilter{
			authorityFilterStateFilter(),
			{
				Name: "envoy.filters.http.router",
				ConfigType: &hcmv3.HttpFilter_TypedConfig{
					TypedConfig: routerAny,
				},
			},
		},
		Http2ProtocolOptions: &corev3.Http2ProtocolOptions{
			AllowConnect: true,
		},
	})

	return hcm
}

func buildConnectRoutes() *routev3.RouteConfiguration {
	return &routev3.RouteConfiguration{
		Name: "default",
		VirtualHosts: []*routev3.VirtualHost{
			{
				Name:    "default",
				Domains: []string{"*"},
				Routes: []*routev3.Route{
					{
						Match: &routev3.RouteMatch{
							PathSpecifier: &routev3.RouteMatch_ConnectMatcher_{},
						},
						Action: &routev3.Route_Route{
							Route: &routev3.RouteAction{
								UpgradeConfigs: []*routev3.RouteAction_UpgradeConfig{
									{
										UpgradeType:   ConnectUpgradeType,
										ConnectConfig: &routev3.RouteAction_UpgradeConfig_ConnectConfig{},
									},
								},
								ClusterSpecifier: &routev3.RouteAction_Cluster{
									Cluster: MainInternalName,
								},
								// Envoy applies the route timeout to a CONNECT tunnel's whole
								// lifetime, not just its headers; left at Envoy's 15s default
								// this would silently kill every tunnel after 15s. 0 disables
								// it -- the tunnel's lifetime is bounded by idle timeout,
								// drain, and the peers themselves.
								Timeout: durationpb.New(0),
							},
						},
					},
				},
			},
		},
	}
}

// buildHcm builds the HTTP ext_proc-fronted HCM shared by the ingress_http,
// ingress_https, and main_internal listeners. captureAuthority runs
// authorityFilterStateFilter to populate the actor's :authority for ingress
// listeners; main_internal passes false, since connect_terminate already
// shared the correct value and re-deriving it here would clobber it with
// the tunneled protocol's own, unrelated :authority.
func (x *XdsServer) buildHcm(statPrefix string, captureAuthority bool) *anypb.Any {
	extProcConfig := newAny(&extprocv3filter.ExternalProcessor{
		GrpcService: &corev3.GrpcService{
			TargetSpecifier: &corev3.GrpcService_EnvoyGrpc_{
				EnvoyGrpc: &corev3.GrpcService_EnvoyGrpc{
					ClusterName: ClusterName,
				},
			},
			Timeout: durationpb.New(x.extProcMessageTimeout),
		},
		MutationRules: &mutationrulesv3.HeaderMutationRules{
			AllowAllRouting: &wrapperspb.BoolValue{Value: true},
		},
		// Bound how long Envoy waits for the router's ext_proc response. Must
		// cover the parking budget (see SetExtProcMessageTimeout): a parked
		// request is held open here until the router itself resolves or sheds it.
		MessageTimeout: durationpb.New(x.extProcMessageTimeout),
		ProcessingMode: &extprocv3filter.ProcessingMode{
			RequestHeaderMode:   extprocv3filter.ProcessingMode_SEND,
			ResponseHeaderMode:  extprocv3filter.ProcessingMode_SKIP,
			RequestBodyMode:     extprocv3filter.ProcessingMode_NONE,
			ResponseBodyMode:    extprocv3filter.ProcessingMode_NONE,
			RequestTrailerMode:  extprocv3filter.ProcessingMode_SKIP,
			ResponseTrailerMode: extprocv3filter.ProcessingMode_SKIP,
		},
		// Passes the resolved actor's authority as a request attribute (see
		// ingress.AuthorityFilterStateAttribute, ingress.HandleRequestHeaders)
		// and lets the response write the resolved worker address into
		// ingress.OriginalDstMetadataKey.
		RequestAttributes: []string{ingress.AuthorityFilterStateAttribute},
		MetadataOptions: &extprocv3filter.MetadataOptions{
			ForwardingNamespaces: &extprocv3filter.MetadataOptions_MetadataNamespaces{
				Untyped: []string{ingress.OriginalDstMetadataKey},
			},
			ReceivingNamespaces: &extprocv3filter.MetadataOptions_MetadataNamespaces{
				Untyped: []string{ingress.OriginalDstMetadataKey},
			},
		},
	})

	routerAny := newAny(&routerv3.Router{})

	accessLogConfig := newAny(&streamaccesslogv3.StdoutAccessLog{})

	httpFilters := []*hcmv3.HttpFilter{}
	if captureAuthority {
		httpFilters = append(httpFilters, authorityFilterStateFilter())
	}
	httpFilters = append(httpFilters,
		&hcmv3.HttpFilter{
			Name: httpExtProcFilterName,
			ConfigType: &hcmv3.HttpFilter_TypedConfig{
				TypedConfig: extProcConfig,
			},
		},
		&hcmv3.HttpFilter{
			Name: "envoy.filters.http.router",
			ConfigType: &hcmv3.HttpFilter_TypedConfig{
				TypedConfig: routerAny,
			},
		},
	)

	hcm := newAny(&hcmv3.HttpConnectionManager{
		StatPrefix:        statPrefix,
		GenerateRequestId: &wrapperspb.BoolValue{Value: true},
		Tracing:           x.buildTracing(),
		AccessLog: []*accesslogv3.AccessLog{
			{
				Name: "envoy.access_loggers.stdout",
				ConfigType: &accesslogv3.AccessLog_TypedConfig{
					TypedConfig: accessLogConfig,
				},
			},
		},
		HttpFilters: httpFilters,
		RouteSpecifier: &hcmv3.HttpConnectionManager_Rds{
			Rds: &hcmv3.Rds{
				RouteConfigName: RouteName,
				ConfigSource: &corev3.ConfigSource{
					ResourceApiVersion: corev3.ApiVersion_V3,
					ConfigSourceSpecifier: &corev3.ConfigSource_Ads{
						Ads: &corev3.AggregatedConfigSource{},
					},
				},
			},
		},
	})
	return hcm
}

// buildTracing returns the HCM Tracing block that points Envoy at the
// configured OTLP gRPC collector. Returns nil when no collector is set,
// in which case Envoy emits no spans on its own.
//
// RandomSampling is the root decision for requests arriving without a
// traceparent. Requests already sampled by the caller (kubectl-ate --trace,
// load generators) are continued regardless of the percent, and downstream
// ParentBased samplers keep the decision end to end.
func (x *XdsServer) buildTracing() *hcmv3.HttpConnectionManager_Tracing {
	if x.otlpHost == "" {
		return nil
	}
	otelConfig := newAny(&tracev3.OpenTelemetryConfig{
		GrpcService: &corev3.GrpcService{
			TargetSpecifier: &corev3.GrpcService_EnvoyGrpc_{
				EnvoyGrpc: &corev3.GrpcService_EnvoyGrpc{
					ClusterName: OtlpClusterName,
				},
			},
		},
		ServiceName: "atenet-router-envoy",
	})
	return &hcmv3.HttpConnectionManager_Tracing{
		RandomSampling: &typev3.Percent{Value: x.traceRootSamplingPercent},
		Provider: &tracev3.Tracing_Http{
			Name: "envoy.tracers.opentelemetry",
			ConfigType: &tracev3.Tracing_Http_TypedConfig{
				TypedConfig: otelConfig,
			},
		},
	}
}

// dualStackAdditionalAddresses returns the IPv6 half of a dual-stack ingress
// listener, to pair with a primary 0.0.0.0 socket on the same port. Ipv4Compat
// stays false: clearing IPV6_V6ONLY would collide with that primary.
func dualStackAdditionalAddresses(port uint32) []*listenerv3.AdditionalAddress {
	return []*listenerv3.AdditionalAddress{
		{
			Address: &corev3.Address{
				Address: &corev3.Address_SocketAddress{
					SocketAddress: &corev3.SocketAddress{
						Address:    "::",
						Ipv4Compat: false,
						PortSpecifier: &corev3.SocketAddress_PortValue{
							PortValue: port,
						},
					},
				},
			},
		},
	}
}

func (x *XdsServer) buildListener() *listenerv3.Listener {
	hcm := x.buildHcm("ingress_http", true)

	return &listenerv3.Listener{
		Name: IngressHTTPListener,
		Address: &corev3.Address{
			Address: &corev3.Address_SocketAddress{
				SocketAddress: &corev3.SocketAddress{
					Address: WildcardIP,
					PortSpecifier: &corev3.SocketAddress_PortValue{
						PortValue: uint32(x.ingressPort),
					},
				},
			},
		},
		AdditionalAddresses: dualStackAdditionalAddresses(uint32(x.ingressPort)),
		FilterChains: []*listenerv3.FilterChain{
			{
				Filters: []*listenerv3.Filter{
					{
						Name: "envoy.filters.network.http_connection_manager",
						ConfigType: &listenerv3.Filter_TypedConfig{
							TypedConfig: hcm,
						},
					},
				},
			},
		},
	}
}

// buildDownstreamTlsTransportSocket returns the downstream TLS transport
// socket shared by every TLS-terminating listener: it serves the SDS-fetched
// certificate at HTTPSCertSecretName (see buildTlsSecret), which UpdateSnapshot
// includes whenever any TLS listener (HTTPS or CONNECT-TLS) is configured.
func buildDownstreamTlsTransportSocket() *corev3.TransportSocket {
	tlsConfig := &tlsv3.DownstreamTlsContext{
		CommonTlsContext: &tlsv3.CommonTlsContext{
			TlsCertificateSdsSecretConfigs: []*tlsv3.SdsSecretConfig{
				{
					Name: HTTPSCertSecretName,
					SdsConfig: &corev3.ConfigSource{
						ConfigSourceSpecifier: &corev3.ConfigSource_Ads{
							Ads: &corev3.AggregatedConfigSource{},
						},
						ResourceApiVersion: corev3.ApiVersion_V3,
					},
				},
			},
		},
	}
	tlsConfigAny := newAny(tlsConfig)
	return &corev3.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &corev3.TransportSocket_TypedConfig{
			TypedConfig: tlsConfigAny,
		},
	}
}

func (x *XdsServer) buildHttpsListener() *listenerv3.Listener {
	hcm := x.buildHcm("ingress_https", true)

	return &listenerv3.Listener{
		Name: IngressHTTPSListener,
		Address: &corev3.Address{
			Address: &corev3.Address_SocketAddress{
				SocketAddress: &corev3.SocketAddress{
					Address: WildcardIP,
					PortSpecifier: &corev3.SocketAddress_PortValue{
						PortValue: uint32(x.httpsPort),
					},
				},
			},
		},
		AdditionalAddresses: dualStackAdditionalAddresses(uint32(x.httpsPort)),
		FilterChains: []*listenerv3.FilterChain{
			{
				Filters: []*listenerv3.Filter{
					{
						Name: "envoy.filters.network.http_connection_manager",
						ConfigType: &listenerv3.Filter_TypedConfig{
							TypedConfig: hcm,
						},
					},
				},
				TransportSocket: buildDownstreamTlsTransportSocket(),
			},
		},
	}
}

func (x *XdsServer) buildConnectTerminateListener() *listenerv3.Listener {
	hcm := x.buildConnectTerminateHCM("connect_terminate")

	return &listenerv3.Listener{
		Name: "connect_terminate",
		Address: &corev3.Address{
			Address: &corev3.Address_SocketAddress{
				SocketAddress: &corev3.SocketAddress{
					Address: WildcardIP,
					PortSpecifier: &corev3.SocketAddress_PortValue{
						PortValue: uint32(x.connectPlainTextPort),
					},
				},
			},
		},
		FilterChains: []*listenerv3.FilterChain{
			{
				Filters: []*listenerv3.Filter{
					{
						Name: "envoy.filters.network.http_connection_manager",
						ConfigType: &listenerv3.Filter_TypedConfig{
							TypedConfig: hcm,
						},
					},
				},
			},
		},
	}
}

// buildConnectTerminateTLSListener is buildConnectTerminateListener's TLS
// twin: same CONNECT-terminating HCM, but downstream TLS-wrapped like
// buildHttpsListener, sharing the same SDS-fetched certificate.
func (x *XdsServer) buildConnectTerminateTLSListener() *listenerv3.Listener {
	hcm := x.buildConnectTerminateHCM("connect_terminate_tls")

	return &listenerv3.Listener{
		Name: "connect_terminate_tls",
		Address: &corev3.Address{
			Address: &corev3.Address_SocketAddress{
				SocketAddress: &corev3.SocketAddress{
					Address: WildcardIP,
					PortSpecifier: &corev3.SocketAddress_PortValue{
						PortValue: uint32(x.connectTLSPort),
					},
				},
			},
		},
		FilterChains: []*listenerv3.FilterChain{
			{
				Filters: []*listenerv3.Filter{
					{
						Name: "envoy.filters.network.http_connection_manager",
						ConfigType: &listenerv3.Filter_TypedConfig{
							TypedConfig: hcm,
						},
					},
				},
				TransportSocket: buildDownstreamTlsTransportSocket(),
			},
		},
	}
}

func (x *XdsServer) buildTlsSecret() *tlsv3.Secret {
	return &tlsv3.Secret{
		Name: HTTPSCertSecretName,
		Type: &tlsv3.Secret_TlsCertificate{
			TlsCertificate: &tlsv3.TlsCertificate{
				// The pod certificate is projected as a single PEM bundle
				// holding both the cert chain and the private key, so both
				// DataSources point at the same file.
				CertificateChain: &corev3.DataSource{
					Specifier: &corev3.DataSource_Filename{
						Filename: x.certPath,
					},
				},
				PrivateKey: &corev3.DataSource{
					Specifier: &corev3.DataSource_Filename{
						Filename: x.certPath,
					},
				},
				// By specifying WatchedDirectory, we tell envoy to watch changes to the mounted pod certificate file.
				// See documentation in https://pkg.go.dev/github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3#:~:text=This%20only%20applies%20when%20a%20%E2%80%9CTlsCertificate%E2%80%9C%20is%20delivered%20by%20SDS
				WatchedDirectory: &corev3.WatchedDirectory{
					Path: filepath.Dir(x.certPath),
				},
			},
		},
	}
}

func newAny(msg proto.Message) *anypb.Any {
	aMsg, _ := anypb.New(msg)
	return aMsg
}
