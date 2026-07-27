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
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	accesslogv3 "github.com/envoyproxy/go-control-plane/envoy/config/accesslog/v3"
	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	mutationrulesv3 "github.com/envoyproxy/go-control-plane/envoy/config/common/mutation_rules/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	tracev3 "github.com/envoyproxy/go-control-plane/envoy/config/trace/v3"
	streamaccesslogv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/stream/v3"
	extprocv3filter "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	routerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	httpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	clustergrpc "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointgrpc "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenergrpc "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	routegrpc "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"
	secretgrpc "github.com/envoyproxy/go-control-plane/envoy/service/secret/v3"
	matcherv3 "github.com/envoyproxy/go-control-plane/envoy/type/matcher/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
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
	// ingress by the IP:port the ext_proc puts in OriginalDstHeader, while the
	// request :authority stays the actor DNS name so atunnel can identify the
	// active actor.
	OriginalDstClusterName = "actor_original_dst"
	// OriginalDstHeader carries the resolved worker atunnel address (IP:443).
	OriginalDstHeader = "x-ate-original-dst"
)

// defaultExtProcMessageTimeout is Envoy's per-message ext_proc response timeout
// when request parking is off. With parking on it must cover the park budget,
// otherwise Envoy abandons a parked request (500) long before the router does.
const defaultExtProcMessageTimeout = 5 * time.Second

// defaultExtProcMaxRequests is the circuit-breaker max_requests set on the
// ext_proc cluster: defaultParkedRequestMax plus equal fast-path headroom, so a
// full parking lot cannot starve the millisecond-scale header exchanges of
// requests to already-running actors. See buildCluster.
const defaultExtProcMaxRequests = 2048

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

	httpsPort int
	certPath  string

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

	// extProcMessageTimeout bounds how long Envoy waits for the router's ext_proc
	// response. Must be >= the parking budget so parked requests aren't cut short.
	extProcMessageTimeout time.Duration

	// extProcMaxRequests is the circuit-breaker max_requests on the ext_proc
	// cluster — the hard ceiling on concurrent requests held open against the
	// router's processing server, parked requests included. Must be >= the
	// parking lot size (enforced at startup in Run).
	extProcMaxRequests uint32
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
	}
}

func (x *XdsServer) SetConfig(ingressPort int, extprocPort int, extprocAddr string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.ingressPort = ingressPort
	x.extprocPort = extprocPort
	x.extprocAddr = extprocAddr
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

	// Clusters
	clusters := []types.Resource{
		x.buildCluster(),
		x.buildOriginalDstCluster(),
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
	var secrets []types.Resource
	if x.httpsPort > 0 && x.certPath != "" {
		listeners = append(listeners, x.buildHttpsListener())
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
		grpcServer.GracefulStop()
		return nil
	case err := <-errChan:
		return err
	}
}

func (x *XdsServer) buildCluster() *clusterv3.Cluster {
	h2Opts, _ := anypb.New(&httpv3.HttpProtocolOptions{
		UpstreamProtocolOptions: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_{
			ExplicitHttpConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig{
				ProtocolConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_Http2ProtocolOptions{},
			},
		},
	})

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
			httpProtocolOptionsName: h2Opts,
		},
	}
}

// buildOtlpCollectorCluster builds a STRICT_DNS HTTP/2 cluster that
// targets the OTLP gRPC collector. Required when HCM tracing is enabled
// so Envoy has somewhere to ship spans.
func (x *XdsServer) buildOtlpCollectorCluster() *clusterv3.Cluster {
	h2Opts, _ := anypb.New(&httpv3.HttpProtocolOptions{
		UpstreamProtocolOptions: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_{
			ExplicitHttpConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig{
				ProtocolConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_Http2ProtocolOptions{},
			},
		},
	})

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
			httpProtocolOptionsName: h2Opts,
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
	upstreamTlsAny, _ := anypb.New(upstreamTls)
	return &corev3.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &corev3.TransportSocket_TypedConfig{
			TypedConfig: upstreamTlsAny,
		},
	}
}

// buildOriginalDstCluster dials the exact worker atunnel address supplied by
// the ext_proc in OriginalDstHeader. Unlike the dynamic_forward_proxy cluster,
// it does not derive the destination from :authority, so the request keeps the
// actor DNS name as its Host for atunnel to authorize. mTLS to atunnel is
// applied via the shared upstream transport socket (SPIFFE URI validation).
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
				UseHttpHeader:  true,
				HttpHeaderName: OriginalDstHeader,
			},
		},
	}

	if ts := x.buildUpstreamTransportSocket(); ts != nil {
		cluster.TransportSocket = ts
		// The atunnel ingress server terminates TLS and reverse-proxies to the
		// actor over HTTP/1.1.
		httpOpts, _ := anypb.New(&httpv3.HttpProtocolOptions{
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
								Timeout: durationpb.New(10 * time.Second),
							},
						},
					},
				},
			},
		},
	}
}

func (x *XdsServer) buildHcm(statPrefix string) *anypb.Any {
	extProcConfig, _ := anypb.New(&extprocv3filter.ExternalProcessor{
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
	})

	routerAny, _ := anypb.New(&routerv3.Router{})

	accessLogConfig, _ := anypb.New(&streamaccesslogv3.StdoutAccessLog{})

	hcm, _ := anypb.New(&hcmv3.HttpConnectionManager{
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
		HttpFilters: []*hcmv3.HttpFilter{
			{
				Name: "envoy.filters.http.ext_proc",
				ConfigType: &hcmv3.HttpFilter_TypedConfig{
					TypedConfig: extProcConfig,
				},
			},
			{
				Name: "envoy.filters.http.router",
				ConfigType: &hcmv3.HttpFilter_TypedConfig{
					TypedConfig: routerAny,
				},
			},
		},
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
// `RandomSampling: 100%` makes Envoy ALWAYS sample requests that arrive
// with no parent traceparent. We rely on upstream clients (locust, etc.)
// to gate sampling: requests without a sampled parent are still tagged
// `sampled` here but downstream services in this repo use
// `ParentBased(NeverSample)` so unsampled-by-client requests stay
// unsampled overall.
func (x *XdsServer) buildTracing() *hcmv3.HttpConnectionManager_Tracing {
	if x.otlpHost == "" {
		return nil
	}
	otelConfig, _ := anypb.New(&tracev3.OpenTelemetryConfig{
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
		RandomSampling: &typev3.Percent{Value: 100},
		Provider: &tracev3.Tracing_Http{
			Name: "envoy.tracers.opentelemetry",
			ConfigType: &tracev3.Tracing_Http_TypedConfig{
				TypedConfig: otelConfig,
			},
		},
	}
}

func (x *XdsServer) buildListener() *listenerv3.Listener {
	hcm := x.buildHcm("ingress_http")

	return &listenerv3.Listener{
		Name: IngressHTTPListener,
		Address: &corev3.Address{
			Address: &corev3.Address_SocketAddress{
				SocketAddress: &corev3.SocketAddress{
					Address: "0.0.0.0",
					PortSpecifier: &corev3.SocketAddress_PortValue{
						PortValue: uint32(x.ingressPort),
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

func (x *XdsServer) buildHttpsListener() *listenerv3.Listener {
	hcm := x.buildHcm("ingress_https")

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
	tlsConfigAny, _ := anypb.New(tlsConfig)

	return &listenerv3.Listener{
		Name: IngressHTTPSListener,
		Address: &corev3.Address{
			Address: &corev3.Address_SocketAddress{
				SocketAddress: &corev3.SocketAddress{
					Address: "0.0.0.0",
					PortSpecifier: &corev3.SocketAddress_PortValue{
						PortValue: uint32(x.httpsPort),
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
				TransportSocket: &corev3.TransportSocket{
					Name: "envoy.transport_sockets.tls",
					ConfigType: &corev3.TransportSocket_TypedConfig{
						TypedConfig: tlsConfigAny,
					},
				},
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
