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
	"strconv"
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
	dfpclusterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/clusters/dynamic_forward_proxy/v3"
	dfpcommonv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/common/dynamic_forward_proxy/v3"
	dfpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/dynamic_forward_proxy/v3"
	extprocv3filter "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	routerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	getaddrinfov3 "github.com/envoyproxy/go-control-plane/envoy/extensions/network/dns_resolver/getaddrinfo/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	httpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	clustergrpc "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointgrpc "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenergrpc "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	routegrpc "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"
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
	RouteNameHTTPS       = "substrate_routes_https"
	ClusterName          = "ate-cluster"
	OtlpClusterName      = "otel_collector_cluster"
	DFPClusterName       = "dynamic_forward_proxy_cluster"
	DFPTLSClusterName    = "dynamic_forward_proxy_tls_cluster"

	// SNIHeader carries the actor's mesh DNS name for the upstream TLS
	// handshake. ext_proc rewrites :authority to a bare pod IP, which would
	// make Envoy's auto-SNI derive the SNI from an IP literal, so the TLS
	// cluster reads the SNI from this header instead (see
	// UpstreamHttpProtocolOptions.override_auto_sni_header).
	SNIHeader = "x-substrate-sni"
)

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

	otlpHost string
	otlpPort uint32
}

func NewXdsServer(xdsPort int) *XdsServer {
	cache := cachev3.NewSnapshotCache(true, cachev3.IDHash{}, nil)
	srv := serverv3.NewServer(context.Background(), cache, nil)

	return &XdsServer{
		xdsPort:     xdsPort,
		snapshot:    cache,
		srv:         srv,
		extprocPort: 50051, // matches default extproc port
		extprocAddr: "127.0.0.1",
		ingressPort: 8080,
	}
}

func (x *XdsServer) SetConfig(ingressPort int, extprocPort int, extprocAddr string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.ingressPort = ingressPort
	x.extprocPort = extprocPort
	x.extprocAddr = extprocAddr
}

func (x *XdsServer) SetTlsConfig(httpsPort int, certPath string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.httpsPort = httpsPort
	x.certPath = certPath
}

// SetOtlpCollector enables Envoy-side tracing pointed at the OTLP gRPC
// collector at host:port. addr empty disables tracing. port defaults to
// 4317 if omitted.
func (x *XdsServer) SetOtlpCollector(addr string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if addr == "" {
		x.otlpHost = ""
		x.otlpPort = 0
		return nil
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		portStr = "4317"
	}
	port, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		return fmt.Errorf("parse OTLP collector port from %q: %w", addr, err)
	}
	x.otlpHost = host
	x.otlpPort = uint32(port)
	return nil
}

func (x *XdsServer) UpdateSnapshot() error {
	x.mu.Lock()
	defer x.mu.Unlock()

	x.versionCount++
	ver := strconv.FormatInt(x.versionCount, 10)

	// Clusters
	clusters := []types.Resource{
		x.buildCluster(),
		x.buildDynamicForwardProxyCluster(DFPClusterName, false),
	}
	if x.otlpHost != "" {
		clusters = append(clusters, x.buildOtlpCollectorCluster())
	}

	// Routes
	routes := []types.Resource{
		x.buildRoutes(RouteName, DFPClusterName),
	}

	// Listeners
	listeners := []types.Resource{
		x.buildListener(),
	}

	// The HTTPS ingress gets its own cluster and route configuration so that
	// TLS is re-originated upstream only for traffic that arrived over TLS.
	// All three resources are gated together to keep the snapshot internally
	// consistent (no dangling RDS or CDS reference).
	if x.httpsPort > 0 {
		clusters = append(clusters, x.buildDynamicForwardProxyCluster(DFPTLSClusterName, true))
		routes = append(routes, x.buildRoutes(RouteNameHTTPS, DFPTLSClusterName))
		listeners = append(listeners, x.buildHttpsListener())
	}

	// Snapshot
	snapshot, err := cachev3.NewSnapshot(ver, map[resourcev3.Type][]types.Resource{
		resourcev3.ClusterType:  clusters,
		resourcev3.RouteType:    routes,
		resourcev3.ListenerType: listeners,
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
			"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": h2Opts,
		},
	}
}

func buildDnsCacheConfig() *dfpcommonv3.DnsCacheConfig {
	resolverConfigAny, _ := anypb.New(&getaddrinfov3.GetAddrInfoDnsResolverConfig{})
	return &dfpcommonv3.DnsCacheConfig{
		Name:            "dynamic_forward_proxy_cache_config",
		DnsLookupFamily: clusterv3.Cluster_V4_ONLY,
		TypedDnsResolverConfig: &corev3.TypedExtensionConfig{
			Name:        "envoy.network.dns_resolver.getaddrinfo",
			TypedConfig: resolverConfigAny,
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
			"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": h2Opts,
		},
	}
}

// buildDynamicForwardProxyCluster builds a dynamic forward proxy cluster that
// takes its upstream address from the (ext_proc-rewritten) :authority. With
// useTLS the connection to the actor is re-originated as TLS, for traffic that
// arrived on the router's HTTPS listener.
//
// All dynamic forward proxy resources share one DNS cache, which Envoy keys by
// the cache config's name — the configs must stay byte-identical, so both
// clusters and the DFP filter use buildDnsCacheConfig unmodified.
func (x *XdsServer) buildDynamicForwardProxyCluster(name string, useTLS bool) *clusterv3.Cluster {
	dfpClusterConfig := &dfpclusterv3.ClusterConfig{
		ClusterImplementationSpecifier: &dfpclusterv3.ClusterConfig_DnsCacheConfig{
			DnsCacheConfig: buildDnsCacheConfig(),
		},
		// A dynamic forward proxy cluster with a TLS transport socket demands
		// both auto_sni and auto_san_validation, and rejects the whole CDS
		// update otherwise. SAN validation is meaningless here because the
		// upstream context below carries no validation context at all, so the
		// requirement has to be waived explicitly. Removing this waiver is part
		// of the same change that adds a trusted_ca.
		AllowInsecureClusterOptions: useTLS,
	}

	clusterConfigAny, _ := anypb.New(dfpClusterConfig)

	cluster := &clusterv3.Cluster{
		Name:     name,
		LbPolicy: clusterv3.Cluster_CLUSTER_PROVIDED,
		ClusterDiscoveryType: &clusterv3.Cluster_ClusterType{
			ClusterType: &clusterv3.Cluster_CustomClusterType{
				Name:        "envoy.clusters.dynamic_forward_proxy",
				TypedConfig: clusterConfigAny,
			},
		},
	}

	if !useTLS {
		return cluster
	}

	// The actor's certificate is deliberately not verified: actors have no
	// platform-issued identity yet, so they serve certs they mint themselves.
	// This buys encryption in transit, not upstream authentication. The SNI
	// below is still set correctly, so enabling verification later is a matter
	// of adding a validation context here.
	tlsAny, _ := anypb.New(&tlsv3.UpstreamTlsContext{
		CommonTlsContext: &tlsv3.CommonTlsContext{
			AlpnProtocols: []string{"http/1.1"},
		},
	})
	cluster.TransportSocket = &corev3.TransportSocket{
		Name: "envoy.transport_sockets.tls",
		ConfigType: &corev3.TransportSocket_TypedConfig{
			TypedConfig: tlsAny,
		},
	}

	// ExplicitHttpConfig pins HTTP/1.1, matching the protocol Envoy would pick
	// implicitly for the plaintext cluster; it is required because
	// UpstreamHttpProtocolOptions can only be carried inside HttpProtocolOptions,
	// whose upstream_protocol_options oneof must be set.
	protoOpts, _ := anypb.New(&httpv3.HttpProtocolOptions{
		UpstreamProtocolOptions: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_{
			ExplicitHttpConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig{
				ProtocolConfig: &httpv3.HttpProtocolOptions_ExplicitHttpConfig_HttpProtocolOptions{},
			},
		},
		UpstreamHttpProtocolOptions: &corev3.UpstreamHttpProtocolOptions{
			AutoSni:               true,
			OverrideAutoSniHeader: SNIHeader,
		},
	})
	cluster.TypedExtensionProtocolOptions = map[string]*anypb.Any{
		"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": protoOpts,
	}

	return cluster
}

func (x *XdsServer) buildRoutes(routeName, clusterName string) *routev3.RouteConfiguration {
	return &routev3.RouteConfiguration{
		Name: routeName,
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
									Cluster: clusterName,
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

func (x *XdsServer) buildHcm(statPrefix, routeName string) *anypb.Any {
	extProcConfig, _ := anypb.New(&extprocv3filter.ExternalProcessor{
		GrpcService: &corev3.GrpcService{
			TargetSpecifier: &corev3.GrpcService_EnvoyGrpc_{
				EnvoyGrpc: &corev3.GrpcService_EnvoyGrpc{
					ClusterName: ClusterName,
				},
			},
			Timeout: durationpb.New(5 * time.Second),
		},
		MutationRules: &mutationrulesv3.HeaderMutationRules{
			AllowAllRouting: &wrapperspb.BoolValue{Value: true},
		},
		// The downstream connection's TLS version tells ext_proc which port to
		// route the actor on. See requestTLSVersion.
		RequestAttributes: []string{tlsVersionAttribute},
		// Explicitly configure the message timeout to avoid the 200ms default
		MessageTimeout: durationpb.New(5 * time.Second),
		ProcessingMode: &extprocv3filter.ProcessingMode{
			RequestHeaderMode:   extprocv3filter.ProcessingMode_SEND,
			ResponseHeaderMode:  extprocv3filter.ProcessingMode_SKIP,
			RequestBodyMode:     extprocv3filter.ProcessingMode_NONE,
			ResponseBodyMode:    extprocv3filter.ProcessingMode_NONE,
			RequestTrailerMode:  extprocv3filter.ProcessingMode_SKIP,
			ResponseTrailerMode: extprocv3filter.ProcessingMode_SKIP,
		},
	})

	dfpFilterConfig, _ := anypb.New(&dfpv3.FilterConfig{
		ImplementationSpecifier: &dfpv3.FilterConfig_DnsCacheConfig{
			DnsCacheConfig: buildDnsCacheConfig(),
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
				Name: "envoy.filters.http.dynamic_forward_proxy",
				ConfigType: &hcmv3.HttpFilter_TypedConfig{
					TypedConfig: dfpFilterConfig,
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
				RouteConfigName: routeName,
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
	hcm := x.buildHcm("ingress_http", RouteName)

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
	hcm := x.buildHcm("ingress_https", RouteNameHTTPS)

	tlsConfig := &tlsv3.DownstreamTlsContext{
		CommonTlsContext: &tlsv3.CommonTlsContext{
			TlsCertificates: []*tlsv3.TlsCertificate{
				x.buildTlsCertificate(),
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

func (x *XdsServer) buildTlsCertificate() *tlsv3.TlsCertificate {
	return &tlsv3.TlsCertificate{
		CertificateChain: &corev3.DataSource{
			Specifier: &corev3.DataSource_Filename{
				Filename: x.certPath,
			},
		},
		PrivateKey: &corev3.DataSource{
			Specifier: &corev3.DataSource_Filename{
				Filename: x.certPath, // Assuming combined file
			},
		},
	}
}
