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
	"regexp"
	"slices"
	"strings"
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	setfilterstatecommonv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/common/set_filter_state/v3"
	setfilterstatev3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/set_filter_state/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	setfilterstatenetworkv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/set_filter_state/v3"
	tcpproxyv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/tcp_proxy/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	httpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/spf13/pflag"

	"go.ate.dev/substrate/cmd/atenet/internal/router/extproc"
	"go.ate.dev/substrate/internal/atenet"
	atunnelingress "go.ate.dev/substrate/internal/atunnel/ingress"
)

// upstreamConnectSnapshot also enables the CONNECT listener, so main_internal
// is covered.
func upstreamConnectSnapshot(t *testing.T) *cachev3.Snapshot {
	t.Helper()
	server := NewXdsServer(18000)
	server.SetConfig(50053, "127.0.0.1")
	server.SetConnectTLSPort(8444)
	setIngressTLS(server)
	server.SetUpstreamTls("/run/bundle.pem", "/run/trust.pem")
	server.SetUpstreamConnect(true)
	if err := server.UpdateSnapshot(); err != nil {
		t.Fatalf("UpdateSnapshot failed: %v", err)
	}
	res, err := server.snapshot.GetSnapshot(NodeID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}
	snap := res.(*cachev3.Snapshot)
	if err := snap.Consistent(); err != nil {
		t.Fatalf("Integrity check failed on snapshot: %v", err)
	}
	return snap
}

// Parses real flags so the default and the wiring into the xDS server are
// tested together.
func TestUpstreamConnectFlag(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantErr     string // substring from validate; empty means valid
		wantTunnels bool
	}{
		{
			name:        "default tunnels to atunnel",
			wantTunnels: true,
		},
		{
			name: "false reaches atunnel over HTTPS",
			args: []string{"--upstream-connect=false"},
		},
		{
			name: "false without upstream mTLS accepted",
			args: []string{"--upstream-connect=false", "--upstream-credential-bundle="},
		},
		{
			name:    "default without upstream mTLS rejected",
			args:    []string{"--upstream-credential-bundle="},
			wantErr: "--upstream-connect=false",
		},
	}
	caPath, certPath := writeTestTLSMaterial(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var cfg routerConfig
			fs := pflag.NewFlagSet("router", pflag.ContinueOnError)
			bindRouterFlags(fs, &cfg)
			args := []string{
				"--mode=ingress",
				"--otlp-collector-address=",
				"--envoy-cert-path=" + certPath,
				"--ingress-client-ca-file=" + caPath,
				"--ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/demo/sa/client",
			}
			if err := fs.Parse(append(args, tc.args...)); err != nil {
				t.Fatalf("parsing flags: %v", err)
			}
			err := cfg.validate()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("validate() = %v, want an error containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validate() = %v, want valid", err)
			}

			s := &RouterServer{cfg: cfg}
			x, err := s.newEnvoyXdsServer(t.Context(), cfg.ParkedRequest.Normalized(), 0)
			if err != nil {
				t.Fatalf("newEnvoyXdsServer: %v", err)
			}
			res, err := x.snapshot.GetSnapshot(NodeID)
			if err != nil {
				t.Fatalf("Failed to get snapshot: %v", err)
			}
			snap := res.(*cachev3.Snapshot)
			if err := snap.Consistent(); err != nil {
				t.Fatalf("Integrity check failed on snapshot: %v", err)
			}

			var routed []string
			rc := snap.GetResources(resourcev3.RouteType)[RouteName].(*routev3.RouteConfiguration)
			for _, r := range rc.GetVirtualHosts()[0].GetRoutes() {
				routed = append(routed, r.GetRoute().GetCluster())
			}
			want := []string{OriginalDstClusterName}
			if tc.wantTunnels {
				want = []string{ActorTunnelHTTP2ClusterName, ActorTunnelHTTP1ClusterName}
			}
			if !slices.Equal(routed, want) {
				t.Errorf("routes send to %v, want %v", routed, want)
			}
			clusters := snap.GetResources(resourcev3.ClusterType)
			for _, name := range routed {
				if _, ok := clusters[name]; !ok {
					t.Errorf("a route sends to cluster %q, which the snapshot lacks", name)
				}
			}
			if _, ok := snap.GetResources(resourcev3.ListenerType)[ActorConnectName]; ok != tc.wantTunnels {
				t.Errorf("listener %q present = %v, want %v", ActorConnectName, ok, tc.wantTunnels)
			}
		})
	}
}

func TestXdsServer_UpstreamConnectResources(t *testing.T) {
	snap := upstreamConnectSnapshot(t)

	clusters := snap.GetResources(resourcev3.ClusterType)
	for _, name := range []string{ClusterName, MainInternalName, ActorTunnelHTTP1ClusterName, ActorTunnelHTTP2ClusterName, ActorConnectName} {
		if _, ok := clusters[name]; !ok {
			t.Errorf("cluster %q missing", name)
		}
	}
	if _, ok := clusters[OriginalDstClusterName]; ok {
		t.Errorf("cluster %q built although no route sends to it", OriginalDstClusterName)
	}
	if len(clusters) != 5 {
		t.Errorf("got %d clusters, want 5", len(clusters))
	}

	listeners := snap.GetResources(resourcev3.ListenerType)
	l, ok := listeners[ActorConnectName]
	if !ok {
		t.Fatalf("listener %q missing", ActorConnectName)
	}
	if l.(*listenerv3.Listener).GetInternalListener() == nil {
		t.Errorf("listener %q is not an internal listener", ActorConnectName)
	}

	// The tunnel filter copies ext_proc's decision, and the router picks a
	// tunnel by it. The worker identity filter follows it (see
	// TestXdsServer_WorkerIdentityFilter).
	for _, name := range []string{IngressHTTPSListener, MainInternalName} {
		hcm := &hcmv3.HttpConnectionManager{}
		if err := listeners[name].(*listenerv3.Listener).GetFilterChains()[0].GetFilters()[0].GetTypedConfig().UnmarshalTo(hcm); err != nil {
			t.Fatalf("listener %s: unmarshal HCM: %v", name, err)
		}
		filters := hcm.GetHttpFilters()
		var names []string
		for _, f := range filters {
			names = append(names, f.GetName())
		}
		n := len(filters)
		if n < 4 || names[n-4] != httpExtProcFilterName || names[n-3] != "envoy.filters.http.set_filter_state" || names[n-1] != "envoy.filters.http.router" {
			t.Fatalf("listener %s HTTP filters = %v, want them to end with ext_proc, the tunnel set_filter_state, the worker identity filter, router", name, names)
		}
		config := &setfilterstatev3.Config{}
		if err := filters[n-3].GetTypedConfig().UnmarshalTo(config); err != nil {
			t.Fatalf("listener %s: unmarshal set_filter_state: %v", name, err)
		}
		if got := config.GetOnRequestHeaders()[0].GetObjectKey(); got != tunnelKeyFilterStateKey {
			t.Errorf("listener %s: filter after ext_proc sets %q first, want the tunnel filter", name, got)
		}
	}
}

// Envoy pools tunnels by the hash of shared hashable values. A non-hashable
// key lets one actor's tunnel serve another, and a second hashable value
// splits the pool, since Envoy hashes them in map order.
func TestActorTunnelFilterStateFilter(t *testing.T) {
	config := &setfilterstatev3.Config{}
	if err := actorTunnelFilterStateFilter().GetTypedConfig().UnmarshalTo(config); err != nil {
		t.Fatalf("unmarshal set_filter_state config: %v", err)
	}

	const (
		worker = "%DYNAMIC_METADATA(envoy.filters.listener.original_dst:local)%"
		actor  = "%REQ(ate-target-actor)%"
		port   = "%DYNAMIC_METADATA(envoy.filters.listener.original_dst:port)%"
	)
	want := map[string]struct{ factory, format string }{
		tunnelKeyFilterStateKey:    {"envoy.hashable_string", worker + "|" + actor + "|" + port},
		tunnelWorkerFilterStateKey: {"envoy.string", worker},
		tunnelActorFilterStateKey:  {"envoy.string", actor},
		tunnelPortFilterStateKey:   {"envoy.string", port},
	}
	values := config.GetOnRequestHeaders()
	if len(values) != len(want) {
		t.Fatalf("filter sets %d values, want %d", len(values), len(want))
	}
	for _, v := range values {
		w, ok := want[v.GetObjectKey()]
		if !ok {
			t.Errorf("unexpected value %q", v.GetObjectKey())
			continue
		}
		if v.GetFactoryKey() != w.factory {
			t.Errorf("%s factory = %q, want %q", v.GetObjectKey(), v.GetFactoryKey(), w.factory)
		}
		if got := v.GetFormatString().GetTextFormatSource().GetInlineString(); got != w.format {
			t.Errorf("%s format = %q, want %q", v.GetObjectKey(), got, w.format)
		}
		if v.GetSharedWithUpstream() != setfilterstatecommonv3.FilterStateValue_ONCE {
			t.Errorf("%s shared with upstream = %v, want ONCE", v.GetObjectKey(), v.GetSharedWithUpstream())
		}
	}
}

func TestXdsServer_ActorTunnelRoutes(t *testing.T) {
	x := NewXdsServer(0)
	x.SetUpstreamConnect(true)
	routes := x.buildRoutes().GetVirtualHosts()[0].GetRoutes()
	if len(routes) != 2 {
		t.Fatalf("got %d routes, want 2", len(routes))
	}
	grpc, rest := routes[0], routes[1]
	if got := grpc.GetRoute().GetCluster(); got != ActorTunnelHTTP2ClusterName {
		t.Errorf("gRPC route sends to %q, want %q", got, ActorTunnelHTTP2ClusterName)
	}
	if got := rest.GetRoute().GetCluster(); got != ActorTunnelHTTP1ClusterName {
		t.Errorf("fallback route sends to %q, want %q", got, ActorTunnelHTTP1ClusterName)
	}
	if got := rest.GetMatch().GetHeaders(); len(got) != 0 {
		t.Errorf("fallback route matches headers %v, want none", got)
	}

	// Matches atunnel's isGRPC, minus the protocol check a route can't do.
	var method, contentType string
	for _, h := range grpc.GetMatch().GetHeaders() {
		switch h.GetName() {
		case ":method":
			method = h.GetStringMatch().GetExact()
		case "content-type":
			contentType = h.GetStringMatch().GetSafeRegex().GetRegex()
		}
	}
	if method != "POST" {
		t.Errorf("gRPC route matches method %q, want POST", method)
	}
	re, err := regexp.Compile(contentType)
	if err != nil {
		t.Fatalf("gRPC route content-type regex %q: %v", contentType, err)
	}
	for ct, isGRPC := range map[string]bool{
		"application/grpc":               true,
		"application/grpc+proto":         true,
		"application/grpc;charset=utf-8": true,
		"Application/GRPC":               true,
		"application/grpc-web":           false,
		"application/json":               false,
	} {
		if re.MatchString(ct) != isGRPC {
			t.Errorf("content-type %q matches the gRPC route = %v, want %v", ct, !isGRPC, isGRPC)
		}
	}

	for _, r := range routes {
		if got := r.GetRequestHeadersToRemove(); len(got) != 1 || got[0] != atenet.TargetActorHeader {
			t.Errorf("route to %s removes %v, want [%s]: nothing else strips it before the actor", r.GetRoute().GetCluster(), got, atenet.TargetActorHeader)
		}
		if got := r.GetRequestHeadersToAdd(); len(got) != 0 {
			t.Errorf("route to %s adds headers %v, want none: the CONNECT authority carries the port", r.GetRoute().GetCluster(), got)
		}
		if got := r.GetRoute().GetTimeout().AsDuration(); got != defaultRouteTimeout {
			t.Errorf("route to %s timeout = %v, want %v", r.GetRoute().GetCluster(), got, defaultRouteTimeout)
		}
		if got := r.GetRoute().GetIdleTimeout().AsDuration(); got != x.routeIdleTimeout() {
			t.Errorf("route to %s idle timeout = %v, want %v", r.GetRoute().GetCluster(), got, x.routeIdleTimeout())
		}
	}
}

func TestBuildActorConnectListener(t *testing.T) {
	filters := buildActorConnectListener().GetFilterChains()[0].GetFilters()
	if len(filters) != 2 {
		t.Fatalf("got %d network filters, want set_filter_state and tcp_proxy", len(filters))
	}

	setWorker := &setfilterstatenetworkv3.Config{}
	if err := filters[0].GetTypedConfig().UnmarshalTo(setWorker); err != nil {
		t.Fatalf("unmarshal set_filter_state: %v", err)
	}
	values := setWorker.GetOnNewConnection()
	if len(values) != 1 {
		t.Fatalf("set_filter_state sets %d values, want 1", len(values))
	}
	if got := values[0].GetObjectKey(); got != extproc.OriginalDstFilterStateKey {
		t.Errorf("set_filter_state sets %q, want %q for the ORIGINAL_DST cluster", got, extproc.OriginalDstFilterStateKey)
	}
	if got, want := values[0].GetFormatString().GetTextFormatSource().GetInlineString(), "%FILTER_STATE(dev.ate.tunnel.worker:PLAIN)%"; got != want {
		t.Errorf("worker address format = %q, want %q", got, want)
	}

	proxy := &tcpproxyv3.TcpProxy{}
	if err := filters[1].GetTypedConfig().UnmarshalTo(proxy); err != nil {
		t.Fatalf("unmarshal tcp_proxy: %v", err)
	}
	if got := proxy.GetCluster(); got != ActorConnectName {
		t.Errorf("tcp_proxy cluster = %q, want %q", got, ActorConnectName)
	}
	tunnel := proxy.GetTunnelingConfig()
	if got, want := tunnel.GetHostname(), "actor:%FILTER_STATE(dev.ate.tunnel.port:PLAIN)%"; got != want {
		t.Errorf("CONNECT authority = %q, want %q", got, want)
	}
	headers := tunnel.GetHeadersToAdd()
	if len(headers) != 1 || headers[0].GetHeader().GetKey() != atenet.TargetActorHeader ||
		headers[0].GetHeader().GetValue() != "%FILTER_STATE(dev.ate.tunnel.actor:PLAIN)%" {
		t.Errorf("CONNECT headers = %v, want only %s from dev.ate.tunnel.actor", headers, atenet.TargetActorHeader)
	}
	if idle := proxy.GetIdleTimeout(); idle == nil || idle.AsDuration() != 0 {
		t.Errorf("tcp_proxy idle timeout = %v, want 0 (off) so the HTTP side's timeouts fire first", idle)
	}
}

func TestXdsServer_ActorTunnelClusters(t *testing.T) {
	protocolOptions := func(t *testing.T, c *clusterv3.Cluster) *httpv3.HttpProtocolOptions {
		t.Helper()
		opts := &httpv3.HttpProtocolOptions{}
		if err := c.GetTypedExtensionProtocolOptions()[httpProtocolOptionsName].UnmarshalTo(opts); err != nil {
			t.Fatalf("cluster %s: unmarshal protocol options: %v", c.GetName(), err)
		}
		return opts
	}
	checkCircuitBreakers := func(t *testing.T, c *clusterv3.Cluster) {
		t.Helper()
		limits := c.GetCircuitBreakers().GetThresholds()[0]
		for name, got := range map[string]uint32{
			"max_connections":      limits.GetMaxConnections().GetValue(),
			"max_pending_requests": limits.GetMaxPendingRequests().GetValue(),
			"max_requests":         limits.GetMaxRequests().GetValue(),
		} {
			if got != tunnelCircuitBreakerLimit {
				t.Errorf("cluster %s %s = %d, want %d", c.GetName(), name, got, tunnelCircuitBreakerLimit)
			}
		}
	}

	for _, http2 := range []bool{false, true} {
		c := buildActorTunnelCluster("tunnel", http2)
		target := c.GetLoadAssignment().GetEndpoints()[0].GetLbEndpoints()[0].GetEndpoint().GetAddress().GetEnvoyInternalAddress()
		if got := target.GetServerListenerName(); got != ActorConnectName {
			t.Errorf("http2=%v: tunnel cluster sends to %q, want internal listener %q", http2, got, ActorConnectName)
		}
		opts := protocolOptions(t, c)
		explicit := opts.GetExplicitHttpConfig()
		if http2 && explicit.GetHttp2ProtocolOptions() == nil || !http2 && explicit.GetHttpProtocolOptions() == nil {
			t.Errorf("http2=%v: tunnel cluster protocol = %v", http2, explicit)
		}
		if got := opts.GetCommonHttpProtocolOptions().GetIdleTimeout().AsDuration(); got != tunnelIdleTimeout {
			t.Errorf("http2=%v: tunnel idle timeout = %v, want %v", http2, got, tunnelIdleTimeout)
		}
		checkCircuitBreakers(t, c)
	}

	x := NewXdsServer(0)
	x.SetUpstreamTls("/run/bundle.pem", "/run/trust.pem")
	c := x.buildActorConnectCluster()
	if c.GetType() != clusterv3.Cluster_ORIGINAL_DST {
		t.Errorf("actor_connect type = %v, want ORIGINAL_DST", c.GetType())
	}
	lb := c.GetOriginalDstLbConfig()
	if lb.GetMetadataKey() != nil {
		t.Error("actor_connect reads a metadata key; the worker address comes from filter state")
	}
	if got := lb.GetUpstreamPortOverride().GetValue(); got != atunnelingress.DefaultConnectPort {
		t.Errorf("actor_connect port = %d, want atunnel's CONNECT port %d", got, atunnelingress.DefaultConnectPort)
	}
	upstreamTLS := &tlsv3.UpstreamTlsContext{}
	if err := c.GetTransportSocket().GetTypedConfig().UnmarshalTo(upstreamTLS); err != nil {
		t.Fatalf("actor_connect: unmarshal UpstreamTlsContext: %v", err)
	}
	if len(upstreamTLS.GetCommonTlsContext().GetTlsCertificateSdsSecretConfigs()) == 0 {
		t.Error("actor_connect presents no client certificate; atunnel requires mTLS")
	}
	if got := protocolOptions(t, c).GetExplicitHttpConfig().GetHttp2ProtocolOptions(); got == nil {
		t.Error("actor_connect is not HTTP/2; tunnels to one worker would each need a connection")
	}
	checkCircuitBreakers(t, c)
}
