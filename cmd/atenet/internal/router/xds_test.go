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
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	dfpclusterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/clusters/dynamic_forward_proxy/v3"
	extprocv3filter "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	httpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
)

// extProcConfig unpacks the ext_proc filter config from a listener's sole filter
// chain.
func extProcConfig(t *testing.T, l *listenerv3.Listener) *extprocv3filter.ExternalProcessor {
	t.Helper()

	hcm := &hcmv3.HttpConnectionManager{}
	if err := l.GetFilterChains()[0].GetFilters()[0].GetTypedConfig().UnmarshalTo(hcm); err != nil {
		t.Fatalf("Failed to unmarshal connection manager for '%s': %v", l.GetName(), err)
	}

	for _, f := range hcm.GetHttpFilters() {
		if f.GetName() != extProcFilterName {
			continue
		}
		cfg := &extprocv3filter.ExternalProcessor{}
		if err := f.GetTypedConfig().UnmarshalTo(cfg); err != nil {
			t.Fatalf("Failed to unmarshal ext_proc config for '%s': %v", l.GetName(), err)
		}
		return cfg
	}

	t.Fatalf("Listener '%s' has no '%s' filter", l.GetName(), extProcFilterName)
	return nil
}

// assertRequestsTLSVersion checks that ext_proc is sent the downstream TLS
// version. Envoy silently drops attribute names it does not recognise, and an
// undelivered attribute reads as plaintext — which strands the HTTPS ingress on
// the actor's port 80 with no error anywhere in the control plane.
func assertRequestsTLSVersion(t *testing.T, l *listenerv3.Listener) {
	t.Helper()

	if got := extProcConfig(t, l).GetRequestAttributes(); !slices.Contains(got, tlsVersionAttribute) {
		t.Errorf("Listener '%s' requests attributes %v, want to include '%s'", l.GetName(), got, tlsVersionAttribute)
	}
}

// dfpClusterConfig unpacks the dynamic forward proxy config from a cluster's
// custom cluster type.
func dfpClusterConfig(t *testing.T, c *clusterv3.Cluster) *dfpclusterv3.ClusterConfig {
	t.Helper()

	cfg := &dfpclusterv3.ClusterConfig{}
	raw := c.GetClusterType().GetTypedConfig()
	if err := raw.UnmarshalTo(cfg); err != nil {
		t.Fatalf("Failed to unmarshal dynamic forward proxy config for '%s': %v", c.GetName(), err)
	}
	return cfg
}

func TestXdsServer_UpdateSnapshot(t *testing.T) {
	server := NewXdsServer(18000)
	server.SetConfig(8081, 50052, "10.0.0.1")

	err := server.UpdateSnapshot()
	if err != nil {
		t.Fatalf("UpdateSnapshot failed: %v", err)
	}

	res, err := server.snapshot.GetSnapshot(NodeID)
	if err != nil {
		t.Fatalf("Failed to get generated snapshot: %v", err)
	}

	snap, ok := res.(*cachev3.Snapshot)
	if !ok {
		t.Fatalf("Snapshot doesn't conform to type *cachev3.Snapshot, got %T", res)
	}

	// Check consistent snapshot
	if err := snap.Consistent(); err != nil {
		t.Fatalf("Integrity check failed on snapshot: %v", err)
	}

	// Verify clusters generated
	clustersMap := snap.GetResources(resourcev3.ClusterType)
	if len(clustersMap) != 2 {
		t.Errorf("Expected 2 cluster definitions, got %d", len(clustersMap))
	}

	if raw, exists := clustersMap["ate-cluster"]; !exists {
		t.Error("Static 'ate-cluster' is missing from clusters")
	} else {
		c := raw.(*clusterv3.Cluster)
		if c.GetName() != "ate-cluster" {
			t.Errorf("Expected name 'ate-cluster', got %s", c.GetName())
		}

		// Validate Endpoint address mapped from Server parameters
		eps := c.GetLoadAssignment().GetEndpoints()[0].GetLbEndpoints()[0].GetEndpoint().GetAddress().GetSocketAddress()
		if eps.GetAddress() != "10.0.0.1" {
			t.Errorf("Expected address '10.0.0.1', got %s", eps.GetAddress())
		}
		if eps.GetPortValue() != 50052 {
			t.Errorf("Expected port 50052, got %d", eps.GetPortValue())
		}
	}

	if raw, exists := clustersMap["dynamic_forward_proxy_cluster"]; !exists {
		t.Error("'dynamic_forward_proxy_cluster' is missing from clusters")
	} else {
		c := raw.(*clusterv3.Cluster)
		if c.GetName() != "dynamic_forward_proxy_cluster" {
			t.Errorf("Expected 'dynamic_forward_proxy_cluster', got %s", c.GetName())
		}
	}

	// Verify Virtual Hosts generated inside Route configuration
	routesMap := snap.GetResources(resourcev3.RouteType)
	if len(routesMap) != 1 {
		t.Fatalf("Expected 1 route configuration object, got %d", len(routesMap))
	}

	if raw, exists := routesMap[RouteName]; !exists {
		t.Errorf("Route name '%s' is missing from snapshot routes configuration", RouteName)
	} else {
		rc := raw.(*routev3.RouteConfiguration)
		if rc.GetName() != RouteName {
			t.Errorf("Expected route name '%s', got %s", RouteName, rc.GetName())
		}

		if len(rc.GetVirtualHosts()) != 1 {
			t.Fatalf("Expected 1 VirtualHost definition for static routes case, got %d", len(rc.GetVirtualHosts()))
		}

		vh := rc.GetVirtualHosts()[0]
		if len(vh.GetDomains()) != 1 || vh.GetDomains()[0] != "*" {
			t.Errorf("Expected domain '*', got %v", vh.GetDomains())
		}

		if len(vh.GetRoutes()) != 1 {
			t.Fatalf("Expected 1 route in fallback VirtualHost, got %d", len(vh.GetRoutes()))
		}

		fallbackRoute := vh.GetRoutes()[0]
		if fallbackRoute.GetMatch().GetPrefix() != "/" {
			t.Errorf("Expected path mapping prefix '/', got '%s'", fallbackRoute.GetMatch().GetPrefix())
		}
	}

	// Verify listeners generated
	listenersMap := snap.GetResources(resourcev3.ListenerType)
	if len(listenersMap) != 1 {
		t.Fatalf("Expected 1 listener definition, got %d", len(listenersMap))
	}

	if raw, exists := listenersMap[IngressHTTPListener]; !exists {
		t.Errorf("Listener name '%s' is missing from snapshot listeners", IngressHTTPListener)
	} else {
		l := raw.(*listenerv3.Listener)
		sa := l.GetAddress().GetSocketAddress()
		if sa.GetPortValue() != 8081 {
			t.Errorf("Expected port 8081, got %d", sa.GetPortValue())
		}
		if sa.GetAddress() != "0.0.0.0" {
			t.Errorf("Expected address '0.0.0.0', got %s", sa.GetAddress())
		}
		assertRequestsTLSVersion(t, l)
	}
}

func TestXdsServer_UpdateSnapshot_WithHttps(t *testing.T) {
	server := NewXdsServer(18000)
	server.SetConfig(8085, 50053, "127.0.0.1")
	server.SetTlsConfig(8443, "")

	err := server.UpdateSnapshot()
	if err != nil {
		t.Fatalf("UpdateSnapshot failed: %v", err)
	}

	res, err := server.snapshot.GetSnapshot(NodeID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	snap, ok := res.(*cachev3.Snapshot)
	if !ok {
		t.Fatalf("Snapshot doesn't conform to type *cachev3.Snapshot, got %T", res)
	}

	// The HTTPS listener, its route configuration and the TLS upstream cluster
	// are gated together, so enabling TLS adds exactly one of each.
	if err := snap.Consistent(); err != nil {
		t.Fatalf("Integrity check failed on snapshot: %v", err)
	}

	listenersMap := snap.GetResources(resourcev3.ListenerType)
	if len(listenersMap) != 2 {
		t.Fatalf("Expected 2 listener definitions, got %d", len(listenersMap))
	}

	if raw, exists := listenersMap[IngressHTTPSListener]; !exists {
		t.Errorf("Listener name '%s' is missing from snapshot listeners", IngressHTTPSListener)
	} else {
		l := raw.(*listenerv3.Listener)
		sa := l.GetAddress().GetSocketAddress()
		if sa.GetPortValue() != 8443 {
			t.Errorf("Expected port 8443, got %d", sa.GetPortValue())
		}

		// Verify TLS config
		fc := l.GetFilterChains()[0]
		ts := fc.GetTransportSocket()
		if ts.GetName() != "envoy.transport_sockets.tls" {
			t.Errorf("Expected transport socket 'envoy.transport_sockets.tls', got '%s'", ts.GetName())
		}
		assertRequestsTLSVersion(t, l)
	}

	// Both ingresses read the same attribute; the plaintext one relies on Envoy
	// omitting it for connections with no TLS handshake.
	if raw, exists := listenersMap[IngressHTTPListener]; !exists {
		t.Errorf("Listener name '%s' is missing from snapshot listeners", IngressHTTPListener)
	} else {
		assertRequestsTLSVersion(t, raw.(*listenerv3.Listener))
	}

	clustersMap := snap.GetResources(resourcev3.ClusterType)
	if len(clustersMap) != 3 {
		t.Fatalf("Expected 3 cluster definitions, got %d", len(clustersMap))
	}

	if raw, exists := clustersMap[DFPClusterName]; !exists {
		t.Errorf("Cluster '%s' is missing from clusters", DFPClusterName)
	} else {
		c := raw.(*clusterv3.Cluster)
		// The plaintext cluster must stay plaintext.
		if ts := c.GetTransportSocket(); ts != nil {
			t.Errorf("Expected no transport socket on '%s', got '%s'", DFPClusterName, ts.GetName())
		}
		// Nothing is being waived on the plaintext cluster: with no transport
		// socket there is no SNI or SAN requirement to waive.
		if dfpClusterConfig(t, c).GetAllowInsecureClusterOptions() {
			t.Errorf("Expected AllowInsecureClusterOptions to be false on '%s'", DFPClusterName)
		}
	}

	if raw, exists := clustersMap[DFPTLSClusterName]; !exists {
		t.Errorf("Cluster '%s' is missing from clusters", DFPTLSClusterName)
	} else {
		c := raw.(*clusterv3.Cluster)
		if ts := c.GetTransportSocket(); ts.GetName() != "envoy.transport_sockets.tls" {
			t.Errorf("Expected upstream transport socket 'envoy.transport_sockets.tls', got '%s'", ts.GetName())
		}

		// Without this waiver Envoy rejects the entire CDS update, because a
		// dynamic forward proxy cluster with a TLS transport socket requires
		// auto_san_validation, which this cluster deliberately does not do.
		if !dfpClusterConfig(t, c).GetAllowInsecureClusterOptions() {
			t.Errorf("Expected AllowInsecureClusterOptions on '%s'; Envoy rejects the CDS update without it", DFPTLSClusterName)
		}

		// The SNI has to come from SNIHeader: by the time the request reaches
		// the upstream, ext_proc has rewritten :authority to a bare pod IP.
		raw, exists := c.GetTypedExtensionProtocolOptions()["envoy.extensions.upstreams.http.v3.HttpProtocolOptions"]
		if !exists {
			t.Fatalf("Cluster '%s' is missing HttpProtocolOptions", DFPTLSClusterName)
		}
		protoOpts := &httpv3.HttpProtocolOptions{}
		if err := raw.UnmarshalTo(protoOpts); err != nil {
			t.Fatalf("Failed to unmarshal HttpProtocolOptions: %v", err)
		}
		if got := protoOpts.GetUpstreamHttpProtocolOptions().GetOverrideAutoSniHeader(); got != SNIHeader {
			t.Errorf("Expected SNI sourced from header '%s', got '%s'", SNIHeader, got)
		}
		if !protoOpts.GetUpstreamHttpProtocolOptions().GetAutoSni() {
			t.Error("Expected AutoSni to be enabled on the TLS cluster")
		}
	}

	// Each listener routes to its own cluster: only HTTPS ingress is
	// re-originated as TLS.
	routesMap := snap.GetResources(resourcev3.RouteType)
	if len(routesMap) != 2 {
		t.Fatalf("Expected 2 route configuration objects, got %d", len(routesMap))
	}

	for routeName, wantCluster := range map[string]string{
		RouteName:      DFPClusterName,
		RouteNameHTTPS: DFPTLSClusterName,
	} {
		raw, exists := routesMap[routeName]
		if !exists {
			t.Errorf("Route name '%s' is missing from snapshot routes configuration", routeName)
			continue
		}
		rc := raw.(*routev3.RouteConfiguration)
		if len(rc.GetVirtualHosts()) != 1 || len(rc.GetVirtualHosts()[0].GetRoutes()) != 1 {
			t.Errorf("Expected 1 VirtualHost with 1 route in '%s'", routeName)
			continue
		}
		if got := rc.GetVirtualHosts()[0].GetRoutes()[0].GetRoute().GetCluster(); got != wantCluster {
			t.Errorf("Expected route '%s' to target cluster '%s', got '%s'", routeName, wantCluster, got)
		}
	}
}

func TestXdsServer_Serve_Shutdown(t *testing.T) {
	server := NewXdsServer(18000)
	server.SetConfig(8085, 50053, "127.0.0.1")

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create tcp listener: %v", err)
	}
	defer lis.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)

	go func() {
		errChan <- server.Serve(ctx, lis)
	}()

	// Cancel the context to trigger graceful stop
	cancel()

	select {
	case err := <-errChan:
		if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			t.Errorf("Serve error returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout exceeded waiting for Serve to finish graceful closure")
	}
}
