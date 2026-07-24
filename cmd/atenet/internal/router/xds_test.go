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
	"strings"
	"testing"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
)

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
	}
}

func TestXdsServer_UpdateSnapshot_WithHttps(t *testing.T) {
	const certPath = "/run/servicedns.podcert.ate.dev/credential-bundle.pem"

	server := NewXdsServer(18000)
	server.SetConfig(8085, 50053, "127.0.0.1")
	server.SetTlsConfig(8443, certPath)

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

		// Verify the TLS config references the serving cert via SDS rather
		// than embedding it: inline filename DataSources are read only once
		// at listener creation, so rotations would never be picked up.
		fc := l.GetFilterChains()[0]
		ts := fc.GetTransportSocket()
		if ts.GetName() != "envoy.transport_sockets.tls" {
			t.Errorf("Expected transport socket 'envoy.transport_sockets.tls', got '%s'", ts.GetName())
		}
		dtc := &tlsv3.DownstreamTlsContext{}
		if err := ts.GetTypedConfig().UnmarshalTo(dtc); err != nil {
			t.Fatalf("Failed to unmarshal DownstreamTlsContext: %v", err)
		}
		if got := dtc.GetCommonTlsContext().GetTlsCertificates(); len(got) != 0 {
			t.Errorf("Expected no inline TlsCertificates, got %d", len(got))
		}
		sds := dtc.GetCommonTlsContext().GetTlsCertificateSdsSecretConfigs()
		if len(sds) != 1 {
			t.Fatalf("Expected 1 SDS secret config, got %d", len(sds))
		}
		if sds[0].GetName() != HTTPSCertSecretName {
			t.Errorf("Expected SDS secret name '%s', got '%s'", HTTPSCertSecretName, sds[0].GetName())
		}
		if sds[0].GetSdsConfig().GetAds() == nil {
			t.Error("Expected SDS config to use the ADS config source")
		}
	}

	// Verify the Secret resource carries the cert by filename with a watched
	// directory, so Envoy re-reads the files when kubelet rotates the
	// projected volume.
	secretsMap := snap.GetResources(resourcev3.SecretType)
	if len(secretsMap) != 1 {
		t.Fatalf("Expected 1 secret definition, got %d", len(secretsMap))
	}
	raw, exists := secretsMap[HTTPSCertSecretName]
	if !exists {
		t.Fatalf("Secret '%s' is missing from snapshot secrets", HTTPSCertSecretName)
	}
	secret := raw.(*tlsv3.Secret)
	tlsCert := secret.GetTlsCertificate()
	if got := tlsCert.GetCertificateChain().GetFilename(); got != certPath {
		t.Errorf("Expected certificate chain filename '%s', got '%s'", certPath, got)
	}
	if got := tlsCert.GetPrivateKey().GetFilename(); got != certPath {
		t.Errorf("Expected private key filename '%s', got '%s'", certPath, got)
	}
	if got, want := tlsCert.GetWatchedDirectory().GetPath(), "/run/servicedns.podcert.ate.dev"; got != want {
		t.Errorf("Expected watched directory '%s', got '%s'", want, got)
	}
}

func TestXdsServer_UpdateSnapshot_HttpsWithoutCertPath(t *testing.T) {
	server := NewXdsServer(18000)
	server.SetConfig(8085, 50053, "127.0.0.1")
	// This is the default flag combination: --port-https set, no
	// --envoy-cert-path. An SDS secret with an empty filename would be
	// NACKed by Envoy, so the HTTPS listener must be skipped entirely.
	server.SetTlsConfig(8443, "")

	if err := server.UpdateSnapshot(); err != nil {
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

	listenersMap := snap.GetResources(resourcev3.ListenerType)
	if _, exists := listenersMap[IngressHTTPSListener]; exists {
		t.Error("HTTPS listener must not be built without a cert path")
	}
	if len(listenersMap) != 1 {
		t.Errorf("Expected only the HTTP listener without a cert path, got %d listeners", len(listenersMap))
	}
	if got := snap.GetResources(resourcev3.SecretType); len(got) != 0 {
		t.Errorf("Expected no secrets without a cert path, got %d", len(got))
	}
}

func TestXdsServer_UpdateSnapshot_NoHttps_NoSecrets(t *testing.T) {
	server := NewXdsServer(18000)
	server.SetConfig(8085, 50053, "127.0.0.1")

	if err := server.UpdateSnapshot(); err != nil {
		t.Fatalf("UpdateSnapshot failed: %v", err)
	}

	res, err := server.snapshot.GetSnapshot(NodeID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}
	snap := res.(*cachev3.Snapshot)
	if got := snap.GetResources(resourcev3.SecretType); len(got) != 0 {
		t.Errorf("Expected no secrets without TLS config, got %d", len(got))
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
