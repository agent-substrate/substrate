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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	secretgrpc "github.com/envoyproxy/go-control-plane/envoy/service/secret/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/ingress"
	"github.com/agent-substrate/substrate/internal/atunnel"
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

	if raw, exists := clustersMap[OriginalDstClusterName]; !exists {
		t.Errorf("'%s' is missing from clusters", OriginalDstClusterName)
	} else {
		c := raw.(*clusterv3.Cluster)
		if c.GetName() != OriginalDstClusterName {
			t.Errorf("Expected '%s', got %s", OriginalDstClusterName, c.GetName())
		}
		if c.GetType() != clusterv3.Cluster_ORIGINAL_DST {
			t.Errorf("Expected ORIGINAL_DST cluster, got %s", c.GetType())
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

		addrs := l.GetAdditionalAddresses()
		if len(addrs) == 0 {
			t.Fatalf("Expected an additional address on %s, got none", IngressHTTPListener)
		}

		asa := addrs[0].GetAddress().GetSocketAddress()
		if asa.GetAddress() != "::" {
			t.Errorf("Expected additional address '::', got %s", asa.GetAddress())
		}
		if asa.GetIpv4Compat() {
			t.Errorf("Expected additional address Ipv4Compat to be false")
		}
		if asa.GetPortValue() != 8081 {
			t.Errorf("Expected additional port 8081, got %d", asa.GetPortValue())
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
		if sa.GetAddress() != "0.0.0.0" {
			t.Errorf("Expected address '0.0.0.0', got %s", sa.GetAddress())
		}

		addrs := l.GetAdditionalAddresses()
		if len(addrs) == 0 {
			t.Fatalf("Expected an additional address on %s, got none", IngressHTTPSListener)
		}

		asa := addrs[0].GetAddress().GetSocketAddress()
		if asa.GetAddress() != "::" {
			t.Errorf("Expected additional address '::', got %s", asa.GetAddress())
		}
		if asa.GetIpv4Compat() {
			t.Errorf("Expected additional address Ipv4Compat to be false")
		}
		if asa.GetPortValue() != 8443 {
			t.Errorf("Expected additional port 8443, got %d", asa.GetPortValue())
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

// TestXdsServer_UpdateSnapshot_ConnectDisabledByDefault locks in that the
// CONNECT-terminating listeners/cluster are opt-in: with SetConnectPorts never
// called (both ports default to 0), UpdateSnapshot must produce exactly the
// same resources as if CONNECT support didn't exist, matching the HTTPS
// listener's existing httpsPort>0 gating convention.
func TestXdsServer_UpdateSnapshot_ConnectDisabledByDefault(t *testing.T) {
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

	clustersMap := snap.GetResources(resourcev3.ClusterType)
	if _, exists := clustersMap[MainInternalName]; exists {
		t.Errorf("%s cluster must not be built when CONNECT is disabled", MainInternalName)
	}
	if len(clustersMap) != 2 {
		t.Errorf("Expected 2 cluster definitions with CONNECT disabled, got %d", len(clustersMap))
	}

	listenersMap := snap.GetResources(resourcev3.ListenerType)
	for _, name := range []string{"connect_terminate", "connect_terminate_tls", MainInternalName} {
		if _, exists := listenersMap[name]; exists {
			t.Errorf("listener %q must not be built when CONNECT is disabled", name)
		}
	}
	if len(listenersMap) != 1 {
		t.Errorf("Expected only the HTTP listener with CONNECT disabled, got %d listeners", len(listenersMap))
	}
}

// TestXdsServer_UpdateSnapshot_WithConnect enables both the plaintext and TLS
// CONNECT listeners and checks the resources they need are wired up,
// including that the TLS CONNECT listener triggers the shared cert secret
// even when the ordinary HTTPS listener (httpsPort) is left disabled.
func TestXdsServer_UpdateSnapshot_WithConnect(t *testing.T) {
	const certPath = "/run/servicedns.podcert.ate.dev/credential-bundle.pem"

	server := NewXdsServer(18000)
	server.SetConfig(8085, 50053, "127.0.0.1")
	server.SetConnectPorts(8081, 8444)
	// httpsPort left at 0: only the CONNECT-TLS listener wants the cert here.
	server.SetTlsConfig(0, certPath)

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

	clustersMap := snap.GetResources(resourcev3.ClusterType)
	if _, exists := clustersMap[MainInternalName]; !exists {
		t.Errorf("%s cluster missing with CONNECT enabled", MainInternalName)
	}

	listenersMap := snap.GetResources(resourcev3.ListenerType)
	if _, exists := listenersMap[IngressHTTPSListener]; exists {
		t.Error("plain HTTPS listener must not be built when only httpsPort is left disabled")
	}
	if raw, exists := listenersMap["connect_terminate"]; !exists {
		t.Error("connect_terminate listener missing")
	} else if sa := raw.(*listenerv3.Listener).GetAddress().GetSocketAddress(); sa.GetPortValue() != 8081 {
		t.Errorf("Expected connect_terminate port 8081, got %d", sa.GetPortValue())
	}
	if raw, exists := listenersMap["connect_terminate_tls"]; !exists {
		t.Error("connect_terminate_tls listener missing")
	} else {
		l := raw.(*listenerv3.Listener)
		if sa := l.GetAddress().GetSocketAddress(); sa.GetPortValue() != 8444 {
			t.Errorf("Expected connect_terminate_tls port 8444, got %d", sa.GetPortValue())
		}
		ts := l.GetFilterChains()[0].GetTransportSocket()
		if ts.GetName() != "envoy.transport_sockets.tls" {
			t.Errorf("Expected connect_terminate_tls to be TLS-wrapped, got transport socket %q", ts.GetName())
		}
	}
	if _, exists := listenersMap[MainInternalName]; !exists {
		t.Errorf("%s listener missing with CONNECT enabled", MainInternalName)
	}

	// The TLS CONNECT listener alone must be enough to require the secret.
	secretsMap := snap.GetResources(resourcev3.SecretType)
	if _, exists := secretsMap[HTTPSCertSecretName]; !exists {
		t.Error("cert secret missing even though connect_terminate_tls needs it")
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

// TestXdsServer_ServesSecretOverSds fetches the serving cert secret over a
// real SDS stream, as Envoy would, covering the SDS registration in Serve.
func TestXdsServer_ServesSecretOverSds(t *testing.T) {
	const certPath = "/run/servicedns.podcert.ate.dev/credential-bundle.pem"

	server := NewXdsServer(18000)
	server.SetConfig(8085, 50053, "127.0.0.1")
	server.SetTlsConfig(8443, certPath)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create tcp listener: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go server.Serve(ctx, lis)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial xDS server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	streamCtx, streamCancel := context.WithTimeout(ctx, 10*time.Second)
	t.Cleanup(streamCancel)
	stream, err := secretgrpc.NewSecretDiscoveryServiceClient(conn).StreamSecrets(streamCtx)
	if err != nil {
		t.Fatalf("Failed to open SDS stream: %v", err)
	}
	if err := stream.Send(&discoverygrpc.DiscoveryRequest{
		Node:          &corev3.Node{Id: NodeID},
		TypeUrl:       resourcev3.SecretType,
		ResourceNames: []string{HTTPSCertSecretName},
	}); err != nil {
		t.Fatalf("Failed to send SDS discovery request: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Failed to receive SDS discovery response: %v", err)
	}

	resources := resp.GetResources()
	if len(resources) != 1 {
		t.Fatalf("Expected 1 secret resource over SDS, got %d", len(resources))
	}

	secret := &tlsv3.Secret{}
	if err := resources[0].UnmarshalTo(secret); err != nil {
		t.Fatalf("Failed to unmarshal SDS resource into Secret: %v", err)
	}
	if secret.GetName() != HTTPSCertSecretName {
		t.Errorf("Expected secret name '%s', got '%s'", HTTPSCertSecretName, secret.GetName())
	}
	tlsCert := secret.GetTlsCertificate()
	if got := tlsCert.GetCertificateChain().GetFilename(); got != certPath {
		t.Errorf("Expected certificate chain filename '%s', got '%s'", certPath, got)
	}
	if got := tlsCert.GetPrivateKey().GetFilename(); got != certPath {
		t.Errorf("Expected private key filename '%s', got '%s'", certPath, got)
	}
	if got, want := tlsCert.GetWatchedDirectory().GetPath(), filepath.Dir(certPath); got != want {
		t.Errorf("Expected watched directory '%s', got '%s'", want, got)
	}
}

// Symlink names used by kubelet's AtomicWriter in projected volumes.
const (
	dataDirName    = "..data"
	newDataDirName = "..data_tmp"
)

// TestTlsSecret_ProjectedVolumeRotation checks the reload contract the
// secret relies on: a kubelet podCertificate rotation swaps the ..data
// symlink directly inside WatchedDirectory (the move Envoy watches for),
// after which the cert filename resolves to the new bundle. Envoy's actual
// reload behavior is out of unit-test reach and belongs to e2e.
func TestTlsSecret_ProjectedVolumeRotation(t *testing.T) {
	dir := t.TempDir()
	certA := "serving-cert-a"
	certB := "serving-cert-b"
	certPath := filepath.Join(dir, "credential-bundle.pem")
	bundleA := makeServingBundle(t, certA)
	bundleB := makeServingBundle(t, certB)

	const tsDirA = "..2026_07_25_00_00_00.0000000001"
	const tsDirB = "..2026_07_25_00_00_00.0000000002"
	writeProjectedVolume(t, dir, tsDirA, bundleA)

	server := NewXdsServer(18000)
	server.SetTlsConfig(8443, certPath)
	tlsCert := server.buildTlsSecret().GetTlsCertificate()

	chainPath := tlsCert.GetCertificateChain().GetFilename()
	if got := readServingCN(t, chainPath); got != certA {
		t.Fatalf("Expected initial bundle to serve %q, got %q", certA, got)
	}

	swapPath := filepath.Join(tlsCert.GetWatchedDirectory().GetPath(), dataDirName)
	before, err := os.Readlink(swapPath)
	if err != nil {
		t.Fatalf("The rotation symlink is not a direct child of WatchedDirectory: %v", err)
	}

	rotateProjectedVolume(t, dir, tsDirB, tsDirA, bundleB)

	after, err := os.Readlink(swapPath)
	if err != nil {
		t.Fatalf("The rotation symlink left WatchedDirectory after rotation: %v", err)
	}
	if after == before {
		t.Fatalf("Rotation did not retarget the %s symlink (still %q); an in-place write would not trigger Envoy's reload", dataDirName, after)
	}
	if got := readServingCN(t, chainPath); got != certB {
		t.Fatalf("Expected rotated bundle to serve %q, got %q", certB, got)
	}
}

// makeServingBundle returns a podCertificate-style PEM bundle: a PKCS8
// private key followed by a self-signed serving cert with the given CN.
func makeServingBundle(t *testing.T, cn string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create serving certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS8 key: %v", err)
	}
	return append(
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...,
	)
}

// readServingCN loads the bundle as a key pair (the same file for cert and
// key, as Envoy does) and returns the leaf's common name.
func readServingCN(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read bundle %s: %v", path, err)
	}
	pair, err := tls.X509KeyPair(data, data)
	if err != nil {
		t.Fatalf("load bundle %s as key pair: %v", path, err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf certificate from %s: %v", path, err)
	}
	return leaf.Subject.CommonName
}

// writeProjectedVolume lays dir out like a kubelet projected volume:
// payload in a timestamped dir, reached through the ..data symlink.
func writeProjectedVolume(t *testing.T, dir, tsDir string, bundle []byte) {
	t.Helper()
	if err := os.Mkdir(filepath.Join(dir, tsDir), 0o755); err != nil {
		t.Fatalf("create payload dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, tsDir, "credential-bundle.pem"), bundle, 0o600); err != nil {
		t.Fatalf("write bundle payload: %v", err)
	}
	if err := os.Symlink(tsDir, filepath.Join(dir, dataDirName)); err != nil {
		t.Fatalf("create ..data symlink: %v", err)
	}
	if err := os.Symlink(filepath.Join(dataDirName, "credential-bundle.pem"), filepath.Join(dir, "credential-bundle.pem")); err != nil {
		t.Fatalf("create bundle symlink: %v", err)
	}
}

// rotateProjectedVolume swaps in a new payload the way kubelet's
// AtomicWriter does: rename a ..data_tmp symlink over ..data.
// https://github.com/kubernetes/kubernetes/blob/24a5b063a5f2b8d6c2d1d9279758109a7b75d4ad/pkg/volume/util/atomic_writer.go#L114-L119
func rotateProjectedVolume(t *testing.T, dir, newTsDir, oldTsDir string, bundle []byte) {
	t.Helper()
	if err := os.Mkdir(filepath.Join(dir, newTsDir), 0o755); err != nil {
		t.Fatalf("create new payload dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, newTsDir, "credential-bundle.pem"), bundle, 0o600); err != nil {
		t.Fatalf("write new bundle payload: %v", err)
	}
	if err := os.Symlink(newTsDir, filepath.Join(dir, newDataDirName)); err != nil {
		t.Fatalf("create ..data_tmp symlink: %v", err)
	}
	if err := os.Rename(filepath.Join(dir, newDataDirName), filepath.Join(dir, dataDirName)); err != nil {
		t.Fatalf("swap ..data symlink: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(dir, oldTsDir)); err != nil {
		t.Fatalf("remove old payload dir: %v", err)
	}
}

func TestXdsServer_ExtProcCircuitBreaker(t *testing.T) {
	t.Run("DefaultCoversLotPlusHeadroom", func(t *testing.T) {
		x := NewXdsServer(0)
		got := x.buildCluster().GetCircuitBreakers().GetThresholds()[0].GetMaxRequests().GetValue()
		if got != uint32(defaultExtProcMaxRequests) {
			t.Errorf("default max_requests = %d, want %d", got, defaultExtProcMaxRequests)
		}
		if got < uint32(ingress.DefaultParkedRequestMax) {
			t.Errorf("default breaker (%d) below the default lot (%d): a full lot would be truncated by Envoy", got, ingress.DefaultParkedRequestMax)
		}
	})

	t.Run("SetterOverrides", func(t *testing.T) {
		x := NewXdsServer(0)
		x.SetExtProcMaxRequests(4096)
		got := x.buildCluster().GetCircuitBreakers().GetThresholds()[0].GetMaxRequests().GetValue()
		if got != 4096 {
			t.Errorf("max_requests after SetExtProcMaxRequests(4096) = %d, want 4096", got)
		}
	})

	t.Run("NonPositiveKeepsDefault", func(t *testing.T) {
		x := NewXdsServer(0)
		x.SetExtProcMaxRequests(0)
		got := x.buildCluster().GetCircuitBreakers().GetThresholds()[0].GetMaxRequests().GetValue()
		if got != uint32(defaultExtProcMaxRequests) {
			t.Errorf("max_requests after SetExtProcMaxRequests(0) = %d, want default %d", got, defaultExtProcMaxRequests)
		}
	})
}

func TestXdsServer_RouteTimeout(t *testing.T) {
	// routeAction digs out the one workload route buildRoutes emits, which is
	// where Envoy actually reads its timeouts.
	//
	// It pins the route to OriginalDstClusterName rather than trusting position:
	// that is the cluster carrying actor traffic to the worker's atunnel ingress.
	// A change that moves actor traffic onto some other route would otherwise
	// leave this passing while the timeouts govern a route nothing uses.
	routeAction := func(t *testing.T, x *XdsServer) *routev3.RouteAction {
		t.Helper()
		hosts := x.buildRoutes().GetVirtualHosts()
		if len(hosts) != 1 || len(hosts[0].GetRoutes()) != 1 {
			t.Fatalf("buildRoutes() = %d virtual hosts, want exactly 1 with 1 route", len(hosts))
		}
		action := hosts[0].GetRoutes()[0].GetRoute()
		if got := action.GetCluster(); got != OriginalDstClusterName {
			t.Fatalf("workload route targets cluster %q, want %q", got, OriginalDstClusterName)
		}
		return action
	}
	routeTimeout := func(t *testing.T, x *XdsServer) time.Duration {
		t.Helper()
		return routeAction(t, x).GetTimeout().AsDuration()
	}
	idleTimeout := func(t *testing.T, x *XdsServer) time.Duration {
		t.Helper()
		return routeAction(t, x).GetIdleTimeout().AsDuration()
	}

	t.Run("Default", func(t *testing.T) {
		if got := routeTimeout(t, NewXdsServer(0)); got != defaultRouteTimeout {
			t.Errorf("default route timeout = %v, want %v", got, defaultRouteTimeout)
		}
	})

	t.Run("SetterOverrides", func(t *testing.T) {
		x := NewXdsServer(0)
		x.SetRouteTimeout(5 * time.Minute)
		if got := routeTimeout(t, x); got != 5*time.Minute {
			t.Errorf("route timeout after SetRouteTimeout(5m) = %v, want 5m", got)
		}
	})

	// The flag cannot produce a zero: --route-timeout carries defaultRouteTimeout,
	// so an operator who never passes it gets 10s, not 0. The guard is on the
	// setter because SetRouteTimeout is part of the type's API and reachable
	// from any caller, and because a zero here is the one value Envoy reads as
	// "no timeout at all" — a mis-set knob would silently turn every stuck
	// actor into a held-open request rather than failing visibly. The sibling
	// setters guard the same way.
	t.Run("NonPositiveKeepsDefault", func(t *testing.T) {
		for _, d := range []time.Duration{0, -time.Second} {
			x := NewXdsServer(0)
			x.SetRouteTimeout(d)
			if got := routeTimeout(t, x); got != defaultRouteTimeout {
				t.Errorf("route timeout after SetRouteTimeout(%v) = %v, want default %v", d, got, defaultRouteTimeout)
			}
		}
	})

	// The route timeout alone does not bound a long turn. A stream carrying no
	// bytes while the actor works is idle by Envoy's reckoning, and Envoy resets
	// it at the 5m stream idle default whatever the route timeout says. These
	// pin the relationship: the idle timer never bites before the ceiling the
	// operator asked for, and it is not tightened below what applies today.
	t.Run("IdleTimeoutTracksLongerRouteTimeout", func(t *testing.T) {
		x := NewXdsServer(0)
		x.SetRouteTimeout(30 * time.Minute)
		if got := idleTimeout(t, x); got != 30*time.Minute {
			t.Errorf("idle timeout with a 30m route timeout = %v, want 30m: a shorter idle timer would reset the stream first", got)
		}
	})

	t.Run("IdleTimeoutKeepsEnvoyDefaultWhenRouteTimeoutIsShorter", func(t *testing.T) {
		for _, d := range []time.Duration{defaultRouteTimeout, envoyDefaultStreamIdleTimeout} {
			x := NewXdsServer(0)
			x.SetRouteTimeout(d)
			if got := idleTimeout(t, x); got != envoyDefaultStreamIdleTimeout {
				t.Errorf("idle timeout with a %v route timeout = %v, want %v (unchanged from Envoy's default)", d, got, envoyDefaultStreamIdleTimeout)
			}
		}
	})
}

// TestXdsServer_BuildOriginalDstCluster_UsesMetadataKey covers the fix for a
// header-mutation-only LB config: a header only works for HTTP traffic, so
// the ORIGINAL_DST cluster must resolve its destination from
// ingress.OriginalDstMetadataKey/ingress.OriginalDstAddressKey dynamic
// metadata instead (see buildOriginalDstCluster and
// ingress.Handler.HandleRequestHeaders).
func TestXdsServer_BuildOriginalDstCluster_UsesMetadataKey(t *testing.T) {
	x := NewXdsServer(18000)
	lbConfig := x.buildOriginalDstCluster().GetLbConfig().(*clusterv3.Cluster_OriginalDstLbConfig_).OriginalDstLbConfig
	if lbConfig.GetUseHttpHeader() {
		t.Error("UseHttpHeader must not be set: it only applies to HTTP traffic, and dynamic metadata is the mechanism ext_proc uses instead")
	}
	key := lbConfig.GetMetadataKey()
	if key.GetKey() != ingress.OriginalDstMetadataKey {
		t.Errorf("Expected MetadataKey.Key %q, got %q", ingress.OriginalDstMetadataKey, key.GetKey())
	}
	path := key.GetPath()
	if len(path) != 1 || path[0].GetKey() != ingress.OriginalDstAddressKey {
		t.Errorf("Expected MetadataKey.Path [%q], got %v", ingress.OriginalDstAddressKey, path)
	}
}

// TestXdsServer_BuildRoutes_DerivesTargetPortHeader covers the fix for atunnel
// needing the target port as a real header (it can't read Envoy's dynamic
// metadata directly): rather than ext_proc building that header mutation
// itself, the route derives it declaratively from the same
// ingress.OriginalDstMetadataKey/ingress.OriginalDstPortKey metadata ext_proc
// already writes for the cluster's own MetadataKey, via a
// %DYNAMIC_METADATA(...)% command operator.
func TestXdsServer_BuildRoutes_DerivesTargetPortHeader(t *testing.T) {
	x := NewXdsServer(18000)
	route := x.buildRoutes().GetVirtualHosts()[0].GetRoutes()[0]

	headers := route.GetRequestHeadersToAdd()
	if len(headers) != 1 {
		t.Fatalf("Expected exactly 1 request header to add, got %d: %v", len(headers), headers)
	}
	h := headers[0]
	if got, want := h.GetHeader().GetKey(), atunnel.TargetPortHeader; got != want {
		t.Errorf("Expected header key %q, got %q", want, got)
	}
	wantValue := "%DYNAMIC_METADATA(" + ingress.OriginalDstMetadataKey + ":" + ingress.OriginalDstPortKey + ")%"
	if got := h.GetHeader().GetValue(); got != wantValue {
		t.Errorf("Expected header value %q, got %q", wantValue, got)
	}
	if got, want := h.GetAppendAction(), corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD; got != want {
		t.Errorf("Expected append action %s, got %s", want, got)
	}
}

// TestBuildConnectRoutes_DisablesTimeout covers a real bug: unlike a
// WebSocket upgrade, Envoy never disables a route's timeout once a CONNECT
// tunnel is established, so an unset Timeout here would fall back to Envoy's
// global default of 15s and silently kill every CONNECT tunnel through this
// router after 15 seconds regardless of activity.
func TestBuildConnectRoutes_DisablesTimeout(t *testing.T) {
	route := buildConnectRoutes().GetVirtualHosts()[0].GetRoutes()[0]
	timeout := route.GetRoute().GetTimeout()
	if timeout == nil {
		t.Fatal("Expected an explicit Timeout, got nil (falls back to Envoy's 15s default)")
	}
	if timeout.AsDuration() != 0 {
		t.Errorf("Expected Timeout 0 (disabled), got %s", timeout.AsDuration())
	}
}

func TestXdsServer_SetOtlpCollector(t *testing.T) {
	// --otlp-collector-address defaults to OTEL_EXPORTER_OTLP_ENDPOINT, so the
	// URL forms that variable carries have to reduce to the bare host and port
	// an xDS SocketAddress accepts.
	tests := []struct {
		name     string
		addr     string
		wantHost string
		wantPort uint32
	}{
		{"HostPort", "collector.otel-system.svc:4317", "collector.otel-system.svc", 4317},
		{"HostOnlyDefaultsPort", "collector.otel-system.svc", "collector.otel-system.svc", 4317},
		{"HttpURL", "http://collector.otel-system.svc:4317", "collector.otel-system.svc", 4317},
		{"HttpURLNoPort", "http://collector.otel-system.svc", "collector.otel-system.svc", 4317},
		{"HttpURLTrailingSlash", "http://collector.otel-system.svc:4317/", "collector.otel-system.svc", 4317},
		{"HttpURLWithPath", "http://collector.otel-system.svc:4317/v1/traces", "collector.otel-system.svc", 4317},
		{"NonDefaultPort", "http://collector.otel-system.svc:14317", "collector.otel-system.svc", 14317},
		{"IPv6", "[::1]:4317", "::1", 4317},
		{"IPv6URL", "http://[::1]:4317", "::1", 4317},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			x := NewXdsServer(0)
			if err := x.SetOtlpCollector(tc.addr); err != nil {
				t.Fatalf("SetOtlpCollector(%q) failed: %v", tc.addr, err)
			}
			if x.otlpHost != tc.wantHost || x.otlpPort != tc.wantPort {
				t.Errorf("SetOtlpCollector(%q) = %q:%d, want %q:%d", tc.addr, x.otlpHost, x.otlpPort, tc.wantHost, tc.wantPort)
			}

			// The address only matters insofar as it reaches Envoy: it must
			// land in the tracer cluster's socket address, unaltered.
			sock := x.buildOtlpCollectorCluster().GetLoadAssignment().GetEndpoints()[0].GetLbEndpoints()[0].GetEndpoint().GetAddress().GetSocketAddress()
			if sock.GetAddress() != tc.wantHost || sock.GetPortValue() != tc.wantPort {
				t.Errorf("tracer cluster endpoint = %q:%d, want %q:%d", sock.GetAddress(), sock.GetPortValue(), tc.wantHost, tc.wantPort)
			}
		})
	}
}

func TestXdsServer_SetOtlpCollector_Rejects(t *testing.T) {
	// An endpoint Envoy cannot use has to be reported rather than silently
	// accepted: https downgraded to the plaintext tracer cluster would leak
	// spans, and a garbage port would yield a cluster that never connects.
	// Reporting it is as far as this layer goes — setOtlpCollector turns the
	// error into a warning and runs without Envoy tracing, never a startup
	// failure. See TestSetOtlpCollector.
	for _, tc := range []struct {
		name string
		addr string
	}{
		{"Https", "https://collector.otel-system.svc:4317"},
		{"UnknownScheme", "grpc://collector.otel-system.svc:4317"},
		{"NoHost", "http://:4317"},
		{"NonNumericPort", "collector.otel-system.svc:grpc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := NewXdsServer(0).SetOtlpCollector(tc.addr); err == nil {
				t.Errorf("SetOtlpCollector(%q) succeeded, want error", tc.addr)
			}
		})
	}
}

func TestXdsServer_SetOtlpCollector_EmptyDisablesTracing(t *testing.T) {
	// Empty has to stay a working off switch: the router's own spans keep
	// flowing via OTEL_EXPORTER_OTLP_ENDPOINT, but Envoy emits none.
	x := NewXdsServer(0)
	if err := x.SetOtlpCollector(""); err != nil {
		t.Fatalf("SetOtlpCollector(\"\") failed: %v", err)
	}
	if tr := x.buildTracing(); tr != nil {
		t.Errorf("buildTracing() = %v, want nil when no collector is configured", tr)
	}
	if err := x.UpdateSnapshot(); err != nil {
		t.Fatalf("UpdateSnapshot failed: %v", err)
	}
	res, err := x.snapshot.GetSnapshot(NodeID)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if _, ok := res.GetResources(resourcev3.ClusterType)[OtlpClusterName]; ok {
		t.Errorf("snapshot contains cluster %q, want it omitted when tracing is disabled", OtlpClusterName)
	}
}

func TestXdsServer_BuildTracingRandomSamplingFromPolicy(t *testing.T) {
	const collectorAddr = "collector.otel-system.svc:4317"

	tests := []struct {
		name        string
		collector   string
		percent     float64
		setPercent  bool
		wantTracing bool
		wantPercent float64
	}{
		{
			name:        "percent mirrors the resolved policy",
			collector:   collectorAddr,
			percent:     1,
			setPercent:  true,
			wantTracing: true,
			wantPercent: 1,
		},
		{
			name:        "full sampling",
			collector:   collectorAddr,
			percent:     100,
			setPercent:  true,
			wantTracing: true,
			wantPercent: 100,
		},
		{
			// A caller that never threads in a policy must fail toward no
			// root sampling, not toward 100%.
			name:        "setter never called defaults to zero",
			collector:   collectorAddr,
			wantTracing: true,
			wantPercent: 0,
		},
		{
			name:       "no collector yields no tracing block",
			percent:    100,
			setPercent: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := NewXdsServer(0)
			if err := x.SetOtlpCollector(tt.collector); err != nil {
				t.Fatalf("SetOtlpCollector(%q) failed: %v", tt.collector, err)
			}
			if tt.setPercent {
				x.SetTraceRootSamplingPercent(tt.percent)
			}
			tr := x.buildTracing()
			if (tr != nil) != tt.wantTracing {
				t.Fatalf("buildTracing() = %v, want tracing block: %v", tr, tt.wantTracing)
			}
			if !tt.wantTracing {
				return
			}
			if got := tr.GetRandomSampling().GetValue(); got != tt.wantPercent {
				t.Errorf("RandomSampling = %v, want %v", got, tt.wantPercent)
			}
		})
	}
}
