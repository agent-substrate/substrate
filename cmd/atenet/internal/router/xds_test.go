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
	dfpclusterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/clusters/dynamic_forward_proxy/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	httpv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	secretgrpc "github.com/envoyproxy/go-control-plane/envoy/service/secret/v3"
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

// TestDynamicForwardProxyCluster_EnvoyAcceptsHttpProtocolOptions guards a
// coupling that is invisible on the Go side but fatal at runtime.
//
// Envoy refuses a dynamic_forward_proxy cluster that carries
// HttpProtocolOptions unless the cluster config either turns on both auto_sni
// and auto_san_validation or sets allow_insecure_cluster_options. A snapshot
// that breaks the rule is a perfectly well-formed proto and passes
// Consistent(), so nothing here fails: the only symptom is Envoy NACKing every
// CDS push and the cluster silently missing from its config dump, which reads
// as "all actor traffic 503s" rather than as a config error.
func TestDynamicForwardProxyCluster_EnvoyAcceptsHttpProtocolOptions(t *testing.T) {
	cluster := NewXdsServer(18000).buildDynamicForwardProxyCluster()

	var clusterConfig dfpclusterv3.ClusterConfig
	if err := cluster.GetClusterType().GetTypedConfig().UnmarshalTo(&clusterConfig); err != nil {
		t.Fatalf("Failed to unmarshal dynamic forward proxy cluster config: %v", err)
	}

	if !clusterConfig.GetAllowInsecureClusterOptions() {
		t.Errorf("Cluster carries %s but neither allows insecure cluster options nor "+
			"enables auto_sni and auto_san_validation; Envoy will reject every CDS push",
			httpProtocolOptionsName)
	}
}

// TestDynamicForwardProxyCluster_DisablesConnectionReuse pins the fix for the
// 503 storm seen under actor churn. Worker pod IPs are stable while the actor
// sandbox behind them is destroyed on every Suspend, so a pooled connection
// outlives the actor that owned it.
func TestDynamicForwardProxyCluster_DisablesConnectionReuse(t *testing.T) {
	cluster := NewXdsServer(18000).buildDynamicForwardProxyCluster()

	raw, ok := cluster.GetTypedExtensionProtocolOptions()[httpProtocolOptionsName]
	if !ok {
		t.Fatalf("Cluster is missing %s", httpProtocolOptionsName)
	}

	var opts httpv3.HttpProtocolOptions
	if err := raw.UnmarshalTo(&opts); err != nil {
		t.Fatalf("Failed to unmarshal HttpProtocolOptions: %v", err)
	}

	if got := opts.GetCommonHttpProtocolOptions().GetMaxRequestsPerConnection().GetValue(); got != 1 {
		t.Errorf("Expected max_requests_per_connection 1, got %d", got)
	}
}

func TestXdsServer_ExtProcCircuitBreaker(t *testing.T) {
	t.Run("DefaultCoversLotPlusHeadroom", func(t *testing.T) {
		x := NewXdsServer(0)
		got := x.buildCluster().GetCircuitBreakers().GetThresholds()[0].GetMaxRequests().GetValue()
		if got != uint32(defaultExtProcMaxRequests) {
			t.Errorf("default max_requests = %d, want %d", got, defaultExtProcMaxRequests)
		}
		if got < uint32(defaultParkedRequestMax) {
			t.Errorf("default breaker (%d) below the default lot (%d): a full lot would be truncated by Envoy", got, defaultParkedRequestMax)
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
