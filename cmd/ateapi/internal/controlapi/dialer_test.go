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

package controlapi

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

const (
	testAteletSPIFFEID = "spiffe://cluster.local/ns/ate-system/sa/atelet"
	testPodUID         = "5a2e1c9f-0b57-4a52-9f6e-2f6d3a1b8c4d"
)

// leafOpts controls the contents of a test atelet server leaf certificate.
type leafOpts struct {
	// podUID, if non-empty, is embedded in a PodIdentity extension.
	podUID string
	// spiffeID, if non-empty, is added as a URI SAN.
	spiffeID string
	// noServerAuth omits the serverAuth EKU.
	noServerAuth bool
}

// testCA is a self-signed certificate authority used to issue test certificates.
type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}
	return &testCA{cert: cert, key: key, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issueLeaf mints a server-auth leaf certificate signed by the CA, shaped by
// opts.
func (ca *testCA) issueLeaf(t *testing.T, opts leafOpts) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating leaf key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if opts.noServerAuth {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	if opts.spiffeID != "" {
		uri, err := url.Parse(opts.spiffeID)
		if err != nil {
			t.Fatalf("parsing SPIFFE ID %q: %v", opts.spiffeID, err)
		}
		template.URIs = []*url.URL{uri}
	}
	if opts.podUID != "" {
		// AddPodIdentityToCertificate requires all fields to be non-empty;
		// only PodUID matters to these tests.
		err := substratex509.AddPodIdentityToCertificate(&substratex509.PodIdentity{
			Namespace:          "ate-system",
			ServiceAccountName: "atelet",
			ServiceAccountUID:  "sa-uid",
			PodName:            "atelet-abc",
			PodUID:             opts.podUID,
			NodeName:           "node-1",
			NodeUID:            "node-uid",
		}, template)
		if err != nil {
			t.Fatalf("adding PodIdentity extension: %v", err)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("creating leaf certificate: %v", err)
	}
	cert, err := tls.X509KeyPair(
		append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), ca.certPEM...),
		mustMarshalPKCS8(t, key),
	)
	if err != nil {
		t.Fatalf("building key pair: %v", err)
	}
	return cert
}

func mustMarshalPKCS8(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS8 key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

// writeClientBundle writes a credbundle-shaped (PKCS8 key + cert chain) file
// for an arbitrary client identity; buildTLSConfig's own client certificate
// is irrelevant to these tests since atelet's dialer.go never verifies it.
func writeClientBundle(t *testing.T, dir string) string {
	t.Helper()
	ca := newTestCA(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating client key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("creating client certificate: %v", err)
	}
	path := filepath.Join(dir, "client.pem")
	bundle := append(mustMarshalPKCS8(t, key), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		t.Fatalf("writing client bundle: %v", err)
	}
	return path
}

func writeTrustBundle(t *testing.T, dir string, cas ...*testCA) string {
	t.Helper()
	var pemBytes []byte
	for _, ca := range cas {
		pemBytes = append(pemBytes, ca.certPEM...)
	}
	path := filepath.Join(dir, "trust.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("writing trust bundle: %v", err)
	}
	return path
}

// dialHandshake runs one TLS handshake with serverLeaf served over an
// in-memory pipe, returning each side's own error.
func dialHandshake(t *testing.T, clientConfig *tls.Config, serverLeaf tls.Certificate) (serverErr, clientErr error) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	deadline := time.Now().Add(5 * time.Second)
	_ = serverConn.SetDeadline(deadline)
	_ = clientConn.SetDeadline(deadline)
	serverTLS := tls.Server(serverConn, &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{serverLeaf},
	})
	clientTLS := tls.Client(clientConn, clientConfig)
	done := make(chan error, 1)
	go func() {
		err := serverTLS.Handshake()
		_ = serverConn.Close()
		done <- err
	}()
	clientErr = clientTLS.Handshake()
	_ = clientConn.Close()
	return <-done, clientErr
}

func TestBuildTLSConfig(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	otherCA := newTestCA(t)
	clientBundlePath := writeClientBundle(t, dir)
	trustBundlePath := writeTrustBundle(t, dir, ca)

	tlsConfig, err := buildTLSConfig(testAteletSPIFFEID, clientBundlePath, trustBundlePath, testPodUID)
	if err != nil {
		t.Fatalf("buildTLSConfig() error = %v", err)
	}

	tests := []struct {
		name string
		ca   *testCA
		opts leafOpts
		want bool // true if the handshake should succeed
	}{
		{"matching identity succeeds", ca, leafOpts{podUID: testPodUID, spiffeID: testAteletSPIFFEID}, true},
		{"mismatched pod UID fails", ca, leafOpts{podUID: "some-other-uid", spiffeID: testAteletSPIFFEID}, false},
		{"missing pod UID extension fails", ca, leafOpts{spiffeID: testAteletSPIFFEID}, false},
		{"cert from untrusted CA fails", otherCA, leafOpts{podUID: testPodUID, spiffeID: testAteletSPIFFEID}, false},
		{"wrong SPIFFE ID fails", ca, leafOpts{podUID: testPodUID, spiffeID: "spiffe://cluster.local/ns/other/sa/other"}, false},
		{"missing URI SAN fails", ca, leafOpts{podUID: testPodUID}, false},
		{"missing serverAuth EKU fails", ca, leafOpts{podUID: testPodUID, spiffeID: testAteletSPIFFEID, noServerAuth: true}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			leaf := tc.ca.issueLeaf(t, tc.opts)
			_, clientErr := dialHandshake(t, tlsConfig, leaf)
			if succeeded := clientErr == nil; succeeded != tc.want {
				t.Fatalf("client handshake error = %v, want success=%v", clientErr, tc.want)
			}
		})
	}
}

func TestVerifyAteletServerCertRejectsEmptyFields(t *testing.T) {
	if _, err := verifyAteletServerCert("", testPodUID); err == nil {
		t.Fatal("verifyAteletServerCert with no SPIFFE ID succeeded, want error")
	}
	if _, err := verifyAteletServerCert(testAteletSPIFFEID, ""); err == nil {
		t.Fatal("verifyAteletServerCert with no pod UID succeeded, want error")
	}
}

// dialerWithAtelets builds an AteletDialer over the given atelet pods, dialing
// with insecure test credentials.
func dialerWithAtelets(t *testing.T, pods ...*corev1.Pod) *AteletDialer {
	t.Helper()
	return NewAteletDialer(newTestAteletIndexer(t, pods...), installdefaults.SystemNamespace, "", "",
		WithDialCredentials(func(string) (credentials.TransportCredentials, error) {
			return insecure.NewCredentials(), nil
		}))
}

func TestDialForAteletOnNodeTarget(t *testing.T) {
	tests := []struct {
		name       string
		ateletIP   string
		wantTarget string
	}{
		{
			name:       "IPv4 atelet",
			ateletIP:   "10.244.1.7",
			wantTarget: "10.244.1.7:8085",
		},
		{
			name:       "IPv6 atelet is bracketed",
			ateletIP:   "fd00:10:244::7",
			wantTarget: "[fd00:10:244::7]:8085",
		},
		{
			name:       "IPv6 loopback is bracketed",
			ateletIP:   "::1",
			wantTarget: "[::1]:8085",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ateletPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: installdefaults.SystemNamespace, Name: "atelet-abc", UID: "atelet-uid"},
				Spec:       corev1.PodSpec{NodeName: "node-1"},
				Status:     corev1.PodStatus{PodIPs: []corev1.PodIP{{IP: tc.ateletIP}}},
			}

			d := dialerWithAtelets(t, ateletPod)
			conn, err := d.DialForAteletOnNode("node-1")
			if err != nil {
				t.Fatalf("DialForAteletOnNode returned error: %v", err)
			}
			t.Cleanup(func() { conn.Close() })

			if got := conn.Target(); got != tc.wantTarget {
				t.Errorf("dial target = %q, want %q", got, tc.wantTarget)
			}
		})
	}
}

func TestDialForAteletOnNodeNoIPs(t *testing.T) {
	ateletPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: installdefaults.SystemNamespace, Name: "atelet-abc", UID: "atelet-uid"},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	}
	d := dialerWithAtelets(t, ateletPod)
	if _, err := d.DialForAteletOnNode("node-1"); err == nil {
		t.Fatal("DialForAteletOnNode succeeded, want error for atelet with no IPs")
	}
}

// newTestAteletIndexer builds an indexer with the production byNode index
// shape, holding the given atelet pods.
func newTestAteletIndexer(t *testing.T, pods ...*corev1.Pod) cache.Indexer {
	t.Helper()
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNode: func(obj any) ([]string, error) {
			return []string{obj.(*corev1.Pod).Spec.NodeName}, nil
		},
	})
	for _, p := range pods {
		if err := idx.Add(p); err != nil {
			t.Fatalf("adding pod to indexer: %v", err)
		}
	}
	return idx
}

func TestDialForAteletOnNode(t *testing.T) {
	ateletPod := func(name, uid, node, ip string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: name, UID: types.UID(uid)},
			Spec:       corev1.PodSpec{NodeName: node},
			Status:     corev1.PodStatus{PodIPs: []corev1.PodIP{{IP: ip}}},
		}
	}

	t.Run("no atelet on node", func(t *testing.T) {
		d := NewAteletDialer(newTestAteletIndexer(t), installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "", "")
		if _, err := d.DialForAteletOnNode("node1"); !errors.Is(err, ErrNoAteletOnNode) {
			t.Fatalf("DialForAteletOnNode = %v, want ErrNoAteletOnNode", err)
		}
	})

	t.Run("more than one atelet on node", func(t *testing.T) {
		d := NewAteletDialer(newTestAteletIndexer(t,
			ateletPod("atelet-1", "uid-1", "node1", "10.0.0.1"),
			ateletPod("atelet-2", "uid-2", "node1", "10.0.0.2"),
		), installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "", "")
		_, err := d.DialForAteletOnNode("node1")
		if err == nil || errors.Is(err, ErrNoAteletOnNode) {
			t.Fatalf("DialForAteletOnNode = %v, want a non-ErrNoAteletOnNode error", err)
		}
	})

	t.Run("dials and caches the node's atelet", func(t *testing.T) {
		d := NewAteletDialer(newTestAteletIndexer(t,
			ateletPod("atelet-1", "uid-1", "node1", "10.0.0.1"),
		), installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "", "")
		var credsUID string
		d.dialCredentials = func(expectedPodUID string) (credentials.TransportCredentials, error) {
			credsUID = expectedPodUID
			return insecure.NewCredentials(), nil
		}

		conn, err := d.DialForAteletOnNode("node1")
		if err != nil {
			t.Fatalf("DialForAteletOnNode: %v", err)
		}
		if credsUID != "uid-1" {
			t.Errorf("credentials pinned to pod UID %q, want %q", credsUID, "uid-1")
		}
		again, err := d.DialForAteletOnNode("node1")
		if err != nil {
			t.Fatalf("DialForAteletOnNode (cached): %v", err)
		}
		if again != conn {
			t.Error("second DialForAteletOnNode returned a different connection, want the cached one")
		}
	})

	t.Run("redials when the atelet pod's IP changes", func(t *testing.T) {
		idx := newTestAteletIndexer(t, ateletPod("atelet-1", "uid-1", "node1", "10.0.0.1"))
		d := NewAteletDialer(idx, installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "", "",
			WithDialCredentials(func(string) (credentials.TransportCredentials, error) {
				return insecure.NewCredentials(), nil
			}))

		old, err := d.DialForAteletOnNode("node1")
		if err != nil {
			t.Fatalf("DialForAteletOnNode: %v", err)
		}

		// Same pod UID, new IP, as after a node restart.
		if err := idx.Update(ateletPod("atelet-1", "uid-1", "node1", "10.0.0.9")); err != nil {
			t.Fatalf("updating pod in indexer: %v", err)
		}
		fresh, err := d.DialForAteletOnNode("node1")
		if err != nil {
			t.Fatalf("DialForAteletOnNode after IP change: %v", err)
		}
		t.Cleanup(func() { fresh.Close() })

		if fresh == old {
			t.Fatal("DialForAteletOnNode returned the stale connection after the IP changed")
		}
		if got, want := fresh.Target(), "10.0.0.9:8085"; got != want {
			t.Errorf("dial target = %q, want %q", got, want)
		}
		if got := old.GetState(); got != connectivity.Shutdown {
			t.Errorf("stale conn state = %v, want %v", got, connectivity.Shutdown)
		}
		if d.ateletConns.Len() != 1 {
			t.Errorf("cache holds %d conns, want 1", d.ateletConns.Len())
		}
		if again, err := d.DialForAteletOnNode("node1"); err != nil || again != fresh {
			t.Errorf("DialForAteletOnNode = %v, %v, want the new cached connection", again, err)
		}
	})

	t.Run("concurrent callers share one conn after the IP changes", func(t *testing.T) {
		idx := newTestAteletIndexer(t, ateletPod("atelet-1", "uid-1", "node1", "10.0.0.1"))
		d := NewAteletDialer(idx, installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "", "",
			WithDialCredentials(func(string) (credentials.TransportCredentials, error) {
				return insecure.NewCredentials(), nil
			}))
		if _, err := d.DialForAteletOnNode("node1"); err != nil {
			t.Fatalf("DialForAteletOnNode: %v", err)
		}
		if err := idx.Update(ateletPod("atelet-1", "uid-1", "node1", "10.0.0.9")); err != nil {
			t.Fatalf("updating pod in indexer: %v", err)
		}

		const callers = 32
		conns := make([]*grpc.ClientConn, callers)
		var wg sync.WaitGroup
		for i := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := d.DialForAteletOnNode("node1")
				if err != nil {
					t.Errorf("DialForAteletOnNode: %v", err)
				}
				conns[i] = conn
			}()
		}
		wg.Wait()

		for i, conn := range conns {
			if conn != conns[0] {
				t.Fatalf("caller %d got a different conn than caller 0", i)
			}
		}
		if got := conns[0].GetState(); got == connectivity.Shutdown {
			t.Error("shared conn was closed by a concurrent caller")
		}
		if d.ateletConns.Len() != 1 {
			t.Errorf("cache holds %d conns, want 1", d.ateletConns.Len())
		}
	})

	t.Run("closes conns evicted from the cache", func(t *testing.T) {
		d := NewAteletDialer(newTestAteletIndexer(t,
			ateletPod("atelet-1", "uid-1", "node1", "10.0.0.1"),
			ateletPod("atelet-2", "uid-2", "node2", "10.0.0.2"),
		), installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "", "", WithDialCredentials(func(string) (credentials.TransportCredentials, error) {
			return insecure.NewCredentials(), nil
		}))
		d.ateletConns = newAteletConnCache(1)

		first, err := d.DialForAteletOnNode("node1")
		if err != nil {
			t.Fatalf("DialForAteletOnNode(node1): %v", err)
		}
		if _, err := d.DialForAteletOnNode("node2"); err != nil {
			t.Fatalf("DialForAteletOnNode(node2): %v", err)
		}

		if got := first.GetState(); got != connectivity.Shutdown {
			t.Errorf("evicted conn state = %v, want %v (closed on eviction)", got, connectivity.Shutdown)
		}
	})
}
