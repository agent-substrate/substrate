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

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	gatewaySpiffe = "spiffe://cluster.local/ns/ate-system/sa/atenet-egress"
	otherSpiffe   = "spiffe://cluster.local/ns/default/sa/some-other-pod"
)

func TestGatewayTLSConfig(t *testing.T) {
	manifest, err := os.ReadFile("atenet-egress-extproc.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"/run/servicedns.podcert.ate.dev",
		"/run/podidentity.podcert.ate.dev",
	} {
		if !strings.Contains(string(manifest), want) {
			t.Errorf("manifest does not mount %s, which gatewayTLSConfig reads", want)
		}
	}

	// Absent the mounts, which is the case on any developer machine.
	if _, err := gatewayTLSConfig(); err == nil {
		t.Log("gatewayTLSConfig() succeeded; this machine has the pod-certificate mounts")
	}
}

// The handshake is the contract with Envoy, so it is worth exercising rather
// than asserting on the fields of a tls.Config.
func TestServerTLSHandshake(t *testing.T) {
	ca := newCA(t)
	dir := t.TempDir()

	serverBundle := filepath.Join(dir, "serving.pem")
	writeFile(t, serverBundle, ca.issue(t, "atenet-egress-extproc.ate-system.svc", ""))
	caFile := filepath.Join(dir, "trust.pem")
	writeFile(t, caFile, ca.certPEM())

	otherCA := newCA(t)

	tests := []struct {
		name    string
		allowed []string
		// clientCert is the bundle the client presents; empty means none.
		clientCert []byte
		wantErr    bool
	}{
		{
			name:       "gateway identity",
			allowed:    []string{gatewaySpiffe},
			clientCert: ca.issue(t, "", gatewaySpiffe),
		},
		{
			name:       "any verified client when no allowlist",
			clientCert: ca.issue(t, "", otherSpiffe),
		},
		{
			// Chains fine, wrong workload. This is the case a bare chain check
			// would pass.
			name:       "verified client outside the allowlist",
			allowed:    []string{gatewaySpiffe},
			clientCert: ca.issue(t, "", otherSpiffe),
			wantErr:    true,
		},
		{
			name:       "right SPIFFE ID from the wrong CA",
			allowed:    []string{gatewaySpiffe},
			clientCert: otherCA.issue(t, "", gatewaySpiffe),
			wantErr:    true,
		},
		{
			name:    "no client certificate",
			allowed: []string{gatewaySpiffe},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, err := serverTLSConfig(serverBundle, caFile, test.allowed)
			if err != nil {
				t.Fatalf("serverTLSConfig() error = %v", err)
			}

			clientConfig := &tls.Config{
				RootCAs:    ca.pool(t),
				ServerName: "atenet-egress-extproc.ate-system.svc",
				MinVersion: tls.VersionTLS13,
			}
			if test.clientCert != nil {
				clientConfig.Certificates = []tls.Certificate{parseBundle(t, test.clientCert)}
			}

			err = handshake(t, config, clientConfig)
			if test.wantErr && err == nil {
				t.Fatal("handshake succeeded, want failure")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("handshake failed: %v", err)
			}
		})
	}
}

// A rotation must be picked up without a restart, which is the entire reason
// the config resolves its material per handshake.
func TestServerTLSPicksUpRotation(t *testing.T) {
	ca := newCA(t)
	dir := t.TempDir()
	serverBundle := filepath.Join(dir, "serving.pem")
	writeFile(t, serverBundle, ca.issue(t, "atenet-egress-extproc.ate-system.svc", ""))
	caFile := filepath.Join(dir, "trust.pem")
	writeFile(t, caFile, ca.certPEM())

	config, err := serverTLSConfig(serverBundle, caFile, nil)
	if err != nil {
		t.Fatalf("serverTLSConfig() error = %v", err)
	}

	// A client from a CA the server does not yet trust.
	rotated := newCA(t)
	clientConfig := &tls.Config{
		RootCAs:      ca.pool(t),
		ServerName:   "atenet-egress-extproc.ate-system.svc",
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{parseBundle(t, rotated.issue(t, "", gatewaySpiffe))},
	}
	if err := handshake(t, config, clientConfig); err == nil {
		t.Fatal("handshake succeeded before the CA was trusted")
	}

	// Rotate the trust bundle to include it, without rebuilding the config.
	writeFile(t, caFile, append(ca.certPEM(), rotated.certPEM()...))
	if err := handshake(t, config, clientConfig); err != nil {
		t.Fatalf("handshake failed after trust bundle rotation: %v", err)
	}
}

func TestServerTLSConfigRejectsUnreadableMaterial(t *testing.T) {
	ca := newCA(t)
	dir := t.TempDir()
	serverBundle := filepath.Join(dir, "serving.pem")
	writeFile(t, serverBundle, ca.issue(t, "svc", ""))

	if _, err := serverTLSConfig(serverBundle, filepath.Join(dir, "does-not-exist.pem"), nil); err == nil {
		t.Error("serverTLSConfig() = nil error for a missing trust bundle, want failure at startup")
	}

	if _, err := serverTLSConfig(filepath.Join(dir, "does-not-exist.pem"), serverBundle, nil); err == nil {
		t.Error("serverTLSConfig() = nil error for a missing serving bundle, want failure at startup")
	}
}

// handshake connects once over loopback and reports whether the client got a
// working session.
//
// It exchanges a byte rather than stopping at Handshake, because under TLS 1.3
// the client finishes its side before the server has looked at the client
// certificate: a rejected peer sees Handshake return nil and only learns it was
// refused when the alert arrives on the first read. Asserting on Handshake
// alone would report every rejection below as a success.
func handshake(t *testing.T, serverConfig, clientConfig *tls.Config) error {
	t.Helper()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1)
		if _, err := conn.Read(buf); err != nil {
			return
		}
		_, _ = conn.Write(buf)
	}()

	client, err := tls.Dial("tcp", listener.Addr().String(), clientConfig)
	if err != nil {
		return err
	}
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(10 * time.Second))

	if _, err := client.Write([]byte{1}); err != nil {
		return err
	}
	_, err = client.Read(make([]byte, 1))
	return err
}

// testCA issues the credential bundles a pod certificate mount would contain.
type testCA struct {
	cert *x509.Certificate
	der  []byte
	key  *ecdsa.PrivateKey
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, der: der, key: key}
}

// issue returns a credential bundle: a PRIVATE KEY block followed by the leaf,
// which is the shape parseCredentialBundle expects.
func (c *testCA) issue(t *testing.T, dnsName, spiffeID string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	if dnsName != "" {
		template.DNSNames = []string{dnsName}
	}
	if spiffeID != "" {
		uri, err := url.Parse(spiffeID)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = []*url.URL{uri}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	bundle := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	return bundle
}

func (c *testCA) certPEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.der})
}

func (c *testCA) pool(t *testing.T) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(c.certPEM()) {
		t.Fatal("appending test CA")
	}
	return pool
}

func parseBundle(t *testing.T, bundle []byte) tls.Certificate {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "bundle.pem")
	writeFile(t, path, bundle)
	// Round-tripping through the same parser the server uses keeps the test
	// honest about the file shape rather than about crypto/tls.
	cert, err := parseCredentialBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	return *cert
}

func writeFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}
