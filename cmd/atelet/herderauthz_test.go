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
	"errors"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/substratex509"
)

func allowAll(tls.ConnectionState) error { return nil }

// testPodCA is a throwaway stand-in for the podidentity signer: it issues
// leaf certificates shaped like the ones the signer mints (SPIFFE URI SAN,
// clientAuth+serverAuth EKU, PodIdentity extension).
type testPodCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestPodCA(t *testing.T) *testPodCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test podidentity CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testPodCA{cert: cert, key: key}
}

// writeTrustBundle writes the CA certificate as a PEM trust bundle.
func (ca *testPodCA) writeTrustBundle(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "trust-bundle.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// issue mints a leaf for the given Pod identity and returns it both as a
// tls.Certificate and as a credential bundle file on disk.
func (ca *testPodCA) issue(t *testing.T, id *substratex509.PodIdentity) (tls.Certificate, string) {
	t.Helper()
	spiffeURI, err := url.Parse(installdefaults.SPIFFEID(id.Namespace, id.ServiceAccountName))
	if err != nil {
		t.Fatal(err)
	}
	return ca.issueWithURIs(t, id, []*url.URL{spiffeURI})
}

// issueWithURIs is issue with explicit URI SANs, for malformed-identity cases.
func (ca *testPodCA) issueWithURIs(t *testing.T, id *substratex509.PodIdentity, uris []*url.URL) (tls.Certificate, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: rand.Text()},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         uris,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	if err := substratex509.AddPodIdentityToCertificate(id, tmpl); err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	bundle := append(
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	path := filepath.Join(t.TempDir(), "credential-bundle.pem")
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, path
}

func podIdentity(namespace, serviceAccount, nodeName string) *substratex509.PodIdentity {
	return &substratex509.PodIdentity{
		Namespace:          namespace,
		ServiceAccountName: serviceAccount,
		ServiceAccountUID:  serviceAccount + "-uid",
		PodName:            serviceAccount + "-pod",
		PodUID:             serviceAccount + "-" + nodeName + "-pod-uid",
		NodeName:           nodeName,
		NodeUID:            nodeName + "-uid",
	}
}

// handshake runs a full mTLS handshake between a client presenting clientCert
// and a server using serverCfg, and returns the server-side handshake error.
// When the server accepts, it also fails the test unless the client saw a
// clean close, so a client-side failure cannot pass as acceptance.
func handshake(t *testing.T, serverCfg *tls.Config, clientCert tls.Certificate, roots *x509.CertPool) error {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		serverConn.Close()
		clientConn.Close()
	})
	_ = serverConn.SetDeadline(time.Now().Add(10 * time.Second))
	_ = clientConn.SetDeadline(time.Now().Add(10 * time.Second))

	clientErr := make(chan error, 1)
	go func() {
		c := tls.Client(clientConn, &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      roots,
			// The server cert has no DNS SAN, mirroring how ateapi dials atelet
			// by Pod IP; this test exercises server-side authorization only.
			InsecureSkipVerify: true,
		})
		err := c.Handshake()
		if err == nil {
			// In TLS 1.3 the client finishes before the server has checked the
			// client certificate; a read surfaces the server's verdict.
			_, err = c.Read(make([]byte, 1))
		}
		clientErr <- err
		c.Close()
	}()

	s := tls.Server(serverConn, serverCfg)
	err := s.Handshake()
	if err == nil {
		// Unblock the client's read so the goroutine exits.
		s.Close()
	}
	cerr := <-clientErr
	if err == nil && !errors.Is(cerr, io.EOF) {
		t.Errorf("server accepted the handshake but the client saw %v, want io.EOF", cerr)
	}
	return err
}

// wantHandshakeErr fails the test unless err is non-nil and mentions want,
// so a rejection for an unrelated reason (timeout, cipher mismatch) cannot
// pass as an authorization failure.
func wantHandshakeErr(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("handshake succeeded, want an error containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("handshake error = %v, want it to contain %q", err, want)
	}
}

func TestAteomHerderTLSAdmitsOnlyAteapi(t *testing.T) {
	ca := newTestPodCA(t)
	trustBundle := ca.writeTrustBundle(t)
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)

	_, serverBundle := ca.issue(t, podIdentity("ate-system", "atelet", "node-a"))
	ateapiID := spiffeid.RequireFromString("spiffe://cluster.local/ns/ate-system/sa/ate-api-server")
	cfg, err := ateletServerTLSConfig(serverBundle, trustBundle, verifyClientSPIFFEID(ateapiID))
	if err != nil {
		t.Fatalf("ateletServerTLSConfig() error = %v", err)
	}

	if err := handshake(t, cfg, mustIssue(t, ca, podIdentity("ate-system", "ate-api-server", "node-b")), roots); err != nil {
		t.Fatalf("ate-api-server handshake error = %v, want nil", err)
	}

	// Any pod can get a podidentity cert from the same CA; none of these may
	// drive actor lifecycles.
	const notAuthorized = "is not authorized"
	rejected := []struct {
		name   string
		client *substratex509.PodIdentity
	}{
		{name: "workload pod in another namespace", client: podIdentity("tenant", "default", "node-a")},
		{name: "ate-api-server SA name in another namespace", client: podIdentity("tenant", "ate-api-server", "node-a")},
		{name: "other ate-system component", client: podIdentity("ate-system", "atenet-router", "node-a")},
		{name: "atelet on another node", client: podIdentity("ate-system", "atelet", "node-b")},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			wantHandshakeErr(t, handshake(t, cfg, mustIssue(t, ca, tc.client), roots), notAuthorized)
		})
	}

	t.Run("no URI SAN", func(t *testing.T) {
		cert, _ := ca.issueWithURIs(t, podIdentity("ate-system", "ate-api-server", "node-a"), nil)
		wantHandshakeErr(t, handshake(t, cfg, cert, roots), "no valid SPIFFE ID")
	})
	t.Run("ate-api-server plus a second URI SAN", func(t *testing.T) {
		id := podIdentity("ate-system", "ate-api-server", "node-a")
		cert, _ := ca.issueWithURIs(t, id, []*url.URL{
			mustParseURL(t, installdefaults.SPIFFEID(id.Namespace, id.ServiceAccountName)),
			mustParseURL(t, "spiffe://cluster.local/ns/tenant/sa/default"),
		})
		wantHandshakeErr(t, handshake(t, cfg, cert, roots), "no valid SPIFFE ID")
	})
	t.Run("no client certificate", func(t *testing.T) {
		// RequireAndVerifyClientCert rejects before the SPIFFE check runs.
		wantHandshakeErr(t, handshake(t, cfg, tls.Certificate{}, roots), "client didn't provide a certificate")
	})
}

func mustIssue(t *testing.T, ca *testPodCA, id *substratex509.PodIdentity) tls.Certificate {
	t.Helper()
	cert, _ := ca.issue(t, id)
	return cert
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestAteomHerderTLSRejectsUntrustedCA(t *testing.T) {
	ca := newTestPodCA(t)
	rogue := newTestPodCA(t)
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)

	_, serverBundle := ca.issue(t, podIdentity("ate-system", "atelet", "node-a"))
	ateapiID := spiffeid.RequireFromString("spiffe://cluster.local/ns/ate-system/sa/ate-api-server")
	cfg, err := ateletServerTLSConfig(serverBundle, ca.writeTrustBundle(t), verifyClientSPIFFEID(ateapiID))
	if err != nil {
		t.Fatalf("ateletServerTLSConfig() error = %v", err)
	}
	// Right SPIFFE ID, wrong issuer: the chain check must still apply.
	forged, _ := rogue.issue(t, podIdentity("ate-system", "ate-api-server", "node-a"))
	wantHandshakeErr(t, handshake(t, cfg, forged, roots), "unknown authority")
}

// TestCredentialBrokerTLSEnforcesSameNode is a regression test: the same-node
// check used to be set on the outer tls.Config, which crypto/tls discards in
// favor of the GetConfigForClient result, so it never ran during a handshake.
func TestCredentialBrokerTLSEnforcesSameNode(t *testing.T) {
	ca := newTestPodCA(t)
	trustBundle := ca.writeTrustBundle(t)
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)

	ateletID := podIdentity("ate-system", "atelet", "node-a")
	_, serverBundle := ca.issue(t, ateletID)
	cfg, err := ateletServerTLSConfig(serverBundle, trustBundle, verifyClientOnSameNode(ateletID))
	if err != nil {
		t.Fatalf("ateletServerTLSConfig() error = %v", err)
	}

	sameNode, _ := ca.issue(t, podIdentity("workers", "default", "node-a"))
	if err := handshake(t, cfg, sameNode, roots); err != nil {
		t.Fatalf("same-node worker handshake error = %v, want nil", err)
	}
	otherNode, _ := ca.issue(t, podIdentity("workers", "default", "node-b"))
	wantHandshakeErr(t, handshake(t, cfg, otherNode, roots), "is not on node")
}

func TestAteletServerTLSConfigRequiresVerifier(t *testing.T) {
	ca := newTestPodCA(t)
	if _, err := ateletServerTLSConfig("/nonexistent-cred-bundle.pem", ca.writeTrustBundle(t), nil); err == nil {
		t.Fatal("ateletServerTLSConfig() with a nil verifier succeeded, want an error")
	}
}
