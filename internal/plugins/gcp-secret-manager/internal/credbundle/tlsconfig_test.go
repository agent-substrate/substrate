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

package credbundle

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
	"testing"
	"time"
)

const testCallerID = "spiffe://cluster.local/ns/ate-system/sa/caller"

func TestPrepareServerTLSConfigRequiresGetCertificate(t *testing.T) {
	if _, err := PrepareServerTLSConfig(ServerConfig{}); err == nil {
		t.Fatalf("PrepareServerTLSConfig() error = nil, want an error")
	}
}

func TestPrepareServerTLSConfigRejectsUnreadableClientCA(t *testing.T) {
	cfg := ServerConfig{
		GetCertificate: Loader(writeBundle(t, makeBundle(t, 1))),
		ClientCAPath:   filepath.Join(t.TempDir(), "absent.pem"),
		ClientAuth:     tls.RequireAndVerifyClientCert,
	}
	if _, err := PrepareServerTLSConfig(cfg); err == nil {
		t.Fatalf("PrepareServerTLSConfig() error = nil, want a missing-file error")
	}
}

// A caller presenting a certificate that chains to the trust bundle and
// carries the expected SPIFFE URI SAN is accepted; a chain-verified caller
// with the wrong identity is rejected by VerifyPeer, and rotating the trust
// bundle on disk takes effect on the next handshake without rebuilding the
// *tls.Config.
func TestPrepareServerTLSConfigHandshakeAndCARotation(t *testing.T) {
	serverCA := newTestCA(t, "server-ca")
	callerCA1 := newTestCA(t, "caller-ca-1")
	callerCA2 := newTestCA(t, "caller-ca-2")

	serverBundle := writeCredBundle(t, serverCA.issue(t, certOpts{dnsNames: []string{"provider.test"}}))
	clientCAPath := filepath.Join(t.TempDir(), "client-ca.pem")
	writeFileWithMtime(t, clientCAPath, callerCA1.certPEM, time.Now())

	serverCfg, err := PrepareServerTLSConfig(ServerConfig{
		GetCertificate: Loader(serverBundle),
		ClientCAPath:   clientCAPath,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		VerifyPeer:     verifyPeerURI(testCallerID),
	})
	if err != nil {
		t.Fatalf("PrepareServerTLSConfig() error = %v", err)
	}
	roots := rootsOf(serverCA)

	callerClient := func(ca *testCA, uri string) *tls.Config {
		cert := ca.issue(t, certOpts{uris: []string{uri}})
		return &tls.Config{
			MinVersion: tls.VersionTLS13,
			RootCAs:    roots,
			ServerName: "provider.test",
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return &tls.Certificate{Certificate: [][]byte{cert.certDER}, PrivateKey: cert.key}, nil
			},
		}
	}

	if serverErr, clientErr := handshake(t, serverCfg, callerClient(callerCA1, testCallerID)); failed(serverErr, clientErr) {
		t.Fatalf("handshake with the expected caller identity failed: (%v, %v)", serverErr, clientErr)
	}
	if serverErr, _ := handshake(t, serverCfg, callerClient(callerCA1, "spiffe://cluster.local/ns/ate-system/sa/impostor")); serverErr == nil {
		t.Fatalf("handshake with the wrong caller identity succeeded, want VerifyPeer to refuse it")
	}
	if serverErr, clientErr := handshake(t, serverCfg, callerClient(callerCA2, testCallerID)); !failed(serverErr, clientErr) {
		t.Fatalf("handshake with an untrusted caller CA succeeded before rotation")
	}

	// Rotate the client trust bundle to CA2, bumping the mtime past coarse
	// filesystem timestamps.
	writeFileWithMtime(t, clientCAPath, callerCA2.certPEM, time.Now().Add(time.Second))

	if serverErr, clientErr := handshake(t, serverCfg, callerClient(callerCA2, testCallerID)); failed(serverErr, clientErr) {
		t.Fatalf("handshake with the rotated caller CA failed: (%v, %v)", serverErr, clientErr)
	}
	if serverErr, clientErr := handshake(t, serverCfg, callerClient(callerCA1, testCallerID)); !failed(serverErr, clientErr) {
		t.Fatalf("handshake with the pre-rotation caller CA succeeded after rotation")
	}
}

// verifyPeerURI returns a VerifyPeer callback requiring expected among the
// peer leaf certificate's URI SANs.
func verifyPeerURI(expected string) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		leaf := state.PeerCertificates[0]
		for _, u := range leaf.URIs {
			if u.String() == expected {
				return nil
			}
		}
		return errors.New("peer identity does not match expected " + expected)
	}
}

// handshake runs one client handshake against a listener serving serverCfg,
// returning each side's own Handshake error.
func handshake(t *testing.T, serverCfg, clientCfg *tls.Config) (serverErr, clientErr error) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		serverDone <- tls.Server(conn, serverCfg).Handshake()
	}()

	conn, clientErr := tls.Dial("tcp", lis.Addr().String(), clientCfg)
	if clientErr == nil {
		clientErr = conn.Handshake()
		conn.Close()
	}

	return <-serverDone, clientErr
}

func failed(serverErr, clientErr error) bool {
	return serverErr != nil || clientErr != nil
}

// testCA is a self-signed certificate authority used to issue test certificates.
type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	return &testCA{cert: cert, key: key, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

type certOpts struct {
	dnsNames []string
	uris     []string
}

// issued is a leaf certificate and its private key.
type issued struct {
	certDER []byte
	key     *ecdsa.PrivateKey
}

func (c *testCA) issue(t *testing.T, opts certOpts) issued {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	var uris []*url.URL
	for _, u := range opts.uris {
		parsed, err := url.Parse(u)
		if err != nil {
			t.Fatalf("parse URI SAN %q: %v", u, err)
		}
		uris = append(uris, parsed)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     opts.dnsNames,
		URIs:         uris,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatalf("create leaf certificate: %v", err)
	}
	return issued{certDER: der, key: key}
}

// writeCredBundle writes leaf as a credential bundle and returns its path.
func writeCredBundle(t *testing.T, leaf issued) string {
	t.Helper()
	keyDER, err := x509.MarshalPKCS8PrivateKey(leaf.key)
	if err != nil {
		t.Fatalf("marshal PKCS8 key: %v", err)
	}
	bundle := append(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})...,
	)
	return writeBundle(t, bundle)
}

func writeFileWithMtime(t *testing.T, path string, data []byte, mtime time.Time) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatalf("chtimes %s: %v", path, err)
	}
}

func rootsOf(cas ...*testCA) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, ca := range cas {
		pool.AddCert(ca.cert)
	}
	return pool
}
