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
	"strings"
	"testing"
	"time"
)

const (
	testServerID = "spiffe://cluster.local/ns/ate-system/sa/server"
	testClientID = "spiffe://cluster.local/ns/ate-system/sa/client"
)

func TestPrepareClientTLSConfigRequiresFields(t *testing.T) {
	trustPath := writeBundle(t, makeTrustBundle(t, 1))
	tests := []struct {
		name string
		cfg  ClientConfig
	}{
		{"no GetClientCertificate", ClientConfig{TrustBundlePath: trustPath}},
		{"no TrustBundlePath", ClientConfig{GetClientCertificate: ClientLoader(trustPath)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := PrepareClientTLSConfig(tc.cfg); err == nil {
				t.Fatalf("PrepareClientTLSConfig() error = nil, want an error")
			}
		})
	}
}

func TestPrepareClientTLSConfigRejectsUnreadableTrustBundle(t *testing.T) {
	cfg := ClientConfig{
		GetClientCertificate: ClientLoader(writeBundle(t, makeTrustBundle(t, 1))),
		TrustBundlePath:      filepath.Join(t.TempDir(), "absent.pem"),
	}
	if _, err := PrepareClientTLSConfig(cfg); err == nil {
		t.Fatalf("PrepareClientTLSConfig() error = nil, want a missing-file error")
	}
}

func TestPrepareServerTLSConfigRequiresGetCertificate(t *testing.T) {
	if _, err := PrepareServerTLSConfig(ServerConfig{}); err == nil {
		t.Fatalf("PrepareServerTLSConfig() error = nil, want an error")
	}
}

func TestPrepareServerTLSConfigRejectsUnreadableClientCA(t *testing.T) {
	cfg := ServerConfig{
		GetCertificate: Loader(writeBundle(t, makeTrustBundle(t, 1))),
		ClientCAPath:   filepath.Join(t.TempDir(), "absent.pem"),
		ClientAuth:     tls.RequireAndVerifyClientCert,
	}
	if _, err := PrepareServerTLSConfig(cfg); err == nil {
		t.Fatalf("PrepareServerTLSConfig() error = nil, want a missing-file error")
	}
}

// A mutual handshake succeeds when both sides present a certificate chaining
// to the other's trust bundle and carrying the expected SPIFFE URI SAN; an
// identity mismatch on either side fails VerifyPeer even though the chain
// itself verifies.
func TestPrepareClientAndServerMutualHandshake(t *testing.T) {
	serverCA := newTestCA(t, "server-ca")
	clientCA := newTestCA(t, "client-ca")

	serverBundle := writeCredBundle(t, serverCA.issue(t, certOpts{uris: []string{testServerID}}))
	clientBundle := writeCredBundle(t, clientCA.issue(t, certOpts{uris: []string{testClientID}}))
	serverTrust := writeBundle(t, clientCA.certPEM)
	clientTrust := writeBundle(t, serverCA.certPEM)

	serverCfg, err := PrepareServerTLSConfig(ServerConfig{
		GetCertificate: Loader(serverBundle),
		ClientCAPath:   serverTrust,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		VerifyPeer:     verifyPeerURI(testClientID),
		MinVersion:     tls.VersionTLS13,
	})
	if err != nil {
		t.Fatalf("PrepareServerTLSConfig() error = %v", err)
	}
	creds := serverCredentials(serverCfg)

	validClient := func() (*tls.Config, error) {
		return PrepareClientTLSConfig(ClientConfig{
			GetClientCertificate: ClientLoader(clientBundle),
			TrustBundlePath:      clientTrust,
			VerifyPeer:           verifyPeerURI(testServerID),
			MinVersion:           tls.VersionTLS13,
		})
	}
	clientCfg, err := validClient()
	if err != nil {
		t.Fatalf("PrepareClientTLSConfig() error = %v", err)
	}
	if serverErr, clientErr := handshake(t, creds, clientCfg); failed(serverErr, clientErr) {
		t.Fatalf("handshake() errors = (%v, %v), want success", serverErr, clientErr)
	}

	t.Run("wrong client identity rejected by server", func(t *testing.T) {
		impostorBundle := writeCredBundle(t, clientCA.issue(t, certOpts{uris: []string{"spiffe://cluster.local/ns/ate-system/sa/impostor"}}))
		cfg, err := PrepareClientTLSConfig(ClientConfig{
			GetClientCertificate: ClientLoader(impostorBundle),
			TrustBundlePath:      clientTrust,
			VerifyPeer:           verifyPeerURI(testServerID),
			MinVersion:           tls.VersionTLS13,
		})
		if err != nil {
			t.Fatalf("PrepareClientTLSConfig() error = %v", err)
		}
		// The server's own VerifyPeer rejects the impostor identity, so its
		// Handshake() returns that error directly.
		serverErr, _ := handshake(t, creds, cfg)
		if serverErr == nil || !strings.Contains(serverErr.Error(), "does not match") {
			t.Fatalf("server handshake error = %v, want the server's client-identity check to refuse it", serverErr)
		}
	})

	t.Run("wrong server identity rejected by client", func(t *testing.T) {
		otherServerBundle := writeCredBundle(t, serverCA.issue(t, certOpts{uris: []string{"spiffe://cluster.local/ns/ate-system/sa/other-server"}}))
		otherCfg, err := PrepareServerTLSConfig(ServerConfig{
			GetCertificate: Loader(otherServerBundle),
			ClientCAPath:   serverTrust,
			ClientAuth:     tls.RequireAndVerifyClientCert,
			VerifyPeer:     verifyPeerURI(testClientID),
			MinVersion:     tls.VersionTLS13,
		})
		if err != nil {
			t.Fatalf("PrepareServerTLSConfig() error = %v", err)
		}
		cfg, err := validClient()
		if err != nil {
			t.Fatalf("PrepareClientTLSConfig() error = %v", err)
		}
		// The client's own VerifyPeer rejects the impostor identity, so its
		// Handshake() returns that error directly; the server only sees the
		// resulting alert.
		_, clientErr := handshake(t, serverCredentials(otherCfg), cfg)
		if clientErr == nil || !strings.Contains(clientErr.Error(), "does not match") {
			t.Fatalf("client handshake error = %v, want the client's server-identity check to refuse it", clientErr)
		}
	})

	t.Run("untrusted client CA rejected", func(t *testing.T) {
		otherClientCA := newTestCA(t, "other-client-ca")
		cfg, err := PrepareClientTLSConfig(ClientConfig{
			GetClientCertificate: ClientLoader(writeCredBundle(t, otherClientCA.issue(t, certOpts{uris: []string{testClientID}}))),
			TrustBundlePath:      clientTrust,
			VerifyPeer:           verifyPeerURI(testServerID),
			MinVersion:           tls.VersionTLS13,
		})
		if err != nil {
			t.Fatalf("PrepareClientTLSConfig() error = %v", err)
		}
		if serverErr, clientErr := handshake(t, creds, cfg); !failed(serverErr, clientErr) {
			t.Fatalf("handshake() succeeded, want the untrusted client CA to be refused")
		}
	})
}

// Rotating either side's trust-bundle file on disk takes effect on the next
// handshake without rebuilding the *tls.Config, on both the client and the
// server.
func TestPrepareClientAndServerPickUpCARotation(t *testing.T) {
	serverCA := newTestCA(t, "server-ca")
	clientCA1 := newTestCA(t, "client-ca-1")
	clientCA2 := newTestCA(t, "client-ca-2")

	serverBundle := writeCredBundle(t, serverCA.issue(t, certOpts{uris: []string{testServerID}}))
	serverTrustPath := filepath.Join(t.TempDir(), "client-ca.pem")
	writeFileWithMtime(t, serverTrustPath, clientCA1.certPEM, time.Now())
	clientTrust := writeBundle(t, serverCA.certPEM)

	serverCfg, err := PrepareServerTLSConfig(ServerConfig{
		GetCertificate: Loader(serverBundle),
		ClientCAPath:   serverTrustPath,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		VerifyPeer:     verifyPeerURI(testClientID),
		MinVersion:     tls.VersionTLS13,
	})
	if err != nil {
		t.Fatalf("PrepareServerTLSConfig() error = %v", err)
	}
	creds := serverCredentials(serverCfg)

	clientFor := func(ca *testCA) *tls.Config {
		cfg, err := PrepareClientTLSConfig(ClientConfig{
			GetClientCertificate: ClientLoader(writeCredBundle(t, ca.issue(t, certOpts{uris: []string{testClientID}}))),
			TrustBundlePath:      clientTrust,
			VerifyPeer:           verifyPeerURI(testServerID),
			MinVersion:           tls.VersionTLS13,
		})
		if err != nil {
			t.Fatalf("PrepareClientTLSConfig() error = %v", err)
		}
		return cfg
	}

	if serverErr, clientErr := handshake(t, creds, clientFor(clientCA1)); failed(serverErr, clientErr) {
		t.Fatalf("handshake with CA1-signed client cert failed before rotation: (%v, %v)", serverErr, clientErr)
	}
	if serverErr, clientErr := handshake(t, creds, clientFor(clientCA2)); !failed(serverErr, clientErr) {
		t.Fatalf("handshake with CA2-signed client cert before rotation: want an untrusted-chain failure")
	}

	// Rotate the server's client-CA trust bundle to CA2, bumping the mtime
	// past coarse filesystem timestamps.
	writeFileWithMtime(t, serverTrustPath, clientCA2.certPEM, time.Now().Add(time.Second))

	if serverErr, clientErr := handshake(t, creds, clientFor(clientCA2)); failed(serverErr, clientErr) {
		t.Fatalf("handshake with CA2-signed client cert failed after rotation: (%v, %v)", serverErr, clientErr)
	}
	if serverErr, clientErr := handshake(t, creds, clientFor(clientCA1)); !failed(serverErr, clientErr) {
		t.Fatalf("handshake with CA1-signed client cert after rotation: want an untrusted-chain failure")
	}
}

func TestPrepareClientTLSConfigChecksServerName(t *testing.T) {
	serverCA := newTestCA(t, "server-ca")
	serverBundle := writeCredBundle(t, serverCA.issue(t, certOpts{dnsNames: []string{"expected.test"}}))
	clientTrust := writeBundle(t, serverCA.certPEM)
	clientCA := newTestCA(t, "client-ca")
	clientBundle := writeCredBundle(t, clientCA.issue(t, certOpts{}))
	serverTrust := writeBundle(t, clientCA.certPEM)

	serverCfg, err := PrepareServerTLSConfig(ServerConfig{
		GetCertificate: Loader(serverBundle),
		ClientCAPath:   serverTrust,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		MinVersion:     tls.VersionTLS13,
	})
	if err != nil {
		t.Fatalf("PrepareServerTLSConfig() error = %v", err)
	}
	creds := serverCredentials(serverCfg)

	clientWithName := func(name string) *tls.Config {
		cfg, err := PrepareClientTLSConfig(ClientConfig{
			GetClientCertificate: ClientLoader(clientBundle),
			TrustBundlePath:      clientTrust,
			ServerName:           name,
			MinVersion:           tls.VersionTLS13,
		})
		if err != nil {
			t.Fatalf("PrepareClientTLSConfig() error = %v", err)
		}
		return cfg
	}

	if serverErr, clientErr := handshake(t, creds, clientWithName("expected.test")); failed(serverErr, clientErr) {
		t.Fatalf("handshake with matching ServerName failed: (%v, %v)", serverErr, clientErr)
	}
	if serverErr, clientErr := handshake(t, creds, clientWithName("wrong.test")); !failed(serverErr, clientErr) {
		t.Fatalf("handshake with mismatched ServerName: want a hostname-verification failure")
	}
}

// With ClientCAPath empty, the server neither builds nor reloads a client
// trust pool: ClientAuth still applies, but with ClientCAs left nil, a
// presented client certificate chains against the platform root CAs instead
// of a pinned pool (crypto/tls's behavior, not something this helper adds).
func TestPrepareServerTLSConfigWithoutClientCA(t *testing.T) {
	serverCA := newTestCA(t, "server-ca")
	serverBundle := writeCredBundle(t, serverCA.issue(t, certOpts{uris: []string{testServerID}, dnsNames: []string{"server.test"}}))

	serverCfg, err := PrepareServerTLSConfig(ServerConfig{
		GetCertificate: Loader(serverBundle),
		ClientAuth:     tls.VerifyClientCertIfGiven,
	})
	if err != nil {
		t.Fatalf("PrepareServerTLSConfig() error = %v", err)
	}
	if serverCfg.GetConfigForClient != nil {
		t.Fatalf("PrepareServerTLSConfig() with no ClientCAPath set GetConfigForClient, want a plain Config")
	}
	creds := serverCredentials(serverCfg)

	// No client certificate at all.
	noCertClient := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    rootsOf(serverCA),
		ServerName: "server.test",
	}
	if serverErr, clientErr := handshake(t, creds, noCertClient); failed(serverErr, clientErr) {
		t.Fatalf("handshake with no client certificate failed: (%v, %v)", serverErr, clientErr)
	}

	// An unrelated, self-signed client certificate: with no ClientCAs pinned,
	// crypto/tls falls back to the platform root CAs to verify it, so a
	// self-signed test certificate is rejected — not because this helper
	// checks it against anything, but because nothing vouches for it.
	untrustedCA := newTestCA(t, "untrusted-client-ca")
	selfSigned := untrustedCA.issue(t, certOpts{})
	certClient := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    rootsOf(serverCA),
		ServerName: "server.test",
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &tls.Certificate{Certificate: [][]byte{selfSigned.certDER}, PrivateKey: selfSigned.key}, nil
		},
	}
	if serverErr, clientErr := handshake(t, creds, certClient); !failed(serverErr, clientErr) {
		t.Fatalf("handshake with a self-signed, unpinned client certificate succeeded, want it rejected by the platform root fallback")
	}
}

// verifyPeerURI returns a VerifyPeer callback requiring expected among the
// peer leaf certificate's URI SANs, mirroring the SPIFFE-identity checks the
// real call sites layer on top of chain verification.
func verifyPeerURI(expected string) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		leaf := state.PeerCertificates[0]
		for _, u := range leaf.URIs {
			if u.String() == expected {
				return nil
			}
		}
		return errors.New("peer identity " + leaf.URIs[0].String() + " does not match expected " + expected)
	}
}

// serverCredentials wraps cfg as a server-side tls.Config ready for
// handshake's listener, without pulling in a gRPC dependency just for tests.
func serverCredentials(cfg *tls.Config) *tls.Config { return cfg }

// handshake runs one client handshake against a listener serving serverCfg,
// returning each side's own Handshake error.
//
// Which side's error carries the useful text depends on which side detected
// the problem: a side that fails its own VerifyConnection (chain, DNS name,
// or VerifyPeer) returns that error directly, while the peer only sees the
// resulting alert ("remote error: tls: bad certificate"). Callers that assert
// on error text should check the side that actually performs that check;
// callers that only care whether the handshake failed can check either.
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
		tlsConn := tls.Server(conn, serverCfg)
		serverDone <- tlsConn.Handshake()
	}()

	clientCfg = clientCfg.Clone()
	if clientCfg.ServerName == "" {
		clientCfg.ServerName = "ignored"
	}
	conn, clientErr := tls.Dial("tcp", lis.Addr().String(), clientCfg)
	if clientErr == nil {
		clientErr = conn.Handshake()
		conn.Close()
	}

	return <-serverDone, clientErr
}

// failed reports whether a handshake's pair of errors means the handshake
// did not succeed.
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
	return &testCA{cert: cert, key: key, certPEM: encodeCertPEM(der)}
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
	bundle := append(encodeCertPEM(leaf.certDER), encodeKeyPEM(keyDER)...)
	path := filepath.Join(t.TempDir(), "bundle.pem")
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		t.Fatalf("write credential bundle: %v", err)
	}
	return path
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

func encodeCertPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func encodeKeyPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}
