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

package mtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io/fs"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
)

// testCaller is a caller identity shaped like the egress gateway's.
const testCaller = "spiffe://cluster.local/ns/ate-system/sa/atenet-egress"

func certWithURIs(t *testing.T, uris ...string) *x509.Certificate {
	t.Helper()
	cert := &x509.Certificate{}
	for _, u := range uris {
		parsed, err := url.Parse(u)
		if err != nil {
			t.Fatalf("parsing SAN %q: %v", u, err)
		}
		cert.URIs = append(cert.URIs, parsed)
	}
	return cert
}

func TestVerifyCallerSAN(t *testing.T) {
	tests := []struct {
		name    string
		state   tls.ConnectionState
		wantErr bool
	}{
		{
			name:  "matching SAN",
			state: tls.ConnectionState{PeerCertificates: []*x509.Certificate{certWithURIs(t, testCaller)}},
		},
		{
			name:  "matching SAN among several",
			state: tls.ConnectionState{PeerCertificates: []*x509.Certificate{certWithURIs(t, "spiffe://cluster.local/ns/other/sa/x", testCaller)}},
		},
		{
			name:    "wrong SAN",
			state:   tls.ConnectionState{PeerCertificates: []*x509.Certificate{certWithURIs(t, "spiffe://cluster.local/ns/ate-system/sa/impostor")}},
			wantErr: true,
		},
		{
			name:    "no URI SANs",
			state:   tls.ConnectionState{PeerCertificates: []*x509.Certificate{certWithURIs(t)}},
			wantErr: true,
		},
		{
			name:    "no peer certificate",
			state:   tls.ConnectionState{},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyCallerSAN(testCaller)(tc.state)
			if tc.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestServerCredentialsRequiresConfig(t *testing.T) {
	caFile := filepath.Join(t.TempDir(), "client-ca.pem")
	writeFileWithMtime(t, caFile, newCA(t, "client-ca").certPEM, time.Now())
	bundle := writeCredBundle(t, newCA(t, "server-ca").issue(t, certOpts{dnsNames: []string{"credprovider.test"}}))
	valid := Config{ServerBundle: bundle, ClientCAFile: caFile, CallerIdentity: testCaller}
	absent := filepath.Join(t.TempDir(), "absent.pem")

	tests := []struct {
		name   string
		mutate func(*Config)
		// wantIs, when set, must be in the error's chain.
		wantIs error
	}{
		{name: "no server bundle", mutate: func(c *Config) { c.ServerBundle = "" }},
		{name: "no client CA file", mutate: func(c *Config) { c.ClientCAFile = "" }},
		{name: "no caller identity", mutate: func(c *Config) { c.CallerIdentity = "" }},
		{name: "client CA file missing", mutate: func(c *Config) { c.ClientCAFile = absent }, wantIs: fs.ErrNotExist},
		{name: "server bundle missing", mutate: func(c *Config) { c.ServerBundle = absent }, wantIs: fs.ErrNotExist},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			_, err := ServerCredentials(cfg)
			if err == nil {
				t.Fatal("ServerCredentials() error = nil, want an error")
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Errorf("ServerCredentials() error = %v, want one wrapping %v", err, tc.wantIs)
			}
		})
	}
	if _, err := ServerCredentials(valid); err != nil {
		t.Fatalf("ServerCredentials(valid) error = %v", err)
	}
}

// A rotated client trust bundle takes effect on the next connection without
// rebuilding the credentials, and the chain and SAN checks still hold.
func TestServerCredentialsReloadsClientCA(t *testing.T) {
	serverCA := newCA(t, "server-ca")
	serverBundlePath := writeCredBundle(t, serverCA.issue(t, certOpts{dnsNames: []string{"credprovider.test"}}))
	serverRoots := x509.NewCertPool()
	serverRoots.AddCert(serverCA.cert)

	clientCA1 := newCA(t, "client-ca-1")
	clientCA2 := newCA(t, "client-ca-2")

	caFile := filepath.Join(t.TempDir(), "client-ca.pem")
	writeFileWithMtime(t, caFile, clientCA1.certPEM, time.Now())

	creds, err := ServerCredentials(Config{
		ServerBundle:   serverBundlePath,
		ClientCAFile:   caFile,
		CallerIdentity: testCaller,
	})
	if err != nil {
		t.Fatalf("ServerCredentials() error = %v", err)
	}

	fromCA1 := clientCA1.issue(t, certOpts{uris: []string{testCaller}})
	fromCA2 := clientCA2.issue(t, certOpts{uris: []string{testCaller}})
	wrongSAN := clientCA1.issue(t, certOpts{uris: []string{"spiffe://cluster.local/ns/ate-system/sa/impostor"}})

	// Before rotation only CA1 is trusted.
	if err := handshake(t, creds, serverRoots, fromCA1); err != nil {
		t.Fatalf("handshake with CA1-signed client cert failed before rotation: %v", err)
	}
	if err := handshake(t, creds, serverRoots, fromCA2); !isUnknownAuthority(err) {
		t.Fatalf("handshake with CA2-signed client cert before rotation: err = %v, want an unknown-authority failure", err)
	}
	// A valid chain with the wrong SAN is rejected.
	if err := handshake(t, creds, serverRoots, wrongSAN); err == nil || !strings.Contains(err.Error(), "do not include the expected caller identity") {
		t.Fatalf("handshake with wrong-SAN client cert: err = %v, want the caller identity check to refuse it", err)
	}

	// Rotate to CA2, bumping the mtime past coarse timestamps.
	writeFileWithMtime(t, caFile, clientCA2.certPEM, time.Now().Add(time.Second))

	// The same credentials now trust CA2 and not CA1.
	if err := handshake(t, creds, serverRoots, fromCA2); err != nil {
		t.Fatalf("handshake with CA2-signed client cert failed after rotation: %v", err)
	}
	if err := handshake(t, creds, serverRoots, fromCA1); !isUnknownAuthority(err) {
		t.Fatalf("handshake with CA1-signed client cert after rotation: err = %v, want an unknown-authority failure", err)
	}
}

// isUnknownAuthority reports whether err is an untrusted-chain failure.
func isUnknownAuthority(err error) bool {
	var unknown x509.UnknownAuthorityError
	return errors.As(err, &unknown)
}

// The server refuses a TLS 1.2-only client and one with no certificate.
func TestServerCredentialsRejectsWeakClients(t *testing.T) {
	serverCA := newCA(t, "server-ca")
	serverRoots := x509.NewCertPool()
	serverRoots.AddCert(serverCA.cert)
	clientCA := newCA(t, "client-ca")
	caFile := filepath.Join(t.TempDir(), "client-ca.pem")
	writeFileWithMtime(t, caFile, clientCA.certPEM, time.Now())
	creds, err := ServerCredentials(Config{
		ServerBundle:   writeCredBundle(t, serverCA.issue(t, certOpts{dnsNames: []string{"credprovider.test"}})),
		ClientCAFile:   caFile,
		CallerIdentity: testCaller,
	})
	if err != nil {
		t.Fatalf("ServerCredentials() error = %v", err)
	}
	caller := clientCA.issue(t, certOpts{uris: []string{testCaller}})

	for _, tc := range []struct {
		name    string
		mutate  func(*tls.Config)
		wantErr string
	}{
		{
			name:    "TLS 1.2 only",
			mutate:  func(c *tls.Config) { c.MinVersion, c.MaxVersion = tls.VersionTLS12, tls.VersionTLS12 },
			wantErr: "unsupported versions",
		},
		{
			name: "no client certificate",
			mutate: func(c *tls.Config) {
				c.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
					return &tls.Certificate{}, nil
				}
			},
			wantErr: "didn't provide a certificate",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := clientConfig(serverRoots, caller)
			tc.mutate(cfg)
			if err := handshakeWith(t, creds, cfg); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("handshake err = %v, want one mentioning %q", err, tc.wantErr)
			}
		})
	}
}

// handshake runs handshakeWith presenting clientCert and trusting serverRoots.
func handshake(t *testing.T, creds credentials.TransportCredentials, serverRoots *x509.CertPool, clientCert issued) error {
	t.Helper()
	return handshakeWith(t, creds, clientConfig(serverRoots, clientCert))
}

// clientConfig returns a client TLS config that always presents clientCert. With
// Certificates instead, Go sends no certificate unless the server names its
// issuer, so an untrusted-CA test would never reach chain verification.
func clientConfig(serverRoots *x509.CertPool, clientCert issued) *tls.Config {
	cert := &tls.Certificate{Certificate: [][]byte{clientCert.certDER}, PrivateKey: clientCert.key}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    serverRoots,
		ServerName: "credprovider.test",
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return cert, nil
		},
		// The gRPC server credentials enforce ALPN, so offer "h2".
		NextProtos: []string{"h2"},
	}
}

// handshakeWith runs one TLS handshake against creds with clientCfg, returning
// the server's error if any, since refusals are decided there, else the
// client's.
func handshakeWith(t *testing.T, creds credentials.TransportCredentials, clientCfg *tls.Config) error {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		_, _, err = creds.ServerHandshake(conn)
		serverErr <- err
	}()

	conn, clientErr := tls.Dial("tcp", lis.Addr().String(), clientCfg)
	if clientErr == nil {
		clientErr = conn.Handshake()
		conn.Close()
	}

	if err := <-serverErr; err != nil {
		return err
	}
	return clientErr
}

// ca is a self-signed certificate authority used to issue test certificates.
type ca struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newCA(t *testing.T, cn string) *ca {
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
	return &ca{cert: cert, key: key, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
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

func (c *ca) issue(t *testing.T, opts certOpts) issued {
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
	path := filepath.Join(t.TempDir(), "server-bundle.pem")
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
