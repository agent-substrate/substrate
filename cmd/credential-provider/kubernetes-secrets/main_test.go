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
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
)

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

func TestVerifyClientSAN(t *testing.T) {
	const injector = injectorSPIFFEID

	tests := []struct {
		name    string
		state   tls.ConnectionState
		wantErr bool
	}{
		{
			name:  "matching SAN",
			state: tls.ConnectionState{PeerCertificates: []*x509.Certificate{certWithURIs(t, injector)}},
		},
		{
			name:  "matching SAN among several",
			state: tls.ConnectionState{PeerCertificates: []*x509.Certificate{certWithURIs(t, "spiffe://cluster.local/ns/other/sa/x", injector)}},
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
			err := verifyClientSAN(injector)(tc.state)
			if tc.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestBuildServerCredsReloadsClientCA drives real TLS handshakes against the
// credentials buildServerCreds returns, then rewrites the mounted trust bundle
// and confirms a client certificate signed by the newly published CA verifies
// on a new connection without rebuilding the credentials, while both the chain
// check and the injector SAN check still hold.
func TestBuildServerCredsReloadsClientCA(t *testing.T) {
	serverCA := newCA(t, "server-ca")
	serverBundlePath := writeCredBundle(t, serverCA.issue(t, certOpts{dnsNames: []string{"credprovider.test"}}))
	serverRoots := x509.NewCertPool()
	serverRoots.AddCert(serverCA.cert)

	clientCA1 := newCA(t, "client-ca-1")
	clientCA2 := newCA(t, "client-ca-2")

	caFile := filepath.Join(t.TempDir(), "client-ca.pem")
	writeFileWithMtime(t, caFile, clientCA1.certPEM, time.Now())

	// buildServerCreds reads these package-level flags.
	*serverBundle = serverBundlePath
	*clientCAFile = caFile

	creds, err := buildServerCreds(context.Background())
	if err != nil {
		t.Fatalf("buildServerCreds() error = %v", err)
	}

	fromCA1 := clientCA1.issue(t, certOpts{uris: []string{injectorSPIFFEID}})
	fromCA2 := clientCA2.issue(t, certOpts{uris: []string{injectorSPIFFEID}})
	wrongSAN := clientCA1.issue(t, certOpts{uris: []string{"spiffe://cluster.local/ns/ate-system/sa/impostor"}})

	// Before rotation only CA1 is trusted.
	if err := handshake(t, creds, serverRoots, fromCA1); err != nil {
		t.Fatalf("handshake with CA1-signed client cert failed before rotation: %v", err)
	}
	if err := handshake(t, creds, serverRoots, fromCA2); err == nil {
		t.Fatal("handshake with CA2-signed client cert succeeded before rotation, want chain failure")
	}
	// A valid chain with the wrong SAN is still rejected.
	if err := handshake(t, creds, serverRoots, wrongSAN); err == nil {
		t.Fatal("handshake with wrong-SAN client cert succeeded, want SAN failure")
	}

	// Publish CA2 as the mounted trust bundle, bumping the mtime so the change
	// is visible even where timestamps are coarse.
	writeFileWithMtime(t, caFile, clientCA2.certPEM, time.Now().Add(time.Second))

	// The same credentials now accept a CA2-signed certificate on a new
	// connection, without a restart, and CA1 is no longer trusted.
	if err := handshake(t, creds, serverRoots, fromCA2); err != nil {
		t.Fatalf("handshake with CA2-signed client cert failed after rotation: %v", err)
	}
	if err := handshake(t, creds, serverRoots, fromCA1); err == nil {
		t.Fatal("handshake with CA1-signed client cert succeeded after rotation, want chain failure")
	}
}

// handshake performs one TLS handshake against creds, presenting clientCert and
// trusting the server with serverRoots. It reports the first end to fail.
func handshake(t *testing.T, creds credentials.TransportCredentials, serverRoots *x509.CertPool, clientCert issued) error {
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

	clientCfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		RootCAs:      serverRoots,
		ServerName:   "credprovider.test",
		Certificates: []tls.Certificate{{Certificate: [][]byte{clientCert.certDER}, PrivateKey: clientCert.key}},
		// The gRPC server credentials enforce ALPN, so offer "h2".
		NextProtos: []string{"h2"},
	}
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

// writeCredBundle writes a credential bundle (PKCS8 key + leaf certificate) in
// the format credbundle.Parse expects and returns its path.
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
