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

package atunnel

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
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestServeHTTP(t *testing.T) {
	upstreamHost := make(chan string, 3)
	upstreamURL, err := url.Parse("http://actor.internal:80")
	if err != nil {
		t.Fatal(err)
	}

	s := newTestServer(t, upstreamURL)
	s.proxy.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		upstreamHost <- r.Host
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
		}, nil
	})
	if err := s.Activate("team-a", "actor-1"); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name         string
		host         string
		originalHost string
		wantStatus   int
	}{
		{name: "active actor", host: "actor-1.team-a.actors.resources.substrate.ate.dev", wantStatus: http.StatusNoContent},
		{name: "active actor with port", host: "actor-1.team-a.actors.resources.substrate.ate.dev:443", wantStatus: http.StatusNoContent},
		{name: "DNS case insensitive", host: "ACTOR-1.TEAM-A.ACTORS.RESOURCES.SUBSTRATE.ATE.DEV", wantStatus: http.StatusNoContent},
		{name: "router original host", host: "10.0.0.52:443", originalHost: "actor-1.team-a.actors.resources.substrate.ate.dev", wantStatus: http.StatusNoContent},
		{name: "wrong actor", host: "actor-2.team-a.actors.resources.substrate.ate.dev", wantStatus: http.StatusMisdirectedRequest},
		{name: "wrong atespace", host: "actor-1.team-b.actors.resources.substrate.ate.dev", wantStatus: http.StatusMisdirectedRequest},
		{name: "suffix confusion", host: "actor-1.team-a.actors.resources.substrate.ate.dev.example.com", wantStatus: http.StatusMisdirectedRequest},
		{name: "malformed port", host: "actor-1.team-a.actors.resources.substrate.ate.dev:nope", wantStatus: http.StatusMisdirectedRequest},
		{name: "empty", wantStatus: http.StatusMisdirectedRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "https://worker/hello", nil)
			req.Host = tt.host
			if tt.originalHost != "" {
				req.Header.Set(OriginalHostHeader, tt.originalHost)
			}
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusMisdirectedRequest && rec.Header().Get(StaleAssignmentHeader) != "true" {
				t.Errorf("missing %s response header", StaleAssignmentHeader)
			}
		})
	}

	for range 4 {
		if got := <-upstreamHost; got != "actor-1.team-a.actors.resources.substrate.ate.dev" && got != "actor-1.team-a.actors.resources.substrate.ate.dev:443" && got != "ACTOR-1.TEAM-A.ACTORS.RESOURCES.SUBSTRATE.ATE.DEV" {
			t.Errorf("upstream Host = %q", got)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type idleClosingRoundTripper struct {
	closed bool
}

func (t *idleClosingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusNoContent,
		Header:     make(http.Header),
		Body:       http.NoBody,
	}, nil
}

func (t *idleClosingRoundTripper) CloseIdleConnections() {
	t.closed = true
}

func TestDeactivateClosesIdleUpstreamConnections(t *testing.T) {
	upstream, err := url.Parse("http://actor.internal:80")
	if err != nil {
		t.Fatal(err)
	}
	s := newTestServer(t, upstream)
	transport := &idleClosingRoundTripper{}
	s.proxy.Transport = transport
	if err := s.Activate("team-a", "actor-1"); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "https://worker/", nil)
	req.Host = "actor-1.team-a.actors.resources.substrate.ate.dev"
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if err := s.Deactivate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !transport.closed {
		t.Fatal("Deactivate did not close idle upstream connections")
	}
}

func TestInactive(t *testing.T) {
	upstream, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	s := newTestServer(t, upstream)
	req := httptest.NewRequest(http.MethodGet, "https://worker/", nil)
	req.Host = "actor-1.team-a.actors.resources.substrate.ate.dev"

	for _, phase := range []string{"before activation", "after deactivation"} {
		t.Run(phase, func(t *testing.T) {
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)
			if rec.Code != http.StatusMisdirectedRequest {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusMisdirectedRequest)
			}
		})
		if phase == "before activation" {
			if err := s.Activate("team-a", "actor-1"); err != nil {
				t.Fatal(err)
			}
			if err := s.Deactivate(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestMutualTLSClientIdentity(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	serverCert := ca.issue(t, "", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	bundlePath := filepath.Join(dir, "server.pem")
	trustPath := filepath.Join(dir, "trust.pem")
	writeCredentialBundle(t, bundlePath, serverCert)
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	upstream, err := url.Parse("http://actor.internal:80")
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewServer(Config{
		CredentialBundlePath: bundlePath,
		TrustBundlePath:      trustPath,
		AllowedClientID:      "spiffe://cluster.local/ns/ate-system/sa/atenet-router",
		Upstream:             upstream,
	})
	if err != nil {
		t.Fatal(err)
	}

	untrustedCA := newTestCA(t)
	tests := []struct {
		name    string
		cert    tls.Certificate
		wantErr bool
	}{
		{
			name: "allowed identity",
			cert: ca.issue(t, "spiffe://cluster.local/ns/ate-system/sa/atenet-router", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}),
		},
		{
			name:    "wrong identity",
			cert:    ca.issue(t, "spiffe://cluster.local/ns/ate-system/sa/not-the-gateway", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}),
			wantErr: true,
		},
		{
			name:    "untrusted identity",
			cert:    untrustedCA.issue(t, "spiffe://cluster.local/ns/ate-system/sa/atenet-router", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverErr, clientErr := tlsHandshake(s.tlsConfig, &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true, // Test only the server's client authentication here.
				Certificates:       []tls.Certificate{tt.cert},
			})
			gotErr := serverErr != nil || clientErr != nil
			if gotErr != tt.wantErr {
				t.Fatalf("server error = %v, client error = %v, want error %v", serverErr, clientErr, tt.wantErr)
			}
		})
	}
}

func TestDeactivateCancelsInflightRequest(t *testing.T) {
	upstream, err := url.Parse("http://actor.internal:80")
	if err != nil {
		t.Fatal(err)
	}
	s := newTestServer(t, upstream)
	if err := s.Activate("team-a", "actor-1"); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	s.proxy.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		close(started)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		req := httptest.NewRequest(http.MethodGet, "https://worker/", nil)
		req.Host = "actor-1.team-a.actors.resources.substrate.ate.dev"
		s.ServeHTTP(httptest.NewRecorder(), req)
	}()
	<-started
	if err := s.Deactivate(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-done
}

func newTestServer(t *testing.T, upstream *url.URL) *Server {
	t.Helper()
	dir := t.TempDir()
	bundle, trust := makeCertFiles(t, dir)
	s, err := NewServer(Config{
		CredentialBundlePath: bundle,
		TrustBundlePath:      trust,
		AllowedClientID:      "spiffe://cluster.local/ns/ate-system/sa/atenet-router",
		Upstream:             upstream,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func makeCertFiles(t *testing.T, dir string) (bundlePath, trustPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	bundlePath = filepath.Join(dir, "bundle.pem")
	trustPath = filepath.Join(dir, "trust.pem")
	if err := os.WriteFile(bundlePath, append(certPEM, keyPEM...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(trustPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return bundlePath, trustPath
}

type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(now.UnixNano()),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
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
	return &testCA{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

func (ca *testCA) issue(t *testing.T, spiffeID string, usages []x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
	}
	if spiffeID != "" {
		uri, err := url.Parse(spiffeID)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = []*url.URL{uri}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), ca.certPEM...),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func writeCredentialBundle(t *testing.T, path string, cert tls.Certificate) {
	t.Helper()
	key, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("private key has type %T", cert.PrivateKey)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	var bundle []byte
	for _, der := range cert.Certificate {
		bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})...)
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		t.Fatal(err)
	}
}

func tlsHandshake(serverConfig, clientConfig *tls.Config) (serverErr, clientErr error) {
	serverConn, clientConn := net.Pipe()
	serverTLS := tls.Server(serverConn, serverConfig)
	clientTLS := tls.Client(clientConn, clientConfig)
	done := make(chan error, 1)
	go func() {
		err := serverTLS.Handshake()
		_ = serverConn.Close()
		done <- err
	}()
	clientErr = clientTLS.Handshake()
	_ = clientConn.Close()
	serverErr = <-done
	return serverErr, clientErr
}
