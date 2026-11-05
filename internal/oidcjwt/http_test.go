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

package oidcjwt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/testcert"
)

func TestIssuerScopedURL(t *testing.T) {
	for _, tt := range []struct {
		name   string
		url    string
		issuer string
		want   bool
	}{
		{
			name:   "same host root issuer discovery",
			url:    "https://kubernetes.default.svc/.well-known/openid-configuration",
			issuer: "https://kubernetes.default.svc",
			want:   true,
		},
		{
			name:   "same host path issuer jwks",
			url:    "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c/jwks",
			issuer: "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c",
			want:   true,
		},
		{
			name:   "same host sibling path",
			url:    "https://container.googleapis.com/v1/projects/p/locations/l/clusters/other/jwks",
			issuer: "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c",
			want:   false,
		},
		{
			name:   "prefix lookalike",
			url:    "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c-attacker/jwks",
			issuer: "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c",
			want:   false,
		},
		{
			name:   "different host",
			url:    "https://attacker.example/.well-known/openid-configuration",
			issuer: "https://kubernetes.default.svc",
			want:   false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := issuerScopedURL(tt.url, tt.issuer); got != tt.want {
				t.Fatalf("issuerScopedURL(%q, %q) = %v, want %v", tt.url, tt.issuer, got, tt.want)
			}
		})
	}
}

func TestK8sServiceAccountIssuerDiscoveryTransport(t *testing.T) {
	tokenFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokenFile, []byte("test-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	var gotAuth string
	transport := &issuerDiscoveryTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			gotAuth = req.Header.Get("Authorization")
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(nil),
				Header:     make(http.Header),
			}, nil
		}),
		tokenFile: tokenFile,
		issuer:    "https://kubernetes.default.svc",
	}

	req, err := http.NewRequest(http.MethodGet, "https://kubernetes.default.svc/.well-known/openid-configuration", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if _, err := transport.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("Authorization = %q, want Bearer test-token", gotAuth)
	}
}

func TestK8sServiceAccountIssuerDiscoveryTransportSendsTokenToKubernetesJWKSURL(t *testing.T) {
	tokenFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokenFile, []byte("test-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	var gotAuth string
	transport := &issuerDiscoveryTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			gotAuth = req.Header.Get("Authorization")
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(nil),
				Header:     make(http.Header),
			}, nil
		}),
		tokenFile: tokenFile,
		issuer:    "https://kubernetes.default.svc",
	}

	req, err := http.NewRequest(http.MethodGet, "https://172.18.0.2:6443/openid/v1/jwks", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if _, err := transport.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("Authorization = %q, want Bearer test-token", gotAuth)
	}
}

func TestK8sServiceAccountIssuerDiscoveryTransportDoesNotSendTokenToArbitraryURL(t *testing.T) {
	tokenFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokenFile, []byte("test-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	var gotAuth string
	transport := &issuerDiscoveryTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			gotAuth = req.Header.Get("Authorization")
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(nil),
				Header:     make(http.Header),
			}, nil
		}),
		tokenFile: tokenFile,
		issuer:    "https://kubernetes.default.svc",
	}

	req, err := http.NewRequest(http.MethodGet, "https://attacker.example/.well-known/openid-configuration", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if _, err := transport.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want empty", gotAuth)
	}
}

func TestBuildJWTIssuerDiscoveryClientUsesDefaultTransportWithoutDiscoveryToken(t *testing.T) {
	client, err := NewHTTPClient("https://accounts.google.com", "", "")
	if err != nil {
		t.Fatalf("NewHTTPClient() error = %v", err)
	}
	if client.Timeout == 0 {
		t.Fatalf("client timeout = 0, want nonzero timeout")
	}
	if _, ok := client.Transport.(*issuerDiscoveryTransport); ok {
		t.Fatalf("provider without discovery token should not use token transport")
	}
}

func TestNewHTTPClientRequiresCAWithDiscoveryToken(t *testing.T) {
	if _, err := NewHTTPClient("https://kubernetes.default.svc", "", "/token"); err == nil {
		t.Fatal("NewHTTPClient() with a discovery token and no CA succeeded")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// TestNewHTTPClientTrustsRotatedCA swaps the CA file between key fetches, the
// way kubelet updates a projected trust bundle, and breaks it in between.
func TestNewHTTPClientTrustsRotatedCA(t *testing.T) {
	ti := &testIssuer{}
	ti.addRSA("key", &testRSAKey(t).PublicKey)
	ti.server = httptest.NewUnstartedServer(ti.handler(t))
	// Each fetch dials anew, so it can't pass on a connection verified earlier.
	ti.server.Config.SetKeepAlivesEnabled(false)
	var serving atomic.Pointer[tls.Config]
	ti.server.TLS = &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return serving.Load(), nil }}
	serve := func(s testcert.ServerTLS) {
		cert, err := tls.X509KeyPair(s.Certificate, s.PrivateKey)
		if err != nil {
			t.Fatal(err)
		}
		serving.Store(&tls.Config{Certificates: []tls.Certificate{cert}})
	}
	ip := net.ParseIP("127.0.0.1")
	before, after := testcert.NewServerTLS(t, ip), testcert.NewServerTLS(t, ip)
	serve(before)
	ti.server.StartTLS()
	t.Cleanup(ti.server.Close)

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	writeCA := func(pemBytes []byte) {
		tmp := filepath.Join(dir, "ca.pem.tmp")
		if err := os.WriteFile(tmp, pemBytes, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, caFile); err != nil {
			t.Fatal(err)
		}
	}
	writeCA(before.RootCA)
	client, err := NewHTTPClient(ti.issuer(), caFile, "")
	if err != nil {
		t.Fatalf("NewHTTPClient() error = %v", err)
	}
	keys := HTTPKeySource(ti.issuer(), client)

	if _, err := keys.Keys(t.Context()); err != nil {
		t.Fatalf("Keys() with the first CA error = %v", err)
	}
	serve(after)
	var certErr *tls.CertificateVerificationError
	if _, err := keys.Keys(t.Context()); !errors.As(err, &certErr) {
		t.Fatalf("Keys() after the server rotated error = %v, want a CertificateVerificationError", err)
	}
	writeCA(after.RootCA)
	if _, err := keys.Keys(t.Context()); err != nil {
		t.Fatalf("Keys() after the CA file rotated error = %v", err)
	}

	// A CA file that can't be read keeps the last pool.
	if err := os.Remove(caFile); err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Keys(t.Context()); err != nil {
		t.Fatalf("Keys() without the CA file error = %v", err)
	}
	writeCA([]byte("not a certificate\n"))
	if _, err := keys.Keys(t.Context()); err != nil {
		t.Fatalf("Keys() with an invalid CA file error = %v", err)
	}
	third := testcert.NewServerTLS(t, ip)
	serve(third)
	if _, err := keys.Keys(t.Context()); !errors.As(err, &certErr) {
		t.Fatalf("Keys() with the last pool after the server rotated error = %v, want a CertificateVerificationError", err)
	}
	writeCA(third.RootCA)
	if _, err := keys.Keys(t.Context()); err != nil {
		t.Fatalf("Keys() after a valid CA file returned error = %v", err)
	}
}

// TestSystemRootsTrustServiceDNSIssuer checks that a client trusting the
// system roots plus a service DNS trust bundle accepts an in-cluster issuer
// however it is spelled, and that the system roots alone do not.
func TestSystemRootsTrustServiceDNSIssuer(t *testing.T) {
	const issuer = "https://idp.ate-system.svc:443"
	if err := actoridjwt.ValidateIssuer(issuer); err != nil {
		t.Fatal(err)
	}
	ca, err := localca.GenerateCA("servicedns", localca.KeyTypeECDSAP256, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	serving := testcert.ServerTLSWithCA(t, ca, net.ParseIP("127.0.0.1"), "idp.ate-system.svc")
	cert, err := tls.X509KeyPair(serving.Certificate, serving.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	ti := &testIssuer{}
	ti.addRSA("key", &testRSAKey(t).PublicKey)
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, oidcConfigT{Issuer: issuer, JWKSURI: issuer + "/jwks"})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) { writeJSON(t, w, ti.jwks) })
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	bundle := filepath.Join(t.TempDir(), "trust-bundle.pem")
	if err := os.WriteFile(bundle, serving.RootCA, 0o600); err != nil {
		t.Fatal(err)
	}
	// Only the dial goes to the test server. TLS still checks the name in
	// the URL.
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
	}
	keys := func(loadRoots func() (*x509.CertPool, error)) error {
		transport, err := newRootsTransport(base, loadRoots)
		if err != nil {
			t.Fatal(err)
		}
		_, err = HTTPKeySource(issuer, newHTTPClient(transport)).Keys(t.Context())
		return err
	}

	if err := keys(credbundle.SystemPoolLoader(bundle)); err != nil {
		t.Fatalf("Keys() with the system roots and the bundle error = %v", err)
	}
	var certErr *tls.CertificateVerificationError
	if err := keys(x509.SystemCertPool); !errors.As(err, &certErr) {
		t.Fatalf("Keys() with the system roots alone error = %v, want a CertificateVerificationError", err)
	}
}

// TestRootsTransportCloseIdleConnections checks that the client's
// CloseIdleConnections reaches the connections the current transport keeps.
func TestRootsTransportCloseIdleConnections(t *testing.T) {
	var opened, closed atomic.Int32
	closedCh := make(chan struct{}, 8)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			opened.Add(1)
		case http.StateClosed:
			closed.Add(1)
			closedCh <- struct{}{}
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	client, err := NewHTTPClientWithRoots(func() (*x509.CertPool, error) { return roots, nil })
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	client.CloseIdleConnections()
	want := opened.Load()
	if want == 0 {
		t.Fatal("the request opened no connection")
	}
	for closed.Load() < want {
		select {
		case <-closedCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("%d of %d connections closed after CloseIdleConnections", closed.Load(), want)
		}
	}
}
