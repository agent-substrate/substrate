// Copyright 2026 The Agent Substrate Authors
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
	"encoding/pem"
	"errors"
	"io"
	"maps"
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
	const jwksURL = "https://172.18.0.2:6443/openid/v1/jwks"
	tokenFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokenFile, []byte("test-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	for _, tt := range []struct {
		name string
		url  string
		// discoveredJWKS, if set, is passed to withDiscoveredJWKSURL.
		discoveredJWKS string
		wantAuth       string
	}{
		{
			name:     "issuer discovery",
			url:      "https://kubernetes.default.svc/.well-known/openid-configuration",
			wantAuth: "Bearer test-token",
		},
		{
			name:           "discovered jwks on another host",
			url:            jwksURL,
			discoveredJWKS: jwksURL,
			wantAuth:       "Bearer test-token",
		},
		{
			name: "kubernetes jwks path not from discovery",
			url:  "https://attacker.example/openid/v1/jwks",
		},
		{
			name:           "other url than the discovered jwks",
			url:            "https://attacker.example/openid/v1/jwks",
			discoveredJWKS: jwksURL,
		},
		{
			name:           "discovered jwks over http",
			url:            "http://172.18.0.2:6443/openid/v1/jwks",
			discoveredJWKS: "http://172.18.0.2:6443/openid/v1/jwks",
		},
		{
			name: "arbitrary url",
			url:  "https://attacker.example/.well-known/openid-configuration",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
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

			ctx := t.Context()
			if tt.discoveredJWKS != "" {
				ctx = withDiscoveredJWKSURL(ctx, tt.discoveredJWKS)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, tt.url, nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			if _, err := transport.RoundTrip(req); err != nil {
				t.Fatalf("RoundTrip() error = %v", err)
			}
			if gotAuth != tt.wantAuth {
				t.Fatalf("Authorization = %q, want %q", gotAuth, tt.wantAuth)
			}
		})
	}
}

// TestDiscoveryTokenFollowsDiscoveredJWKS checks that discovery sends the token
// to a jwks_uri on another host, as a Kubernetes API server's is, and not to a
// host the jwks_uri redirects to.
func TestDiscoveryTokenFollowsDiscoveredJWKS(t *testing.T) {
	var keys testIssuer
	keys.addRSA("key", &testRSAKey(t).PublicKey)

	for _, tt := range []struct {
		name     string
		jwksPath string
		// wantAuth maps each host that got a request to its Authorization header.
		wantAuth map[string]string
	}{
		{
			name:     "jwks on another host",
			jwksPath: "/openid/v1/jwks",
			wantAuth: map[string]string{"issuer": "Bearer test-token", "jwks_uri": "Bearer test-token"},
		},
		{
			name:     "jwks redirects to another host",
			jwksPath: "/moved",
			wantAuth: map[string]string{"issuer": "Bearer test-token", "jwks_uri": "Bearer test-token", "redirect target": ""},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tokenFile := t.TempDir() + "/token"
			if err := os.WriteFile(tokenFile, []byte("test-token\n"), 0o600); err != nil {
				t.Fatalf("write token: %v", err)
			}

			serveKeys := func(w http.ResponseWriter, _ *http.Request) { writeJSON(t, w, keys.jwks) }
			redirected := httptest.NewTLSServer(http.HandlerFunc(serveKeys))
			t.Cleanup(redirected.Close)

			jwksMux := http.NewServeMux()
			jwksMux.HandleFunc("/openid/v1/jwks", serveKeys)
			jwksMux.HandleFunc("/moved", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, redirected.URL+"/openid/v1/jwks", http.StatusFound)
			})
			jwksServer := httptest.NewTLSServer(jwksMux)
			t.Cleanup(jwksServer.Close)

			var issuer *httptest.Server
			issuer = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, oidcConfigT{Issuer: issuer.URL, JWKSURI: jwksServer.URL + tt.jwksPath})
			}))
			t.Cleanup(issuer.Close)

			// Every httptest TLS server presents the same self-signed certificate.
			caFile := t.TempDir() + "/ca.crt"
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuer.Certificate().Raw})
			if err := os.WriteFile(caFile, ca, 0o600); err != nil {
				t.Fatalf("write CA: %v", err)
			}
			client, err := NewHTTPClient(issuer.URL, caFile, tokenFile)
			if err != nil {
				t.Fatalf("NewHTTPClient() error = %v", err)
			}
			transport, ok := client.Transport.(*issuerDiscoveryTransport)
			if !ok {
				t.Fatalf("client transport is %T, want *issuerDiscoveryTransport", client.Transport)
			}
			hostNames := map[string]string{
				issuer.Listener.Addr().String():     "issuer",
				jwksServer.Listener.Addr().String(): "jwks_uri",
				redirected.Listener.Addr().String(): "redirect target",
			}
			// The client calls RoundTrip on this goroutine, redirects included.
			gotAuth := map[string]string{}
			base := transport.base
			transport.base = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				gotAuth[hostNames[req.URL.Host]] = req.Header.Get("Authorization")
				return base.RoundTrip(req)
			})
			if _, err := discoverKeysForIssuer(t.Context(), client, issuer.URL); err != nil {
				t.Fatalf("discoverKeysForIssuer() error = %v", err)
			}

			if !maps.Equal(gotAuth, tt.wantAuth) {
				t.Errorf("Authorization by host = %q, want %q", gotAuth, tt.wantAuth)
			}
		})
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
