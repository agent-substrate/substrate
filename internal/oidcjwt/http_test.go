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
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

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
