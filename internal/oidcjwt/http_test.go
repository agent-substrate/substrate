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
	"encoding/pem"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
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
