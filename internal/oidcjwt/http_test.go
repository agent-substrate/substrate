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
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/testcert"
	"k8s.io/client-go/rest"
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

func TestNewHTTPClientWithClientConfigUsesDefaultTransportForGCP(t *testing.T) {
	for _, issuer := range []string{
		"https://accounts.google.com",
		"https://container.googleapis.com/v1/projects/p/locations/l/clusters/c",
	} {
		t.Run(issuer, func(t *testing.T) {
			client, err := NewHTTPClientWithClientConfig(issuer, "", "", &rest.Config{
				Host:            "https://kubernetes.example",
				BearerToken:     "kube-token",
				TLSClientConfig: rest.TLSClientConfig{Insecure: true},
			})
			if err != nil {
				t.Fatal(err)
			}
			if client.Timeout == 0 {
				t.Fatal("client timeout = 0, want nonzero timeout")
			}
			transport, ok := client.Transport.(*http.Transport)
			if !ok {
				t.Fatalf("client transport = %T, want default HTTP transport", client.Transport)
			}
			if transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify {
				t.Fatal("GCP transport inherited insecure Kubernetes TLS configuration")
			}
		})
	}
}

func TestNewHTTPClientRequiresCAWithDiscoveryToken(t *testing.T) {
	if _, err := NewHTTPClient("https://kubernetes.default.svc", "", "/token"); err == nil {
		t.Fatal("NewHTTPClient() with a discovery token and no CA succeeded")
	}
	if _, err := NewHTTPClientWithClientConfig("https://kubernetes.default.svc", "", "/token", &rest.Config{
		Host: "https://kubernetes.example",
	}); err == nil {
		t.Fatal("NewHTTPClientWithClientConfig() with a discovery token and no CA succeeded")
	}
}

func TestNewHTTPClientWithClientConfigUsesAnonymousTransport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if auth := req.Header.Get("Authorization"); auth != "" {
			t.Errorf("Authorization = %q, want empty", auth)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewHTTPClientWithClientConfig("https://kubernetes.default.svc", "", "", &rest.Config{
		Host:        "https://kubernetes.example",
		BearerToken: "kube-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.Transport.(*kubeconfigIssuerTransport)
	if !ok {
		t.Fatalf("client transport = %T, want kubeconfig transport", client.Transport)
	}
	if _, ok := transport.base.(*http.Transport); !ok {
		t.Fatalf("base transport = %T, want default HTTP transport", transport.base)
	}
	if transport.base == http.DefaultTransport {
		t.Fatal("base transport is not a clone of the default transport")
	}
	resp, err := client.Get(server.URL + "/custom-jwks")
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("response status = %d, want 200", resp.StatusCode)
	}
}

func TestNewHTTPClientWithClientConfigUsesKubeconfigCredentials(t *testing.T) {
	for _, tt := range []struct {
		name    string
		issuer  string
		url     string
		wantURL string
	}{
		{name: "discovery", issuer: "https://kubernetes.default.svc", url: "https://kubernetes.default.svc/.well-known/openid-configuration", wantURL: "https://kubernetes.example/proxy/.well-known/openid-configuration"},
		{name: "path issuer discovery", issuer: "https://issuer.example/tenant", url: "https://issuer.example/tenant/.well-known/openid-configuration", wantURL: "https://kubernetes.example/proxy/.well-known/openid-configuration"},
		{name: "published proxy JWKS", issuer: "https://kubernetes.default.svc", url: "https://kubernetes.example/proxy/openid/v1/jwks", wantURL: "https://kubernetes.example/proxy/openid/v1/jwks"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var gotAuth, gotURL string
			client, err := NewHTTPClientWithClientConfig(tt.issuer, "", "", &rest.Config{
				Host:        "https://kubernetes.example/proxy",
				BearerToken: "kube-token",
				Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					gotAuth = req.Header.Get("Authorization")
					gotURL = req.URL.String()
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(nil), Header: make(http.Header)}, nil
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			client.Transport.(*kubeconfigIssuerTransport).base = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				t.Fatalf("request %q used anonymous transport", req.URL)
				return nil, nil
			})
			req, err := http.NewRequest(http.MethodGet, tt.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if err := resp.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if gotAuth != "Bearer kube-token" {
				t.Fatalf("Authorization = %q, want kubeconfig bearer token", gotAuth)
			}
			if gotURL != tt.wantURL {
				t.Fatalf("request URL = %q, want %q", gotURL, tt.wantURL)
			}
			if req.URL.String() != tt.url {
				t.Fatalf("original request URL = %q, want %q", req.URL, tt.url)
			}
		})
	}
}

func TestKubeconfigIssuerTransportRedirects(t *testing.T) {
	for _, tt := range []struct {
		name      string
		serverURL string
		location  string
		wantURL   string
		wantAuth  string
		wantErr   bool
	}{
		{name: "root relative", location: "/proxy/discovery.json", wantURL: "https://kubernetes.example/proxy/discovery.json", wantAuth: "Bearer kube-token"},
		{name: "path relative", location: "../discovery.json", wantURL: "https://kubernetes.example/proxy/discovery.json", wantAuth: "Bearer kube-token"},
		{name: "query only", location: "?version=2", wantURL: "https://kubernetes.example/proxy/.well-known/openid-configuration?version=2", wantAuth: "Bearer kube-token"},
		{name: "absolute server", location: "https://kubernetes.example/proxy/discovery.json", wantURL: "https://kubernetes.example/proxy/discovery.json", wantAuth: "Bearer kube-token"},
		{name: "external host", location: "https://external.example/discovery.json", wantURL: "https://external.example/discovery.json"},
		{name: "scheme relative external host", location: "//external.example/discovery.json", wantURL: "https://external.example/discovery.json"},
		{name: "HTTP downgrade", location: "http://kubernetes.example/proxy/discovery.json", wantURL: "http://kubernetes.example/proxy/discovery.json"},
		{name: "outside proxy prefix", location: "/proxy-other/discovery.json", wantURL: "https://kubernetes.example/proxy-other/discovery.json"},
		{name: "encoded parent segment", location: "/proxy/%2e%2e/discovery.json", wantURL: "https://kubernetes.example/proxy/%2e%2e/discovery.json"},
		{name: "escaped proxy relative", serverURL: "https://kubernetes.example/proxy%2F", location: "../discovery.json", wantURL: "https://kubernetes.example/proxy%2F/discovery.json", wantAuth: "Bearer kube-token"},
		{name: "escaped proxy query", serverURL: "https://kubernetes.example/proxy%2F", location: "?version=2", wantURL: "https://kubernetes.example/proxy%2F/.well-known/openid-configuration?version=2", wantAuth: "Bearer kube-token"},
		{name: "malformed location", location: ":invalid", wantErr: true},
		{name: "no location"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var urls, auths []string
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				urls = append(urls, req.URL.String())
				auths = append(auths, req.Header.Get("Authorization"))
				status, header := http.StatusOK, make(http.Header)
				if len(urls) == 1 {
					status = http.StatusTemporaryRedirect
					if tt.location != "" {
						header.Set("Location", tt.location)
					}
				}
				return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
			})
			if tt.serverURL == "" {
				tt.serverURL = "https://kubernetes.example/proxy"
			}
			client, err := NewHTTPClientWithClientConfig("https://issuer.example/tenant", "", "", &rest.Config{
				Host:        tt.serverURL,
				BearerToken: "kube-token",
				Transport:   transport,
			})
			if err != nil {
				t.Fatal(err)
			}
			client.Transport.(*kubeconfigIssuerTransport).base = transport
			const originalURL = "https://issuer.example/tenant/.well-known/openid-configuration"
			req, err := http.NewRequest(http.MethodGet, originalURL, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if resp != nil {
				if err := resp.Body.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if req.URL.String() != originalURL || req.Header.Get("Authorization") != "" {
				t.Fatal("original request was modified")
			}
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "failed to parse Location") {
					t.Fatalf("redirect error = %v, want invalid Location error", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tt.location == "" {
				if len(urls) != 1 || resp.StatusCode != http.StatusTemporaryRedirect || resp.Header.Get("Location") != "" {
					t.Fatalf("response without Location was changed: status = %d, Location = %q, requests = %v", resp.StatusCode, resp.Header.Get("Location"), urls)
				}
				return
			}
			if len(urls) != 2 || urls[0] != strings.TrimRight(tt.serverURL, "/")+oidcDiscoveryPath || auths[0] != "Bearer kube-token" {
				t.Fatalf("requests = %v, authorization = %v, want authenticated discovery and one redirect", urls, auths)
			}
			if urls[1] != tt.wantURL || auths[1] != tt.wantAuth {
				t.Fatalf("redirect = %q, authorization = %q, want %q, %q", urls[1], auths[1], tt.wantURL, tt.wantAuth)
			}
		})
	}
}

func TestKubeconfigIssuerTransportScopesCredentials(t *testing.T) {
	for _, tt := range []struct {
		name      string
		issuer    string
		url       string
		serverURL string
		wantRoute string
		wantURL   string
	}{
		{name: "issuer discovery", issuer: "https://kubernetes.default.svc", url: "https://kubernetes.default.svc/.well-known/openid-configuration", wantRoute: "authenticated", wantURL: "https://kubernetes.example/.well-known/openid-configuration"},
		{name: "path issuer discovery", issuer: "https://issuer.example/prefix", url: "https://issuer.example/prefix/.well-known/openid-configuration", wantRoute: "authenticated", wantURL: "https://kubernetes.example/.well-known/openid-configuration"},
		{name: "Kubernetes JWKS", issuer: "https://kubernetes.default.svc", url: "https://internal.example/openid/v1/jwks", wantRoute: "authenticated", wantURL: "https://kubernetes.example/openid/v1/jwks"},
		{name: "path issuer JWKS", issuer: "https://issuer.example/prefix/", url: "https://issuer.example/prefix/openid/v1/jwks", wantRoute: "authenticated", wantURL: "https://kubernetes.example/openid/v1/jwks"},
		{name: "issuer sibling path", issuer: "https://issuer.example/prefix", url: "https://issuer.example/prefix-other/openid/v1/jwks", wantRoute: "anonymous", wantURL: "https://issuer.example/prefix-other/openid/v1/jwks"},
		{name: "custom issuer JWKS", issuer: "https://kubernetes.default.svc", url: "https://kubernetes.default.svc/custom-jwks", wantRoute: "authenticated", wantURL: "https://kubernetes.example/custom-jwks"},
		{name: "arbitrary URL", issuer: "https://kubernetes.default.svc", url: "https://attacker.example/keys", wantRoute: "anonymous", wantURL: "https://attacker.example/keys"},
		{name: "proxy discovery", issuer: "https://kubernetes.default.svc", url: "https://kubernetes.default.svc/.well-known/openid-configuration", serverURL: "https://kubernetes.example/proxy", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy/.well-known/openid-configuration"},
		{name: "proxy JWKS", issuer: "https://kubernetes.default.svc", url: "https://internal.example/openid/v1/jwks", serverURL: "https://kubernetes.example/proxy/", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy/openid/v1/jwks"},
		{name: "published proxy discovery", issuer: "https://kubernetes.example/proxy", url: "https://kubernetes.example/proxy/.well-known/openid-configuration", serverURL: "https://kubernetes.example/proxy", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy/.well-known/openid-configuration"},
		{name: "published proxy JWKS", issuer: "https://kubernetes.default.svc", url: "https://kubernetes.example/proxy/openid/v1/jwks", serverURL: "https://kubernetes.example/proxy", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy/openid/v1/jwks"},
		{name: "same host published proxy JWKS", issuer: "https://kubernetes.example", url: "https://kubernetes.example/proxy/openid/v1/jwks", serverURL: "https://kubernetes.example/proxy", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy/openid/v1/jwks"},
		{name: "proxy path issuer JWKS", issuer: "https://issuer.example/tenant", url: "https://issuer.example/tenant/openid/v1/jwks", serverURL: "https://kubernetes.example/proxy", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy/openid/v1/jwks"},
		{name: "other host proxy JWKS", issuer: "https://kubernetes.default.svc", url: "https://attacker.example/proxy/openid/v1/jwks", serverURL: "https://kubernetes.example/proxy", wantRoute: "anonymous", wantURL: "https://attacker.example/proxy/openid/v1/jwks"},
		{name: "escaped proxy path", issuer: "https://kubernetes.default.svc", url: "https://kubernetes.default.svc/keys%2Fcurrent", serverURL: "https://kubernetes.example/proxy%2Fcluster", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy%2Fcluster/keys%2Fcurrent"},
		{name: "escaped issuer path", issuer: "https://issuer.example/prefix%2Ftenant", url: "https://issuer.example/prefix%2Ftenant/keys%2Fcurrent", serverURL: "https://kubernetes.example/proxy%2Fcluster", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy%2Fcluster/keys%2Fcurrent"},
		{name: "escaped trailing proxy slash", issuer: "https://issuer.example", url: "https://issuer.example/.well-known/openid-configuration", serverURL: "https://kubernetes.example/proxy%2F", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy%2F/.well-known/openid-configuration"},
		{name: "escaped trailing issuer slash", issuer: "https://issuer.example/tenant%2F", url: "https://issuer.example/tenant%2F/.well-known/openid-configuration", serverURL: "https://kubernetes.example/proxy", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy/.well-known/openid-configuration"},
		{name: "published escaped trailing proxy slash", issuer: "https://issuer.example", url: "https://kubernetes.example/proxy%2F/openid/v1/jwks", serverURL: "https://kubernetes.example/proxy%2F", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy%2F/openid/v1/jwks"},
		{name: "published proxy redirect target", issuer: "https://issuer.example/tenant", url: "https://kubernetes.example/proxy/discovery.json", serverURL: "https://kubernetes.example/proxy", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy/discovery.json"},
		{name: "server sibling path", issuer: "https://issuer.example/tenant", url: "https://kubernetes.example/proxy-other/discovery.json", serverURL: "https://kubernetes.example/proxy", wantRoute: "anonymous", wantURL: "https://kubernetes.example/proxy-other/discovery.json"},
		{name: "server encoded parent segment", issuer: "https://issuer.example/tenant", url: "https://kubernetes.example/proxy/%2e%2e/discovery.json", serverURL: "https://kubernetes.example/proxy", wantRoute: "anonymous", wantURL: "https://kubernetes.example/proxy/%2e%2e/discovery.json"},
		{name: "server escaped traversal", issuer: "https://issuer.example/tenant", url: "https://kubernetes.example/proxy/keys%2F..%2F..%2Fdiscovery.json", serverURL: "https://kubernetes.example/proxy", wantRoute: "anonymous", wantURL: "https://kubernetes.example/proxy/keys%2F..%2F..%2Fdiscovery.json"},
		{name: "issuer parent segment", issuer: "https://issuer.example/tenant", url: "https://issuer.example/tenant/../discovery.json", serverURL: "https://kubernetes.example/proxy", wantRoute: "anonymous", wantURL: "https://issuer.example/tenant/../discovery.json"},
		{name: "discovery query", issuer: "https://issuer.example/prefix", url: "https://issuer.example/prefix/.well-known/openid-configuration?hint=a%2Fb", serverURL: "https://kubernetes.example/proxy", wantRoute: "authenticated", wantURL: "https://kubernetes.example/proxy/.well-known/openid-configuration?hint=a%2Fb"},
		{name: "proxy anonymous URL", issuer: "https://kubernetes.default.svc", url: "https://attacker.example/keys", serverURL: "https://kubernetes.example/proxy", wantRoute: "anonymous", wantURL: "https://attacker.example/keys"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gotRoute, gotURL := "", ""
			response := func(route string) roundTripFunc {
				return func(req *http.Request) (*http.Response, error) {
					gotRoute = route
					gotURL = req.URL.String()
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(nil), Header: make(http.Header)}, nil
				}
			}
			issuerURL, err := url.Parse(tt.issuer)
			if err != nil {
				t.Fatal(err)
			}
			if tt.serverURL == "" {
				tt.serverURL = "https://kubernetes.example"
			}
			serverURL, err := url.Parse(tt.serverURL)
			if err != nil {
				t.Fatal(err)
			}
			transport := &kubeconfigIssuerTransport{
				base:       response("anonymous"),
				kubernetes: response("authenticated"),
				issuerURL:  issuerURL,
				serverURL:  serverURL,
			}
			req, err := http.NewRequest(http.MethodGet, tt.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			if err := resp.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if gotRoute != tt.wantRoute {
				t.Fatalf("route = %q, want %q", gotRoute, tt.wantRoute)
			}
			if gotURL != tt.wantURL {
				t.Fatalf("request URL = %q, want %q", gotURL, tt.wantURL)
			}
		})
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
