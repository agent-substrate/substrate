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
	"io"
	"net/http"
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
	tokenFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokenFile, []byte("test-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	var gotAuth string
	transport := &k8sServiceAccountIssuerDiscoveryTransport{
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
	transport := &k8sServiceAccountIssuerDiscoveryTransport{
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
	transport := &k8sServiceAccountIssuerDiscoveryTransport{
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

func TestBuildK8sServiceAccountIssuerDiscoveryClientUsesDefaultTransportForExternalIssuer(t *testing.T) {
	client := buildK8sServiceAccountIssuerDiscoveryClient(t.Context(), "/does/not/matter", "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c")
	if client == nil {
		t.Fatalf("buildK8sServiceAccountIssuerDiscoveryClient() = nil, want client")
	}
	if client.Timeout == 0 {
		t.Fatalf("client timeout = 0, want nonzero timeout")
	}
	if _, ok := client.Transport.(*k8sServiceAccountIssuerDiscoveryTransport); ok {
		t.Fatalf("external issuer should not use ServiceAccount token transport")
	}
}

func TestIsInClusterKubernetesIssuer(t *testing.T) {
	for _, tt := range []struct {
		issuer string
		want   bool
	}{
		{issuer: "https://kubernetes.default.svc", want: true},
		{issuer: "https://kubernetes.default.svc.cluster.local", want: true},
		{issuer: "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c", want: false},
		{issuer: "https://attacker.example", want: false},
	} {
		t.Run(tt.issuer, func(t *testing.T) {
			if got := isInClusterKubernetesIssuer(tt.issuer); got != tt.want {
				t.Fatalf("isInClusterKubernetesIssuer(%q) = %v, want %v", tt.issuer, got, tt.want)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
