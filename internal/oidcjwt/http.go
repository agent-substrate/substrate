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
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// NewHTTPClient returns a client for OIDC discovery and JWKS requests.
func NewHTTPClient(issuer, certificateAuthorityFile, discoveryTokenFile string) (*http.Client, error) {
	if discoveryTokenFile != "" && certificateAuthorityFile == "" {
		return nil, fmt.Errorf("discovery token file requires a certificate authority file")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if certificateAuthorityFile != "" {
		ca, err := os.ReadFile(certificateAuthorityFile)
		if err != nil {
			return nil, fmt.Errorf("read certificate authority file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("certificate authority file %q contains no certificates", certificateAuthorityFile)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool}
	}
	var roundTripper http.RoundTripper = transport
	if discoveryTokenFile != "" {
		roundTripper = &issuerDiscoveryTransport{base: transport, tokenFile: discoveryTokenFile, issuer: issuer}
	}
	return &http.Client{Timeout: 10 * time.Second, Transport: roundTripper}, nil
}

// issuerDiscoveryTransport sends the token in tokenFile as a bearer token on
// requests under issuer and on the HTTPS jwks_uri from issuer's discovery
// document. A rotated token is used from the next request.
type issuerDiscoveryTransport struct {
	base      http.RoundTripper
	tokenFile string
	issuer    string
}

func (t *issuerDiscoveryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// A Kubernetes API server's jwks_uri uses its advertised address, which is
	// usually not the issuer's host.
	if issuerScopedURL(req.URL.String(), t.issuer) || isDiscoveredJWKSURL(req) {
		token, err := os.ReadFile(t.tokenFile)
		if err != nil {
			return nil, fmt.Errorf("read discovery token file: %w", err)
		}
		trimmed := strings.TrimSpace(string(token))
		if trimmed == "" {
			return nil, fmt.Errorf("discovery token file %q is empty", t.tokenFile)
		}
		req = req.Clone(req.Context())
		req.Header.Set("Authorization", "Bearer "+trimmed)
	}
	return t.base.RoundTrip(req)
}

func issuerScopedURL(rawURL, issuer string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	issuerURL, err := url.Parse(issuer)
	if err != nil {
		return false
	}
	if !strings.EqualFold(u.Scheme, issuerURL.Scheme) || !strings.EqualFold(u.Host, issuerURL.Host) {
		return false
	}
	issuerPath := strings.TrimRight(issuerURL.EscapedPath(), "/")
	if issuerPath == "" {
		issuerPath = "/"
	}
	requestPath := u.EscapedPath()
	if issuerPath == "/" {
		return strings.HasPrefix(requestPath, "/")
	}
	return requestPath == issuerPath || strings.HasPrefix(requestPath, issuerPath+"/")
}

type discoveredJWKSURLKey struct{}

// withDiscoveredJWKSURL returns a copy of ctx in which issuerDiscoveryTransport
// sends the discovery token to jwksURL, which must be the jwks_uri from the
// issuer's discovery document.
func withDiscoveredJWKSURL(ctx context.Context, jwksURL string) context.Context {
	return context.WithValue(ctx, discoveredJWKSURLKey{}, jwksURL)
}

// isDiscoveredJWKSURL reports whether req is an HTTPS request for exactly the
// URL that withDiscoveredJWKSURL set on its context.
func isDiscoveredJWKSURL(req *http.Request) bool {
	jwksURL, ok := req.Context().Value(discoveredJWKSURLKey{}).(string)
	if !ok {
		return false
	}
	u, err := url.Parse(jwksURL)
	if err != nil {
		return false
	}
	// A redirect keeps the context but changes the URL, so it does not match.
	return strings.EqualFold(req.URL.Scheme, "https") && req.URL.String() == u.String()
}
