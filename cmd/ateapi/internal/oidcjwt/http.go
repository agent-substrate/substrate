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

// issuerDiscoveryTransport injects a bearer token for requests within the
// configured issuer and Kubernetes' standard JWKS path. Reads the token file
// on every request so rotation is handled automatically.
type issuerDiscoveryTransport struct {
	base      http.RoundTripper
	tokenFile string
	issuer    string
}

func (t *issuerDiscoveryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if issuerScopedURL(req.URL.String(), t.issuer) || isKubernetesJWKSURL(req.URL.String()) {
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

func isKubernetesJWKSURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Scheme, "https") && u.EscapedPath() == "/openid/v1/jwks"
}
