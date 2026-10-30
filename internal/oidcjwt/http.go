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
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/credbundle"
)

// NewHTTPClient returns a client for OIDC discovery and JWKS requests.
//
// certificateAuthorityFile, when set, replaces the system roots. The client
// re-reads the file when it changes and verifies new connections against its
// current contents. A failed re-read keeps the last pool and logs a warning.
func NewHTTPClient(issuer, certificateAuthorityFile, discoveryTokenFile string) (*http.Client, error) {
	if discoveryTokenFile != "" && certificateAuthorityFile == "" {
		return nil, fmt.Errorf("discovery token file requires a certificate authority file")
	}
	var roundTripper http.RoundTripper = http.DefaultTransport.(*http.Transport).Clone()
	if certificateAuthorityFile != "" {
		var err error
		roundTripper, err = newRootsTransport(roundTripper.(*http.Transport), credbundle.PoolLoader(certificateAuthorityFile))
		if err != nil {
			return nil, err
		}
	}
	if discoveryTokenFile != "" {
		roundTripper = &issuerDiscoveryTransport{base: roundTripper, tokenFile: discoveryTokenFile, issuer: issuer}
	}
	return &http.Client{Timeout: 10 * time.Second, Transport: roundTripper}, nil
}

// rootsTransport verifies servers against the current contents of a CA file.
// A tls.Config's RootCAs can't change once the config is in use, so it builds
// a new transport from base whenever loadRoots returns a different pool.
type rootsTransport struct {
	base      *http.Transport
	loadRoots func() (*x509.CertPool, error)

	mu        sync.Mutex
	roots     *x509.CertPool
	transport *http.Transport
}

// newRootsTransport loads the pool once so an unreadable CA file fails here
// rather than on the first request.
func newRootsTransport(base *http.Transport, loadRoots func() (*x509.CertPool, error)) (*rootsTransport, error) {
	roots, err := loadRoots()
	if err != nil {
		return nil, fmt.Errorf("read certificate authority file: %w", err)
	}
	t := &rootsTransport{base: base, loadRoots: loadRoots}
	t.use(roots)
	return t, nil
}

func (t *rootsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.current(req.Context()).RoundTrip(req)
}

// current returns the transport for the CA file's current pool, or the last
// one when the file can't be read, such as mid-rotation. loadRoots runs under
// mu so concurrent requests can't swap back to an older pool.
func (t *rootsTransport) current(ctx context.Context) *http.Transport {
	t.mu.Lock()
	defer t.mu.Unlock()
	roots, err := t.loadRoots()
	if err != nil {
		slog.WarnContext(ctx, "Could not re-read the certificate authority file; using the last one", slog.Any("err", err))
		return t.transport
	}
	t.use(roots)
	return t.transport
}

// use switches to a transport that trusts roots, unless it already does.
// t.mu must be held, or t not yet shared.
func (t *rootsTransport) use(roots *x509.CertPool) {
	if roots == t.roots {
		return
	}
	transport := t.base.Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots}
	if t.transport != nil {
		t.transport.CloseIdleConnections()
	}
	t.roots, t.transport = roots, transport
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
