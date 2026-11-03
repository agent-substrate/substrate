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
	"path"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/credbundle"

	"k8s.io/client-go/rest"
)

const (
	oidcDiscoveryPath  = "/.well-known/openid-configuration"
	kubernetesJWKSPath = "/openid/v1/jwks"
	httpClientTimeout  = 10 * time.Second
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
	return newHTTPClient(roundTripper), nil
}

// NewHTTPClientWithRoots returns a client for OIDC discovery and JWKS
// requests that verifies servers against the pool loadRoots returns. It calls
// loadRoots for each request and builds a new transport only when the pool
// changes, as for NewHTTPClient's CA file.
func NewHTTPClientWithRoots(loadRoots func() (*x509.CertPool, error)) (*http.Client, error) {
	transport, err := newRootsTransport(http.DefaultTransport.(*http.Transport).Clone(), loadRoots)
	if err != nil {
		return nil, err
	}
	return newHTTPClient(transport), nil
}

func newHTTPClient(transport http.RoundTripper) *http.Client {
	return &http.Client{Timeout: 10 * time.Second, Transport: transport}
}

// rootsTransport verifies servers against the pool loadRoots returns.
// A tls.Config's RootCAs can't change once the config is in use, so it builds
// a new transport from base whenever loadRoots returns a different pool.
type rootsTransport struct {
	base      *http.Transport
	loadRoots func() (*x509.CertPool, error)

	mu        sync.Mutex
	roots     *x509.CertPool
	transport *http.Transport
}

// newRootsTransport loads the pool once so a pool that can't be loaded fails
// here rather than on the first request.
func newRootsTransport(base *http.Transport, loadRoots func() (*x509.CertPool, error)) (*rootsTransport, error) {
	roots, err := loadRoots()
	if err != nil {
		return nil, fmt.Errorf("while loading the CA pool: %w", err)
	}
	t := &rootsTransport{base: base, loadRoots: loadRoots}
	t.use(roots)
	return t, nil
}

func (t *rootsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.current(req.Context()).RoundTrip(req)
}

// current returns the transport for the pool loadRoots returns, or the last
// one when loadRoots fails, such as mid-rotation. loadRoots runs under
// mu so concurrent requests can't swap back to an older pool.
func (t *rootsTransport) current(ctx context.Context) *http.Transport {
	t.mu.Lock()
	defer t.mu.Unlock()
	roots, err := t.loadRoots()
	if err != nil {
		slog.WarnContext(ctx, "Could not reload the CA pool; using the last one", slog.Any("err", err))
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
	return strings.EqualFold(u.Scheme, "https") && u.EscapedPath() == kubernetesJWKSPath
}

func NewHTTPClientWithClientConfig(issuer, caFile, tokenFile string, config *rest.Config) (*http.Client, error) {
	issuerURL, err := url.Parse(issuer)
	if err != nil {
		return nil, err
	}
	if tokenFile != "" || config == nil ||
		strings.EqualFold(issuerURL.Hostname(), "accounts.google.com") ||
		strings.EqualFold(issuerURL.Hostname(), "container.googleapis.com") {
		return NewHTTPClient(issuer, caFile, tokenFile)
	}
	serverURL, err := url.Parse(config.Host)
	if err != nil || serverURL.Scheme != "https" || serverURL.Host == "" {
		return nil, fmt.Errorf("KUBECONFIG API server must be an HTTPS URL")
	}
	transport, err := rest.TransportFor(config)
	if err != nil {
		return nil, fmt.Errorf("build KUBECONFIG discovery transport: %w", err)
	}
	base := http.DefaultTransport.(*http.Transport).Clone()
	roundTripper := &kubeconfigIssuerTransport{
		base: base, kubernetes: transport, issuerURL: issuerURL, serverURL: serverURL}
	return &http.Client{Timeout: httpClientTimeout, Transport: roundTripper}, nil
}

type kubeconfigIssuerTransport struct {
	base       http.RoundTripper
	kubernetes http.RoundTripper
	issuerURL  *url.URL
	serverURL  *url.URL
}

func (t *kubeconfigIssuerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	targetURL := t.kubernetesURL(req.URL)
	if targetURL == nil {
		return t.base.RoundTrip(req)
	}
	req = req.Clone(req.Context())
	req.URL = targetURL
	req.Host = targetURL.Host
	resp, err := t.kubernetes.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	return resolveRedirectLocation(resp, targetURL), nil
}

func resolveRedirectLocation(resp *http.Response, targetURL *url.URL) *http.Response {
	// http.Client resolves redirects against the original, unmodified request.
	location := resp.Header.Get("Location")
	if location == "" {
		return resp
	}
	resolved, err := targetURL.Parse(location)
	if err == nil {
		resp.Header.Set("Location", resolved.String())
	}
	return resp
}

func (t *kubeconfigIssuerTransport) kubernetesURL(requestURL *url.URL) *url.URL {
	if requestURL.Scheme != "https" {
		return nil
	}
	targetURL := *requestURL
	prefixRawPath := ""
	switch {
	case issuerScopedURL(requestURL.String(), t.serverURL.String()):
		prefixRawPath = t.serverURL.EscapedPath()
	case isKubernetesJWKSURL(requestURL.String()):
		targetURL.Path = kubernetesJWKSPath
		targetURL.RawPath = ""
	case issuerScopedURL(requestURL.String(), t.issuerURL.String()):
		prefixRawPath = t.issuerURL.EscapedPath()
	default:
		return nil
	}
	targetURL.RawPath = replacePathPrefix(targetURL.EscapedPath(), prefixRawPath, t.serverURL.EscapedPath())
	// EscapedPath always returns valid percent-encoding.
	targetURL.Path, _ = url.PathUnescape(targetURL.RawPath)
	serverPath := strings.TrimRight(path.Clean("/"+t.serverURL.Path), "/")
	targetPath := path.Clean(targetURL.Path)
	if targetPath != serverPath && !strings.HasPrefix(targetPath, serverPath+"/") {
		return nil
	}
	targetURL.Host = t.serverURL.Host
	return &targetURL
}

func replacePathPrefix(path, oldPrefix, newPrefix string) string {
	return strings.TrimRight(newPrefix, "/") + strings.TrimPrefix(path, strings.TrimRight(oldPrefix, "/"))
}
