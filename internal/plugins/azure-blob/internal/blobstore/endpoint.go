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

package blobstore

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// endpointTransport sends requests only to the configured storage account
// endpoint and never follows redirects. The SDK attaches the plugin's storage
// credential to every request a client makes, so a request to any other
// scheme or host would hand that credential to it.
type endpointTransport struct {
	scheme string
	host   string
	client *http.Client
}

func newEndpointTransport(endpoint *url.URL, base http.RoundTripper) *endpointTransport {
	return &endpointTransport{
		scheme: strings.ToLower(endpoint.Scheme),
		host:   strings.ToLower(endpoint.Host),
		client: &http.Client{
			Transport: base,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Do implements policy.Transporter.
func (t *endpointTransport) Do(req *http.Request) (*http.Response, error) {
	if !strings.EqualFold(req.URL.Scheme, t.scheme) || !strings.EqualFold(req.URL.Host, t.host) {
		return nil, &foreignEndpointError{scheme: req.URL.Scheme, host: req.URL.Host, want: t.scheme + "://" + t.host}
	}
	return t.client.Do(req)
}

type foreignEndpointError struct {
	scheme, host, want string
}

func (e *foreignEndpointError) Error() string {
	return fmt.Sprintf("refusing to send a request to %s://%s: only the configured storage account endpoint %s may receive credentials", e.scheme, e.host, e.want)
}

// NonRetriable stops the SDK retry policy from repeating the request.
func (*foreignEndpointError) NonRetriable() {}

// defaultTransport carries over the connection pool, TLS, and HTTP/2 health
// check settings of the Azure SDK's default HTTP transport, which a custom
// policy.Transporter replaces.
func defaultTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 10
	transport.TLSClientConfig = &tls.Config{
		MinVersion:    tls.VersionTLS12,
		Renegotiation: tls.RenegotiateFreelyAsClient,
	}
	transport.HTTP2 = &http.HTTP2Config{
		SendPingTimeout: 10 * time.Second,
		PingTimeout:     5 * time.Second,
	}
	return transport
}
