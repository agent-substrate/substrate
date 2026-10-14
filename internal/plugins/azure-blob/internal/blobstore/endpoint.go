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
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// defaultStallTimeout bounds how long a storage request may make no progress.
// It is long enough for Azure to answer a 4 MiB block upload or a list page,
// and short enough that a stalled endpoint fails an RPC with Unavailable
// after the SDK's retries instead of holding its caller indefinitely.
const defaultStallTimeout = time.Minute

// endpointTransport sends requests only to the configured storage account
// endpoint and never follows redirects. The SDK attaches the plugin's storage
// credential to every request a client makes, so a request to any other
// scheme or host would hand that credential to it.
//
// It also cancels a request that makes no progress for stallTimeout: while
// it sends the request body, while it waits for the response headers after
// the last body read, or during any one read of the response body. Time the
// caller spends between response body reads does not count, so a slow
// consumer of a long download is not cut off.
type endpointTransport struct {
	scheme       string
	host         string
	client       *http.Client
	stallTimeout time.Duration
}

func newEndpointTransport(endpoint *url.URL, base http.RoundTripper, stallTimeout time.Duration) *endpointTransport {
	return &endpointTransport{
		scheme: strings.ToLower(endpoint.Scheme),
		host:   strings.ToLower(endpoint.Host),
		client: &http.Client{
			Transport: base,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		stallTimeout: stallTimeout,
	}
}

// Do implements policy.Transporter.
func (t *endpointTransport) Do(req *http.Request) (*http.Response, error) {
	if !strings.EqualFold(req.URL.Scheme, t.scheme) || !strings.EqualFold(req.URL.Host, t.host) {
		return nil, &foreignEndpointError{scheme: req.URL.Scheme, host: req.URL.Host, want: t.scheme + "://" + t.host}
	}
	ctx, cancel := context.WithCancel(req.Context())
	watch := newStallWatch(t.stallTimeout, cancel)
	out := req.WithContext(ctx)
	if req.Body != nil && req.Body != http.NoBody {
		out.Body = &requestBody{ReadCloser: req.Body, watch: watch}
	}
	resp, err := t.client.Do(out)
	watch.responded()
	if err != nil {
		cancel()
		if watch.isStalled() {
			return nil, &stallError{timeout: t.stallTimeout}
		}
		return nil, err
	}
	resp.Body = &responseBody{body: resp.Body, watch: watch}
	return resp, nil
}

// stallWatch cancels a request's context when its timer fires. The timer runs
// from the start of the request until its response headers arrive, restarting
// at each read of the request body, and then only during response body reads.
type stallWatch struct {
	timeout time.Duration
	cancel  context.CancelFunc

	mu          sync.Mutex
	timer       *time.Timer
	gotResponse bool
	stalled     bool
}

func newStallWatch(timeout time.Duration, cancel context.CancelFunc) *stallWatch {
	w := &stallWatch{timeout: timeout, cancel: cancel}
	w.timer = time.AfterFunc(timeout, w.fire)
	return w
}

func (w *stallWatch) fire() {
	w.mu.Lock()
	w.stalled = true
	w.mu.Unlock()
	w.cancel()
}

// requestProgress restarts the timer after the transport reads more of the
// request body, unless the response has already arrived.
func (w *stallWatch) requestProgress() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.gotResponse {
		w.timer.Reset(w.timeout)
	}
}

// responded stops the timer once the transport returns a response or error.
func (w *stallWatch) responded() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.gotResponse = true
	w.timer.Stop()
}

func (w *stallWatch) startRead() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.timer.Reset(w.timeout)
}

func (w *stallWatch) endRead() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.timer.Stop()
}

func (w *stallWatch) isStalled() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stalled
}

// requestBody reports each read of the request body to the stall watch.
type requestBody struct {
	io.ReadCloser
	watch *stallWatch
}

func (b *requestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.watch.requestProgress()
	return n, err
}

// responseBody times each read of the response body and releases the
// request's context when closed.
type responseBody struct {
	body  io.ReadCloser
	watch *stallWatch
}

func (b *responseBody) Read(p []byte) (int, error) {
	b.watch.startRead()
	n, err := b.body.Read(p)
	b.watch.endRead()
	if err != nil && err != io.EOF && b.watch.isStalled() {
		// Returned unwrapped: the SDK's download retry reader resumes only
		// after an error that is itself a net.Error.
		return n, &stallError{timeout: b.watch.timeout}
	}
	return n, err
}

func (b *responseBody) Close() error {
	b.watch.endRead()
	err := b.body.Close()
	b.watch.cancel()
	return err
}

// stallError reports a request canceled for making no progress. It is a
// timeout net.Error, so the SDK retries the request or resumes the download,
// and the plugin reports Unavailable once those retries are spent.
type stallError struct {
	timeout time.Duration
}

func (e *stallError) Error() string {
	return fmt.Sprintf("storage request made no progress for %s", e.timeout)
}

func (*stallError) Timeout() bool   { return true }
func (*stallError) Temporary() bool { return true }

var _ net.Error = (*stallError)(nil)

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
