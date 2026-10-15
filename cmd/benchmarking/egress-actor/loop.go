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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/egresstunnels"
)

// maxBodyBytes bounds what the actor reads, from a /start body or from an
// endpoint's response.
const maxBodyBytes = 1 << 20

// server serves the egresstunnels routes. At most one loop runs at a time.
type server struct {
	// endpointURL maps an endpoint number and scheme to the URL the loop
	// requests. Tests point it at local servers.
	endpointURL func(i int, scheme string) string
	// rootCAs verifies HTTPS endpoints; nil uses the system roots, which
	// SSL_CERT_FILE extends with the gateway's MITM CA. Tests set their own.
	rootCAs *x509.CertPool
	// dial opens the loop's connections; nil uses a plain net.Dialer. Tests
	// send the in-cluster endpoint names to local listeners through it.
	dial dialFunc

	mu  sync.Mutex
	run *loopRun // nil when no loop is running
}

func newServer() *server {
	return &server{endpointURL: egresstunnels.EndpointURL}
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+egresstunnels.ReadyzRoute, func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST "+egresstunnels.StartRoute, s.handleStart)
	mux.HandleFunc("GET "+egresstunnels.StatsRoute, s.handleStats)
	mux.HandleFunc("POST "+egresstunnels.StopRoute, s.handleStop)
	return mux
}

func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	var req egresstunnels.StartRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(&req); err != nil {
		http.Error(w, "decoding start request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.run != nil {
		http.Error(w, "a loop is already running; POST "+egresstunnels.StopRoute+" first", http.StatusConflict)
		return
	}
	urls := make([]string, req.Endpoints)
	for i := range urls {
		urls[i] = s.endpointURL(i, req.URLScheme())
	}
	s.run = startLoop(req, urls, s.rootCAs, s.dial)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleStats(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	run := s.run
	s.mu.Unlock()
	if run == nil {
		http.Error(w, "no loop is running", http.StatusNotFound)
		return
	}
	writeJSON(w, run.snapshot())
}

func (s *server) handleStop(w http.ResponseWriter, _ *http.Request) {
	// Held across the wait so a /start cannot slip in before the loop exits.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.run == nil {
		http.Error(w, "no loop is running", http.StatusNotFound)
		return
	}
	stats := s.run.stop()
	s.run = nil
	writeJSON(w, stats)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// loopRun is one started loop and what it has measured.
type loopRun struct {
	cancel    context.CancelFunc
	wg        sync.WaitGroup // one goroutine per endpoint
	started   time.Time
	transport *http.Transport

	mu    sync.Mutex
	stats egresstunnels.Stats
	last  []time.Time // start of each endpoint's most recent request
}

func startLoop(req egresstunnels.StartRequest, urls []string, roots *x509.CertPool, dial dialFunc) *loopRun {
	timeoutMs := req.RequestTimeoutMs
	if timeoutMs == 0 {
		timeoutMs = egresstunnels.DefaultRequestTimeoutMs
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &loopRun{
		cancel:    cancel,
		started:   time.Now(),
		transport: newTransport(req.NewConnPerRequest, roots, dial),
	}
	r.stats.Scheme = req.URLScheme()
	r.stats.Endpoints = make([]egresstunnels.Endpoint, len(urls))
	r.last = make([]time.Time, len(urls))
	client := &http.Client{
		Transport: r.transport,
		// Count a redirect as the endpoint's answer rather than following it
		// somewhere the egress policy may not allow.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	timeout := time.Duration(timeoutMs) * time.Millisecond
	interval := time.Duration(req.IntervalMs) * time.Millisecond
	period := interval * time.Duration(len(urls))
	for i, u := range urls {
		r.wg.Go(func() { r.endpoint(ctx, client, i, u, timeout, interval*time.Duration(i), period) })
	}
	return r
}

// dialFunc opens one connection, as net.Dialer.DialContext does.
type dialFunc = func(ctx context.Context, network, addr string) (net.Conn, error)

// newTransport keeps one connection alive per endpoint, or none at all when
// newConnPerRequest is set, and verifies HTTPS endpoints against roots. It
// ignores proxy environment variables: the sandbox's egress capture is the
// only proxy under test.
func newTransport(newConnPerRequest bool, roots *x509.CertPool, dial dialFunc) *http.Transport {
	if dial == nil {
		dial = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	return &http.Transport{
		DialContext:         dial,
		TLSClientConfig:     &tls.Config{RootCAs: roots},
		ForceAttemptHTTP2:   false, // HTTP/1.1 over either scheme: one request per connection at a time
		MaxIdleConns:        0,     // no total cap, so C endpoints keep C connections
		MaxIdleConnsPerHost: 1,     // one request in flight per endpoint, so one per endpoint is enough
		// No IdleConnTimeout: a kept-alive connection the client closed for
		// idling would redial like one the gateway reset, and the gateway's
		// stream idle timeout is what the run measures.
		IdleConnTimeout:   0,
		DisableKeepAlives: newConnPerRequest,
		// No TLSHandshakeTimeout: the request's own timeout bounds the
		// handshake, so a stalled one is counted as a timeout like any other.
	}
}

// endpoint waits offset, opens endpoint i, then requests it once per period
// until ctx ends. The opens are one interval apart across endpoints, so C
// handshakes do not reach the gateway at once. At most one request is in
// flight per endpoint, and the period is counted from the start of the open:
// a request that outlasts the period delays only this endpoint, whose next
// request starts as soon as the slow one returns.
func (r *loopRun) endpoint(ctx context.Context, client *http.Client, i int, u string, timeout, offset, period time.Duration) {
	if !pause(ctx, offset) {
		return
	}
	var tick <-chan time.Time
	if period > 0 {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		tick = ticker.C
	}
	r.request(ctx, client, i, u, timeout)
	if ctx.Err() != nil {
		return // stopped mid-open: not an open
	}
	r.mu.Lock()
	r.stats.Opened++
	if r.stats.Opened == int64(len(r.stats.Endpoints)) {
		r.stats.OpeningDone = time.Since(r.started)
	}
	r.mu.Unlock()
	for ctx.Err() == nil {
		if tick != nil {
			select {
			case <-ctx.Done():
				return
			case <-tick:
			}
		}
		r.request(ctx, client, i, u, timeout)
	}
}

// pause waits d, or until ctx ends, and reports whether to go on.
func pause(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// requestTrace collects what httptrace reports about one request. The dial,
// and with it the DNS callbacks, can still be running on another goroutine
// after a timed-out request returns, hence the lock.
type requestTrace struct {
	mu         sync.Mutex
	gotConn    bool
	newConn    bool
	dnsStart   time.Time
	dns        time.Duration
	didDNS     bool
	tlsStart   time.Time
	tls        time.Duration
	tlsStarted bool
	didTLS     bool
}

func (t *requestTrace) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.gotConn, t.newConn = true, !info.Reused
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.dnsStart = time.Now()
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.dns, t.didDNS = time.Since(t.dnsStart), true
		},
		TLSHandshakeStart: func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.tlsStart, t.tlsStarted = time.Now(), true
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			t.mu.Lock()
			defer t.mu.Unlock()
			if err == nil {
				t.tls, t.didTLS = time.Since(t.tlsStart), true
			}
		},
	}
}

func (r *loopRun) request(ctx context.Context, client *http.Client, i int, u string, timeout time.Duration) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	trace := &requestTrace{}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(reqCtx, trace.clientTrace()), http.MethodGet, u, nil)
	if err != nil {
		r.record(i, 0, 0, err, trace)
		return
	}

	start := time.Now()
	r.mu.Lock()
	if last := r.last[i]; !last.IsZero() {
		gap := start.Sub(last)
		r.stats.LongestGap = max(r.stats.LongestGap, gap)
		r.stats.Endpoints[i].LongestGap = max(r.stats.Endpoints[i].LongestGap, gap)
	}
	r.last[i] = start
	r.mu.Unlock()
	status := 0
	resp, err := client.Do(req)
	if err == nil {
		status = resp.StatusCode
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		resp.Body.Close()
	}
	latency := time.Since(start)
	if ctx.Err() != nil {
		// Stopped mid-request: the failure is the stop, not the egress path.
		return
	}
	r.record(i, latency, status, err, trace)
}

func (r *loopRun) record(i int, latency time.Duration, status int, err error, trace *requestTrace) {
	trace.mu.Lock()
	newConn := trace.gotConn && trace.newConn
	didDNS, dns := trace.didDNS, trace.dns
	tlsStarted, didTLS, tlsTime := trace.tlsStarted, trace.didTLS, trace.tls
	trace.mu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.stats
	first := s.Endpoints[i].Requests == 0
	s.Requests++
	s.Endpoints[i].Requests++
	if newConn {
		s.NewConns++
	}
	if didDNS {
		s.DNS.Record(dns)
	}
	if didTLS {
		s.TLS.Record(tlsTime)
	}
	class := egresstunnels.Classify(err, status)
	if class != "" && tlsStarted && !didTLS && !strings.HasPrefix(class, "tls: ") {
		// A failure inside the handshake, such as the gateway closing a
		// connection whose SNI no rule allows, is told apart from the same
		// error in an HTTP exchange.
		class = "tls handshake: " + class
	}
	if class == "" {
		s.Successes++
		if first {
			s.Opening.Record(latency)
		} else {
			s.Latency.Record(latency)
		}
		return
	}
	if s.Errors == nil {
		s.Errors = make(map[string]int64)
	}
	s.Errors[class]++
	s.Endpoints[i].Errors++
}

// snapshot returns a copy of the stats so far.
func (r *loopRun) snapshot() *egresstunnels.Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stats.Clone()
	s.Elapsed = time.Since(r.started)
	s.Rounds = s.Endpoints[0].Requests
	for _, e := range s.Endpoints[1:] {
		s.Rounds = min(s.Rounds, e.Requests)
	}
	return s
}

// stop cancels the loop, waits for every endpoint's goroutine to exit, and
// returns the final stats.
func (r *loopRun) stop() *egresstunnels.Stats {
	r.cancel()
	r.wg.Wait()
	r.transport.CloseIdleConnections()
	return r.snapshot()
}
