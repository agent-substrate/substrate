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

package actor

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/targetcert"
)

// target is an endpoint that counts the TCP connections opened to it.
type target struct {
	*httptest.Server
	conns    atomic.Int64
	requests atomic.Int64
}

func newTarget(t *testing.T, h http.HandlerFunc) *target {
	t.Helper()
	return newTargetConnState(t, h, nil)
}

// newTargetConnState is newTarget with onState called on every connection
// state change, after the connection count.
func newTargetConnState(t *testing.T, h http.HandlerFunc, onState func(net.Conn, http.ConnState)) *target {
	t.Helper()
	tg := &target{}
	tg.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg.requests.Add(1)
		h(w, r)
	}))
	tg.Config.ConnState = func(c net.Conn, s http.ConnState) {
		if s == http.StateNew {
			tg.conns.Add(1)
		}
		if onState != nil {
			onState(c, s)
		}
	}
	tg.Start()
	t.Cleanup(tg.Close)
	return tg
}

func okHandler(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }

// quietLog discards a test server's own error log.
var quietLog = log.New(io.Discard, "", 0)

// host is the target's host:port.
func (tg *target) host() string { return tg.Listener.Addr().String() }

// serve returns the actor's handler with endpoint i mapped to hosts[i], a
// host:port, in place of the in-cluster Service names. The URL keeps the
// scheme the loop asks for.
func serve(hosts ...string) http.Handler {
	return serveTLS(nil, hosts...)
}

// serveTLS is serve, verifying HTTPS endpoints against roots.
func serveTLS(roots *x509.CertPool, hosts ...string) http.Handler {
	s := newServer()
	s.rootCAs = roots
	s.endpointURL = func(i int, scheme string) string { return scheme + "://" + hosts[i] + "/" }
	return s.handler()
}

// serveTargets is serve over n fresh targets, one per endpoint. Endpoints
// that share one host would share the transport's single idle connection per
// host and redial on every request.
func serveTargets(t *testing.T, n int) http.Handler {
	t.Helper()
	hosts := make([]string, n)
	for i := range hosts {
		hosts[i] = newTarget(t, okHandler).host()
	}
	return serve(hosts...)
}

// newTLSTarget is newTarget serving HTTPS with httptest's certificate, which
// the returned pool trusts.
func newTLSTarget(t *testing.T, h http.HandlerFunc) (*target, *x509.CertPool) {
	t.Helper()
	tg := &target{}
	tg.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg.requests.Add(1)
		h(w, r)
	}))
	tg.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			tg.conns.Add(1)
		}
	}
	tg.Config.ErrorLog = quietLog // failed handshakes are what some tests expect
	tg.StartTLS()
	t.Cleanup(tg.Close)
	roots := x509.NewCertPool()
	roots.AddCert(tg.Certificate())
	return tg, roots
}

func post(t *testing.T, h http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, &buf))
	return rec
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// decodeStats reads a 200 answer's Stats.
func decodeStats(t *testing.T, what string, rec *httptest.ResponseRecorder) *Stats {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("%s = %d %s", what, rec.Code, rec.Body)
	}
	var s Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
		t.Fatalf("%s: decoding %s: %v", what, rec.Body, err)
	}
	return &s
}

// The poll and the stop of a running loop, each with the per-endpoint counts.
const (
	statsPath = StatsRoute + "?endpoints=1"
	stopPath  = StopRoute + "?endpoints=1"
)

// collectPath is POST /collect's path for window id w, with the per-endpoint
// counts when asked.
func collectPath(w int64, endpoints bool) string {
	path := fmt.Sprintf("%s?%s=%d", CollectRoute, WindowParam, w)
	if endpoints {
		path += "&endpoints=1"
	}
	return path
}

// start posts /start with req.
func start(t *testing.T, h http.Handler, req StartRequest) *httptest.ResponseRecorder {
	t.Helper()
	return post(t, h, StartRoute, req)
}

// readStats polls the counters.
func readStats(t *testing.T, h http.Handler) *Stats {
	t.Helper()
	return decodeStats(t, "GET /stats", get(h, statsPath))
}

// collectStats cuts window w, as the driver numbers them from 1: the counters
// and the distributions since the previous cut.
func collectStats(t *testing.T, h http.Handler, w int64) *Stats {
	t.Helper()
	return decodeStats(t, "POST /collect", post(t, h, collectPath(w, true), nil))
}

// stopLoop stops the loop and returns its final Stats, whose distributions
// cover everything since the previous cut.
func stopLoop(t *testing.T, h http.Handler) *Stats {
	t.Helper()
	return decodeStats(t, "POST /stop", post(t, h, stopPath, nil))
}

// runLoop starts a loop with req, lets it run until cond holds on the live
// counters (or fails after 10s), then stops it and returns the final Stats.
func runLoop(t *testing.T, h http.Handler, req StartRequest, cond func(*Stats) bool) *Stats {
	t.Helper()
	if rec := start(t, h, req); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	waitFor(t, h, cond)
	return stopLoop(t, h)
}

// waitFor polls the counters until cond holds, or fails after 10s.
func waitFor(t *testing.T, h http.Handler, cond func(*Stats) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := readStats(t, h)
		if cond(s) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not reached; last stats %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func atLeast(n int64) func(*Stats) bool {
	return func(s *Stats) bool { return s.Requests >= n }
}

// samples is the count of a distribution in s, 0 when absent.
func samples(s *Stats, name Distribution) uint64 {
	return s.Distributions[name].Count
}

func TestKeepAliveReusesOneConnectionPerEndpoint(t *testing.T) {
	t.Parallel()
	targets := []*target{newTarget(t, okHandler), newTarget(t, okHandler), newTarget(t, okHandler)}
	var hosts []string
	for _, tg := range targets {
		hosts = append(hosts, tg.host())
	}
	h := serve(hosts...)

	s := runLoop(t, h, StartRequest{Endpoints: len(hosts)}, func(s *Stats) bool { return s.Rounds >= 10 })
	if s.Scheme != SchemeHTTP || samples(s, TLS) != 0 || samples(s, DNS) != 0 {
		t.Errorf("a start with no scheme against IP hosts ran over %q with %d TLS handshakes and %d DNS lookups, want http with neither", s.Scheme, samples(s, TLS), samples(s, DNS))
	}

	if s.Successes != s.Requests || len(s.Errors) != 0 {
		t.Errorf("successes %d of %d requests, errors %v", s.Successes, s.Requests, s.Errors)
	}
	for i, tg := range targets {
		if got := tg.conns.Load(); got != 1 {
			t.Errorf("target %d saw %d connections, want 1 kept alive", i, got)
		}
	}
	if s.NewConns != int64(len(targets)) {
		t.Errorf("NewConns = %d, want %d (one per endpoint)", s.NewConns, len(targets))
	}
	if opening, latency := samples(s, Opening), samples(s, Latency); opening != uint64(len(targets)) || int64(opening+latency) != s.Successes {
		t.Errorf("opening %d and latency %d samples for %d successes; want one opening per endpoint and the rest latency", opening, latency, s.Successes)
	}
	for i, e := range s.Endpoints {
		if e.Requests < 10 {
			t.Errorf("endpoint %d got %d requests, want at least the %d rounds the loop was stopped at", i, e.Requests, 10)
		}
	}
}

func TestStopDuringOpeningDoesNotCountTheCancelledOpen(t *testing.T) {
	t.Parallel()
	first := newTarget(t, okHandler)
	blocked := newTarget(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // holds the second open until the client gives up
	})
	h := serve(first.host(), blocked.host())

	s := runLoop(t, h, StartRequest{Endpoints: 2}, func(s *Stats) bool { return s.Opened >= 1 })
	if s.Opened != 1 {
		t.Errorf("Opened = %d after stopping during the second open, want 1", s.Opened)
	}
	if s.Rounds != 0 || s.OpeningDone != 0 || s.Endpoints[1].Requests != 0 {
		t.Errorf("Rounds = %d, OpeningDone = %v, endpoint 1 requests = %d during the opening, want 0, 0 and 0", s.Rounds, s.OpeningDone, s.Endpoints[1].Requests)
	}
}

func TestRequestTimeoutIsClassified(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	slow := newTarget(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(release) })
	h := serve(slow.host())

	s := runLoop(t, h, StartRequest{Endpoints: 1, RequestTimeoutMs: 20}, atLeast(2))

	if s.Errors["timeout"] != s.Requests || s.Successes != 0 {
		t.Errorf("errors %v, successes %d for %d requests; want every request a timeout", s.Errors, s.Successes, s.Requests)
	}
	if n := samples(s, Latency); n != 0 {
		t.Errorf("failed requests recorded %d latency samples, want 0", n)
	}
	if s.Endpoints[0].Errors != s.Requests {
		t.Errorf("endpoint errors = %d, want %d", s.Endpoints[0].Errors, s.Requests)
	}
}

func TestNon200IsAnError(t *testing.T) {
	t.Parallel()
	ok := newTarget(t, okHandler)
	failing := newTarget(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "egress denied", http.StatusServiceUnavailable)
	})
	h := serve(ok.host(), failing.host())

	s := runLoop(t, h, StartRequest{Endpoints: 2}, atLeast(10))

	if s.Errors["HTTP 503"] == 0 {
		t.Errorf("errors = %v, want HTTP 503 counted", s.Errors)
	}
	if s.Endpoints[0].Errors != 0 || s.Endpoints[1].Errors != s.Endpoints[1].Requests {
		t.Errorf("endpoints = %+v, want only the second one failing", s.Endpoints)
	}
}

func TestStartStopLifecycle(t *testing.T) {
	t.Parallel()
	tg := newTarget(t, okHandler)
	h := serve(tg.host())
	req := StartRequest{Endpoints: 1}

	if rec := get(h, StatsRoute); rec.Code != http.StatusNotFound {
		t.Errorf("GET /stats before start = %d, want 404", rec.Code)
	}
	if rec := post(t, h, StopRoute, nil); rec.Code != http.StatusNotFound {
		t.Errorf("POST /stop before start = %d, want 404", rec.Code)
	}

	first := runLoop(t, h, req, atLeast(5))
	time.Sleep(20 * time.Millisecond)
	// A request the stop cut short can still reach the target after /stop
	// returns; nothing else may.
	if extra := tg.requests.Load() - first.Requests; extra > 1 {
		t.Errorf("target got %d requests the stopped loop did not count, want at most the one cut short by the stop", extra)
	}

	if rec := start(t, h, req); rec.Code != http.StatusNoContent {
		t.Fatalf("restart = %d %s", rec.Code, rec.Body)
	}
	if rec := start(t, h, req); rec.Code != http.StatusConflict {
		t.Errorf("second /start while running = %d, want 409", rec.Code)
	}
	second := stopLoop(t, h)
	// The target saw every request of both runs (plus any cut short by a
	// stop), so a second run that started from zero counts no more than the
	// target saw after the first run.
	if sinceRestart := tg.requests.Load() - first.Requests; second.Requests > sinceRestart {
		t.Errorf("restarted loop counted %d requests, more than the %d sent since the restart: the old counts were kept", second.Requests, sinceRestart)
	}
}

func TestStartRejectsBadRequests(t *testing.T) {
	t.Parallel()
	h := newServer().handler()
	for _, tc := range []struct{ name, body string }{
		{"not json", `not json`},
		{"empty object", `{}`},
		{"invalid", `{"endpoints":1,"scheme":"ftp"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, StartRoute, strings.NewReader(tc.body)))
			if rec.Code != http.StatusBadRequest {
				t.Errorf("POST /start %s = %d, want 400", tc.body, rec.Code)
			}
		})
	}
	if rec := get(h, ReadyzRoute); rec.Code != http.StatusOK {
		t.Errorf("GET /readyz = %d, want 200", rec.Code)
	}
}

func TestDefaultURLsAreTheEndpointServices(t *testing.T) {
	t.Parallel()
	s := newServer()
	for _, i := range []int{0, 9, 255} {
		for _, scheme := range []string{SchemeHTTP, SchemeHTTPS} {
			if got, want := s.endpointURL(i, scheme), EndpointURL(i, scheme); got != want {
				t.Errorf("endpoint %d over %s = %q, want %q", i, scheme, got, want)
			}
		}
	}
	if s.rootCAs != nil {
		t.Error("the actor's roots are set by default; they must stay nil so SSL_CERT_FILE applies")
	}
	if s.dial != nil {
		t.Error("the actor dials through a hook by default; it must stay nil so a plain dialer is used")
	}
}

func TestHTTPSFailuresAreClassified(t *testing.T) {
	t.Parallel()
	bundle, err := targetcert.Generate(time.Now(), EndpointDomain)
	if err != nil {
		t.Fatal(err)
	}
	endpointCert, err := tls.X509KeyPair(bundle.Cert, bundle.Key)
	if err != nil {
		t.Fatal(err)
	}
	endpointRoots := x509.NewCertPool()
	endpointRoots.AppendCertsFromPEM(bundle.CA)
	// Serves the endpoints' certificate, which names no IP address.
	wrongName := &target{Server: httptest.NewUnstartedServer(http.HandlerFunc(okHandler))}
	wrongName.TLS = &tls.Config{Certificates: []tls.Certificate{endpointCert}}
	wrongName.Config.ErrorLog = quietLog
	wrongName.StartTLS()
	t.Cleanup(wrongName.Close)

	untrusted, _ := newTLSTarget(t, okHandler)
	plain := newTarget(t, okHandler)

	// Accepts, reads the ClientHello so closing sends a FIN rather than a
	// reset, and closes: a peer that refuses the handshake, as the gateway
	// does for an SNI no rule allows.
	closer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closer.Close() })
	go func() {
		for {
			conn, err := closer.Accept()
			if err != nil {
				return
			}
			_, _ = conn.Read(make([]byte, 4096))
			conn.Close()
		}
	}()

	for _, tc := range []struct {
		name  string
		roots *x509.CertPool
		host  string
		want  string
	}{
		{"untrusted CA", x509.NewCertPool(), untrusted.host(), "tls: unknown authority"},
		{"hostname mismatch", endpointRoots, wrongName.host(), "tls: hostname mismatch"},
		{"plain HTTP server", x509.NewCertPool(), plain.host(), "tls: not a TLS server"},
		{"peer closes during handshake", x509.NewCertPool(), closer.Addr().String(), "tls handshake: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := serveTLS(tc.roots, tc.host)
			s := runLoop(t, h, StartRequest{Endpoints: 1, Scheme: SchemeHTTPS}, atLeast(2))
			var matched int64
			for class, n := range s.Errors {
				if strings.HasPrefix(class, tc.want) {
					matched += n
				}
			}
			if matched != s.Requests || s.Successes != 0 || samples(s, TLS) != 0 {
				t.Errorf("errors %v, %d successes, %d handshakes for %d requests; want every request classed %q", s.Errors, s.Successes, samples(s, TLS), s.Requests, tc.want)
			}
		})
	}
}

// dialTo returns a dial function that connects host:port to addrs[host]
// instead, so a loop can request the in-cluster endpoint names against local
// listeners. TLS still verifies the name the loop asked for.
func dialTo(addrs map[string]string) dialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		real, ok := addrs[host]
		if !ok {
			return nil, fmt.Errorf("no local listener for %s", addr)
		}
		return (&net.Dialer{}).DialContext(ctx, network, real)
	}
}

// targetCertServer is an HTTPS target serving the endpoints' certificate.
func targetCertServer(t *testing.T, bundle *targetcert.Bundle) *target {
	t.Helper()
	cert, err := tls.X509KeyPair(bundle.Cert, bundle.Key)
	if err != nil {
		t.Fatal(err)
	}
	tg := &target{Server: httptest.NewUnstartedServer(http.HandlerFunc(okHandler))}
	tg.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			tg.conns.Add(1)
		}
	}
	tg.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	tg.StartTLS()
	t.Cleanup(tg.Close)
	return tg
}

// TestEndpointsByNameOverBothSchemes runs the loop as the actor does in the
// cluster: the default endpoint URLs, eight endpoints, and over HTTPS the
// target's own certificate verified under each endpoint's name.
func TestEndpointsByNameOverBothSchemes(t *testing.T) {
	t.Parallel()
	const c = 8
	bundle, err := targetcert.Generate(time.Now(), EndpointDomain)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle.CA) {
		t.Fatal("no CA in the bundle")
	}
	for _, scheme := range []string{SchemeHTTP, SchemeHTTPS} {
		t.Run(scheme, func(t *testing.T) {
			t.Parallel()
			addrs := make(map[string]string, c)
			targets := make([]*target, c)
			for i := range c {
				if scheme == SchemeHTTPS {
					targets[i] = targetCertServer(t, bundle)
				} else {
					targets[i] = newTarget(t, okHandler)
				}
				addrs[EndpointHost(i)] = targets[i].host()
			}
			s := newServer()
			s.rootCAs = roots
			s.dial = dialTo(addrs)
			h := s.handler()

			const interval = 2 * time.Millisecond
			st := runLoop(t, h, StartRequest{Endpoints: c, Scheme: scheme, IntervalMs: int64(interval / time.Millisecond)},
				func(s *Stats) bool { return s.Rounds >= 3 })

			if st.Scheme != scheme || st.Opened != c || st.Rounds < 3 {
				t.Errorf("Scheme = %q, Opened = %d, Rounds = %d, want %s, %d and at least 3", st.Scheme, st.Opened, st.Rounds, scheme, c)
			}
			if st.Successes != st.Requests || len(st.Errors) != 0 {
				t.Errorf("successes %d of %d requests, errors %v", st.Successes, st.Requests, st.Errors)
			}
			if st.NewConns != c {
				t.Errorf("NewConns = %d, want one per endpoint (%d)", st.NewConns, c)
			}
			wantTLS := uint64(0)
			if scheme == SchemeHTTPS {
				wantTLS = c
			}
			if n := samples(st, TLS); n != wantTLS {
				t.Errorf("TLS handshakes = %d, want %d", n, wantTLS)
			}
			for i, tg := range targets {
				if got := tg.conns.Load(); got != 1 {
					t.Errorf("endpoint %d saw %d connections, want 1 kept alive", i, got)
				}
			}
			// Every endpoint is requested once per period of C intervals; a gap
			// is about one period, never anywhere near Elapsed.
			if period := c * interval; st.LongestGap < period/2 || st.LongestGap > 5*time.Second {
				t.Errorf("LongestGap = %v, want about one period (%v) and well under Elapsed", st.LongestGap, period)
			}
		})
	}
}

// TestResetConnectionIsRedialedWithoutAnError pins how a tunnel the gateway
// reset shows up: the loop redials, net/http retries the GET, and NewConns
// grows by one while Errors stays empty.
func TestResetConnectionIsRedialedWithoutAnError(t *testing.T) {
	t.Parallel()
	var cutOnce atomic.Bool
	cut := newTargetConnState(t, okHandler, func(c net.Conn, s http.ConnState) {
		// Closes the server side once the first response is out, as the
		// gateway does to an idle tunnel.
		if s == http.StateIdle && cutOnce.CompareAndSwap(false, true) {
			c.Close()
		}
	})
	steady := []*target{newTarget(t, okHandler), newTarget(t, okHandler)}
	h := serve(steady[0].host(), cut.host(), steady[1].host())

	s := runLoop(t, h, StartRequest{Endpoints: 3, IntervalMs: 2}, func(s *Stats) bool { return s.Rounds >= 2 })

	if s.Successes != s.Requests || len(s.Errors) != 0 {
		t.Errorf("successes %d of %d requests, errors %v; want the reset absorbed", s.Successes, s.Requests, s.Errors)
	}
	if s.NewConns != 4 {
		t.Errorf("NewConns = %d, want 4: one per endpoint plus the redial", s.NewConns)
	}
	if got := cut.conns.Load(); got != 2 {
		t.Errorf("the cut endpoint saw %d connections, want 2", got)
	}
	for i, tg := range steady {
		if got := tg.conns.Load(); got != 1 {
			t.Errorf("steady endpoint %d saw %d connections, want 1", i, got)
		}
	}
}

// pipeNet serves a handler over in-memory connections, so a loop can run
// inside a synctest bubble, where a real socket would not count as blocked.
type pipeNet struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
	srv   *http.Server
}

func newPipeNet(h http.Handler) *pipeNet {
	p := &pipeNet{conns: make(chan net.Conn), done: make(chan struct{}), srv: &http.Server{Handler: h, ErrorLog: quietLog}}
	go p.srv.Serve(p)
	return p
}

func (p *pipeNet) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case p.conns <- server:
		return client, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *pipeNet) Accept() (net.Conn, error) {
	select {
	case c := <-p.conns:
		return c, nil
	case <-p.done:
		return nil, net.ErrClosed
	}
}

func (p *pipeNet) Close() error {
	p.once.Do(func() { close(p.done) })
	return nil
}

func (p *pipeNet) Addr() net.Addr { return pipeAddr{} }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// startPipeLoop serves the actor over pn, with endpoint i named endpoint-i,
// and starts a loop with req. It returns the handler and a counters reader
// that waits for the bubble to settle first.
func startPipeLoop(t *testing.T, pn *pipeNet, req StartRequest) (http.Handler, func() *Stats) {
	t.Helper()
	s := newServer()
	s.dial = pn.dial
	s.endpointURL = func(i int, scheme string) string { return fmt.Sprintf("%s://endpoint-%d/", scheme, i) }
	h := s.handler()
	if rec := start(t, h, req); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	return h, func() *Stats {
		synctest.Wait()
		return readStats(t, h)
	}
}

// TestPacingAndGapsAreExact runs the loop on a fake clock with instant
// endpoints, so the schedule can be checked to the millisecond: endpoint i
// opens i intervals in, each endpoint is then requested once per period of C
// intervals, and so every gap is one period. Each step cuts a window, and the
// windows add up to one opening per opened endpoint and the rest latency.
func TestPacingAndGapsAreExact(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet(http.HandlerFunc(okHandler))
		defer pn.srv.Close()
		const c, interval = 4, 100 * time.Millisecond
		const period = c * interval
		start := time.Now()
		h, _ := startPipeLoop(t, pn, StartRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond)})
		var opening, latency uint64
		for step, tc := range []struct {
			at                       time.Duration
			requests, opened, rounds int64
			openingDone, longestGap  time.Duration
			why                      string
		}{
			{interval / 2, 1, 1, 0, 0, 0, "endpoint 0 opens at once, the others wait their turn"},
			{period - interval/2, c, c, 1, (c - 1) * interval, 0, "one open per interval; the last completes at (C-1) intervals"},
			{2*period - interval/2, 2 * c, c, 2, (c - 1) * interval, period, "each endpoint's second request comes one period after its first"},
			{3*period - interval/2, 3 * c, c, 3, (c - 1) * interval, period, "steady state: every gap is one period"},
		} {
			time.Sleep(tc.at - time.Since(start))
			synctest.Wait()
			st := collectStats(t, h, int64(step+1))
			if st.Requests != tc.requests || st.Opened != tc.opened || st.Rounds != tc.rounds || st.OpeningDone != tc.openingDone || st.LongestGap != tc.longestGap {
				t.Errorf("at %v: requests %d, opened %d, rounds %d, opening done %v, longest gap %v; want %d, %d, %d, %v, %v (%s)",
					tc.at, st.Requests, st.Opened, st.Rounds, st.OpeningDone, st.LongestGap, tc.requests, tc.opened, tc.rounds, tc.openingDone, tc.longestGap, tc.why)
			}
			opening += samples(st, Opening)
			latency += samples(st, Latency)
			if int64(opening) != tc.opened || int64(opening+latency) != st.Successes {
				t.Errorf("at %v: %d opening and %d latency samples so far for %d successes; want one opening per opened endpoint (%d) and the rest latency",
					tc.at, opening, latency, st.Successes, tc.opened)
			}
		}
		synctest.Wait()
		for i, e := range readStats(t, h).Endpoints {
			if e.LongestGap != period {
				t.Errorf("endpoint %d: longest gap %v, want one period (%v)", i, e.LongestGap, period)
			}
		}
		stopLoop(t, h)
	})
}

// TestOffsetShiftsEveryEndpoint pins that offsetMs delays the whole schedule
// and nothing else: endpoint 0 opens at the offset, the last endpoint at the
// offset plus C-1 intervals, and every gap is still one period.
func TestOffsetShiftsEveryEndpoint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet(http.HandlerFunc(okHandler))
		defer pn.srv.Close()
		const c, interval, offset = 4, 100 * time.Millisecond, 50 * time.Millisecond
		const period = c * interval
		start := time.Now()
		h, stats := startPipeLoop(t, pn, StartRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond), OffsetMs: int64(offset / time.Millisecond)})
		for _, tc := range []struct {
			at                       time.Duration
			requests, opened, rounds int64
			openingDone, longestGap  time.Duration
			why                      string
		}{
			{offset / 2, 0, 0, 0, 0, 0, "nothing opens before the offset"},
			{offset + interval/2, 1, 1, 0, 0, 0, "endpoint 0 opens at the offset"},
			{offset + period - interval/2, c, c, 1, offset + (c-1)*interval, 0, "the last open completes at the offset plus C-1 intervals"},
			{offset + 2*period - interval/2, 2 * c, c, 2, offset + (c-1)*interval, period, "each repeat comes one period after the open it follows"},
		} {
			time.Sleep(tc.at - time.Since(start))
			st := stats()
			if st.Requests != tc.requests || st.Opened != tc.opened || st.Rounds != tc.rounds || st.OpeningDone != tc.openingDone || st.LongestGap != tc.longestGap {
				t.Errorf("at %v: requests %d, opened %d, rounds %d, opening done %v, longest gap %v; want %d, %d, %d, %v, %v (%s)",
					tc.at, st.Requests, st.Opened, st.Rounds, st.OpeningDone, st.LongestGap, tc.requests, tc.opened, tc.rounds, tc.openingDone, tc.longestGap, tc.why)
			}
		}
		for i, e := range stats().Endpoints {
			if e.LongestGap != period {
				t.Errorf("endpoint %d: longest gap %v, want one period (%v)", i, e.LongestGap, period)
			}
		}
		stopLoop(t, h)
	})
}

// TestSlowEndpointDelaysOnlyItself pins that a request hanging until its
// timeout on one endpoint leaves the other endpoints' gaps at one period, and
// that the hung endpoint's own gap is the timeout: its next request starts as
// soon as the slow one returns.
func TestSlowEndpointDelaysOnlyItself(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Host == "endpoint-0" {
				<-r.Context().Done()
				return
			}
			okHandler(w, r)
		}))
		defer pn.srv.Close()
		const c, interval, timeout = 3, 50 * time.Millisecond, 300 * time.Millisecond
		const period = c * interval
		h, stats := startPipeLoop(t, pn, StartRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond), RequestTimeoutMs: int64(timeout / time.Millisecond)})

		time.Sleep(time.Second + interval/2)
		st := stats()
		for i := 1; i < c; i++ {
			if got := st.Endpoints[i].LongestGap; got != period {
				t.Errorf("endpoint %d: longest gap %v, want one period (%v) with endpoint 0 hung", i, got, period)
			}
			if st.Endpoints[i].Errors != 0 {
				t.Errorf("endpoint %d: %d errors, want 0", i, st.Endpoints[i].Errors)
			}
		}
		if st.Endpoints[0].LongestGap != timeout || st.LongestGap != timeout {
			t.Errorf("endpoint 0 longest gap %v, overall %v; want the timeout (%v) for both", st.Endpoints[0].LongestGap, st.LongestGap, timeout)
		}
		if st.Errors["timeout"] == 0 || st.Errors["timeout"] != st.Endpoints[0].Errors {
			t.Errorf("errors %v, endpoint 0 errors %d; want every timeout on endpoint 0", st.Errors, st.Endpoints[0].Errors)
		}
		if st.Opened != c || st.OpeningDone != timeout {
			t.Errorf("Opened = %d, OpeningDone = %v; want %d and %v: the hung open counts once it times out, and it completes last", st.Opened, st.OpeningDone, c, timeout)
		}
		stopLoop(t, h)
	})
}

// TestSlowOpensDoNotDelayOpenedEndpoints pins the schedule when opens are
// slow, as they are while a gateway works through a fleet's handshakes: an
// endpoint already opened is requested one period after its open while the
// others are still opening, and a slow open stretches only its own
// endpoint's first gap, to the open's latency.
func TestSlowOpensDoNotDelayOpenedEndpoints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const c, interval, openLatency = 4, 100 * time.Millisecond, 500 * time.Millisecond
		const period = c * interval
		var mu sync.Mutex
		opened := map[string]bool{}
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			first := !opened[r.Host]
			opened[r.Host] = true
			mu.Unlock()
			if first && r.Host != "endpoint-0" {
				select {
				case <-time.After(openLatency):
				case <-r.Context().Done():
					return
				}
			}
			okHandler(w, r)
		}))
		defer pn.srv.Close()
		start := time.Now()
		h, stats := startPipeLoop(t, pn, StartRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond)})

		// endpoint 0 opened at once; its second request is due at one period,
		// while endpoints 1 to 3 are still inside their first.
		time.Sleep(period + interval/2)
		st := stats()
		if st.Opened != 1 || st.Endpoints[0].Requests != 2 || st.Endpoints[0].LongestGap != period || st.Requests != 2 {
			t.Errorf("at %v: opened %d, endpoint 0 %d requests with gap %v, %d requests in all; want 1, 2, %v, 2: the opened endpoint keeps its period while the others open",
				time.Since(start), st.Opened, st.Endpoints[0].Requests, st.Endpoints[0].LongestGap, st.Requests, period)
		}

		// The last open, endpoint 3's, started at 3 intervals and took
		// openLatency; after that each slow endpoint's next request came
		// at once, since its period had already passed.
		time.Sleep(3*period + interval/2 - time.Since(start))
		st = stats()
		if st.Opened != c || st.OpeningDone != (c-1)*interval+openLatency {
			t.Errorf("Opened = %d, OpeningDone = %v; want %d and %v", st.Opened, st.OpeningDone, c, (c-1)*interval+openLatency)
		}
		if st.Endpoints[0].LongestGap != period || st.Endpoints[0].Requests != 4 {
			t.Errorf("endpoint 0: longest gap %v, %d requests; want %v and 4: the slow opens of the others did not touch it", st.Endpoints[0].LongestGap, st.Endpoints[0].Requests, period)
		}
		for i := 1; i < c; i++ {
			if got := st.Endpoints[i].LongestGap; got != openLatency {
				t.Errorf("endpoint %d: longest gap %v, want its own open latency (%v)", i, got, openLatency)
			}
		}
		final := stopLoop(t, h)
		if op := final.Distributions[Opening]; st.LongestGap != openLatency || op.Count != c || op.Max != openLatency.Seconds() {
			t.Errorf("LongestGap = %v, opening samples %d with max %v; want %v, %d and %v", st.LongestGap, op.Count, op.Max, openLatency, c, openLatency.Seconds())
		}
	})
}

// TestSlowRequestResumesOnTheNextTick pins what a request longer than the
// period does to its own endpoint: the next request starts as soon as the
// slow one returns, so the gap is the request's latency, not latency plus a
// period.
func TestSlowRequestResumesOnTheNextTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const period, latency = 50 * time.Millisecond, 120 * time.Millisecond
		var mu sync.Mutex
		var starts []time.Duration
		start := time.Now()
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			starts = append(starts, time.Since(start))
			mu.Unlock()
			select {
			case <-time.After(latency):
				okHandler(w, r)
			case <-r.Context().Done():
			}
		}))
		defer pn.srv.Close()
		h, stats := startPipeLoop(t, pn, StartRequest{Endpoints: 1, IntervalMs: int64(period / time.Millisecond)})

		time.Sleep(4*latency + period/2)
		st := stats()
		mu.Lock()
		got := slices.Clone(starts)
		mu.Unlock()
		want := []time.Duration{0, latency, 2 * latency, 3 * latency, 4 * latency}
		if !slices.Equal(got, want) {
			t.Errorf("request starts = %v, want %v: back to back once each outlasts the period", got, want)
		}
		if st.LongestGap != latency {
			t.Errorf("LongestGap = %v, want the request latency (%v)", st.LongestGap, latency)
		}
		stopLoop(t, h)
	})
}

// TestStopWithRequestsInFlightReturnsAtOnce pins that a stop neither waits
// for the in-flight requests' timeouts nor leaks their goroutines.
func TestStopWithRequestsInFlightReturnsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
		defer pn.srv.Close()
		const c = 8
		h, _ := startPipeLoop(t, pn, StartRequest{Endpoints: c, RequestTimeoutMs: 5000})
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()

		before := time.Now()
		st := stopLoop(t, h)
		if waited := time.Since(before); waited != 0 {
			t.Errorf("/stop took %v with %d requests in flight, want 0: the stop must not wait for a timeout", waited, c)
		}
		if st.Requests != 0 || st.Opened != 0 || len(st.Distributions) != 0 {
			t.Errorf("Requests = %d, Opened = %d, distributions %v after stopping mid-open, want 0, 0 and none", st.Requests, st.Opened, st.Distributions)
		}
		if rec := get(h, StatsRoute); rec.Code != http.StatusNotFound {
			t.Errorf("GET /stats after stop = %d, want 404", rec.Code)
		}
	})
}

// TestTwoCutsPartitionTheRun pins what a window is: each /collect answers
// with the samples since the previous one, so the windows of a run add up to
// the whole, and a window's min and max come from its own samples alone.
func TestTwoCutsPartitionTheRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const c, interval = 2, 100 * time.Millisecond
		const fast, slow = 10 * time.Millisecond, 100 * time.Millisecond
		var delay atomic.Int64
		delay.Store(int64(fast))
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(time.Duration(delay.Load())):
				okHandler(w, r)
			case <-r.Context().Done():
			}
		}))
		defer pn.srv.Close()
		start := time.Now()
		h, _ := startPipeLoop(t, pn, StartRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond)})
		cut := func(w int64, at time.Duration) *Stats {
			time.Sleep(at - time.Since(start))
			synctest.Wait()
			return collectStats(t, h, w)
		}

		// Two opens and one repeat each, all at the fast latency.
		first := cut(1, 350*time.Millisecond)
		if op, l := first.Distributions[Opening], first.Distributions[Latency]; first.Successes != 4 || op.Count != 2 || l.Count != 2 || l.Min != fast.Seconds() || l.Max != fast.Seconds() {
			t.Errorf("first window: %d successes, opening %d, latency %d in [%v, %v]; want 4, 2, 2 in [%v, %v]", first.Successes, op.Count, l.Count, l.Min, l.Max, fast.Seconds(), fast.Seconds())
		}

		// Four repeats at the slow latency: the second window must not
		// remember the fast ones, and a /stats poll inside it must not
		// consume them: only /collect cuts.
		delay.Store(int64(slow))
		time.Sleep(650*time.Millisecond - time.Since(start))
		synctest.Wait()
		if polled := readStats(t, h); polled.Successes != 6 || polled.Distributions != nil {
			t.Errorf("/stats inside the second window: %d successes, distributions %v; want 6 and none", polled.Successes, polled.Distributions)
		}
		second := cut(2, 850*time.Millisecond)
		if _, ok := second.Distributions[Opening]; ok {
			t.Errorf("second window has an opening distribution: %+v", second.Distributions[Opening])
		}
		if l := second.Distributions[Latency]; second.Successes != 8 || l.Count != 4 || l.Min != slow.Seconds() || l.Max != slow.Seconds() {
			t.Errorf("second window: %d successes, latency %d in [%v, %v]; want 8, 4 in [%v, %v]", second.Successes, l.Count, l.Min, l.Max, slow.Seconds(), slow.Seconds())
		}

		// Three more repeats, then the stop's tail window.
		time.Sleep(1150*time.Millisecond - time.Since(start))
		synctest.Wait()
		final := stopLoop(t, h)
		var total uint64
		for _, s := range []*Stats{first, second, final} {
			total += samples(s, Opening) + samples(s, Latency)
		}
		if final.Successes != 11 || int64(total) != final.Successes {
			t.Errorf("windows hold %d samples for %d successes in all; want 11 and 11", total, final.Successes)
		}
	})
}

// TestSettledWindowHasNoDNSOrTLS pins the emptiness the driver checks in a
// settled window: once every tunnel is open, a window with no opens in it
// carries no lookup and no handshake.
func TestSettledWindowHasNoDNSOrTLS(t *testing.T) {
	t.Parallel()
	byName := newTarget(t, okHandler)
	u, err := url.Parse(byName.URL)
	if err != nil {
		t.Fatal(err)
	}
	tlsTarget, roots := newTLSTarget(t, okHandler)
	for _, tc := range []struct {
		name string
		h    http.Handler
		req  StartRequest
		dist Distribution
	}{
		{"dns over http by name", serve("localhost:" + u.Port()), StartRequest{Endpoints: 1}, DNS},
		{"tls over https", serveTLS(roots, tlsTarget.host()), StartRequest{Endpoints: 1, Scheme: SchemeHTTPS}, TLS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if rec := start(t, tc.h, tc.req); rec.Code != http.StatusNoContent {
				t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
			}
			waitFor(t, tc.h, atLeast(2))
			first := collectStats(t, tc.h, 1)
			if samples(first, tc.dist) == 0 {
				t.Errorf("the window with the open has no %s sample: %+v", tc.dist, first.Distributions)
			}
			waitFor(t, tc.h, atLeast(first.Requests+2))
			second := collectStats(t, tc.h, 2)
			if p, ok := second.Distributions[tc.dist]; ok {
				t.Errorf("a window with no open carries %s: %+v", tc.dist, p)
			}
			if samples(second, Latency) == 0 || second.Successes <= first.Successes {
				t.Errorf("second window: latency %d samples, successes %d after %d; want repeats in it", samples(second, Latency), second.Successes, first.Successes)
			}
			stopLoop(t, tc.h)
		})
	}
}

func TestStopReturnsFinalStats(t *testing.T) {
	t.Parallel()
	tg := newTarget(t, okHandler)
	h := serve(tg.host())
	for _, tc := range []struct {
		name string
		rec  *httptest.ResponseRecorder
	}{
		{"GET /stats", get(h, StatsRoute)},
		{"POST /collect", post(t, h, CollectRoute, nil)},
		{"POST /stop", post(t, h, StopRoute, nil)},
	} {
		if tc.rec.Code != http.StatusNotFound || strings.TrimSpace(tc.rec.Body.String()) != NoLoopMessage {
			t.Errorf("%s with no loop = %d %q, want 404 %q", tc.name, tc.rec.Code, tc.rec.Body, NoLoopMessage)
		}
	}
	if rec := start(t, h, StartRequest{Endpoints: 1}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	waitFor(t, h, atLeast(1))
	rec := post(t, h, StopRoute, nil)
	final := decodeStats(t, "POST /stop", rec)
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" || final.Scheme != SchemeHTTP || final.Endpoints != nil || final.Requests == 0 {
		t.Errorf("POST /stop answered %q with scheme %q, endpoints %v and %d requests; want JSON, http, no endpoints unless asked, and the requests made", ct, final.Scheme, final.Endpoints, final.Requests)
	}
	for _, tc := range []struct {
		name string
		rec  *httptest.ResponseRecorder
	}{
		{"GET /stats", get(h, StatsRoute)},
		{"POST /collect", post(t, h, collectPath(1, false), nil)},
	} {
		if tc.rec.Code != http.StatusNotFound {
			t.Errorf("%s after stop = %d, want 404", tc.name, tc.rec.Code)
		}
	}

	// The final report is kept: a /stop the driver did not hear is answered
	// again, byte for byte, with or without the endpoints, until a new loop
	// replaces it.
	if again := post(t, h, StopRoute, nil); again.Code != http.StatusOK || !bytes.Equal(again.Body.Bytes(), rec.Body.Bytes()) {
		t.Errorf("second POST /stop = %d %s; want 200 and the first answer %s", again.Code, again.Body, rec.Body)
	}
	if withEP := decodeStats(t, "third POST /stop", post(t, h, stopPath, nil)); len(withEP.Endpoints) != 1 || withEP.Requests != final.Requests {
		t.Errorf("POST /stop?endpoints=1 after stop: %d endpoints, %d requests; want 1 and %d", len(withEP.Endpoints), withEP.Requests, final.Requests)
	}
	if rec := start(t, h, StartRequest{Endpoints: 1, IntervalMs: 60_000, OffsetMs: 60_000}); rec.Code != http.StatusNoContent {
		t.Fatalf("second POST /start = %d %s", rec.Code, rec.Body)
	}
	if next := stopLoop(t, h); next.Requests != 0 {
		t.Errorf("the stop after a new /start reports %d requests; want 0, the new loop's own report", next.Requests)
	}
}

// TestCollectWindowIDs drives /collect through the ids a driver sends and the
// ones it must not: the first cut is 1, the last id is answered again, the
// next id cuts, and anything else is refused with the next id named.
func TestCollectWindowIDs(t *testing.T) {
	t.Parallel()
	h := serveTargets(t, 1)
	if rec := start(t, h, StartRequest{Endpoints: 1, IntervalMs: 60_000, OffsetMs: 60_000}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	t.Cleanup(func() { stopLoop(t, h) })
	route := CollectRoute + "?" + WindowParam + "="
	// The steps run in order on one loop; each names the next id the body
	// must carry when the cut is refused.
	for _, tc := range []struct {
		name string
		path string
		want int
		next int64
	}{
		{"missing", CollectRoute, http.StatusBadRequest, 1},
		{"zero", route + "0", http.StatusBadRequest, 1},
		{"negative", route + "-1", http.StatusBadRequest, 1},
		{"non-numeric", route + "one", http.StatusBadRequest, 1},
		{"skip ahead before the first", route + "2", http.StatusConflict, 1},
		{"first cut is 1", route + "1", http.StatusOK, 0},
		{"last again", route + "1", http.StatusOK, 0},
		{"last again with endpoints", route + "1&endpoints=1", http.StatusOK, 0},
		{"next cuts", route + "2", http.StatusOK, 0},
		{"lower", route + "1", http.StatusConflict, 3},
		{"zero after cuts", route + "0", http.StatusBadRequest, 3},
		{"skip ahead", route + "4", http.StatusConflict, 3},
		{"next cuts again", route + "3", http.StatusOK, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := post(t, h, tc.path, nil)
			if rec.Code != tc.want {
				t.Fatalf("POST %s = %d %s, want %d", tc.path, rec.Code, rec.Body, tc.want)
			}
			if tc.next == 0 {
				decodeStats(t, "POST "+tc.path, rec)
				return
			}
			if want := fmt.Sprintf("the next window is %d", tc.next); !strings.Contains(rec.Body.String(), want) {
				t.Errorf("POST %s body %q does not say %q", tc.path, rec.Body, want)
			}
		})
	}
}

// TestRepeatedCollectConsumesNothing pins the retry: the same id answered
// again is the same bytes, the sample recorded between the two answers not
// in it, and that sample is in the next window, not lost.
func TestRepeatedCollectConsumesNothing(t *testing.T) {
	t.Parallel()
	run, err := newLoopRun(StartRequest{Endpoints: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.rec.shutdown(context.Background()) })
	s := newServer()
	s.run = run
	h := s.handler()
	run.record(0, 5*time.Millisecond, http.StatusOK, nil, &requestTrace{})
	first := post(t, h, collectPath(1, false), nil)
	run.record(0, 7*time.Millisecond, http.StatusOK, nil, &requestTrace{})
	again := post(t, h, collectPath(1, false), nil)
	if first.Code != http.StatusOK || again.Code != http.StatusOK || !bytes.Equal(first.Body.Bytes(), again.Body.Bytes()) {
		t.Errorf("window 1 twice = %d %s and %d %s; want 200 and the same bytes", first.Code, first.Body, again.Code, again.Body)
	}
	withEP := decodeStats(t, "window 1 with endpoints", post(t, h, collectPath(1, true), nil))
	if withEP.Requests != 1 || len(withEP.Endpoints) != 1 || samples(withEP, Opening) != 1 {
		t.Errorf("window 1 with endpoints: %d requests, %d endpoints, %d opening samples; want 1, 1, 1", withEP.Requests, len(withEP.Endpoints), samples(withEP, Opening))
	}
	second := collectStats(t, h, 2)
	if l := second.Distributions[Latency]; second.Requests != 2 || l.Count != 1 || l.Min != 0.007 {
		t.Errorf("window 2: %d requests, latency %d samples with min %v; want 2, 1 and 0.007: the sample between the two answers", second.Requests, l.Count, l.Min)
	}
}

// TestRequestsAtOpeningDone pins the count the blended window prices: the
// requests this actor had finished when its last endpoint opened, repeats on
// the endpoints already open included, and fixed from then on.
func TestRequestsAtOpeningDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const c, interval, openLatency = 2, 100 * time.Millisecond, 250 * time.Millisecond
		var mu sync.Mutex
		opened := map[string]bool{}
		pn := newPipeNet(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			first := !opened[r.Host]
			opened[r.Host] = true
			mu.Unlock()
			if first && r.Host == "endpoint-1" {
				select {
				case <-time.After(openLatency):
				case <-r.Context().Done():
					return
				}
			}
			okHandler(w, r)
		}))
		defer pn.srv.Close()
		start := time.Now()
		h, stats := startPipeLoop(t, pn, StartRequest{Endpoints: c, IntervalMs: int64(interval / time.Millisecond)})

		// Endpoint 0 opens at 0 and repeats at 200; endpoint 1 opens at 100
		// and finishes at 350: the third request. Its first repeat follows at
		// once, since its period passed during the open.
		time.Sleep(interval + openLatency - time.Since(start))
		st := stats()
		if st.Opened != c || st.OpeningDone != interval+openLatency || st.RequestsAtOpeningDone != 3 || st.Requests != 4 {
			t.Errorf("at %v: opened %d at %v with %d requests, %d requests now; want %d at %v with 3, 4", time.Since(start), st.Opened, st.OpeningDone, st.RequestsAtOpeningDone, st.Requests, c, interval+openLatency)
		}
		time.Sleep(time.Second - time.Since(start))
		if st = stats(); st.RequestsAtOpeningDone != 3 || st.Requests <= 4 {
			t.Errorf("at %v: requestsAtOpeningDone %d with %d requests; want still 3 with more than 4", time.Since(start), st.RequestsAtOpeningDone, st.Requests)
		}
		if final := stopLoop(t, h); final.RequestsAtOpeningDone != 3 {
			t.Errorf("final requestsAtOpeningDone = %d, want 3", final.RequestsAtOpeningDone)
		}
	})
}

// TestFreshRecorderPerLoop pins that a loop starts from nothing: the first
// collect after start-stop-start is an empty window.
func TestFreshRecorderPerLoop(t *testing.T) {
	t.Parallel()
	h := serveTargets(t, 2)
	first := runLoop(t, h, StartRequest{Endpoints: 2}, atLeast(10))
	if samples(first, Latency) == 0 {
		t.Fatalf("first loop recorded no latency over %d requests", first.Requests)
	}
	// Nothing in the new loop fires for a minute, so its first collect sees
	// only what the previous loop might have left behind.
	if rec := start(t, h, StartRequest{Endpoints: 2, IntervalMs: 60_000, OffsetMs: 60_000}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	second := collectStats(t, h, 1)
	if second.Requests != 0 || second.Opened != 0 || len(second.Distributions) != 0 {
		t.Errorf("second loop's first collect: %d requests, %d opened, distributions %v; want an empty window", second.Requests, second.Opened, second.Distributions)
	}
	stopLoop(t, h)
}

// TestCollectInvariantUnderRace cuts windows while requests land: every
// success records exactly one opening or latency sample, so each window's
// samples equal the growth of Successes since the previous cut, and the
// windows add up to the final count. Every tenth cut is asked for twice, as
// a driver retries it, and must come back unchanged.
func TestCollectInvariantUnderRace(t *testing.T) {
	t.Parallel()
	h := serveTargets(t, 8)
	if rec := start(t, h, StartRequest{Endpoints: 8}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	var wg sync.WaitGroup
	var badReads atomic.Int64
	for range 4 {
		wg.Go(func() {
			for range 50 {
				st := readStats(t, h)
				var failed int64
				for _, n := range st.Errors {
					failed += n
				}
				if st.Successes+failed != st.Requests || st.Distributions != nil {
					badReads.Add(1)
				}
			}
		})
	}
	var total uint64
	var prev int64
	var badCuts, badRetries int
	for id := int64(1); id <= 200; id++ {
		rec := post(t, h, collectPath(id, true), nil)
		w := decodeStats(t, "POST /collect", rec)
		if id%10 == 0 {
			if again := post(t, h, collectPath(id, true), nil); !bytes.Equal(again.Body.Bytes(), rec.Body.Bytes()) {
				badRetries++
			}
		}
		got := samples(w, Opening) + samples(w, Latency)
		if int64(got) != w.Successes-prev {
			badCuts++
		}
		prev, total = w.Successes, total+got
	}
	wg.Wait()
	final := stopLoop(t, h)
	total += samples(final, Opening) + samples(final, Latency)
	if badReads.Load() > 0 || badCuts > 0 || badRetries > 0 {
		t.Errorf("%d reads broke successes + errors = requests, %d cuts broke window = growth of successes, %d retried cuts came back changed", badReads.Load(), badCuts, badRetries)
	}
	if int64(total) != final.Successes || final.Successes == 0 {
		t.Errorf("windows hold %d samples for %d successes in all", total, final.Successes)
	}
}

// TestCollectDuringStopIsNever500 races a driver's cuts and many reads
// against a stop: each one answers 200 while the loop runs or 404 once it is
// stopped, never a 500 from a shut-down reader.
func TestCollectDuringStopIsNever500(t *testing.T) {
	t.Parallel()
	h := serveTargets(t, 8)
	if rec := start(t, h, StartRequest{Endpoints: 8}); rec.Code != http.StatusNoContent {
		t.Fatalf("POST /start = %d %s", rec.Code, rec.Body)
	}
	var wg sync.WaitGroup
	var ok, gone, other atomic.Int64
	stopped := make(chan struct{})
	for i := range 16 {
		wg.Go(func() {
			var window int64
			for {
				var rec *httptest.ResponseRecorder
				if i == 0 {
					window++
					rec = post(t, h, collectPath(window, false), nil)
				} else {
					rec = get(h, StatsRoute)
				}
				switch {
				case rec.Code == http.StatusOK:
					ok.Add(1)
				case rec.Code == http.StatusNotFound && strings.TrimSpace(rec.Body.String()) == NoLoopMessage:
					gone.Add(1)
				default:
					other.Add(1)
					t.Errorf("answered %d %s", rec.Code, rec.Body)
				}
				select {
				case <-stopped:
					return
				default:
				}
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	stopLoop(t, h)
	close(stopped)
	wg.Wait()
	if rec := post(t, h, collectPath(1, false), nil); rec.Code != http.StatusNotFound {
		t.Errorf("POST /collect after stop = %d, want 404", rec.Code)
	}
	if ok.Load() == 0 || other.Load() != 0 {
		t.Errorf("%d answers were 200, %d 404, %d something else", ok.Load(), gone.Load(), other.Load())
	}
}

// TestEveryRouteCarriesProtocolHeader checks the header a driver reads to
// tell this actor from an image built before this contract, on every answer
// the actor gives, errors and unknown routes included. The value is 2: the
// images that sent 1 took no window id on /collect.
func TestEveryRouteCarriesProtocolHeader(t *testing.T) {
	t.Parallel()
	if ProtocolVersion != "2" {
		t.Errorf("ProtocolVersion = %q, want 2; a driver requires it", ProtocolVersion)
	}
	h := newServer().handler()
	for _, tc := range []struct {
		name string
		rec  *httptest.ResponseRecorder
		want int
	}{
		{"readyz", get(h, ReadyzRoute), http.StatusOK},
		{"start rejected", post(t, h, StartRoute, map[string]int{"endpoints": 0}), http.StatusBadRequest},
		{"stats without a loop", get(h, StatsRoute), http.StatusNotFound},
		{"collect without a loop", post(t, h, collectPath(1, false), nil), http.StatusNotFound},
		{"stop without a loop", post(t, h, StopRoute, nil), http.StatusNotFound},
		{"unknown route", get(h, "/metrics"), http.StatusNotFound},
	} {
		if tc.rec.Code != tc.want {
			t.Errorf("%s: status %d, want %d", tc.name, tc.rec.Code, tc.want)
		}
		if got := tc.rec.Header().Get(ProtocolHeader); got != ProtocolVersion {
			t.Errorf("%s: %s = %q, want %q", tc.name, ProtocolHeader, got, ProtocolVersion)
		}
	}
}

// TestPayloadBudget pins the answer sizes at the largest C, since the driver
// polls every actor: /stats stays under 1 KB, a /collect carries the four
// distributions in a few KB, and the per-endpoint counts come only when
// asked for.
func TestPayloadBudget(t *testing.T) {
	t.Parallel()
	const c = MaxEndpoints
	run, err := newLoopRun(StartRequest{Endpoints: c, Scheme: SchemeHTTPS})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.rec.shutdown(context.Background()) })
	run.stats.Requests, run.stats.Successes, run.stats.NewConns, run.stats.Opened = 12345*c, 12339*c, c, c
	run.stats.OpeningDone, run.stats.LongestGap = 102400*time.Millisecond, 12345678*time.Microsecond
	run.stats.Errors = map[string]int64{"timeout": 3 * c, "HTTP 503": 3 * c}
	run.rounds = 12345
	for i := range c {
		run.stats.Endpoints[i] = Endpoint{Requests: 12345, Errors: 6, LongestGap: 12345678 * time.Microsecond}
	}
	// Every bucket non-zero, so no count is a short "0".
	fill := func() {
		for _, name := range DistributionNames {
			for _, b := range Bounds {
				run.rec.record(context.Background(), name, b)
			}
		}
	}
	s := newServer()
	s.run = run
	h := s.handler()

	fill()
	collect := post(t, h, collectPath(1, false), nil)
	fill()
	collectEndpoints := post(t, h, collectPath(2, true), nil)
	stats := get(h, StatsRoute)
	t.Logf("at C=%d: /stats %d B, /collect %d B, /collect?endpoints=1 %d B", c, stats.Body.Len(), collect.Body.Len(), collectEndpoints.Body.Len())
	for _, tc := range []struct {
		name          string
		rec           *httptest.ResponseRecorder
		budget        int
		endpoints     bool
		distributions bool
	}{
		{"/stats", stats, 1 << 10, false, false},
		{"/collect", collect, 12 << 10, false, true},
		{"/collect?endpoints=1", collectEndpoints, 80 << 10, true, true},
	} {
		if tc.rec.Code != http.StatusOK {
			t.Fatalf("%s = %d %s", tc.name, tc.rec.Code, tc.rec.Body)
		}
		if tc.rec.Body.Len() >= tc.budget {
			t.Errorf("%s = %d B, budget %d B", tc.name, tc.rec.Body.Len(), tc.budget)
		}
		var st Stats
		if err := json.Unmarshal(tc.rec.Body.Bytes(), &st); err != nil {
			t.Fatal(err)
		}
		if (len(st.Endpoints) == c) != tc.endpoints || (len(st.Distributions) == len(DistributionNames)) != tc.distributions || st.Rounds != 12345 {
			t.Errorf("%s carries %d endpoints, %d distributions and rounds %d; want endpoints %v, distributions %v and rounds 12345", tc.name, len(st.Endpoints), len(st.Distributions), st.Rounds, tc.endpoints, tc.distributions)
		}
	}
}
