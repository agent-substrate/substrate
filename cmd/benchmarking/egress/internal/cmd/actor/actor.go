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
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptrace"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

// Command is the workload of the egress scale test. Once told to start,
// it opens every endpoint, one per interval, then on one goroutine per
// endpoint sends it an HTTP GET once per period of C intervals, and counts
// what happened under one lock.
//
// The loop starts on POST /start rather than at boot: every actor restores
// from a golden snapshot taken after the template's warmup, so a loop started
// at boot would run in the golden actor, which has no egress policy, and be
// frozen into every actor's snapshot, failed requests included.
func Command() *cobra.Command {
	var listen string
	cmd := &cobra.Command{
		Use:   "actor",
		Short: "Hold one tunnel per endpoint open through the gateway and count what happened",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runActor(cmd.Context(), listen)
		},
	}
	cmd.Flags().StringVar(&listen, "listen", ":80", "Address to serve the readyz, start, stats, collect and stop routes on.")
	return cmd
}

// runActor serves the actor's routes on listen until ctx ends.
func runActor(ctx context.Context, listen string) error {
	srv := &http.Server{
		Addr:              listen,
		Handler:           newServer().handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("benchmark-egress actor listening", "addr", listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serving: %w", err)
	}
	return nil
}

// maxBodyBytes bounds what the actor reads, from a /start body or from an
// endpoint's response.
const maxBodyBytes = 1 << 20

// server serves the actor routes. At most one loop runs at a time.
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
	// final is the last stopped loop's Stats, endpoints included, kept until
	// the next /start so a repeated /stop answers it again.
	final *Stats
}

func newServer() *server {
	return &server{endpointURL: EndpointURL}
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+ReadyzRoute, func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST "+StartRoute, s.handleStart)
	mux.HandleFunc("GET "+StatsRoute, s.handleStats)
	mux.HandleFunc("POST "+CollectRoute, s.handleCollect)
	mux.HandleFunc("POST "+StopRoute, s.handleStop)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(ProtocolHeader, ProtocolVersion)
		mux.ServeHTTP(w, r)
	})
}

func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	var req StartRequest
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
		http.Error(w, "a loop is already running; POST "+StopRoute+" first", http.StatusConflict)
		return
	}
	urls := make([]string, req.Endpoints)
	for i := range urls {
		urls[i] = s.endpointURL(i, req.URLScheme())
	}
	run, err := startLoop(req, urls, s.rootCAs, s.dial)
	if err != nil {
		http.Error(w, "starting loop: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.run, s.final = run, nil
	w.WriteHeader(http.StatusNoContent)
}

// withEndpoints reports whether the request asked for the per-endpoint
// counts.
func withEndpoints(r *http.Request) bool {
	return r.URL.Query().Get("endpoints") == "1"
}

// view is a kept Stats as a request asked for it: the per-endpoint slice
// stays only with ?endpoints=1. The kept value is not touched.
func view(s Stats, endpoints bool) Stats {
	if !endpoints {
		s.Endpoints = nil
	}
	return s
}

// runOr404 returns the loop, or answers 404 and returns nil.
func (s *server) runOr404(w http.ResponseWriter) *loopRun {
	s.mu.Lock()
	run := s.run
	s.mu.Unlock()
	if run == nil {
		http.Error(w, NoLoopMessage, http.StatusNotFound)
	}
	return run
}

func (s *server) handleStats(w http.ResponseWriter, r *http.Request) {
	run := s.runOr404(w)
	if run == nil {
		return
	}
	st, ok := run.snapshot(withEndpoints(r))
	if !ok {
		http.Error(w, NoLoopMessage, http.StatusNotFound)
		return
	}
	writeJSON(w, st)
}

func (s *server) handleCollect(w http.ResponseWriter, r *http.Request) {
	run := s.runOr404(w)
	if run == nil {
		return
	}
	raw := r.URL.Query().Get(WindowParam)
	window, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || window < 1 {
		http.Error(w, fmt.Sprintf("%s must be a positive integer, got %q; the next window is %d", WindowParam, raw, run.nextWindow()), http.StatusBadRequest)
		return
	}
	st, err := run.collect(window, withEndpoints(r))
	var seq *sequenceError
	switch {
	case errors.Is(err, errNoLoop):
		http.Error(w, NoLoopMessage, http.StatusNotFound)
	case errors.As(err, &seq):
		http.Error(w, err.Error(), http.StatusConflict)
	case err != nil:
		http.Error(w, "collecting: "+err.Error(), http.StatusInternalServerError)
	default:
		writeJSON(w, st)
	}
}

func (s *server) handleStop(w http.ResponseWriter, r *http.Request) {
	// Held across the wait so a /start cannot slip in before the loop exits.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.run == nil {
		if s.final == nil {
			http.Error(w, NoLoopMessage, http.StatusNotFound)
			return
		}
		writeJSON(w, view(*s.final, withEndpoints(r)))
		return
	}
	st, err := s.run.stop()
	s.run = nil
	if err != nil {
		http.Error(w, "collecting: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.final = &st
	writeJSON(w, view(st, withEndpoints(r)))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// loopRun is one started loop and what it has measured: the counters under
// its lock, the distributions in its recorder.
type loopRun struct {
	cancel    context.CancelFunc
	wg        sync.WaitGroup // one goroutine per endpoint
	started   time.Time
	transport *http.Transport
	rec       *recorder

	mu sync.Mutex
	// stopped is set by stop before it releases the recorder, so a collect
	// that raced the stop answers 404 instead of reading a shut-down reader.
	stopped bool
	// stats holds the counters; Elapsed and Rounds are derived at snapshot,
	// Distributions at collect.
	stats Stats
	last  []time.Time // start of each endpoint's most recent request
	// rounds is Stats.Rounds kept current: the least-visited endpoint's
	// request count.
	rounds int64
	// window is the id of the last cut, 0 before the first, and cut its
	// Stats with the endpoints, so the same id asked again is answered
	// without a new cut.
	window int64
	cut    Stats
}

// errNoLoop is a read of a loop that has been stopped.
var errNoLoop = errors.New(NoLoopMessage)

// sequenceError is a window id that is neither the last cut nor the next.
type sequenceError struct{ got, last int64 }

func (e *sequenceError) Error() string {
	return fmt.Sprintf("window %d is out of sequence: the last cut is %d, the next window is %d", e.got, e.last, e.last+1)
}

// newLoopRun is a loop over req's endpoints with a fresh recorder and no
// goroutines yet; startLoop runs it.
func newLoopRun(req StartRequest) (*loopRun, error) {
	rec, err := newRecorder()
	if err != nil {
		return nil, err
	}
	return &loopRun{
		started: time.Now(),
		rec:     rec,
		stats:   Stats{Scheme: req.URLScheme(), Endpoints: make([]Endpoint, req.Endpoints)},
		last:    make([]time.Time, req.Endpoints),
	}, nil
}

func startLoop(req StartRequest, urls []string, roots *x509.CertPool, dial dialFunc) (*loopRun, error) {
	timeoutMs := req.RequestTimeoutMs
	if timeoutMs == 0 {
		timeoutMs = DefaultRequestTimeoutMs
	}
	r, err := newLoopRun(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel, r.transport = cancel, newTransport(roots, dial)
	client := &http.Client{
		Transport: r.transport,
		// Count a redirect as the endpoint's answer rather than following it
		// somewhere the egress policy may not allow.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	timeout := time.Duration(timeoutMs) * time.Millisecond
	interval := time.Duration(req.IntervalMs) * time.Millisecond
	offset := time.Duration(req.OffsetMs) * time.Millisecond
	period := interval * time.Duration(len(urls))
	for i, u := range urls {
		r.wg.Go(func() { r.endpoint(ctx, client, i, u, timeout, offset+interval*time.Duration(i), period) })
	}
	return r, nil
}

// dialFunc opens one connection, as net.Dialer.DialContext does.
type dialFunc = func(ctx context.Context, network, addr string) (net.Conn, error)

// newTransport keeps one connection alive per endpoint and verifies HTTPS
// endpoints against roots. It ignores proxy environment variables: the
// sandbox's egress capture is the only proxy under test.
func newTransport(roots *x509.CertPool, dial dialFunc) *http.Transport {
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
		IdleConnTimeout: 0,
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
		r.stats.RequestsAtOpeningDone = r.stats.Requests
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

// requestTrace collects what httptrace reports about one request, as the
// times its steps happened: a zero time is a step that did not. The dial,
// and with it the DNS and TLS callbacks, can still be running on another
// goroutine after a timed-out request returns, hence the lock.
type requestTrace struct {
	mu sync.Mutex
	// newConn is set when the request went out on a connection opened for
	// it rather than a kept-alive one.
	newConn  bool
	dnsStart time.Time
	dnsDone  time.Time
	tlsStart time.Time
	tlsDone  time.Time // set only when the handshake completed
}

func (t *requestTrace) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.newConn = !info.Reused
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.dnsStart = time.Now()
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.dnsDone = time.Now()
		},
		TLSHandshakeStart: func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.tlsStart = time.Now()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			t.mu.Lock()
			defer t.mu.Unlock()
			if err == nil {
				t.tlsDone = time.Now()
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
	newConn := trace.newConn
	didDNS, dns := !trace.dnsDone.IsZero(), trace.dnsDone.Sub(trace.dnsStart)
	tlsStarted, didTLS, tlsTime := !trace.tlsStart.IsZero(), !trace.tlsDone.IsZero(), trace.tlsDone.Sub(trace.tlsStart)
	trace.mu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	s, ctx := &r.stats, context.Background()
	first := s.Endpoints[i].Requests == 0
	s.Requests++
	s.Endpoints[i].Requests++
	if s.Endpoints[i].Requests-1 == r.rounds {
		// Only the least-visited endpoint can raise the minimum.
		r.rounds = slices.MinFunc(s.Endpoints, func(a, b Endpoint) int { return cmp.Compare(a.Requests, b.Requests) }).Requests
	}
	if newConn {
		s.NewConns++
	}
	if didDNS {
		r.rec.record(ctx, DNS, dns.Seconds())
	}
	if didTLS {
		r.rec.record(ctx, TLS, tlsTime.Seconds())
	}
	class := classify(err, status)
	if class != "" && tlsStarted && !didTLS && !strings.HasPrefix(class, "tls: ") {
		// A failure inside the handshake, such as the gateway closing a
		// connection whose SNI no rule allows, is told apart from the same
		// error in an HTTP exchange.
		class = "tls handshake: " + class
	}
	if class == "" {
		s.Successes++
		if first {
			r.rec.record(ctx, Opening, latency.Seconds())
		} else {
			r.rec.record(ctx, Latency, latency.Seconds())
		}
		return
	}
	if s.Errors == nil {
		s.Errors = make(map[string]int64)
	}
	s.Errors[class]++
	s.Endpoints[i].Errors++
}

// snapshotLocked copies the counters so far, with Elapsed derived. The
// caller holds r.mu.
func (r *loopRun) snapshotLocked(endpoints bool) Stats {
	s := r.stats
	s.Elapsed, s.Rounds = time.Since(r.started), r.rounds
	s.Errors = maps.Clone(s.Errors)
	if endpoints {
		s.Endpoints = slices.Clone(s.Endpoints)
	} else {
		s.Endpoints = nil
	}
	return s
}

// snapshot answers GET /stats: the counters alone. ok is false once the
// loop is stopped.
func (r *loopRun) snapshot(endpoints bool) (Stats, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return Stats{}, false
	}
	return r.snapshotLocked(endpoints), true
}

// nextWindow is the id the next cut must carry.
func (r *loopRun) nextWindow() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.window + 1
}

// collect answers POST /collect for window id w, at least 1. The id after the
// last cut's cuts a new window: the counters and the distributions since the
// previous cut, under the lock every record takes, so no sample lands between
// the two. The last cut's id answers that cut again, so a lost answer costs
// the driver a retry rather than a window. Any other id is a sequenceError,
// and a stopped loop errNoLoop.
func (r *loopRun) collect(w int64, endpoints bool) (Stats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case r.stopped:
		return Stats{}, errNoLoop
	case w == r.window:
		return view(r.cut, endpoints), nil
	case w != r.window+1:
		return Stats{}, &sequenceError{got: w, last: r.window}
	}
	s, err := r.collectLocked()
	if err != nil {
		return Stats{}, err
	}
	r.window, r.cut = w, s
	return view(s, endpoints), nil
}

// collectLocked cuts a window with r.mu held, endpoints included. The cut uses
// its own context: a client that went away must not cost the driver a window.
func (r *loopRun) collectLocked() (Stats, error) {
	s := r.snapshotLocked(true)
	d, err := r.rec.collect(context.Background())
	if err != nil {
		return Stats{}, err
	}
	s.Distributions = d
	return s, nil
}

// stop cancels the loop, waits for every endpoint's goroutine to exit, and
// returns the final Stats: the counters, endpoints included, and the tail
// window. It then releases the recorder; a collect that raced the stop
// answers 404.
func (r *loopRun) stop() (Stats, error) {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	if r.transport != nil {
		r.transport.CloseIdleConnections()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = true
	s, err := r.collectLocked()
	// Nothing is attached to the provider, so there is nothing to flush and
	// a failed shutdown loses nothing. Its own context, as for the collect: a
	// client that hung up must not leave the provider running.
	_ = r.rec.shutdown(context.Background())
	return s, err
}
