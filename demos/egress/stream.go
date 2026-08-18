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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// The streaming probe is deliberately split into a start and a poll, rather
// than one call that blocks until the stream ends. A caller reaches this Actor
// through the ingress router, whose routes carry a timeout of their own, so a
// request that waited out a half-minute stream would be cut by the ingress
// before it could report on the egress. Starting the read in the background
// keeps every call to the Actor short and moves the waiting to the caller.

// streamProbeRequest asks for one long-lived stream to be read in the
// background. It is the body of POST /stream.
type streamProbeRequest struct {
	// URL is the origin to stream from: http(s) for SSE, ws(s) for WebSocket.
	URL string `json:"url"`
	// Protocol is "sse" or "websocket".
	Protocol string `json:"protocol"`
	// Hold is how long to keep reading before declaring success.
	Hold string `json:"hold,omitempty"`
}

// streamProbeStatus is the state of one probe. POST /stream returns it with
// only ID set; GET /stream?id=... returns it filled in.
type streamProbeStatus struct {
	ID     string `json:"id,omitempty"`
	Events int    `json:"events"`
	First  string `json:"first,omitempty"`
	Last   string `json:"last,omitempty"`
	// ElapsedMs is how long the stream stayed open. Against a proxy that cuts
	// long streams this is the interesting number: it names the timeout.
	ElapsedMs int64  `json:"elapsedMs"`
	Done      bool   `json:"done"`
	Error     string `json:"error,omitempty"`
}

// streamProbe is one in-flight stream. Its fields are written by the reading
// goroutine and read by whatever polls GET /stream, so all access is guarded.
type streamProbe struct {
	started time.Time

	mu      sync.Mutex
	events  int
	first   string
	last    string
	elapsed time.Duration
	done    bool
	err     string
}

func (p *streamProbe) record(event string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.events == 0 {
		p.first = event
	}
	p.last = event
	p.events++
}

func (p *streamProbe) finish(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.elapsed = time.Since(p.started)
	p.done = true
	if err != nil {
		p.err = err.Error()
	}
}

func (p *streamProbe) status() streamProbeStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	elapsed := p.elapsed
	if !p.done {
		elapsed = time.Since(p.started)
	}
	return streamProbeStatus{
		Events:    p.events,
		First:     p.first,
		Last:      p.last,
		ElapsedMs: elapsed.Milliseconds(),
		Done:      p.done,
		Error:     p.err,
	}
}

// streamProbeRegistry holds probes between the call that starts one and the
// calls that poll it.
type streamProbeRegistry struct {
	mu     sync.Mutex
	nextID atomic.Uint64
	probes map[string]*streamProbe
}

func newStreamProbeRegistry() *streamProbeRegistry {
	return &streamProbeRegistry{probes: make(map[string]*streamProbe)}
}

func (r *streamProbeRegistry) add(probe *streamProbe) string {
	id := fmt.Sprintf("stream-%d", r.nextID.Add(1))
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probes[id] = probe
	return id
}

func (r *streamProbeRegistry) get(id string) (*streamProbe, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	probe, ok := r.probes[id]
	return probe, ok
}

func handleStreamProbe(registry *streamProbeRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			startStreamProbe(w, r, registry)
		case http.MethodGet:
			id := r.URL.Query().Get("id")
			probe, ok := registry.get(id)
			if !ok {
				writeStreamProbeJSON(w, http.StatusNotFound, streamProbeStatus{Error: fmt.Sprintf("no such probe: %q", id)})
				return
			}
			writeStreamProbeJSON(w, http.StatusOK, probe.status())
		default:
			w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
			writeStreamProbeJSON(w, http.StatusMethodNotAllowed, streamProbeStatus{Error: "method must be GET or POST"})
		}
	}
}

func startStreamProbe(w http.ResponseWriter, r *http.Request, registry *streamProbeRegistry) {
	const defaultHold = 30 * time.Second
	// A probe holds a connection open for its whole life, so an unbounded hold
	// would let one call pin an outbound connection indefinitely.
	const maxHold = 5 * time.Minute

	var input streamProbeRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err := decoder.Decode(&input); err != nil {
		writeStreamProbeJSON(w, http.StatusBadRequest, streamProbeStatus{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
		return
	}
	if err := validateStreamURL(input.Protocol, input.URL); err != nil {
		writeStreamProbeJSON(w, http.StatusBadRequest, streamProbeStatus{Error: err.Error()})
		return
	}

	hold := defaultHold
	if input.Hold != "" {
		parsed, err := time.ParseDuration(input.Hold)
		if err != nil {
			writeStreamProbeJSON(w, http.StatusBadRequest, streamProbeStatus{Error: fmt.Sprintf("invalid hold: %v", err)})
			return
		}
		hold = min(parsed, maxHold)
	}

	probe := &streamProbe{started: time.Now()}
	id := registry.add(probe)
	go probe.run(input.Protocol, input.URL, hold)

	writeStreamProbeJSON(w, http.StatusAccepted, streamProbeStatus{ID: id})
}

// run reads the stream until hold elapses. Reaching the hold is the success
// case, so the error it finishes with is exactly "something ended this stream
// before we meant to end it".
func (p *streamProbe) run(protocol, target string, hold time.Duration) {
	// Not r.Context(): the request that started this probe has already been
	// answered, and its context is cancelled the moment the handler returns.
	ctx, cancel := context.WithTimeout(context.Background(), hold)
	defer cancel()

	var err error
	switch protocol {
	case streamProtocolSSE:
		err = p.readSSE(ctx, target)
	case streamProtocolWebSocket:
		err = p.readWebSocket(ctx, target, time.Now().Add(hold))
	default:
		err = fmt.Errorf("unsupported protocol %q", protocol)
	}
	p.finish(err)
}

func (p *streamProbe) readSSE(ctx context.Context, target string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	request.Header.Set("Accept", "text/event-stream")

	// Not the package-level client: its Timeout is a ceiling on the whole
	// response body, which for a stream means a ceiling on the stream.
	response, err := (&http.Client{}).Do(request)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("origin returned HTTP %d", response.StatusCode)
	}

	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		if data, ok := strings.CutPrefix(scanner.Text(), "data: "); ok {
			p.record(data)
		}
	}
	return streamEndReason(ctx, scanner.Err())
}

func (p *streamProbe) readWebSocket(ctx context.Context, target string, deadline time.Time) error {
	connection, response, err := websocket.DefaultDialer.DialContext(ctx, target, nil)
	if err != nil {
		if response != nil {
			// A proxy that will not carry the upgrade answers here, and its
			// status code is the most useful thing we can report.
			return fmt.Errorf("upgrading: %w (origin returned HTTP %d)", err, response.StatusCode)
		}
		return fmt.Errorf("upgrading: %w", err)
	}
	defer connection.Close()

	if err := connection.SetReadDeadline(deadline); err != nil {
		return fmt.Errorf("setting read deadline: %w", err)
	}
	for {
		_, message, err := connection.ReadMessage()
		if err != nil {
			return streamEndReason(ctx, err)
		}
		p.record(string(message))
	}
}

// streamEndReason decides whether the end of a stream was ours or someone
// else's. Our own deadline firing is the success the probe was asked for; every
// other ending, including a clean close by the origin, means the stream did not
// last as long as it was supposed to.
func streamEndReason(ctx context.Context, err error) error {
	if ctx.Err() != nil || errors.Is(err, os.ErrDeadlineExceeded) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stream ended early: %w", err)
	}
	return errors.New("stream ended early: origin closed it")
}

const (
	streamProtocolSSE       = "sse"
	streamProtocolWebSocket = "websocket"
)

func validateStreamURL(protocol, raw string) error {
	var schemes []string
	switch protocol {
	case streamProtocolSSE:
		schemes = []string{"http", "https"}
	case streamProtocolWebSocket:
		schemes = []string{"ws", "wss"}
	default:
		return fmt.Errorf("protocol must be %q or %q", streamProtocolSSE, streamProtocolWebSocket)
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if parsed.Scheme != schemes[0] && parsed.Scheme != schemes[1] {
		return fmt.Errorf("URL scheme must be %s or %s for protocol %q", schemes[0], schemes[1], protocol)
	}
	if parsed.Hostname() == "" {
		return fmt.Errorf("URL must include a hostname")
	}
	return nil
}

func writeStreamProbeJSON(w http.ResponseWriter, status int, response streamProbeStatus) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}
