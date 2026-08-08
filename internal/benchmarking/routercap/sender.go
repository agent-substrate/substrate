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

// The HTTP client that puts one ping through the router, addressing an actor by Host
// header and counting connections as they are dialed.

package routercap

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// pingPath is the glutton actor's echo endpoint.
const pingPath = "/ping"

// countingDialer wraps the transport's dialer so the generator can report on
// its own connection behavior. If keep-alive silently stops working, the
// generator's socket-per-request cliff would look exactly like the router's.
type countingDialer struct {
	inner  *net.Dialer
	opened atomic.Int64
	live   atomic.Int64
}

func (d *countingDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	c, err := d.inner.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	d.opened.Add(1)
	d.live.Add(1)
	return &countedConn{Conn: c, dialer: d}, nil
}

type countedConn struct {
	net.Conn
	dialer *countingDialer
	closed atomic.Bool
}

func (c *countedConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.dialer.live.Add(-1)
	}
	return c.Conn.Close()
}

// Sender issues pings to the router, round-robin across the warm actor pool
// and, when the router runs more than one replica, round-robin across the
// replicas too.
type Sender struct {
	client  *http.Client
	dialer  *countingDialer
	urls    []string
	actors  []Actor
	next    atomic.Uint64
	lastOpn int64

	// dispatched counts requests handed to the transport, used with the dialer's
	// connection count to derive requests-per-connection.
	dispatched atomic.Int64
	lastDisp   int64
}

// SenderConfig configures the generator's transport.
type SenderConfig struct {
	// RouterURLs are the router pods' plaintext HTTP listeners, e.g.
	// http://10.0.0.5:8080, one per replica, addressed by pod IP so no Service,
	// kube-proxy hop or DNS lookup sits inside the measured path. Requests
	// round-robin over the list, with a per-host connection pool per replica.
	RouterURLs []string
	Actors     []Actor
	// MaxConnections sizes the idle pool and must be at least the run's
	// in-flight cap: Go closes idle connections above MaxIdleConnsPerHost, so
	// a smaller pool churns connections at exactly the peak load.
	MaxConnections int
	// RequestTimeout bounds one ping. Timeouts are counted as failures and
	// contribute their full latency to the percentiles rather than vanishing.
	RequestTimeout time.Duration
}

// NewSender builds the generator's HTTP client.
func NewSender(cfg SenderConfig) (*Sender, error) {
	if len(cfg.RouterURLs) == 0 || cfg.RouterURLs[0] == "" {
		return nil, fmt.Errorf("at least one router URL is required")
	}
	if len(cfg.Actors) == 0 {
		return nil, fmt.Errorf("sender needs at least one warm actor")
	}
	maxConns := cfg.MaxConnections
	if maxConns <= 0 {
		maxConns = 1024
	}
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	d := &countingDialer{inner: &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}}
	tr := &http.Transport{
		DialContext:     d.DialContext,
		MaxIdleConns:    maxConns,
		IdleConnTimeout: 120 * time.Second,
		// Sized to the in-flight cap for the reason on MaxConnections.
		MaxIdleConnsPerHost: maxConns,
		// Unbounded on purpose: a per-host cap would make the transport queue
		// requests internally, turning the open loop closed. The pacer's
		// in-flight cap is the only concurrency bound, and it sheds visibly.
		MaxConnsPerHost: 0,
		// HTTP/1.1 only: the upstream hop and the real client are HTTP/1.1,
		// and h2 would multiplex requests onto a handful of connections.
		ForceAttemptHTTP2:  false,
		DisableCompression: true,
		DisableKeepAlives:  false,
	}
	urls := make([]string, len(cfg.RouterURLs))
	for i, u := range cfg.RouterURLs {
		urls[i] = u + pingPath
	}
	return &Sender{
		client: &http.Client{Transport: tr, Timeout: timeout},
		dialer: d,
		urls:   urls,
		actors: cfg.Actors,
	}, nil
}

// Send issues one ping and classifies the result. It is the SendFunc the pacer
// calls; the pacer owns timing, this owns the request.
func (s *Sender) Send(ctx context.Context) (Outcome, int) {
	n := s.next.Add(1) - 1
	a := s.actors[int(n)%len(s.actors)]
	// One counter drives both rotations, so the actor->replica mapping is
	// sticky whenever the replica count divides the actor count — deliberate:
	// each replica serves a fixed share of actors on its own warm pool.
	url := s.urls[int(n)%len(s.urls)]
	s.dispatched.Add(1)

	message := uuid.NewString()
	body, err := proto.Marshal(&gluttonpb.PingRequest{Message: message})
	if err != nil {
		return OutcomeBadBody, 0
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return OutcomeTransportError, 0
	}
	// The router routes on Host, not on the URL. The pod-IP URL only decides
	// which socket the bytes go down.
	req.Host = a.Host
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := s.client.Do(req)
	if err != nil {
		return OutcomeTransportError, 0
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		// A body that could not be drained also leaves the connection
		// unreusable, which is why the read happens even on the error paths.
		return OutcomeTransportError, resp.StatusCode
	}
	if resp.StatusCode >= 400 {
		return OutcomeHTTPError, resp.StatusCode
	}

	pong := &gluttonpb.PingResponse{}
	if err := proto.Unmarshal(respBody, pong); err != nil {
		return OutcomeBadBody, resp.StatusCode
	}
	if pong.Message != message {
		// Something answered, but not the actor addressed: a misroute that
		// returns 200 must not score as a success.
		return OutcomeBadBody, resp.StatusCode
	}
	return OutcomeOK, resp.StatusCode
}

// Stats returns the transport's behavior since the last call, so consecutive
// calls partition the run into non-overlapping windows the same way the counter
// deltas elsewhere do.
func (s *Sender) Stats() ClientStats {
	opened := s.dialer.opened.Load()
	dispatched := s.dispatched.Load()
	newConns := float64(opened - s.lastOpn)
	reqs := float64(dispatched - s.lastDisp)
	s.lastOpn, s.lastDisp = opened, dispatched

	cs := ClientStats{NewConnections: newConns, ConnectionsInUse: s.dialer.live.Load()}
	if newConns > 0 {
		cs.RequestsPerConnection = reqs / newConns
	}
	return cs
}

// CloseIdleConnections releases the pool. Called at teardown so the generator
// does not leave thousands of sockets in TIME_WAIT for the next arm's Job.
func (s *Sender) CloseIdleConnections() {
	s.client.CloseIdleConnections()
}
