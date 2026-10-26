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

// Package sink holds the peers atunnel talks to in the benchmark: the ingress
// actor in each actor's sandbox, behind ingress, and the fake egress gateway,
// behind egress. Both answer with a fixed body and count what they see.
package sink

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// IngressActorStats are the fake actor's counters.
type IngressActorStats struct {
	Requests        uint64 `json:"requests"`
	ConnectionsSeen uint64 `json:"connections_seen"`
	ConnectionsOpen int64  `json:"connections_open"`
}

// IngressActor stands in for an actor's program receiving ingress: an HTTP
// server answering every request with the same body. atunnel's ingress reaches it over HTTP/1.1,
// or h2c for gRPC, and through CONNECT tunnels as raw HTTP/1.1.
type IngressActor struct {
	body            []byte
	requests        atomic.Uint64
	connectionsSeen atomic.Uint64
	connectionsOpen atomic.Int64
}

// NewIngressActor returns an actor answering with bodyBytes of body.
func NewIngressActor(bodyBytes int) *IngressActor {
	return &IngressActor{body: fixedBody(bodyBytes)}
}

// Stats snapshots the counters.
func (a *IngressActor) Stats() IngressActorStats {
	return IngressActorStats{
		Requests:        a.requests.Load(),
		ConnectionsSeen: a.connectionsSeen.Load(),
		ConnectionsOpen: a.connectionsOpen.Load(),
	}
}

// Paths that make the ingress actor a slow peer instead of answering.
const (
	// StreamPath answers with a body that never ends.
	StreamPath = "/stream"
	// DiscardPath reads the request body to its end and drops it.
	DiscardPath = "/discard"
	// StallPath answers at once, reads the number of request body bytes in
	// the drain query parameter, and then none until the unix time in until,
	// after which it closes the connection. It needs the bound: an HTTP/1.1
	// server notices a client going away only by reading.
	StallPath = "/stall"
)

func (a *IngressActor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.requests.Add(1)
	switch r.URL.Path {
	case StreamPath:
		a.stream(w)
		return
	case DiscardPath:
		_, _ = io.Copy(io.Discard, r.Body)
		return
	case StallPath:
		until, err1 := strconv.ParseInt(r.URL.Query().Get("until"), 10, 64)
		drain, err2 := strconv.ParseInt(r.URL.Query().Get("drain"), 10, 64)
		if err1 != nil || err2 != nil {
			http.Error(w, "until and drain must be integers", http.StatusBadRequest)
			return
		}
		rc := http.NewResponseController(w)
		// Read the body after answering, as a server streaming both ways
		// does.
		_ = rc.EnableFullDuplex()
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusOK)
		_ = rc.Flush()
		stall(r.Context(), r.Body, time.Unix(until, 0), drain)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", strconv.Itoa(len(a.body)))
	_, _ = w.Write(a.body)
}

// stream writes the fixed body over and over until the client goes away.
func (a *IngressActor) stream(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/octet-stream")
	rc := http.NewResponseController(w)
	chunk := fixedBody(sourceChunk)
	for {
		if _, err := w.Write(chunk); err != nil {
			return
		}
		if err := rc.Flush(); err != nil {
			return
		}
	}
}

// Serve serves HTTP/1.1 and h2c on lis until ctx ends.
func (a *IngressActor) Serve(ctx context.Context, lis net.Listener) error {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		Handler:           a,
		Protocols:         protocols,
		ReadHeaderTimeout: 10 * time.Second,
		ConnState: func(_ net.Conn, state http.ConnState) {
			switch state {
			case http.StateNew:
				a.connectionsSeen.Add(1)
				a.connectionsOpen.Add(1)
			case http.StateClosed, http.StateHijacked:
				a.connectionsOpen.Add(-1)
			}
		},
	}
	return serveUntil(ctx, server, func() error { return server.Serve(lis) })
}

// serveUntil runs serve until ctx ends, then closes server.
func serveUntil(ctx context.Context, server *http.Server, serve func() error) error {
	stop := context.AfterFunc(ctx, func() { _ = server.Close() })
	defer stop()
	if err := serve(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func fixedBody(n int) []byte {
	body := make([]byte, n)
	for i := range body {
		body[i] = 'a' + byte(i%26)
	}
	return body
}
