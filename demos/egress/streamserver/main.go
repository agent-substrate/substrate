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

// Command streamserver is a long-lived-stream origin for the egress tests. It
// emits the same tick sequence two ways -- Server-Sent Events on /sse and
// WebSocket text frames on /ws -- so a test can hold a stream open across a
// proxy's timeout boundary and find out whether the proxy cut it.
//
// It ticks until the peer goes away rather than sending a fixed number of
// events. What these tests ask is how long a stream survives, and an origin
// that ended the stream itself would be answering that question for them.
package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	const defaultListenAddress = ":8080"
	// One tick a second is frequent enough that a test can tell a live stream
	// from a stalled one within a couple of ticks, and sparse enough that a
	// half-minute hold is tens of events rather than thousands.
	const defaultTickInterval = time.Second

	address := defaultListenAddress
	if value := os.Getenv("LISTEN_ADDRESS"); value != "" {
		address = value
	}
	interval := defaultTickInterval
	if value := os.Getenv("TICK_INTERVAL"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			slog.Error("invalid TICK_INTERVAL", "value", value, "error", err)
			os.Exit(1)
		}
		interval = parsed
	}

	slog.Info("starting stream server", "address", address, "tickInterval", interval)
	if err := http.ListenAndServe(address, newHandler(interval)); err != nil {
		slog.Error("stream server stopped", "error", err)
		os.Exit(1)
	}
}

func newHandler(interval time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/sse", handleSSE(interval))
	mux.HandleFunc("/ws", handleWebSocket(interval))
	return mux
}

// tick is the payload of the nth event. Numbering them lets a reader tell a
// stream that was cut and resumed from one that ran unbroken.
func tick(n int) string {
	return fmt.Sprintf("tick-%d", n)
}

func handleSSE(interval time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		header := w.Header()
		header.Set("Content-Type", "text/event-stream")
		// Without this an intermediary is free to buffer the whole stream,
		// which would make a cut one indistinguishable from a slow one.
		header.Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for n := 0; ; n++ {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", tick(n)); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func handleWebSocket(interval time.Duration) http.HandlerFunc {
	// Same-origin checking is the default and would reject the actor, which
	// sends no Origin header of its own. This fixture is reachable only from
	// inside the test namespace and serves one hard-coded tick sequence.
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	return func(w http.ResponseWriter, r *http.Request) {
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			// Upgrade has already written a response by this point.
			slog.Error("websocket upgrade failed", "error", err)
			return
		}
		defer connection.Close()

		// gorilla only processes close and ping frames from inside a read call,
		// so without draining, a peer that has left is noticed only when a write
		// eventually fails.
		go func() {
			for {
				if _, _, err := connection.ReadMessage(); err != nil {
					connection.Close()
					return
				}
			}
		}()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for n := 0; ; n++ {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}
			if err := connection.WriteMessage(websocket.TextMessage, []byte(tick(n))); err != nil {
				return
			}
		}
	}
}
