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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// The probe's whole purpose is to distinguish "the stream lasted as long as we
// asked" from "something ended it sooner", so every test here is one of those
// two outcomes. They run on a scale of hundreds of milliseconds; what the
// e2e test measures in tens of seconds is the same distinction.
const (
	testHold         = 400 * time.Millisecond
	testTickInterval = 20 * time.Millisecond
)

func TestStreamProbeHoldsSSEOpen(t *testing.T) {
	origin := httptest.NewServer(sseTickHandler(t, 0))
	defer origin.Close()

	status := runStreamProbe(t, streamProtocolSSE, origin.URL, testHold)
	if status.Error != "" {
		t.Fatalf("probe reported an error after %dms: %s", status.ElapsedMs, status.Error)
	}
	if elapsed := time.Duration(status.ElapsedMs) * time.Millisecond; elapsed < testHold {
		t.Errorf("stream stayed open %v, want at least %v", elapsed, testHold)
	}
	if status.Events == 0 {
		t.Error("probe recorded no events, so it never read the stream")
	}
	if status.First != "tick-0" {
		t.Errorf("first event = %q, want %q", status.First, "tick-0")
	}
}

// TestStreamProbeReportsSSECutShort is the failure the e2e test is built to
// catch, reproduced locally: an origin that ends the stream early must come
// back as an error rather than as a short but successful hold.
func TestStreamProbeReportsSSECutShort(t *testing.T) {
	const eventsBeforeClose = 3
	origin := httptest.NewServer(sseTickHandler(t, eventsBeforeClose))
	defer origin.Close()

	status := runStreamProbe(t, streamProtocolSSE, origin.URL, testHold)
	if status.Error == "" {
		t.Fatalf("probe reported success after %dms and %d events, want an error", status.ElapsedMs, status.Events)
	}
	if elapsed := time.Duration(status.ElapsedMs) * time.Millisecond; elapsed >= testHold {
		t.Errorf("stream stayed open %v, want it to have ended before the %v hold", elapsed, testHold)
	}
	if status.Events != eventsBeforeClose {
		t.Errorf("events = %d, want %d", status.Events, eventsBeforeClose)
	}
}

func TestStreamProbeHoldsWebSocketOpen(t *testing.T) {
	origin := httptest.NewServer(webSocketTickHandler(t, 0))
	defer origin.Close()

	status := runStreamProbe(t, streamProtocolWebSocket, webSocketURL(origin.URL), testHold)
	if status.Error != "" {
		t.Fatalf("probe reported an error after %dms: %s", status.ElapsedMs, status.Error)
	}
	if elapsed := time.Duration(status.ElapsedMs) * time.Millisecond; elapsed < testHold {
		t.Errorf("stream stayed open %v, want at least %v", elapsed, testHold)
	}
	if status.Events == 0 {
		t.Error("probe recorded no events, so it never read the stream")
	}
}

func TestStreamProbeReportsWebSocketCutShort(t *testing.T) {
	const eventsBeforeClose = 3
	origin := httptest.NewServer(webSocketTickHandler(t, eventsBeforeClose))
	defer origin.Close()

	status := runStreamProbe(t, streamProtocolWebSocket, webSocketURL(origin.URL), testHold)
	if status.Error == "" {
		t.Fatalf("probe reported success after %dms and %d events, want an error", status.ElapsedMs, status.Events)
	}
	if status.Events != eventsBeforeClose {
		t.Errorf("events = %d, want %d", status.Events, eventsBeforeClose)
	}
}

// TestStreamProbeRefusesUpgradeFailure covers what a proxy that will not carry a
// WebSocket upgrade looks like from here: the handshake is answered with a plain
// HTTP status, and that status is what the probe must report.
func TestStreamProbeRefusesUpgradeFailure(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no upgrades here", http.StatusBadGateway)
	}))
	defer origin.Close()

	status := runStreamProbe(t, streamProtocolWebSocket, webSocketURL(origin.URL), testHold)
	if status.Error == "" {
		t.Fatal("probe reported success against an origin that refused the upgrade")
	}
	if !strings.Contains(status.Error, "502") {
		t.Errorf("error = %q, want it to name the 502 the origin answered with", status.Error)
	}
}

func TestStreamProbeInvalidRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		status int
	}{
		{name: "method", method: http.MethodDelete, path: "/stream", body: `{}`, status: http.StatusMethodNotAllowed},
		{name: "malformed JSON", method: http.MethodPost, path: "/stream", body: `{`, status: http.StatusBadRequest},
		{name: "unknown protocol", method: http.MethodPost, path: "/stream", body: `{"url":"http://o/","protocol":"grpc"}`, status: http.StatusBadRequest},
		// The scheme has to agree with the protocol: a ws:// URL handed to the
		// HTTP client, or an http:// URL to the dialer, fails deep in a library
		// rather than as a bad request.
		{name: "sse with a ws URL", method: http.MethodPost, path: "/stream", body: `{"url":"ws://o/","protocol":"sse"}`, status: http.StatusBadRequest},
		{name: "websocket with an http URL", method: http.MethodPost, path: "/stream", body: `{"url":"http://o/","protocol":"websocket"}`, status: http.StatusBadRequest},
		{name: "missing hostname", method: http.MethodPost, path: "/stream", body: `{"url":"http:///sse","protocol":"sse"}`, status: http.StatusBadRequest},
		{name: "invalid hold", method: http.MethodPost, path: "/stream", body: `{"url":"http://o/","protocol":"sse","hold":"soon"}`, status: http.StatusBadRequest},
		{name: "unknown probe id", method: http.MethodGet, path: "/stream?id=stream-404", body: "", status: http.StatusNotFound},
	}

	handler := newHandler(http.DefaultClient)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(test.method, test.path, strings.NewReader(test.body)))
			if recorder.Code != test.status {
				t.Errorf("status = %d, want %d; body = %s", recorder.Code, test.status, recorder.Body.String())
			}
		})
	}
}

// runStreamProbe drives one probe through the handler the way the e2e test
// drives it through the ingress: start it, then poll until it is done.
func runStreamProbe(t *testing.T, protocol, url string, hold time.Duration) streamProbeStatus {
	t.Helper()
	handler := newHandler(http.DefaultClient)

	payload, err := json.Marshal(streamProbeRequest{URL: url, Protocol: protocol, Hold: hold.String()})
	if err != nil {
		t.Fatalf("marshaling the stream probe request: %v", err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/stream", strings.NewReader(string(payload))))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("starting the probe returned %d, want 202; body = %s", recorder.Code, recorder.Body.String())
	}
	var started streamProbeStatus
	if err := json.NewDecoder(recorder.Body).Decode(&started); err != nil {
		t.Fatalf("decoding the start response: %v", err)
	}
	if started.ID == "" {
		t.Fatal("start response carried no probe id")
	}

	deadline := time.Now().Add(hold + 10*time.Second)
	for {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/stream?id="+started.ID, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("polling probe %s returned %d; body = %s", started.ID, recorder.Code, recorder.Body.String())
		}
		var status streamProbeStatus
		if err := json.NewDecoder(recorder.Body).Decode(&status); err != nil {
			t.Fatalf("decoding probe %s status: %v", started.ID, err)
		}
		if status.Done {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("probe %s never finished; last seen with %d events over %dms", started.ID, status.Events, status.ElapsedMs)
		}
		time.Sleep(testTickInterval)
	}
}

// sseTickHandler serves the same event stream as demos/egress/streamserver. A
// nonzero stopAfter makes it hang up mid-stream, standing in for a proxy that
// cuts the connection.
func sseTickHandler(t *testing.T, stopAfter int) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		for n := 0; stopAfter == 0 || n < stopAfter; n++ {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(testTickInterval):
			}
			if _, err := fmt.Fprintf(w, "data: tick-%d\n\n", n); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}
}

func webSocketTickHandler(t *testing.T, stopAfter int) http.HandlerFunc {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return func(w http.ResponseWriter, r *http.Request) {
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrading the test origin's connection: %v", err)
			return
		}
		defer connection.Close()
		for n := 0; stopAfter == 0 || n < stopAfter; n++ {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(testTickInterval):
			}
			if err := connection.WriteMessage(websocket.TextMessage, fmt.Appendf(nil, "tick-%d", n)); err != nil {
				return
			}
		}
	}
}

func webSocketURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}
