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
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFetch(t *testing.T) {
	const traceparent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			t.Errorf("upstream method = %s, want GET", r.Method)
		}
		if got := r.Header.Get("traceparent"); got != traceparent {
			t.Errorf("upstream traceparent = %q, want %q", got, traceparent)
		}
		return &http.Response{
			StatusCode: http.StatusTeapot,
			Body:       io.NopCloser(strings.NewReader("hello from upstream")),
			Header:     make(http.Header),
		}, nil
	})}

	payload, err := json.Marshal(fetchRequest{URL: "https://allowed.example/"})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(payload)))
	request.Header.Set("traceparent", traceparent)
	newHandler(client).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTeapot)
	}
	var got fetchResponse
	if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if got.StatusCode != http.StatusTeapot || got.Body != "hello from upstream" {
		t.Errorf("response = %+v", got)
	}
}

func TestInvalidRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   string
		status int
	}{
		{name: "method", method: http.MethodGet, body: `{}`, status: http.StatusMethodNotAllowed},
		{name: "malformed JSON", method: http.MethodPost, body: `{`, status: http.StatusBadRequest},
		{name: "missing hostname", method: http.MethodPost, body: `{"url":"https:///path"}`, status: http.StatusBadRequest},
		{name: "unsupported scheme", method: http.MethodPost, body: `{"url":"file:///etc/passwd"}`, status: http.StatusBadRequest},
	}

	handler := newHandler(http.DefaultClient)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, "/", strings.NewReader(test.body))
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Errorf("status = %d, want %d; body = %s", recorder.Code, test.status, recorder.Body.String())
			}
		})
	}
}

func TestOutboundFailure(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("blocked")
	})}
	handler := newHandler(client)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"url":"https://example.com/"}`))

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
}

// TestTCPProbeServerSpeaksFirst covers the ordering the probe exists for: the
// peer's greeting is reported without the probe having written anything, and
// the reply to Send comes back separately.
func TestTCPProbeServerSpeaksFirst(t *testing.T) {
	const banner = "TESTBANNER/1.0\r\n"
	address := startTestPeer(t, func(connection net.Conn) {
		if _, err := io.WriteString(connection, banner); err != nil {
			return
		}
		buffer := make([]byte, 64)
		n, err := connection.Read(buffer)
		if err != nil {
			return
		}
		_, _ = connection.Write(buffer[:n])
	})

	got := probe(t, tcpProbeRequest{Address: address, Send: "ping", Timeout: "5s"}, http.StatusOK)
	if got.Banner != banner {
		t.Errorf("banner = %q, want %q", got.Banner, banner)
	}
	if got.Received != "ping" {
		t.Errorf("received = %q, want %q", got.Received, "ping")
	}
}

// TestTCPProbeSilentPeer records that silence is a result, not an error: a
// client-speaks-first peer yields an empty banner and a 200, so a test can tell
// the two shapes apart.
func TestTCPProbeSilentPeer(t *testing.T) {
	address := startTestPeer(t, func(connection net.Conn) {
		buffer := make([]byte, 64)
		n, err := connection.Read(buffer)
		if err != nil {
			return
		}
		_, _ = connection.Write(buffer[:n])
	})

	got := probe(t, tcpProbeRequest{Address: address, Send: "ping", Timeout: "250ms"}, http.StatusOK)
	if got.Banner != "" {
		t.Errorf("banner = %q, want empty for a peer that does not speak first", got.Banner)
	}
	if got.Received != "ping" {
		t.Errorf("received = %q, want %q", got.Received, "ping")
	}
}

func TestTCPProbeReadBytesCapsTheBanner(t *testing.T) {
	address := startTestPeer(t, func(connection net.Conn) {
		_, _ = io.WriteString(connection, "0123456789")
	})

	got := probe(t, tcpProbeRequest{Address: address, ReadBytes: 4, Timeout: "5s"}, http.StatusOK)
	if got.Banner != "0123" {
		t.Errorf("banner = %q, want %q", got.Banner, "0123")
	}
}

func TestTCPProbeInvalidRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   string
		status int
	}{
		{name: "method", method: http.MethodGet, body: `{}`, status: http.StatusMethodNotAllowed},
		{name: "malformed JSON", method: http.MethodPost, body: `{`, status: http.StatusBadRequest},
		{name: "address without port", method: http.MethodPost, body: `{"address":"example.com"}`, status: http.StatusBadRequest},
		{name: "invalid timeout", method: http.MethodPost, body: `{"address":"127.0.0.1:9","timeout":"soon"}`, status: http.StatusBadRequest},
	}

	handler := newHandler(http.DefaultClient)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, "/tcp", strings.NewReader(test.body))
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Errorf("status = %d, want %d; body = %s", recorder.Code, test.status, recorder.Body.String())
			}
		})
	}
}

func TestTCPProbeDialFailure(t *testing.T) {
	// Port 0 is not connectable, so this fails without depending on which
	// ports happen to be free.
	got := probe(t, tcpProbeRequest{Address: "127.0.0.1:0", Timeout: "2s"}, http.StatusBadGateway)
	if got.Error == "" {
		t.Error("error = empty, want a dial failure")
	}
}

// startTestPeer listens on loopback and hands each connection to serve. It
// returns the address to probe.
func startTestPeer(t *testing.T, serve func(net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { listener.Close() })

	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer connection.Close()
				serve(connection)
			}()
		}
	}()
	return listener.Addr().String()
}

func probe(t *testing.T, input tcpProbeRequest, wantStatus int) tcpProbeResponse {
	t.Helper()
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/tcp", strings.NewReader(string(payload)))
	newHandler(http.DefaultClient).ServeHTTP(recorder, request)

	if recorder.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, wantStatus, recorder.Body.String())
	}
	var got tcpProbeResponse
	if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	return got
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
