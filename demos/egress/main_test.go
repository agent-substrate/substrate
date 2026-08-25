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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
