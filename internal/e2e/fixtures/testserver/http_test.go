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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type trackingReader struct {
	r    io.Reader
	eof  bool
	read int
}

func (t *trackingReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	t.read += n
	if errors.Is(err, io.EOF) {
		t.eof = true
	}
	return n, err
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func TestHTTPHandlerHealthz(t *testing.T) {
	recorder := httptest.NewRecorder()
	newHTTPHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("GET /healthz = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func TestHTTPHandlerFetchPostDrainsBody(t *testing.T) {
	payload := strings.Repeat("payload-bytes-", 1024)
	body := &trackingReader{r: strings.NewReader(payload)}

	recorder := httptest.NewRecorder()
	newHTTPHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/fetch", body))

	if recorder.Code != http.StatusOK {
		t.Fatalf("POST /fetch = %d, want %d", recorder.Code, http.StatusOK)
	}
	if !body.eof || body.read != len(payload) {
		t.Errorf("POST /fetch read %d bytes (eof=%v), want %d bytes and eof=true before responding", body.read, body.eof, len(payload))
	}
}

func TestHTTPHandlerFetchPostIncompleteBodyFails(t *testing.T) {
	recorder := httptest.NewRecorder()
	newHTTPHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/fetch", failingReader{}))
	if recorder.Code == http.StatusOK {
		t.Errorf("POST /fetch with truncated body = %d, want non-200 error status", recorder.Code)
	}
}

func TestHTTPHandlerFetchRejectsNonPost(t *testing.T) {
	recorder := httptest.NewRecorder()
	newHTTPHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/fetch", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /fetch = %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
}
