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

package e2e

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/resources"
)

func TestRouterClientPostJSON(t *testing.T) {
	client := &RouterClient{
		baseURL: "http://router.test",
		http: &http.Client{Transport: testRoundTripper(func(request *http.Request) (*http.Response, error) {
			if got := request.Header.Get(atenet.TargetActorHeader); got != "demo/fetcher" {
				t.Errorf("target actor = %q, want demo/fetcher", got)
			}
			if request.Method != http.MethodPost {
				t.Errorf("method = %q, want POST", request.Method)
			}
			if request.Host != "router.test" {
				t.Errorf("host = %q, want router.test", request.Host)
			}
			if request.URL.Path != "/fetch" {
				t.Errorf("path = %q, want /fetch", request.URL.Path)
			}
			if request.Header.Get("Content-Type") != "application/json" {
				t.Errorf("content type = %q, want application/json", request.Header.Get("Content-Type"))
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatalf("reading body: %v", err)
			}
			if string(body) != `{"url":"https://example.com/"}` {
				t.Errorf("body = %q", body)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Header:     make(http.Header),
			}, nil
		})},
	}

	actorRef := resources.ActorRef{Atespace: "demo", Name: "fetcher"}
	response, err := client.PostJSON(context.Background(), actorRef, "/fetch", []byte(`{"url":"https://example.com/"}`))
	if err != nil {
		t.Fatalf("PostJSON: %v", err)
	}
	response.Body.Close()
}

func TestRouterClientGetJSON(t *testing.T) {
	answers := map[string]struct {
		status int
		body   string
	}{
		"/ok":      {http.StatusOK, `{"name":"fetcher"}`},
		"/missing": {http.StatusNotFound, "no route"},
		"/garbled": {http.StatusOK, "not json"},
	}
	client := &RouterClient{
		baseURL: "http://router.test",
		http: &http.Client{Transport: testRoundTripper(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodGet {
				t.Errorf("method = %q, want GET", request.Method)
			}
			if got := request.Header.Get(atenet.TargetActorHeader); got != "demo/fetcher" {
				t.Errorf("target actor = %q, want demo/fetcher", got)
			}
			answer := answers[request.URL.Path]
			return &http.Response{
				StatusCode: answer.status,
				Body:       io.NopCloser(strings.NewReader(answer.body)),
				Header:     make(http.Header),
			}, nil
		})},
	}
	actorRef := resources.ActorRef{Atespace: "demo", Name: "fetcher"}

	var out struct{ Name string }
	if err := client.GetJSON(context.Background(), actorRef, "/ok", &out); err != nil {
		t.Fatalf("GetJSON /ok: %v", err)
	}
	if out.Name != "fetcher" {
		t.Errorf("decoded name = %q, want fetcher", out.Name)
	}

	err := client.GetJSON(context.Background(), actorRef, "/missing", &out)
	if err == nil || !strings.Contains(err.Error(), "status 404") || !strings.Contains(err.Error(), "no route") {
		t.Errorf("GetJSON /missing = %v, want an error carrying the status and body", err)
	}
	if err := client.GetJSON(context.Background(), actorRef, "/garbled", &out); err == nil || !strings.HasPrefix(err.Error(), "decoding /garbled") {
		t.Errorf("GetJSON /garbled = %v, want a decode error", err)
	}
}

type testRoundTripper func(*http.Request) (*http.Response, error)

func (f testRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
