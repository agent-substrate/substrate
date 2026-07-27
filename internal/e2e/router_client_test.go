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

	"github.com/agent-substrate/substrate/internal/resources"
)

// SEE(lior): this file used to also hold TestResolveHTTPTargetPort and
// TestIsPodReady. Main moved both into internal/portforward/portforward_test.go
// (upstream 4453b5e7, "Prefactoring: Consolidate logic to port-forward to a
// service Pod") and deleted this file, so git rename-matched this branch's
// version of it against the new portforward test and reported the whole thing as
// a conflict. Took main's move as-is and re-added only the net-new PostJSON test
// here, rather than resurrecting the port-forward tests in a package that no
// longer owns that code.
func TestRouterClientPostJSON(t *testing.T) {
	client := &RouterClient{
		baseURL: "http://router.test",
		http: &http.Client{Transport: testRoundTripper(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodPost {
				t.Errorf("method = %q, want POST", request.Method)
			}
			if request.Host != "fetcher.demo.actors.resources.substrate.ate.dev" {
				t.Errorf("host = %q", request.Host)
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

type testRoundTripper func(*http.Request) (*http.Response, error)

func (f testRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
