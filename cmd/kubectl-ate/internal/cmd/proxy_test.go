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

package cmd

import (
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/resources"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestActorProxy(t *testing.T) {
	var got *http.Request
	transport := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		got = r
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("counter: 1")), Request: r}, nil
	})
	proxy := httptest.NewServer(newActorProxy(resources.ActorRef{Atespace: "team", Name: "my-counter"}, transport))
	defer proxy.Close()

	req, err := http.NewRequest(http.MethodPost, proxy.URL+"/process?x=1", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	// A client cannot pick another actor.
	req.Header.Set(atenet.TargetActorHeader, "other/actor")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusOK || string(body) != "counter: 1" {
		t.Errorf("response = %d %q, want 200 %q", resp.StatusCode, body, "counter: 1")
	}
	if want := "https://atenet-router.ate-system.svc/process?x=1"; got.URL.String() != want {
		t.Errorf("URL = %q, want %q", got.URL, want)
	}
	if want := strings.TrimPrefix(proxy.URL, "http://"); got.Host != want {
		t.Errorf("Host = %q, want %q", got.Host, want)
	}
	if values, want := got.Header.Values(atenet.TargetActorHeader), []string{"team/my-counter"}; !slices.Equal(values, want) {
		t.Errorf("%s = %q, want %q", atenet.TargetActorHeader, values, want)
	}
}
