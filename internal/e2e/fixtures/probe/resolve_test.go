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
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

// callResolve invokes the handler for host and decodes its response.
func callResolve(t *testing.T, host string) resolveResult {
	t.Helper()
	recorder := httptest.NewRecorder()
	resolve(recorder, httptest.NewRequest(http.MethodGet, "/resolve?host="+host, nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("/resolve for %q answered HTTP %d, want 200: a failed lookup is a result, not an error", host, recorder.Code)
	}
	var out resolveResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &out); err != nil {
		t.Fatalf("decoding the /resolve response: %v (body %q)", err, recorder.Body)
	}
	return out
}

// TestResolve covers the answer shape the UDP-egress e2e suite asserts on: the
// addresses come back, so a test can check the name resolved to the Service it
// deployed rather than settling for "the lookup returned".
//
// localhost, because it resolves from /etc/hosts with no nameserver involved --
// this pins the handler, not the machine's DNS.
func TestResolve(t *testing.T) {
	got := callResolve(t, "localhost")
	if got.Error != "" {
		t.Fatalf("resolving localhost failed: %s", got.Error)
	}
	if !slices.Contains(got.Addresses, "127.0.0.1") && !slices.Contains(got.Addresses, "::1") {
		t.Errorf("localhost resolved to %v, want a loopback address", got.Addresses)
	}
	if got.Host != "localhost" {
		t.Errorf("Host = %q, want the name that was asked for", got.Host)
	}
}

// TestResolveMissingHost covers the caller error. It must not look like a
// resolver failure to a test asserting DNS is broken -- both carry Error, but
// only this one carries it without a lookup having happened.
func TestResolveMissingHost(t *testing.T) {
	got := callResolve(t, "")
	if got.Error == "" {
		t.Error("Error is empty with no host to look up")
	}
	if len(got.Addresses) != 0 {
		t.Errorf("Addresses = %v with no host to look up, want none", got.Addresses)
	}
}
