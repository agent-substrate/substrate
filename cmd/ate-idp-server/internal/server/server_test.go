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

package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const (
	esKeySet   = `{"keys":[{"kty":"EC","kid":"es","use":"sig","alg":"ES256","crv":"P-256","x":"eA","y":"eQ"}]}`
	bothKeySet = `{"keys":[{"kty":"RSA","kid":"rs","use":"sig","alg":"RS256","n":"bg","e":"AQAB"},` +
		`{"kty":"EC","kid":"es","use":"sig","alg":"ES256","crv":"P-256","x":"eA","y":"eQ"}]}`
)

func newLoadedServer(t *testing.T, issuer, jwks string) *Server {
	t.Helper()
	s, err := New(issuer)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Load([]byte(jwks)); err != nil {
		t.Fatal(err)
	}
	return s
}

func get(s *Server, method, target string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(method, target, nil))
	return w
}

type discoveryDoc struct {
	Issuer     string   `json:"issuer"`
	JWKSURI    string   `json:"jwks_uri"`
	Algorithms []string `json:"id_token_signing_alg_values_supported"`
}

func TestServesDocumentsUnderIssuerPath(t *testing.T) {
	tests := []struct {
		issuer      string
		prefixes    []string
		wantJWKSURI string
	}{
		{issuer: "https://idp.ate-system.svc", prefixes: []string{""}, wantJWKSURI: "https://idp.ate-system.svc/openid/v1/jwks"},
		{issuer: "https://idp.example.com/prod/", prefixes: []string{"/prod"}, wantJWKSURI: "https://idp.example.com/prod/openid/v1/jwks"},
		{issuer: "https://idp.example.com/a/../b", prefixes: []string{"/b", "/a/../b"}, wantJWKSURI: "https://idp.example.com/a/../b/openid/v1/jwks"},
	}
	for _, tt := range tests {
		s := newLoadedServer(t, tt.issuer, esKeySet)
		for _, prefix := range tt.prefixes {
			w := get(s, http.MethodGet, prefix+"/openid/v1/jwks")
			if w.Code != http.StatusOK || w.Body.String() != esKeySet {
				t.Errorf("%s: GET %s/openid/v1/jwks = %d %q, want 200 and the key set", tt.issuer, prefix, w.Code, w.Body)
			}

			w = get(s, http.MethodGet, prefix+"/.well-known/openid-configuration")
			if w.Code != http.StatusOK {
				t.Fatalf("%s: GET %s/.well-known/openid-configuration = %d, want 200", tt.issuer, prefix, w.Code)
			}
			var doc discoveryDoc
			if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
				t.Fatal(err)
			}
			if doc.Issuer != tt.issuer || doc.JWKSURI != tt.wantJWKSURI {
				t.Errorf("%s: discovery issuer %q, jwks_uri %q; want %q, %q", tt.issuer, doc.Issuer, doc.JWKSURI, tt.issuer, tt.wantJWKSURI)
			}
			wantHeaders := map[string]string{
				"Content-Type":                "application/json; charset=utf-8",
				"Cache-Control":               "public, max-age=60",
				"Access-Control-Allow-Origin": "*",
			}
			for name, want := range wantHeaders {
				if got := w.Header().Get(name); got != want {
					t.Errorf("%s: %s = %q, want %q", tt.issuer, name, got, want)
				}
			}
		}
	}
}

func TestNotReadyUntilLoaded(t *testing.T) {
	s, err := New("https://idp.ate-system.svc")
	if err != nil {
		t.Fatal(err)
	}
	for target, want := range map[string]int{
		"/healthz":                          http.StatusOK,
		"/readyz":                           http.StatusServiceUnavailable,
		"/.well-known/openid-configuration": http.StatusServiceUnavailable,
		"/openid/v1/jwks":                   http.StatusServiceUnavailable,
	} {
		if got := get(s, http.MethodGet, target).Code; got != want {
			t.Errorf("before Load: GET %s = %d, want %d", target, got, want)
		}
	}

	if err := s.Load([]byte(esKeySet)); err != nil {
		t.Fatal(err)
	}
	if got := get(s, http.MethodGet, "/readyz").Code; got != http.StatusOK {
		t.Errorf("after Load: GET /readyz = %d, want 200", got)
	}
}

func TestLoadRejectsBadKeySetsAndKeepsLastGood(t *testing.T) {
	s := newLoadedServer(t, "https://idp.ate-system.svc", esKeySet)
	for name, jwks := range map[string]string{
		"not JSON":        "{",
		"no keys":         `{"keys":[]}`,
		"key without alg": `{"keys":[{"kty":"EC","kid":"es"}]}`,
	} {
		if err := s.Load([]byte(jwks)); err == nil {
			t.Errorf("Load(%s) returned nil error", name)
		}
	}
	if got := get(s, http.MethodGet, "/openid/v1/jwks").Body.String(); got != esKeySet {
		t.Errorf("served key set = %q after rejected loads, want the last good one", got)
	}
}

func TestDiscoveryAdvertisesKeySetAlgorithms(t *testing.T) {
	s := newLoadedServer(t, "https://idp.ate-system.svc", bothKeySet)
	var doc discoveryDoc
	if err := json.Unmarshal(get(s, http.MethodGet, "/.well-known/openid-configuration").Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]string{"ES256", "RS256"}, doc.Algorithms); diff != "" {
		t.Errorf("algorithms (-want +got):\n%s", diff)
	}
}

func TestMethodsAndUnknownPaths(t *testing.T) {
	s := newLoadedServer(t, "https://idp.ate-system.svc", esKeySet)

	w := get(s, http.MethodHead, "/openid/v1/jwks")
	if w.Code != http.StatusOK || w.Body.Len() != 0 || w.Header().Get("Content-Type") == "" {
		t.Errorf("HEAD = %d with %d body bytes, want 200, headers, and no body", w.Code, w.Body.Len())
	}

	w = get(s, http.MethodPost, "/openid/v1/jwks")
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "GET, HEAD" {
		t.Errorf("POST = %d, Allow %q; want 405, Allow GET, HEAD", w.Code, w.Header().Get("Allow"))
	}

	if got := get(s, http.MethodGet, "/openid/v1/other").Code; got != http.StatusNotFound {
		t.Errorf("GET unknown path = %d, want 404", got)
	}
}

func TestWatchFileReloadsOnChange(t *testing.T) {
	s, err := New("https://idp.ate-system.svc")
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(file, []byte(esKeySet), 0o600); err != nil {
		t.Fatal(err)
	}
	go s.WatchFile(t.Context(), file, 10*time.Millisecond)
	waitForKeySet(t, s, esKeySet)

	if err := os.WriteFile(file, []byte(bothKeySet), 0o600); err != nil {
		t.Fatal(err)
	}
	waitForKeySet(t, s, bothKeySet)

	if err := os.WriteFile(file, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := get(s, http.MethodGet, "/openid/v1/jwks").Body.String(); got != bothKeySet {
		t.Errorf("served key set = %q after an invalid file, want the last good one", got)
	}
}

func waitForKeySet(t *testing.T, s *Server, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for get(s, http.MethodGet, "/openid/v1/jwks").Body.String() != want {
		if time.Now().After(deadline) {
			t.Fatalf("key set never became %q", want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
