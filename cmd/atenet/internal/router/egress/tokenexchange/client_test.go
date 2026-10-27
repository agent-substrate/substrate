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

package tokenexchange

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	subjectToken = "subject.jwt.token"
	audience     = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/ate/providers/actors"
)

// newTestServer starts a TLS server running handler at /token and returns it
// with a Client that trusts its certificate.
func newTestServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *Client) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/token", handler)
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	c, err := New(writeCAFile(t, srv))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv, c
}

func writeCAFile(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.crt")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(path, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func respond(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}
}

func TestExchangeSendsTokenExchangeForm(t *testing.T) {
	for _, tt := range []struct {
		name   string
		scopes []string
		want   url.Values
	}{{
		name:   "scopes are space-joined",
		scopes: []string{"https://www.googleapis.com/auth/cloud-platform", "read"},
		want: url.Values{
			"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
			"subject_token":        {subjectToken},
			"subject_token_type":   {"urn:ietf:params:oauth:token-type:jwt"},
			"audience":             {audience},
			"scope":                {"https://www.googleapis.com/auth/cloud-platform read"},
			"requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
		},
	}, {
		name: "no scope without scopes",
		want: url.Values{
			"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
			"subject_token":        {subjectToken},
			"subject_token_type":   {"urn:ietf:params:oauth:token-type:jwt"},
			"audience":             {audience},
			"requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
		},
	}} {
		t.Run(tt.name, func(t *testing.T) {
			var got url.Values
			srv, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("method = %s, want POST", r.Method)
				}
				if ct := r.Header.Get("Content-Type"); ct != "application/x-www-form-urlencoded" {
					t.Errorf("Content-Type = %q, want application/x-www-form-urlencoded", ct)
				}
				if err := r.ParseForm(); err != nil {
					t.Errorf("ParseForm: %v", err)
				}
				got = r.PostForm
				respond(http.StatusOK, `{"access_token":"at","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":600}`)(w, r)
			})
			if _, err := c.Exchange(t.Context(), srv.URL+"/token", subjectToken, audience, tt.scopes); err != nil {
				t.Fatalf("Exchange: %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("form (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExchangeReturnsToken(t *testing.T) {
	for _, tt := range []struct {
		name          string
		body          string
		wantExpiresIn time.Duration
	}{{
		name:          "with expires_in",
		body:          `{"access_token":"ya29.token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":600}`,
		wantExpiresIn: 600 * time.Second,
	}, {
		name: "without expires_in or issued_token_type",
		body: `{"access_token":"ya29.token","token_type":"Bearer"}`,
	}} {
		t.Run(tt.name, func(t *testing.T) {
			srv, c := newTestServer(t, respond(http.StatusOK, tt.body))
			before := time.Now()
			tok, err := c.Exchange(t.Context(), srv.URL+"/token", subjectToken, audience, nil)
			after := time.Now()
			if err != nil {
				t.Fatalf("Exchange: %v", err)
			}
			if tok.AccessToken != "ya29.token" {
				t.Errorf("AccessToken = %q, want ya29.token", tok.AccessToken)
			}
			if tt.wantExpiresIn == 0 {
				if !tok.ExpiresAt.IsZero() {
					t.Errorf("ExpiresAt = %v, want zero", tok.ExpiresAt)
				}
				return
			}
			if tok.ExpiresAt.Before(before.Add(tt.wantExpiresIn)) || tok.ExpiresAt.After(after.Add(tt.wantExpiresIn)) {
				t.Errorf("ExpiresAt = %v, want %v after the exchange", tok.ExpiresAt, tt.wantExpiresIn)
			}
		})
	}
}

func TestExchangeErrors(t *testing.T) {
	oauthError := func(code string) string {
		return fmt.Sprintf(`{"error":%q,"error_description":"details"}`, code)
	}
	for _, tt := range []struct {
		name     string
		status   int
		body     string
		wantCode codes.Code
	}{
		{"invalid_grant", http.StatusBadRequest, oauthError("invalid_grant"), codes.PermissionDenied},
		{"invalid_target", http.StatusBadRequest, oauthError("invalid_target"), codes.PermissionDenied},
		{"unauthorized_client", http.StatusUnauthorized, oauthError("unauthorized_client"), codes.PermissionDenied},
		{"invalid_scope", http.StatusBadRequest, oauthError("invalid_scope"), codes.PermissionDenied},
		{"access_denied", http.StatusBadRequest, oauthError("access_denied"), codes.PermissionDenied},
		{"invalid_request", http.StatusBadRequest, oauthError("invalid_request"), codes.FailedPrecondition},
		{"invalid_client", http.StatusUnauthorized, oauthError("invalid_client"), codes.FailedPrecondition},
		{"unsupported_grant_type", http.StatusBadRequest, oauthError("unsupported_grant_type"), codes.FailedPrecondition},
		{"400 without an error code", http.StatusBadRequest, "bad request", codes.FailedPrecondition},
		{"404", http.StatusNotFound, "not found", codes.FailedPrecondition},
		{"429", http.StatusTooManyRequests, "slow down", codes.Unavailable},
		{"500", http.StatusInternalServerError, "", codes.Unavailable},
		{"503", http.StatusServiceUnavailable, oauthError("temporarily_unavailable"), codes.Unavailable},
		{"200 with a body that is not JSON", http.StatusOK, "<html>", codes.Unavailable},
		{"200 without access_token", http.StatusOK, `{"token_type":"Bearer","expires_in":600}`, codes.Unavailable},
		{"200 with another issued_token_type", http.StatusOK, `{"access_token":"at","issued_token_type":"urn:ietf:params:oauth:token-type:jwt"}`, codes.Unavailable},
		{"200 with a control character in access_token", http.StatusOK, `{"access_token":"at\r\nX-Injected: 1"}`, codes.Unavailable},
		{"200 with non-ASCII in access_token", http.StatusOK, `{"access_token":"atÿ"}`, codes.Unavailable},
		{"200 over 1 MiB", http.StatusOK, `{"access_token":"at"}` + strings.Repeat(" ", maxResponseBytes), codes.Unavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv, c := newTestServer(t, respond(tt.status, tt.body))
			_, err := c.Exchange(t.Context(), srv.URL+"/token", subjectToken, audience, nil)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("Exchange error = %v (code %v), want code %v", err, got, tt.wantCode)
			}
			if strings.Contains(err.Error(), subjectToken) {
				t.Errorf("error %q contains the subject token", err)
			}
		})
	}
}

func TestExchangeDoesNotFollowRedirects(t *testing.T) {
	var followed atomic.Bool
	srv, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("redirected") {
			followed.Store(true)
			respond(http.StatusOK, `{"access_token":"at"}`)(w, r)
			return
		}
		http.Redirect(w, r, "/token?redirected", http.StatusTemporaryRedirect)
	})
	_, err := c.Exchange(t.Context(), srv.URL+"/token", subjectToken, audience, nil)
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("Exchange error = %v (code %v), want code %v", err, got, codes.FailedPrecondition)
	}
	if followed.Load() {
		t.Error("the client followed the redirect")
	}
}

func TestExchangeClipsErrorDescription(t *testing.T) {
	description := strings.Repeat("d", maxDetailBytes+50)
	srv, c := newTestServer(t, respond(http.StatusBadRequest, fmt.Sprintf(`{"error":"invalid_request","error_description":%q}`, description)))
	_, err := c.Exchange(t.Context(), srv.URL+"/token", subjectToken, audience, nil)
	if err == nil {
		t.Fatal("Exchange succeeded, want an error")
	}
	if !strings.Contains(err.Error(), `"`+description[:maxDetailBytes]+`"`) {
		t.Errorf("error %q does not carry the description clipped to %d bytes", err, maxDetailBytes)
	}
}

func TestExchangeHonorsContextDeadline(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	srv, c := newTestServer(t, func(http.ResponseWriter, *http.Request) {
		<-release
	})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := c.Exchange(ctx, srv.URL+"/token", subjectToken, audience, nil)
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Errorf("Exchange error = %v (code %v), want code %v", err, got, codes.DeadlineExceeded)
	}
}

func TestExchangeUnreachableEndpoint(t *testing.T) {
	srv, c := newTestServer(t, respond(http.StatusOK, `{"access_token":"at"}`))
	srv.Close()
	_, err := c.Exchange(t.Context(), srv.URL+"/token", subjectToken, audience, nil)
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("Exchange error = %v (code %v), want code %v", err, got, codes.Unavailable)
	}
}

func TestNewTrustsCAFile(t *testing.T) {
	srv, trusting := newTestServer(t, respond(http.StatusOK, `{"access_token":"at"}`))
	if _, err := trusting.Exchange(t.Context(), srv.URL+"/token", subjectToken, audience, nil); err != nil {
		t.Errorf("Exchange with the server's CA: %v", err)
	}

	systemOnly, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = systemOnly.Exchange(t.Context(), srv.URL+"/token", subjectToken, audience, nil)
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("Exchange without the server's CA: error = %v (code %v), want code %v", err, got, codes.Unavailable)
	}
}

func TestNewRejectsUnusableCAFile(t *testing.T) {
	noCerts := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(noCerts, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"missing file":        filepath.Join(t.TempDir(), "missing.pem"),
		"file without a cert": noCerts,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(path); err == nil {
				t.Errorf("New(%q) succeeded, want an error", path)
			}
		})
	}
}
