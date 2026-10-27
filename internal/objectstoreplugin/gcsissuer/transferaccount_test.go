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

package gcsissuer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/api/option"
)

const testAccount = "ate-snapshot-transfer@proj.iam.gserviceaccount.com"

// fakeIAMCredentials answers generateAccessToken with a token that has left
// of its life remaining, counting calls.
type fakeIAMCredentials struct {
	t      *testing.T
	mu     sync.Mutex
	calls  int
	left   time.Duration
	status int
}

func (f *fakeIAMCredentials) handler(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if want := "/v1/projects/-/serviceAccounts/" + testAccount + ":generateAccessToken"; r.URL.Path != want {
		f.t.Errorf("path = %q, want %q", r.URL.Path, want)
	}
	var req struct {
		Scope    []string `json:"scope"`
		Lifetime string   `json:"lifetime"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		f.t.Errorf("decode request: %v", err)
	}
	if len(req.Scope) != 1 || req.Scope[0] != cloudPlatformScope || req.Lifetime != "3600s" {
		f.t.Errorf("request = %+v, want the cloud-platform scope for 3600s", req)
	}
	if f.status != 0 {
		w.WriteHeader(f.status)
		_, _ = w.Write([]byte(`{"error":{"code":403,"message":"denied"}}`))
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{
		"accessToken": "transfer-token",
		"expireTime":  time.Now().Add(f.left).UTC().Format(time.RFC3339),
	})
}

func (f *fakeIAMCredentials) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeIAMCredentials) options(t *testing.T) []option.ClientOption {
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(srv.Close)
	return []option.ClientOption{option.WithEndpoint(srv.URL), option.WithoutAuthentication()}
}

func TestTransferAccountSource_FetchesAgainWithLessThanFortyMinutesLeft(t *testing.T) {
	for _, tc := range []struct {
		name      string
		left      time.Duration
		wantCalls int
	}{
		{"45 minutes left is reused", 45 * time.Minute, 1},
		{"39 minutes left is fetched again", 39 * time.Minute, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			iam := &fakeIAMCredentials{t: t, left: tc.left}
			src, err := newTransferAccountSource(t.Context(), testAccount, iam.options(t)...)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				tok, err := src.Token()
				if err != nil {
					t.Fatalf("Token: %v", err)
				}
				if tok.AccessToken != "transfer-token" {
					t.Fatalf("token = %q, want the transfer account's", tok.AccessToken)
				}
			}
			if got := iam.callCount(); got != tc.wantCalls {
				t.Errorf("IAM Credentials calls = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

func TestTransferAccountSource_NamesTheAccountWhenRefused(t *testing.T) {
	iam := &fakeIAMCredentials{t: t, status: http.StatusForbidden}
	src, err := newTransferAccountSource(t.Context(), testAccount, iam.options(t)...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Token(); err == nil || !strings.Contains(err.Error(), testAccount) {
		t.Fatalf("Token error = %v, want one naming %s", err, testAccount)
	}
}

func TestTransferAccountSource_KeepsTheLastTokenWhenAFetchFails(t *testing.T) {
	for _, tc := range []struct {
		name    string
		left    time.Duration
		wantErr bool
	}{
		{"an unexpired token is returned", 39 * time.Minute, false},
		{"a token that would expire during the STS call is not", 20 * time.Second, true},
		{"an expired token is not", -time.Minute, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			iam := &fakeIAMCredentials{t: t, left: tc.left}
			src, err := newTransferAccountSource(t.Context(), testAccount, iam.options(t)...)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := src.Token(); err != nil {
				t.Fatalf("first Token: %v", err)
			}
			iam.mu.Lock()
			iam.status = http.StatusServiceUnavailable
			iam.mu.Unlock()
			tok, err := src.Token()
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("second Token error = %v, want error %v", err, tc.wantErr)
			}
			if !tc.wantErr && tok.AccessToken != "transfer-token" {
				t.Errorf("token = %q, want the previous one", tok.AccessToken)
			}
			if got := iam.callCount(); got != 2 {
				t.Errorf("IAM Credentials calls = %d, want 2", got)
			}
		})
	}
}

func TestNew_NeedsATransferAccount(t *testing.T) {
	if _, err := New(t.Context(), ""); err == nil || !strings.Contains(err.Error(), "transfer service account") {
		t.Fatalf("New without a transfer account = %v, want an error naming it", err)
	}
}

// TestIssue_DownscopesTheTransferAccountsToken checks the issuer's source is
// the impersonated account: STS reports no lifetime, so the downscoped token
// expires with the token the IAM Credentials API returned.
func TestIssue_DownscopesTheTransferAccountsToken(t *testing.T) {
	iam := &fakeIAMCredentials{t: t, left: time.Hour}
	sts := &fakeSTS{t: t}
	srv := httptest.NewServer(http.HandlerFunc(sts.handler))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	i, err := New(t.Context(), testAccount, WithIAMCredentialsOptions(iam.options(t)...),
		WithHTTPClient(&http.Client{Transport: redirect{sts: sts, target: target}}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tok, err := i.Issue(t.Context(), "9f1e", testGrants, false)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if sts.last["subject_token"] != "transfer-token" {
		t.Errorf("STS subject_token = %q, want the transfer account's token", sts.last["subject_token"])
	}
	if left := time.Until(tok.Expiry); left < sourceRefreshMargin {
		t.Errorf("downscoped token has %v left, want at least %v", left, sourceRefreshMargin)
	}
	if iam.callCount() != 1 {
		t.Errorf("IAM Credentials calls = %d, want 1", iam.callCount())
	}
}
