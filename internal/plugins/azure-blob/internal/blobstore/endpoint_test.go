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

package blobstore

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

const testToken = "test-storage-token"

type staticCredential struct{}

func (staticCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: testToken, ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// recorder counts requests and the Authorization headers they carried.
type recorder struct {
	mu             sync.Mutex
	requests       int
	authorizations []string
	handler        http.HandlerFunc
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.requests++
	if auth := req.Header.Get("Authorization"); auth != "" {
		r.authorizations = append(r.authorizations, auth)
	}
	r.mu.Unlock()
	if r.handler != nil {
		r.handler(w, req)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (r *recorder) seen() (int, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.requests, append([]string(nil), r.authorizations...)
}

func TestAzureDoesNotFollowRedirectsAwayFromAccount(t *testing.T) {
	other := &recorder{}
	otherServer := httptest.NewTLSServer(other)
	defer otherServer.Close()
	account := &recorder{handler: func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, otherServer.URL+req.URL.RequestURI(), http.StatusTemporaryRedirect)
	}}
	accountServer := httptest.NewTLSServer(account)
	defer accountServer.Close()

	// httptest TLS servers share one certificate, so this transport trusts
	// both servers and only the endpoint check keeps requests off otherServer.
	store := newTokenStore(t, accountServer.URL, staticCredential{}, accountServer.Client().Transport, policy.RetryOptions{})
	ctx := t.Context()
	if err := store.Delete(ctx, "prefix/file"); err == nil {
		t.Error("Delete succeeded through a redirect")
	}
	if _, err := store.List(ctx, "prefix/"); err == nil {
		t.Error("List succeeded through a redirect")
	}
	if _, err := store.Download(ctx, "prefix/file"); err == nil {
		t.Error("Download succeeded through a redirect")
	}

	if _, auths := account.seen(); len(auths) == 0 || auths[0] != "Bearer "+testToken {
		t.Fatalf("account endpoint saw Authorization %q, want the storage token", auths)
	}
	if requests, auths := other.seen(); requests != 0 {
		t.Fatalf("redirect target received %d requests with Authorization %q, want none", requests, auths)
	}
}

func TestEndpointTransportRejectsOtherEndpoints(t *testing.T) {
	account := &recorder{}
	server := httptest.NewTLSServer(account)
	defer server.Close()
	store := newTokenStore(t, server.URL, staticCredential{}, server.Client().Transport, policy.RetryOptions{})
	other := &recorder{}
	otherServer := httptest.NewTLSServer(other)
	defer otherServer.Close()
	transport := newEndpointTransport(mustParseURL(t, server.URL), server.Client().Transport)

	for _, tc := range []struct {
		name   string
		target string
	}{
		{name: "other host", target: "https://attacker.example/snapshots/file"},
		{name: "other port", target: otherServer.URL + "/snapshots/file"},
		{name: "plain http", target: "http://" + strings.TrimPrefix(server.URL, "https://") + "/snapshots/file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, tc.target, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+testToken)
			resp, err := transport.Do(req)
			if err == nil {
				resp.Body.Close()
				t.Fatalf("Do(%s) succeeded, want refusal", tc.target)
			}
			var foreign *foreignEndpointError
			if !errors.As(err, &foreign) {
				t.Fatalf("Do(%s) = %v, want foreignEndpointError", tc.target, err)
			}
		})
	}
	for name, r := range map[string]*recorder{"account": account, "other": other} {
		if requests, _ := r.seen(); requests != 0 {
			t.Fatalf("%s endpoint received %d requests for other endpoints", name, requests)
		}
	}

	// The configured endpoint itself is reachable, with the token attached.
	if err := store.Delete(t.Context(), "prefix/file"); err != nil {
		t.Fatalf("Delete on the configured endpoint: %v", err)
	}
	if _, auths := account.seen(); len(auths) != 1 || auths[0] != "Bearer "+testToken {
		t.Fatalf("account endpoint saw Authorization %q, want the storage token once", auths)
	}
}

func TestNewAzureRequiresHTTPS(t *testing.T) {
	_, err := NewAzure("http://account.blob.core.windows.net", []string{"snapshots"}, staticCredential{})
	if err == nil || !strings.Contains(err.Error(), "must use https") {
		t.Fatalf("NewAzure(http) = %v, want https error", err)
	}
}

// newTokenStore returns the store for container "snapshots" in the account at
// accountURL.
func newTokenStore(t *testing.T, accountURL string, credential azcore.TokenCredential, base http.RoundTripper, retry policy.RetryOptions) Store {
	t.Helper()
	stores, err := newTokenAzure(accountURL, []string{"snapshots"}, credential, base, retry)
	if err != nil {
		t.Fatal(err)
	}
	return stores["snapshots"]
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
