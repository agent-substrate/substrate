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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	transport := newEndpointTransport(mustParseURL(t, server.URL), server.Client().Transport, defaultStallTimeout)

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

const testStallTimeout = 200 * time.Millisecond

// newStallStore returns a store for server that cancels a request after
// testStallTimeout without progress and does not retry it.
func newStallStore(t *testing.T, server *httptest.Server) Store {
	t.Helper()
	stores, err := newTokenAzure(server.URL, []string{"snapshots"}, staticCredential{}, server.Client().Transport, policy.RetryOptions{MaxRetries: -1}, testStallTimeout)
	if err != nil {
		t.Fatal(err)
	}
	return stores["snapshots"]
}

// stall blocks a handler until the client gives up on the request.
func stall(t *testing.T, r *http.Request) {
	select {
	case <-r.Context().Done():
	case <-time.After(30 * time.Second):
		t.Error("client did not cancel the stalled request")
	}
}

func TestStalledRequestsAreUnavailable(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			// Accept the block, then never answer.
			_, _ = io.Copy(io.Discard, r.Body)
		}
		stall(t, r)
	}))
	defer server.Close()
	store := newStallStore(t, server)
	ctx := t.Context()

	for name, call := range map[string]func() error{
		"Delete":     func() error { return store.Delete(ctx, "prefix/file") },
		"List":       func() error { _, err := store.List(ctx, "prefix/"); return err },
		"Download":   func() error { _, err := store.Download(ctx, "prefix/file"); return err },
		"StageBlock": func() error { return store.StageBlock(ctx, "prefix/file", "YmxvY2s=", []byte("data"), nil) },
	} {
		start := time.Now()
		err := call()
		if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "no progress") {
			t.Errorf("%s against a stalled endpoint = %v, want ErrUnavailable for no progress", name, err)
		}
		if elapsed := time.Since(start); elapsed > 10*time.Second {
			t.Errorf("%s took %s against a stalled endpoint", name, elapsed)
		}
	}
}

// TestStalledDownloadResumes stalls the first response after one byte of the
// body; the download resumes from that byte with a new request.
func TestStalledDownloadResumes(t *testing.T) {
	const data = "snapshot bytes"
	var mu sync.Mutex
	var ranges []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ranges = append(ranges, r.Header.Get("x-ms-range"))
		mu.Unlock()
		offset := 0
		if rng := r.Header.Get("x-ms-range"); rng != "" {
			if _, err := fmt.Sscanf(rng, "bytes=%d-", &offset); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		remaining := data[offset:]
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Length", fmt.Sprint(len(remaining)))
		if offset > 0 {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(data)-1, len(data)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, remaining)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, remaining[:1])
		w.(http.Flusher).Flush()
		stall(t, r)
	}))
	defer server.Close()

	download, err := newStallStore(t, server).Download(t.Context(), "prefix/file")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer download.Body.Close()
	got, err := io.ReadAll(download.Body)
	if err != nil || string(got) != data {
		t.Fatalf("reading the body = %q, %v; want %q", got, err, data)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"", "bytes=1-"}; !slices.Equal(ranges, want) {
		t.Fatalf("GET ranges = %q, want %q", ranges, want)
	}
}

// TestSlowDownloadConsumerIsNotStalled pauses between reads of a download for
// longer than the stall timeout: only time spent waiting on the endpoint counts.
func TestSlowDownloadConsumerIsNotStalled(t *testing.T) {
	const data = "snapshot bytes"
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		_, _ = io.WriteString(w, data)
	}))
	defer server.Close()

	download, err := newStallStore(t, server).Download(t.Context(), "prefix/file")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer download.Body.Close()
	first := make([]byte, 1)
	if _, err := io.ReadFull(download.Body, first); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * testStallTimeout)
	rest, err := io.ReadAll(download.Body)
	if err != nil || string(first)+string(rest) != data {
		t.Fatalf("reading the body = %q, %v; want %q", string(first)+string(rest), err, data)
	}
	if n := requests.Load(); n != 1 {
		t.Fatalf("server saw %d requests, want 1", n)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// TestResentRequestBodyRestartsStallTimer sends the body from GetBody, as
// net/http does when it retries on a new connection, one byte at a time over
// more than the stall timeout: each read is progress.
func TestResentRequestBodyRestartsStallTimer(t *testing.T) {
	const data = "block data"
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.GetBody == nil {
			return nil, errors.New("request has no GetBody")
		}
		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		var got []byte
		b := make([]byte, 1)
		for {
			n, err := body.Read(b)
			got = append(got, b[:n]...)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			time.Sleep(testStallTimeout / 2)
		}
		if err := req.Context().Err(); err != nil {
			return nil, fmt.Errorf("context after sending %q: %w", got, err)
		}
		return &http.Response{StatusCode: http.StatusCreated, Body: http.NoBody, Request: req}, nil
	})
	endpoint, err := url.Parse("https://account.blob.core.windows.net")
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, endpoint.String()+"/snapshots/file", strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := newEndpointTransport(endpoint, base, testStallTimeout).Do(req)
	if err != nil {
		t.Fatalf("Do() = %v, want the resent body to count as progress", err)
	}
	_ = resp.Body.Close()
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
	stores, err := newTokenAzure(accountURL, []string{"snapshots"}, credential, base, retry, defaultStallTimeout)
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
