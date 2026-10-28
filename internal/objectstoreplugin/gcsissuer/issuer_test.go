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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google/downscope"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/objectstoreplugin/gcsboundary"
	"go.ate.dev/substrate/internal/resources"
)

func prefix(uri string) resources.StoragePrefix {
	p, err := resources.ParseStoragePrefix(uri)
	if err != nil {
		panic(err)
	}
	return p
}

var (
	rwGrant    = actorscope.Grant{Prefix: prefix("gs://snaps/root/atespaces/team-a/actors/9f1e"), Access: actorscope.ReadWrite}
	roGrant    = actorscope.Grant{Prefix: prefix("gs://snaps/root/atespaces/ate-golden/tags/7a3c"), Access: actorscope.ReadOnly}
	testGrants = []actorscope.Grant{rwGrant, roGrant}
)

// fakeSTS records the last exchange request and answers with a token.
type fakeSTS struct {
	t       *testing.T
	calls   int
	status  int
	body    string
	lastURL string
	last    map[string]string
}

func (f *fakeSTS) handler(w http.ResponseWriter, r *http.Request) {
	f.calls++
	if err := r.ParseForm(); err != nil {
		f.t.Errorf("parse form: %v", err)
	}
	f.last = map[string]string{}
	for k := range r.PostForm {
		f.last[k] = r.PostForm.Get(k)
	}
	if f.status != 0 {
		w.WriteHeader(f.status)
	}
	body := f.body
	if body == "" {
		body = `{"access_token":"downscoped-` + strings.Repeat("x", 8) + `","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer"}`
	}
	_, _ = w.Write([]byte(body))
}

// redirect sends every request to the fake STS, recording where it was bound.
type redirect struct {
	sts    *fakeSTS
	target *url.URL
}

func (r redirect) RoundTrip(req *http.Request) (*http.Response, error) {
	r.sts.lastURL = req.URL.String()
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host = r.target.Scheme, r.target.Host
	return http.DefaultTransport.RoundTrip(req)
}

func newTestIssuer(t *testing.T, sts *fakeSTS, src oauth2.TokenSource, now func() time.Time) *Issuer {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(sts.handler))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: redirect{sts: sts, target: target}}
	i, err := New(t.Context(), "", WithTokenSource(src), WithHTTPClient(client), WithClock(now), WithCacheSize(2))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return i
}

func staticSource(expiry time.Time) oauth2.TokenSource {
	return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "source-token", Expiry: expiry})
}

func TestIssue_ExchangesWithTheBoundary(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	expiry := now.Add(time.Hour)
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(expiry), func() time.Time { return now })

	tok, err := i.Issue(t.Context(), "9f1e", testGrants, false)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if !strings.HasPrefix(tok.AccessToken, "downscoped-") {
		t.Errorf("AccessToken = %q", tok.AccessToken)
	}
	if !tok.Expiry.Equal(expiry) {
		t.Errorf("Expiry = %v, want the source token's %v", tok.Expiry, expiry)
	}
	if sts.lastURL != "https://sts.googleapis.com/v1/token" {
		t.Errorf("exchanged at %s", sts.lastURL)
	}
	for k, want := range map[string]string{
		"grant_type":           "urn:ietf:params:oauth:grant-type:token-exchange",
		"subject_token_type":   "urn:ietf:params:oauth:token-type:access_token",
		"requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
		"subject_token":        "source-token",
	} {
		if got := sts.last[k]; got != want {
			t.Errorf("form %s = %q, want %q", k, got, want)
		}
	}
	var options struct {
		AccessBoundary struct {
			AccessBoundaryRules []downscope.AccessBoundaryRule `json:"accessBoundaryRules"`
		} `json:"accessBoundary"`
	}
	if err := json.Unmarshal([]byte(sts.last["options"]), &options); err != nil {
		t.Fatalf("options is not a boundary: %v", err)
	}
	want, _ := gcsboundary.FromGrants(testGrants)
	if diff := cmp.Diff(want, options.AccessBoundary.AccessBoundaryRules); diff != "" {
		t.Errorf("rules on the wire (-want +got):\n%s", diff)
	}
}

func TestIssue_CachesPerActorUntilNearExpiry(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	clock := now
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return clock })

	first, err := i.Issue(t.Context(), "9f1e", testGrants, false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := i.Issue(t.Context(), "9f1e", testGrants, false)
	if err != nil {
		t.Fatal(err)
	}
	if sts.calls != 1 || second != first {
		t.Fatalf("second Issue made %d STS calls in total and returned a different token", sts.calls)
	}
	if _, err := i.Issue(t.Context(), "other", testGrants, false); err != nil {
		t.Fatal(err)
	}
	if sts.calls != 2 {
		t.Fatalf("a different actor should exchange, STS calls = %d", sts.calls)
	}
	// With less than minServeLifetime left the cached token is not served.
	clock = now.Add(time.Hour - minServeLifetime + time.Second)
	if _, err := i.Issue(t.Context(), "9f1e", testGrants, false); err != nil {
		t.Fatal(err)
	}
	if sts.calls != 3 {
		t.Fatalf("a token with under %v left should be re-minted, STS calls = %d", minServeLifetime, sts.calls)
	}
}

func TestIssue_ForceBypassesTheCache(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
	for _, force := range []bool{false, true} {
		if _, err := i.Issue(t.Context(), "9f1e", testGrants, force); err != nil {
			t.Fatal(err)
		}
	}
	if sts.calls != 2 {
		t.Fatalf("force should exchange again, STS calls = %d", sts.calls)
	}
}

func TestIssue_CacheIsBounded(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
	for _, uid := range []string{"a", "b", "c", "a"} {
		if _, err := i.Issue(t.Context(), uid, testGrants, false); err != nil {
			t.Fatal(err)
		}
	}
	// Size 2: c evicted a, so the last call for a exchanged again.
	if sts.calls != 4 || i.cache.Len() != 2 {
		t.Fatalf("STS calls = %d, cache len = %d, want 4 and 2", sts.calls, i.cache.Len())
	}
}

func TestIssue_ServesTheCachedTokenWhenItCoversTheGrants(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	rwOther := actorscope.Grant{Prefix: prefix("gs://other/root/atespaces/team-a/actors/9f1e"), Access: actorscope.ReadWrite}
	roOnOwn := actorscope.Grant{Prefix: rwGrant.Prefix, Access: actorscope.ReadOnly}
	for _, tc := range []struct {
		name      string
		first     []actorscope.Grant
		second    []actorscope.Grant
		wantMints int
	}{
		{"same grants", testGrants, testGrants, 1},
		{"tag dropped after the first suspend", testGrants, []actorscope.Grant{rwGrant}, 1},
		{"read-only covered by read-write", testGrants, []actorscope.Grant{roOnOwn}, 1},
		{"repoint adds a location", testGrants, []actorscope.Grant{rwGrant, rwOther, roGrant}, 2},
		{"read-write not covered by read-only", []actorscope.Grant{roOnOwn}, []actorscope.Grant{rwGrant}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sts := &fakeSTS{t: t}
			i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
			for _, grants := range [][]actorscope.Grant{tc.first, tc.second} {
				if _, err := i.Issue(t.Context(), "9f1e", grants, false); err != nil {
					t.Fatal(err)
				}
			}
			if sts.calls != tc.wantMints {
				t.Fatalf("STS calls = %d, want %d", sts.calls, tc.wantMints)
			}
		})
	}
}

func TestIssue_RefusesEmptyGrantsEvenWhenCached(t *testing.T) {
	// An actor that lost its generations derives no grants. The cached token
	// would cover an empty request, so the refusal has to come first.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
	if _, err := i.Issue(t.Context(), "9f1e", testGrants, false); err != nil {
		t.Fatal(err)
	}
	if _, err := i.Issue(t.Context(), "9f1e", nil, false); err == nil {
		t.Fatal("Issue with no grants served the cached token")
	}
	if sts.calls != 1 {
		t.Fatalf("STS calls = %d, want 1", sts.calls)
	}
}

func TestIssue_StopsWhenTheCallerGivesUp(t *testing.T) {
	// STS hangs. A cancelled caller must get an error at once, not after the
	// client timeout.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: redirect{sts: &fakeSTS{t: t}, target: target}, Timeout: time.Minute}
	i, err := New(t.Context(), "", WithTokenSource(staticSource(now.Add(time.Hour))), WithHTTPClient(client), WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := i.Issue(ctx, "9f1e", testGrants, false); err == nil {
		t.Fatal("Issue succeeded against a hanging STS")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Issue took %v to notice the cancelled context", took)
	}
}

func TestWithCacheSize_ZeroIsBounded(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t}
	srv := httptest.NewServer(http.HandlerFunc(sts.handler))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	i, err := New(t.Context(), "", WithTokenSource(staticSource(now.Add(time.Hour))), WithHTTPClient(&http.Client{Transport: redirect{sts: sts, target: target}}), WithClock(func() time.Time { return now }), WithCacheSize(0))
	if err != nil {
		t.Fatal(err)
	}
	for _, uid := range []string{"a", "b"} {
		if _, err := i.Issue(t.Context(), uid, testGrants, false); err != nil {
			t.Fatal(err)
		}
	}
	if i.cache.Len() != 1 {
		t.Fatalf("cache len = %d, want 1", i.cache.Len())
	}
}

func TestIssue_DoesNotCacheAShortLivedToken(t *testing.T) {
	// The source credential can hand out a token with less than the floor
	// left. It is returned, so the transfer can try, but not cached.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(minServeLifetime-time.Minute)), func() time.Time { return now })
	for range 2 {
		tok, err := i.Issue(t.Context(), "9f1e", testGrants, false)
		if err != nil {
			t.Fatal(err)
		}
		if tok.AccessToken == "" {
			t.Fatal("no token returned")
		}
	}
	if sts.calls != 2 || i.cache.Len() != 0 {
		t.Fatalf("STS calls = %d, cache len = %d, want 2 and 0", sts.calls, i.cache.Len())
	}
}

func TestIssue_Errors(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	for _, tc := range []struct {
		name   string
		sts    *fakeSTS
		source oauth2.TokenSource
		grants []actorscope.Grant
		want   string
		// code is Internal for a failure the same request would hit again
		// and Unknown for an STS or source failure, which carries no code.
		code codes.Code
	}{
		{"source without expiry", &fakeSTS{t: t}, staticSource(time.Time{}), testGrants, "no expiry", codes.Unknown},
		{"STS refuses", &fakeSTS{t: t, status: 400, body: `{"error":"invalid_grant"}`}, staticSource(now.Add(time.Hour)), testGrants, "400", codes.Unknown},
		{"STS answers without a token", &fakeSTS{t: t, body: `{}`}, staticSource(now.Add(time.Hour)), testGrants, "no access token", codes.Unknown},
		{"no grants", &fakeSTS{t: t}, staticSource(now.Add(time.Hour)), nil, "no storage grants", codes.Internal},
		{"too many grants", &fakeSTS{t: t}, staticSource(now.Add(time.Hour)), manyGrants(11), "greater than 10", codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i := newTestIssuer(t, tc.sts, tc.source, clock)
			_, err := i.Issue(t.Context(), "9f1e", tc.grants, false)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
			if status.Code(err) != tc.code {
				t.Errorf("code = %v, want %v", status.Code(err), tc.code)
			}
		})
	}
}

func manyGrants(n int) []actorscope.Grant {
	var gs []actorscope.Grant
	for i := range n {
		gs = append(gs, actorscope.Grant{Prefix: prefix(fmt.Sprintf("gs://b/p%d", i)), Access: actorscope.ReadOnly})
	}
	return gs
}
