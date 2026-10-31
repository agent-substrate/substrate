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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"golang.org/x/oauth2"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/objectstoreplugin/gcsboundary"
	pb "go.ate.dev/substrate/internal/proto/clientsideboundarypb"
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

// fakeSTS records the last exchange request and answers with an intermediary
// token and a session key it keeps, so a test can open the boundaries minted
// with it.
type fakeSTS struct {
	t      *testing.T
	status int
	body   string
	// expiresIn is the lifetime reported: an hour when zero, as STS reports
	// for a service account's fresh token, and none when negative.
	expiresIn int
	// unpaddedKey sends the session key without base64 padding.
	unpaddedKey bool
	// holdSubject pauses a request for that subject token: the handler
	// closes held and waits for release before answering.
	holdSubject   string
	held, release chan struct{}
	heldOnce      sync.Once

	mu      sync.Mutex
	calls   int
	lastURL string
	last    map[string]string
	key     *keyset.Handle
}

func (f *fakeSTS) handler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		f.t.Errorf("parse form: %v", err)
	}
	if f.holdSubject != "" && r.PostForm.Get("subject_token") == f.holdSubject {
		// Once, so a second held request leaves the call counts to report it
		// rather than panicking on a closed channel.
		f.heldOnce.Do(func() { close(f.held) })
		<-f.release
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.last = map[string]string{}
	for k := range r.PostForm {
		f.last[k] = r.PostForm.Get(k)
	}
	if f.status != 0 {
		w.WriteHeader(f.status)
	}
	if f.body != "" {
		_, _ = w.Write([]byte(f.body))
		return
	}
	resp := map[string]any{
		"access_token":                fmt.Sprintf("intermediary-%d", f.calls),
		"issued_token_type":           intermediaryTokenType,
		"token_type":                  "Bearer",
		"access_boundary_session_key": f.sessionKey(),
	}
	switch {
	case f.expiresIn > 0:
		resp["expires_in"] = f.expiresIn
	case f.expiresIn == 0:
		resp["expires_in"] = 3600
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// sessionKey is the session key STS hands out, a serialized Tink keyset.
func (f *fakeSTS) sessionKey() string {
	for {
		if f.key == nil {
			h, err := keyset.NewHandle(aead.AES256GCMKeyTemplate())
			if err != nil {
				f.t.Fatal(err)
			}
			f.key = h
		}
		var buf bytes.Buffer
		if err := insecurecleartextkeyset.Write(f.key, keyset.NewBinaryWriter(&buf)); err != nil {
			f.t.Fatal(err)
		}
		key := base64.StdEncoding.EncodeToString(buf.Bytes())
		if !f.unpaddedKey {
			return key
		}
		// A key whose encoding needs no padding would not test anything, so
		// draw another.
		if strings.HasSuffix(key, "=") {
			return strings.TrimRight(key, "=")
		}
		f.key = nil
	}
}

// open splits a minted token into its intermediary token and the boundary,
// decrypted with the session key, as STS does.
func (f *fakeSTS) open(t *testing.T, token string) (string, *pb.ClientSideAccessBoundary) {
	t.Helper()
	inter, sealed, ok := strings.Cut(token, ".")
	if !ok {
		t.Fatalf("token %q has no boundary", token)
	}
	ct, err := base64.RawURLEncoding.DecodeString(sealed)
	if err != nil {
		t.Fatalf("boundary is not unpadded base64url: %v", err)
	}
	f.mu.Lock()
	a, err := aead.New(f.key)
	f.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	plain, err := a.Decrypt(ct, []byte{})
	if err != nil {
		t.Fatalf("decrypting the boundary: %v", err)
	}
	var b pb.ClientSideAccessBoundary
	if err := proto.Unmarshal(plain, &b); err != nil {
		t.Fatalf("decoding the boundary: %v", err)
	}
	return inter, &b
}

// redirect sends every request to the fake STS, recording where it was bound.
type redirect struct {
	sts    *fakeSTS
	target *url.URL
}

func (r redirect) RoundTrip(req *http.Request) (*http.Response, error) {
	r.sts.mu.Lock()
	r.sts.lastURL = req.URL.String()
	r.sts.mu.Unlock()
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

// hold pauses STS requests for subject until the test releases them. The
// release on cleanup runs before the server's close, so a request still
// paused does not block it.
func (f *fakeSTS) hold(t *testing.T, subject string) {
	f.holdSubject, f.held, f.release = subject, make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-f.release:
		default:
			close(f.release)
		}
	})
}

func staticSource(expiry time.Time) oauth2.TokenSource {
	return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "source-token", Expiry: expiry})
}

func TestIssue_MintsTheBoundaryIntoTheToken(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	expiry := now.Add(time.Hour)
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(expiry), func() time.Time { return now })

	tok, err := i.Issue(t.Context(), "9f1e", testGrants, false)
	if err != nil {
		t.Fatalf("Issue: %v", err)
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
		"requested_token_type": "urn:ietf:params:oauth:token-type:access_boundary_intermediary_token",
		"subject_token":        "source-token",
	} {
		if got := sts.last[k]; got != want {
			t.Errorf("form %s = %q, want %q", k, got, want)
		}
	}
	inter, got := sts.open(t, tok.AccessToken)
	if inter != "intermediary-1" {
		t.Errorf("token starts with %q, want the intermediary token", inter)
	}
	rules, err := gcsboundary.FromGrants(testGrants)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.GetAccessBoundaryRules()) != len(rules) {
		t.Fatalf("boundary has %d rules, want %d", len(got.GetAccessBoundaryRules()), len(rules))
	}
	wantPaths := []string{
		"projects/_/buckets/snaps/objects/root/atespaces/team-a/actors/9f1e/",
		"projects/_/buckets/snaps/objects/root/atespaces/ate-golden/tags/7a3c/",
	}
	for n, want := range rules {
		rule := got.GetAccessBoundaryRules()[n]
		if rule.GetAvailableResource() != want.Resource {
			t.Errorf("rule %d resource = %q, want %q", n, rule.GetAvailableResource(), want.Resource)
		}
		if diff := cmp.Diff(want.Permissions, rule.GetAvailablePermissions()); diff != "" {
			t.Errorf("rule %d permissions (-want +got):\n%s", n, diff)
		}
		if got := conditionPrefix(t, rule.GetCompiledAvailabilityCondition()); got != wantPaths[n] {
			t.Errorf("rule %d condition limits objects to %q, want %q", n, got, wantPaths[n])
		}
	}
}

// conditionPrefix checks that raw is the parsed CEL expression
// resource.name.startsWith(path) and returns path.
func conditionPrefix(t *testing.T, raw []byte) string {
	t.Helper()
	var e exprpb.Expr
	if err := proto.Unmarshal(raw, &e); err != nil {
		t.Fatalf("condition is not an Expr: %v", err)
	}
	call := e.GetCallExpr()
	sel := call.GetTarget().GetSelectExpr()
	if call.GetFunction() != "startsWith" || sel.GetField() != "name" || sel.GetOperand().GetIdentExpr().GetName() != "resource" || len(call.GetArgs()) != 1 {
		t.Fatalf("condition is %v, want resource.name.startsWith(path)", &e)
	}
	return call.GetArgs()[0].GetConstExpr().GetStringValue()
}

func TestIssue_ConditionKeepsAQuoteInThePath(t *testing.T) {
	// Substrate accepts a quote in a storage location. gcsboundary escapes it
	// in the CEL literal, and the parsed constant must hold the raw path.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
	grant := actorscope.Grant{Prefix: prefix("gs://snaps/tenant's/atespaces/team-a/actors/9f1e"), Access: actorscope.ReadWrite}
	tok, err := i.Issue(t.Context(), "9f1e", []actorscope.Grant{grant}, false)
	if err != nil {
		t.Fatal(err)
	}
	_, b := sts.open(t, tok.AccessToken)
	want := "projects/_/buckets/snaps/objects/tenant's/atespaces/team-a/actors/9f1e/"
	if got := conditionPrefix(t, b.GetAccessBoundaryRules()[0].GetCompiledAvailabilityCondition()); got != want {
		t.Errorf("condition limits objects to %q, want %q", got, want)
	}
}

func TestIssue_FallsBackToTheLatestIntermediaryWhenAnExchangeFails(t *testing.T) {
	// The source token is refreshed and STS refuses the exchange. Mints use
	// the previous intermediary while it has more than stsTimeout left.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		later   time.Duration
		wantErr bool
	}{
		{"the latest has 40 minutes left", 20 * time.Minute, false},
		{"the latest has just over stsTimeout left", time.Hour - stsTimeout - time.Second, false},
		{"the latest has stsTimeout left", time.Hour - stsTimeout, true},
		{"the latest has expired", time.Hour + time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := now
			src := &changingSource{tok: oauth2.Token{AccessToken: "source-1", Expiry: now.Add(time.Hour)}}
			sts := &fakeSTS{t: t}
			i := newTestIssuer(t, sts, src, func() time.Time { return clock })
			if _, err := i.Issue(t.Context(), "a", testGrants, true); err != nil {
				t.Fatal(err)
			}
			clock = now.Add(tc.later)
			src.mu.Lock()
			src.tok = oauth2.Token{AccessToken: "source-2", Expiry: clock.Add(time.Hour)}
			src.mu.Unlock()
			sts.mu.Lock()
			sts.status, sts.body = http.StatusServiceUnavailable, `{"error":"unavailable"}`
			sts.mu.Unlock()
			tok, err := i.Issue(t.Context(), "b", testGrants, true)
			if tc.wantErr {
				if err == nil {
					t.Fatal("Issue fell back to an intermediary with stsTimeout or less left")
				}
				return
			}
			if err != nil {
				t.Fatalf("Issue: %v", err)
			}
			if inter, _ := sts.open(t, tok.AccessToken); inter != "intermediary-1" {
				t.Errorf("token starts with %q, want the previous intermediary token", inter)
			}
		})
	}
}

// changingSource returns whatever token it holds, so a test can rotate it.
type changingSource struct {
	mu  sync.Mutex
	tok oauth2.Token
}

func (s *changingSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tok := s.tok
	return &tok, nil
}

func TestIssue_ExchangesOncePerSourceToken(t *testing.T) {
	// The first intermediary has less than minServeLifetime left, so the mint
	// after the refresh waits for the new exchange.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	src := &changingSource{tok: oauth2.Token{AccessToken: "source-1", Expiry: now.Add(minServeLifetime - time.Minute)}}
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, src, func() time.Time { return now })
	for _, uid := range []string{"a", "b", "c"} {
		if _, err := i.Issue(t.Context(), uid, testGrants, true); err != nil {
			t.Fatal(err)
		}
	}
	if sts.calls != 1 {
		t.Fatalf("three mints on one source token made %d STS calls, want 1", sts.calls)
	}
	src.mu.Lock()
	src.tok = oauth2.Token{AccessToken: "source-2", Expiry: now.Add(time.Hour)}
	src.mu.Unlock()
	tok, err := i.Issue(t.Context(), "a", testGrants, true)
	if err != nil {
		t.Fatal(err)
	}
	if sts.calls != 2 || sts.last["subject_token"] != "source-2" {
		t.Fatalf("a new source token made %d STS calls in total with subject %q, want 2 with source-2", sts.calls, sts.last["subject_token"])
	}
	if inter, _ := sts.open(t, tok.AccessToken); inter != "intermediary-2" {
		t.Errorf("token starts with %q, want the new intermediary token", inter)
	}
}

func TestIssue_ExpiresWithTheIntermediaryToken(t *testing.T) {
	// STS reports the source token's remaining life, a second or so short of
	// it. A longer lifetime is not trusted past the source token.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	source := now.Add(time.Hour)
	for _, tc := range []struct {
		name      string
		expiresIn int
		want      time.Time
	}{
		{"what STS reports", 3599, now.Add(3599 * time.Second)},
		{"longer than the source token", 7200, source},
		{"no lifetime reported", -1, source},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sts := &fakeSTS{t: t, expiresIn: tc.expiresIn}
			i := newTestIssuer(t, sts, staticSource(source), func() time.Time { return now })
			tok, err := i.Issue(t.Context(), "9f1e", testGrants, false)
			if err != nil {
				t.Fatal(err)
			}
			if !tok.Expiry.Equal(tc.want) {
				t.Errorf("Expiry = %v, want %v", tok.Expiry, tc.want)
			}
		})
	}
}

func TestIssue_ExchangeOutlivesACallerThatGivesUp(t *testing.T) {
	// The first mint on a new source token starts the exchange and gives up.
	// The exchange still finishes, and the next mint uses it.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
	sts.hold(t, "source-token")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error)
	go func() {
		_, err := i.Issue(ctx, "a", testGrants, true)
		done <- err
	}()
	<-sts.held
	cancel()
	if err := <-done; err == nil {
		t.Fatal("Issue succeeded for a caller that gave up")
	}
	close(sts.release)
	if _, err := i.Issue(t.Context(), "b", testGrants, true); err != nil {
		t.Fatalf("Issue after the first caller gave up: %v", err)
	}
	if sts.calls != 1 {
		t.Fatalf("STS calls = %d, want 1: the exchange should have survived the first caller", sts.calls)
	}
}

func TestIssue_ReplacedSourceTokenReusesItsIntermediary(t *testing.T) {
	// A mint can hold the source token from just before a refresh, after
	// another mint has exchanged the refreshed one. It reuses the old token's
	// intermediary rather than exchanging the old token again.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	src := &changingSource{tok: oauth2.Token{AccessToken: "source-1", Expiry: now.Add(40 * time.Minute)}}
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, src, func() time.Time { return now })
	setSource := func(tok oauth2.Token) {
		src.mu.Lock()
		src.tok = tok
		src.mu.Unlock()
	}
	old, err := i.Issue(t.Context(), "a", testGrants, true)
	if err != nil {
		t.Fatal(err)
	}
	refreshed := oauth2.Token{AccessToken: "source-2", Expiry: now.Add(time.Hour)}
	setSource(refreshed)
	if _, err := i.Issue(t.Context(), "b", testGrants, true); err != nil {
		t.Fatal(err)
	}
	// The refreshed token is exchanged in the background. Joining it waits
	// for it.
	<-i.exchange(t.Context(), &refreshed)
	setSource(oauth2.Token{AccessToken: "source-1", Expiry: now.Add(40 * time.Minute)})
	stale, err := i.Issue(t.Context(), "c", testGrants, true)
	if err != nil {
		t.Fatal(err)
	}
	oldInter, _ := sts.open(t, old.AccessToken)
	if staleInter, _ := sts.open(t, stale.AccessToken); staleInter != oldInter || sts.calls != 2 {
		t.Fatalf("a mint on the replaced token used %q with %d STS calls, want %q with 2", staleInter, sts.calls, oldInter)
	}
}

func TestIssue_AnOlderExchangeFinishingLateKeepsTheNewerIntermediary(t *testing.T) {
	// A mint starts an exchange for the source token from just before a
	// refresh. A second mint exchanges the refreshed token and finishes first.
	// The first exchange finishing later must not change what mints on the
	// refreshed token use.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	src := &changingSource{tok: oauth2.Token{AccessToken: "source-1", Expiry: now.Add(40 * time.Minute)}}
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, src, func() time.Time { return now })
	sts.hold(t, "source-1")
	done := make(chan error)
	go func() {
		_, err := i.Issue(t.Context(), "a", testGrants, true)
		done <- err
	}()
	<-sts.held
	src.mu.Lock()
	src.tok = oauth2.Token{AccessToken: "source-2", Expiry: now.Add(time.Hour)}
	src.mu.Unlock()
	newer, err := i.Issue(t.Context(), "b", testGrants, true)
	if err != nil {
		t.Fatal(err)
	}
	close(sts.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	later, err := i.Issue(t.Context(), "c", testGrants, true)
	if err != nil {
		t.Fatal(err)
	}
	newerInter, _ := sts.open(t, newer.AccessToken)
	if laterInter, _ := sts.open(t, later.AccessToken); laterInter != newerInter || sts.calls != 2 {
		t.Fatalf("after the older exchange finished, a mint used %q with %d STS calls, want %q with 2", laterInter, sts.calls, newerInter)
	}
	// The next refresh fails. Mints fall back to the newer intermediary, not
	// the one whose exchange finished last.
	src.mu.Lock()
	src.tok = oauth2.Token{AccessToken: "source-3", Expiry: now.Add(2 * time.Hour)}
	src.mu.Unlock()
	sts.mu.Lock()
	sts.status, sts.body = http.StatusServiceUnavailable, `{"error":"unavailable"}`
	sts.mu.Unlock()
	fallback, err := i.Issue(t.Context(), "d", testGrants, true)
	if err != nil {
		t.Fatal(err)
	}
	if inter, _ := sts.open(t, fallback.AccessToken); inter != newerInter {
		t.Errorf("after a failed refresh, a mint used %q, want the newer %q", inter, newerInter)
	}
}

func TestIssue_ExchangesAgainOnceTheIntermediaryExpires(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	clock := now
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return clock })
	if _, err := i.Issue(t.Context(), "a", testGrants, true); err != nil {
		t.Fatal(err)
	}
	clock = now.Add(time.Hour + time.Second)
	if _, err := i.Issue(t.Context(), "a", testGrants, true); err != nil {
		t.Fatal(err)
	}
	if sts.calls != 2 {
		t.Fatalf("STS calls = %d, want 2: the expired intermediary should not be reused", sts.calls)
	}
}

func TestIssue_RefreshesInTheBackground(t *testing.T) {
	// The previous intermediary has 40 minutes left when the source token is
	// refreshed. A mint uses it at once, even with STS hanging, and later mints
	// use the new intermediary once its exchange is done.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	src := &changingSource{tok: oauth2.Token{AccessToken: "source-1", Expiry: now.Add(40 * time.Minute)}}
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, src, func() time.Time { return now })
	if _, err := i.Issue(t.Context(), "a", testGrants, true); err != nil {
		t.Fatal(err)
	}
	refreshed := oauth2.Token{AccessToken: "source-2", Expiry: now.Add(time.Hour)}
	src.mu.Lock()
	src.tok = refreshed
	src.mu.Unlock()
	sts.hold(t, "source-2")
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	tok, err := i.Issue(ctx, "b", testGrants, true)
	if err != nil {
		t.Fatalf("Issue waited for a hanging exchange: %v", err)
	}
	if inter, _ := sts.open(t, tok.AccessToken); inter != "intermediary-1" {
		t.Errorf("token starts with %q, want the previous intermediary token", inter)
	}
	close(sts.release)
	<-i.exchange(t.Context(), &refreshed)
	tok, err = i.Issue(t.Context(), "c", testGrants, true)
	if err != nil {
		t.Fatal(err)
	}
	if inter, _ := sts.open(t, tok.AccessToken); inter != "intermediary-2" || sts.calls != 2 {
		t.Errorf("after the exchange, a mint used %q with %d STS calls, want intermediary-2 with 2", inter, sts.calls)
	}
}

func TestIssue_PausesExchangesAfterAFailure(t *testing.T) {
	// STS refuses after a refresh. Mints keep using the previous intermediary
	// without calling STS again until stsTimeout has passed.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	clock := now
	src := &changingSource{tok: oauth2.Token{AccessToken: "source-1", Expiry: now.Add(time.Hour)}}
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, src, func() time.Time { return clock })
	if _, err := i.Issue(t.Context(), "a", testGrants, true); err != nil {
		t.Fatal(err)
	}
	refreshed := oauth2.Token{AccessToken: "source-2", Expiry: now.Add(2 * time.Hour)}
	src.mu.Lock()
	src.tok = refreshed
	src.mu.Unlock()
	sts.mu.Lock()
	sts.status, sts.body = http.StatusTooManyRequests, `{"error":"quota_exceeded"}`
	sts.mu.Unlock()
	for n := range 5 {
		if _, err := i.Issue(t.Context(), fmt.Sprintf("actor-%d", n), testGrants, true); err != nil {
			t.Fatal(err)
		}
		// Joining the exchange the mint started lets it finish, so the next
		// mint sees the failure.
		<-i.exchange(t.Context(), &refreshed)
	}
	if sts.calls != 2 {
		t.Fatalf("5 mints after a refused exchange made %d STS calls in total, want 2", sts.calls)
	}
	clock = now.Add(stsTimeout + time.Second)
	if _, err := i.Issue(t.Context(), "later", testGrants, true); err != nil {
		t.Fatal(err)
	}
	<-i.exchange(t.Context(), &refreshed)
	if sts.calls != 3 {
		t.Fatalf("a mint after the pause made %d STS calls in total, want 3", sts.calls)
	}
}

func TestIssue_RetriesSoonWithoutAFallback(t *testing.T) {
	// With no intermediary to fall back on, one failed exchange fails mints
	// only until minRetryDelay has passed.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	clock := now
	sts := &fakeSTS{t: t, status: http.StatusServiceUnavailable, body: `{"error":"unavailable"}`}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return clock })
	if _, err := i.Issue(t.Context(), "a", testGrants, false); err == nil {
		t.Fatal("Issue succeeded against a failing STS")
	}
	clock = now.Add(minRetryDelay / 2)
	if _, err := i.Issue(t.Context(), "a", testGrants, false); err == nil || !strings.Contains(err.Error(), "paused") || sts.calls != 1 {
		t.Fatalf("a mint during the pause got %v with %d STS calls, want a paused error with 1", err, sts.calls)
	}
	sts.mu.Lock()
	sts.status, sts.body = 0, ""
	sts.mu.Unlock()
	clock = now.Add(minRetryDelay + time.Millisecond)
	if _, err := i.Issue(t.Context(), "a", testGrants, false); err != nil || sts.calls != 2 {
		t.Fatalf("a mint after the pause got %v with %d STS calls, want success with 2", err, sts.calls)
	}
}

func TestIssue_RetryPauseDoublesAndResets(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	clock := now
	src := &changingSource{tok: oauth2.Token{AccessToken: "source-1", Expiry: now.Add(time.Hour)}}
	sts := &fakeSTS{t: t, status: http.StatusServiceUnavailable, body: `{"error":"unavailable"}`}
	i := newTestIssuer(t, sts, src, func() time.Time { return clock })
	for _, want := range []time.Duration{1, 2, 4, 8, 16, 30, 30} {
		if _, err := i.Issue(t.Context(), "a", testGrants, true); err == nil {
			t.Fatal("Issue succeeded against a failing STS")
		}
		f := i.failure.Load()
		if f == nil || f.delay != want*time.Second {
			t.Fatalf("pause after failure = %v, want %v", f, want*time.Second)
		}
		clock = f.until
	}
	// A success clears the pause, so the next failure starts at
	// minRetryDelay again.
	sts.mu.Lock()
	sts.status, sts.body = 0, ""
	sts.mu.Unlock()
	if _, err := i.Issue(t.Context(), "a", testGrants, true); err != nil {
		t.Fatal(err)
	}
	src.mu.Lock()
	src.tok = oauth2.Token{AccessToken: "source-2", Expiry: clock.Add(minServeLifetime - time.Minute)}
	src.mu.Unlock()
	sts.mu.Lock()
	sts.status, sts.body = http.StatusServiceUnavailable, `{"error":"unavailable"}`
	sts.mu.Unlock()
	// The previous intermediary now has under minServeLifetime left, so the
	// mint waits for the exchange, which fails. It may still fall back.
	clock = clock.Add(time.Hour - minServeLifetime + time.Minute)
	if _, err := i.Issue(t.Context(), "b", testGrants, true); err != nil && !strings.Contains(err.Error(), "503") {
		t.Fatal(err)
	}
	if f := i.failure.Load(); f == nil || f.delay != minRetryDelay {
		t.Fatalf("pause after a success and a new failure = %v, want %v", f, minRetryDelay)
	}
}

func TestIssue_AnExchangeTimeoutIsNotTheCallersDeadline(t *testing.T) {
	// The exchange times out on its own deadline while the caller's has time
	// left. Neither that mint nor the paused ones after it may report
	// DeadlineExceeded, which the exchange would pass on as the caller's code.
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t}
	srv := httptest.NewServer(http.HandlerFunc(sts.handler))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	i, err := New(t.Context(), "", WithTokenSource(staticSource(now.Add(time.Hour))), WithClock(func() time.Time { return now }),
		WithHTTPClient(&http.Client{Transport: redirect{sts: sts, target: target}, Timeout: 50 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	sts.hold(t, "source-token")
	for _, what := range []string{"the mint that ran the exchange", "a paused mint"} {
		_, err := i.Issue(t.Context(), "a", testGrants, false)
		if err == nil || errors.Is(err, context.DeadlineExceeded) || status.Code(err) != codes.Unknown {
			t.Fatalf("%s got %v (code %v), want an error without a code or context.DeadlineExceeded", what, err, status.Code(err))
		}
	}
}

func TestIssue_AcceptsASessionKeyWithoutPadding(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t, unpaddedKey: true}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
	tok, err := i.Issue(t.Context(), "9f1e", testGrants, false)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	sts.open(t, tok.AccessToken)
}

func TestIssue_ConcurrentMintsShareOneExchange(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
	var wg sync.WaitGroup
	for n := range 20 {
		wg.Go(func() {
			if _, err := i.Issue(t.Context(), fmt.Sprintf("actor-%d", n), testGrants, true); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if sts.calls != 1 {
		t.Fatalf("20 concurrent mints on one source token made %d STS calls, want 1", sts.calls)
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
	if second != first {
		t.Fatal("second Issue minted again instead of serving the cached token")
	}
	other, err := i.Issue(t.Context(), "other", testGrants, false)
	if err != nil {
		t.Fatal(err)
	}
	if other == first {
		t.Fatal("a different actor was served the first actor's token")
	}
	// With less than minServeLifetime left the cached token is not served.
	clock = now.Add(time.Hour - minServeLifetime + time.Second)
	third, err := i.Issue(t.Context(), "9f1e", testGrants, false)
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatalf("a token with under %v left was served from the cache", minServeLifetime)
	}
}

func TestIssue_ForceBypassesTheCache(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
	var toks []string
	for _, force := range []bool{false, true} {
		tok, err := i.Issue(t.Context(), "9f1e", testGrants, force)
		if err != nil {
			t.Fatal(err)
		}
		toks = append(toks, tok.AccessToken)
	}
	if toks[0] == toks[1] {
		t.Fatal("force served the cached token instead of minting")
	}
}

func TestIssue_CacheIsBounded(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
	var toks []string
	for _, uid := range []string{"a", "b", "c", "a"} {
		tok, err := i.Issue(t.Context(), uid, testGrants, false)
		if err != nil {
			t.Fatal(err)
		}
		toks = append(toks, tok.AccessToken)
	}
	// Size 2: c evicted a, so the last call for a minted again.
	if toks[3] == toks[0] || i.cache.Len() != 2 {
		t.Fatalf("a served from the cache after eviction: %v, cache len = %d, want 2", toks[3] == toks[0], i.cache.Len())
	}
}

func TestIssue_ServesTheCachedTokenWhenItCoversTheGrants(t *testing.T) {
	now := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	rwOther := actorscope.Grant{Prefix: prefix("gs://other/root/atespaces/team-a/actors/9f1e"), Access: actorscope.ReadWrite}
	roOnOwn := actorscope.Grant{Prefix: rwGrant.Prefix, Access: actorscope.ReadOnly}
	for _, tc := range []struct {
		name    string
		first   []actorscope.Grant
		second  []actorscope.Grant
		wantHit bool
	}{
		{"same grants", testGrants, testGrants, true},
		{"tag dropped after the first suspend", testGrants, []actorscope.Grant{rwGrant}, true},
		{"read-only covered by read-write", testGrants, []actorscope.Grant{roOnOwn}, true},
		{"repoint adds a location", testGrants, []actorscope.Grant{rwGrant, rwOther, roGrant}, false},
		{"read-write not covered by read-only", []actorscope.Grant{roOnOwn}, []actorscope.Grant{rwGrant}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sts := &fakeSTS{t: t}
			i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
			var toks []string
			for _, grants := range [][]actorscope.Grant{tc.first, tc.second} {
				tok, err := i.Issue(t.Context(), "9f1e", grants, false)
				if err != nil {
					t.Fatal(err)
				}
				toks = append(toks, tok.AccessToken)
			}
			if hit := toks[0] == toks[1]; hit != tc.wantHit {
				t.Fatalf("second Issue served the cached token: %v, want %v", hit, tc.wantHit)
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
	sts := &fakeSTS{t: t}
	i := newTestIssuer(t, sts, staticSource(now.Add(time.Hour)), func() time.Time { return now })
	sts.hold(t, "source-token")
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
	var toks []string
	for range 2 {
		tok, err := i.Issue(t.Context(), "9f1e", testGrants, false)
		if err != nil {
			t.Fatal(err)
		}
		if tok.AccessToken == "" {
			t.Fatal("no token returned")
		}
		toks = append(toks, tok.AccessToken)
	}
	if toks[0] == toks[1] || i.cache.Len() != 0 {
		t.Fatalf("short-lived token served from the cache: %v, cache len = %d, want 0", toks[0] == toks[1], i.cache.Len())
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
		{"no expiry from STS or the source", &fakeSTS{t: t, expiresIn: -1}, staticSource(time.Time{}), testGrants, "no expiry", codes.Unknown},
		{"STS refuses", &fakeSTS{t: t, status: 400, body: `{"error":"invalid_grant"}`}, staticSource(now.Add(time.Hour)), testGrants, "400", codes.Unknown},
		{"STS answers without a token", &fakeSTS{t: t, body: `{}`}, staticSource(now.Add(time.Hour)), testGrants, "no access token", codes.Unknown},
		{"STS answers without a session key", &fakeSTS{t: t, body: `{"access_token":"intermediary"}`}, staticSource(now.Add(time.Hour)), testGrants, "no session key", codes.Unknown},
		{"session key is not a keyset", &fakeSTS{t: t, body: `{"access_token":"intermediary","access_boundary_session_key":"bm90IGEga2V5c2V0"}`}, staticSource(now.Add(time.Hour)), testGrants, "session key", codes.Unknown},
		{"no grants", &fakeSTS{t: t}, staticSource(now.Add(time.Hour)), nil, "no storage grants", codes.Internal},
		{"too many grants", &fakeSTS{t: t}, staticSource(now.Add(time.Hour)), manyGrants(11), "1 to 10 rules, got 11", codes.Internal},
		{"boundary over 4096 bytes", &fakeSTS{t: t}, staticSource(now.Add(time.Hour)), longGrants(10, 400), "more than the 4096", codes.Internal},
		{"condition over 512 bytes", &fakeSTS{t: t}, staticSource(now.Add(time.Hour)), longGrants(1, 500), "more than the 512", codes.Internal},
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

// longGrants is n grants whose paths are length bytes, each within the
// literal limit, so only the boundary's total size is over.
func longGrants(n, length int) []actorscope.Grant {
	var gs []actorscope.Grant
	for i := range n {
		path := fmt.Sprintf("p%d/", i)
		path += strings.Repeat("x", length-len(path))
		gs = append(gs, actorscope.Grant{Prefix: prefix("gs://b/" + path), Access: actorscope.ReadOnly})
	}
	return gs
}
