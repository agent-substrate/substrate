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

package ateclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type fakeTokenClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeTokenClock(now time.Time) *fakeTokenClock {
	return &fakeTokenClock{now: now}
}

func (c *fakeTokenClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeTokenClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func tokenResponse(token string, expiresAt time.Time) *authv1.TokenRequest {
	return &authv1.TokenRequest{
		Status: authv1.TokenRequestStatus{
			Token:               token,
			ExpirationTimestamp: metav1.NewTime(expiresAt),
		},
	}
}

func authorization(t *testing.T, creds *bearerTokenCreds, ctx context.Context) (string, error) {
	t.Helper()
	metadata, err := creds.GetRequestMetadata(ctx)
	if err != nil {
		return "", err
	}
	return metadata["authorization"], nil
}

func requireBearerToken(t *testing.T, creds *bearerTokenCreds, ctx context.Context, token string) {
	t.Helper()
	got, err := authorization(t, creds, ctx)
	if err != nil {
		t.Fatalf("GetRequestMetadata(): %v", err)
	}
	if want := "Bearer " + token; got != want {
		t.Errorf("authorization = %q, want %q", got, want)
	}
}

func requireTokenRequests(t *testing.T, calls *atomic.Int32, want int32) {
	t.Helper()
	if got := calls.Load(); got != want {
		t.Fatalf("TokenRequest calls = %d, want %d", got, want)
	}
}

func noBearerTokenRefreshJitter() time.Duration { return 0 }

func newTestBearerTokenCreds(t *testing.T, requestToken tokenRequestFunc, now func() time.Time) *bearerTokenCreds {
	t.Helper()
	creds, err := newBearerTokenCreds(context.Background(), requestToken, now, noBearerTokenRefreshJitter)
	if err != nil {
		t.Fatalf("newBearerTokenCreds: %v", err)
	}
	return creds
}

func waitForBearerTokenRefresh(t *testing.T, creds *bearerTokenCreds) {
	t.Helper()
	creds.mu.Lock()
	done := creds.refreshDone
	creds.mu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bearer token refresh")
	}
}

func disableBearerTokenBackoffJitter(creds *bearerTokenCreds) {
	creds.mu.Lock()
	defer creds.mu.Unlock()
	creds.backoff = newBearerTokenBackoff(0)
}

type blockingTokenSource struct {
	clock   *fakeTokenClock
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

func newBlockingTokenSource(clock *fakeTokenClock) *blockingTokenSource {
	return &blockingTokenSource{
		clock:   clock,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingTokenSource) request(ctx context.Context) (*authv1.TokenRequest, error) {
	if s.calls.Add(1) == 1 {
		return tokenResponse("token-1", s.clock.Now().Add(time.Hour)), nil
	}
	close(s.started)
	select {
	case <-s.release:
		return tokenResponse("token-2", s.clock.Now().Add(time.Hour)), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestRequestBearerToken(t *testing.T) {
	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor("create", "serviceaccounts", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if got, want := action.GetNamespace(), bearerTokenNamespace; got != want {
			t.Errorf("namespace = %q, want %q", got, want)
		}
		if got, want := action.GetSubresource(), "token"; got != want {
			t.Errorf("subresource = %q, want %q", got, want)
		}
		createAction, ok := action.(k8stesting.CreateActionImpl)
		if !ok {
			t.Fatalf("action type = %T, want CreateActionImpl", action)
		}
		if got, want := createAction.Name, bearerTokenServiceAccount; got != want {
			t.Errorf("service account = %q, want %q", got, want)
		}
		request, ok := createAction.GetObject().(*authv1.TokenRequest)
		if !ok {
			t.Fatalf("request type = %T, want *authv1.TokenRequest", createAction.GetObject())
		}
		if got, want := request.Spec.Audiences, []string{apiServerName}; len(got) != 1 || got[0] != want[0] {
			t.Errorf("audiences = %v, want %v", got, want)
		}
		if request.Spec.ExpirationSeconds == nil {
			t.Fatal("ExpirationSeconds is nil")
		}
		if got, want := *request.Spec.ExpirationSeconds, int64(bearerTokenTTL/time.Second); got != want {
			t.Errorf("ExpirationSeconds = %d, want %d", got, want)
		}
		return true, tokenResponse("token-1", now.Add(bearerTokenTTL)), nil
	})

	response, err := requestBearerToken(context.Background(), clientset)
	if err != nil {
		t.Fatalf("requestBearerToken: %v", err)
	}
	if got, want := response.Status.Token, "token-1"; got != want {
		t.Errorf("token = %q, want %q", got, want)
	}
}

func TestBearerTokenCredsInitialRequestCancellation(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	requestExited := make(chan struct{})
	requestToken := func(ctx context.Context) (*authv1.TokenRequest, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		close(requestExited)
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := newBearerTokenCreds(ctx, requestToken, time.Now, noBearerTokenRefreshJitter)
		result <- err
	}()

	<-started
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("newBearerTokenCreds error = %v, want context.Canceled", err)
		}
		if !strings.Contains(err.Error(), "requesting ateapi bearer token") {
			t.Errorf("newBearerTokenCreds error = %v, want request context", err)
		}
	case <-time.After(time.Second):
		t.Fatal("constructor did not return after caller cancellation")
	}
	select {
	case <-requestExited:
	case <-time.After(time.Second):
		t.Fatal("initial TokenRequest outlived canceled constructor")
	}
	requireTokenRequests(t, &calls, 1)
}

func TestBearerTokenCredsRefreshAtEightyPercent(t *testing.T) {
	start := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	clock := newFakeTokenClock(start)
	var calls atomic.Int32
	requestToken := func(context.Context) (*authv1.TokenRequest, error) {
		call := calls.Add(1)
		return tokenResponse(fmt.Sprintf("token-%d", call), clock.Now().Add(10*time.Minute)), nil
	}
	creds := newTestBearerTokenCreds(t, requestToken, clock.Now)

	if !creds.RequireTransportSecurity() {
		t.Error("RequireTransportSecurity() = false, want true")
	}
	clock.Advance(8*time.Minute - time.Second)
	requireBearerToken(t, creds, context.Background(), "token-1")
	requireTokenRequests(t, &calls, 1)

	// Exactly at refreshAt: the token is still served while a refresh starts.
	clock.Advance(time.Second)
	requireBearerToken(t, creds, context.Background(), "token-1")
	waitForBearerTokenRefresh(t, creds)
	requireBearerToken(t, creds, context.Background(), "token-2")
	requireTokenRequests(t, &calls, 2)
}

func TestBearerTokenCredsRefreshFailure(t *testing.T) {
	start := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	clock := newFakeTokenClock(start)
	var calls atomic.Int32
	requestToken := func(context.Context) (*authv1.TokenRequest, error) {
		switch calls.Add(1) {
		case 1:
			return tokenResponse("token-1", start.Add(time.Hour)), nil
		case 2:
			return nil, errors.New("apiserver unavailable")
		default:
			return tokenResponse("token-2", clock.Now().Add(time.Hour)), nil
		}
	}
	creds := newTestBearerTokenCreds(t, requestToken, clock.Now)
	disableBearerTokenBackoffJitter(creds)

	clock.Advance(49 * time.Minute)
	requireBearerToken(t, creds, context.Background(), "token-1")
	waitForBearerTokenRefresh(t, creds)
	for range 10 {
		requireBearerToken(t, creds, context.Background(), "token-1")
	}
	requireTokenRequests(t, &calls, 2)

	clock.Advance(bearerTokenBackoffInitial)
	requireBearerToken(t, creds, context.Background(), "token-1")
	waitForBearerTokenRefresh(t, creds)
	requireBearerToken(t, creds, context.Background(), "token-2")
	requireTokenRequests(t, &calls, 3)
}

func TestBearerTokenCredsRefreshRetryAtUsableExpiry(t *testing.T) {
	start := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	clock := newFakeTokenClock(start)
	var calls atomic.Int32
	requestToken := func(context.Context) (*authv1.TokenRequest, error) {
		switch calls.Add(1) {
		case 1:
			return tokenResponse("token-1", start.Add(time.Hour)), nil
		case 2:
			return nil, errors.New("apiserver unavailable")
		default:
			return tokenResponse("token-2", clock.Now().Add(time.Hour)), nil
		}
	}
	creds := newTestBearerTokenCreds(t, requestToken, clock.Now)
	creds.mu.Lock()
	creds.backoff.Duration = bearerTokenBackoffCap
	creds.backoff.Steps = 0
	creds.backoff.Jitter = 0
	creds.mu.Unlock()

	clock.Advance(time.Hour - bearerTokenExpirySafetyMargin - time.Second)
	requireBearerToken(t, creds, context.Background(), "token-1")
	waitForBearerTokenRefresh(t, creds)
	requireTokenRequests(t, &calls, 2)

	clock.Advance(time.Second)
	requireBearerToken(t, creds, context.Background(), "token-2")
	requireTokenRequests(t, &calls, 3)
}

func TestBearerTokenCredsRefreshBackoff(t *testing.T) {
	start := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	clock := newFakeTokenClock(start)
	var calls atomic.Int32
	requestToken := func(context.Context) (*authv1.TokenRequest, error) {
		switch calls.Add(1) {
		case 1:
			return tokenResponse("token-1", start.Add(10*time.Minute)), nil
		case 2, 4:
			return nil, errors.New("apiserver unavailable")
		default:
			return tokenResponse(fmt.Sprintf("token-%d", calls.Load()), clock.Now().Add(time.Hour)), nil
		}
	}
	creds := newTestBearerTokenCreds(t, requestToken, clock.Now)
	disableBearerTokenBackoffJitter(creds)
	clock.Advance(10 * time.Minute)

	if _, err := authorization(t, creds, context.Background()); err == nil {
		t.Fatal("metadata with expired token and failed refresh: got nil error")
	}
	const callers = 20
	results := make(chan error, callers)
	for range callers {
		go func() {
			_, err := authorization(t, creds, context.Background())
			results <- err
		}()
	}
	for range callers {
		if err := <-results; err == nil {
			t.Error("metadata during negative-cache window: got nil error")
		}
	}
	requireTokenRequests(t, &calls, 2)

	clock.Advance(bearerTokenBackoffInitial)
	requireBearerToken(t, creds, context.Background(), "token-3")

	clock.Advance(49 * time.Minute)
	requireBearerToken(t, creds, context.Background(), "token-3")
	waitForBearerTokenRefresh(t, creds)
	requireTokenRequests(t, &calls, 4)

	clock.Advance(bearerTokenBackoffInitial)
	requireBearerToken(t, creds, context.Background(), "token-3")
	waitForBearerTokenRefresh(t, creds)
	requireTokenRequests(t, &calls, 5)
}

func TestBearerTokenCredsCallersWithinSafetyMarginShareRefresh(t *testing.T) {
	clock := newFakeTokenClock(time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC))
	source := newBlockingTokenSource(clock)
	creds := newTestBearerTokenCreds(t, source.request, clock.Now)
	clock.Advance(time.Hour - bearerTokenExpirySafetyMargin)

	const callers = 50
	results := make(chan error, callers)
	for range callers {
		go func() {
			got, err := authorization(t, creds, context.Background())
			if err == nil && got != "Bearer token-2" {
				err = fmt.Errorf("authorization = %q, want %q", got, "Bearer token-2")
			}
			results <- err
		}()
	}
	<-source.started
	requireTokenRequests(t, &source.calls, 2)
	close(source.release)

	for range callers {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("expired caller: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("expired caller did not receive shared refresh result")
		}
	}
	requireTokenRequests(t, &source.calls, 2)
}

func TestBearerTokenCredsValidCallersDoNotWaitForRefresh(t *testing.T) {
	clock := newFakeTokenClock(time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC))
	source := newBlockingTokenSource(clock)
	creds := newTestBearerTokenCreds(t, source.request, clock.Now)
	clock.Advance(49 * time.Minute)

	requireBearerToken(t, creds, context.Background(), "token-1")
	<-source.started

	const callers = 20
	results := make(chan error, callers)
	for range callers {
		go func() {
			got, err := authorization(t, creds, context.Background())
			if err == nil && got != "Bearer token-1" {
				err = fmt.Errorf("authorization = %q, want %q", got, "Bearer token-1")
			}
			results <- err
		}()
	}
	for range callers {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("concurrent metadata: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("valid-token RPC blocked behind refresh")
		}
	}
	requireTokenRequests(t, &source.calls, 2)

	close(source.release)
	waitForBearerTokenRefresh(t, creds)
	requireBearerToken(t, creds, context.Background(), "token-2")
}

func TestBearerTokenCredsRefreshTimeout(t *testing.T) {
	start := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	clock := newFakeTokenClock(start)
	var calls atomic.Int32
	refreshTimedOut := make(chan struct{})
	requestToken := func(ctx context.Context) (*authv1.TokenRequest, error) {
		if calls.Add(1) == 1 {
			return tokenResponse("token-1", start.Add(time.Hour)), nil
		}
		<-ctx.Done()
		close(refreshTimedOut)
		return nil, ctx.Err()
	}
	creds := newTestBearerTokenCreds(t, requestToken, clock.Now)
	creds.refreshTimeout = 20 * time.Millisecond
	clock.Advance(49 * time.Minute)

	requireBearerToken(t, creds, context.Background(), "token-1")
	select {
	case <-refreshTimedOut:
	case <-time.After(time.Second):
		t.Fatal("TokenRequest did not honor internal refresh timeout")
	}
	waitForBearerTokenRefresh(t, creds)
	requireTokenRequests(t, &calls, 2)
}

func TestBearerTokenCredsCanceledWaiter(t *testing.T) {
	clock := newFakeTokenClock(time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC))
	source := newBlockingTokenSource(clock)
	creds := newTestBearerTokenCreds(t, source.request, clock.Now)
	clock.Advance(time.Hour)

	leaderDone := make(chan error, 1)
	go func() {
		_, err := authorization(t, creds, context.Background())
		leaderDone <- err
	}()
	<-source.started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := authorization(t, creds, ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
	}

	close(source.release)
	if err := <-leaderDone; err != nil {
		t.Fatalf("refresh leader: %v", err)
	}
	requireTokenRequests(t, &source.calls, 2)
}

func TestBearerTokenCredsRefreshOutlivesCanceledInitiator(t *testing.T) {
	clock := newFakeTokenClock(time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC))
	source := newBlockingTokenSource(clock)
	creds := newTestBearerTokenCreds(t, source.request, clock.Now)
	clock.Advance(time.Hour - bearerTokenExpirySafetyMargin)

	ctx, cancel := context.WithCancel(context.Background())
	initiatorDone := make(chan error, 1)
	go func() {
		_, err := authorization(t, creds, ctx)
		initiatorDone <- err
	}()
	<-source.started

	cancel()
	if err := <-initiatorDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled initiator error = %v, want context.Canceled", err)
	}

	// The refresh is detached from the initiator and must complete for other callers.
	close(source.release)
	requireBearerToken(t, creds, context.Background(), "token-2")
	requireTokenRequests(t, &source.calls, 2)
}

func TestBearerTokenCredsInvalidRefreshResponse(t *testing.T) {
	start := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	clock := newFakeTokenClock(start)
	var calls atomic.Int32
	requestToken := func(context.Context) (*authv1.TokenRequest, error) {
		switch calls.Add(1) {
		case 1:
			return tokenResponse("initial-secret", start.Add(10*time.Minute)), nil
		case 2, 3:
			return &authv1.TokenRequest{Status: authv1.TokenRequestStatus{Token: "refresh-secret"}}, nil
		default:
			return tokenResponse("recovered", clock.Now().Add(time.Hour)), nil
		}
	}
	creds := newTestBearerTokenCreds(t, requestToken, clock.Now)
	disableBearerTokenBackoffJitter(creds)

	clock.Advance(9 * time.Minute)
	requireBearerToken(t, creds, context.Background(), "initial-secret")
	waitForBearerTokenRefresh(t, creds)
	requireBearerToken(t, creds, context.Background(), "initial-secret")
	requireTokenRequests(t, &calls, 2)

	clock.Advance(time.Minute)
	if _, err := authorization(t, creds, context.Background()); err == nil {
		t.Fatal("metadata with expired token and invalid refresh: got nil error")
	} else {
		if !strings.Contains(err.Error(), "invalid ateapi bearer token response") {
			t.Errorf("refresh error = %v, want invalid response context", err)
		}
		if strings.Contains(err.Error(), "initial-secret") || strings.Contains(err.Error(), "refresh-secret") {
			t.Errorf("refresh error exposed a token: %v", err)
		}
	}
	requireTokenRequests(t, &calls, 3)
	if _, err := authorization(t, creds, context.Background()); err == nil {
		t.Fatal("metadata during expired retry delay: got nil error")
	}
	requireTokenRequests(t, &calls, 3)

	clock.Advance(2 * bearerTokenBackoffInitial)
	requireBearerToken(t, creds, context.Background(), "recovered")
	requireTokenRequests(t, &calls, 4)
}

func TestRandomBearerTokenRefreshJitter(t *testing.T) {
	for range 100 {
		if got := randomBearerTokenRefreshJitter(); got < 0 || got > bearerTokenRefreshJitterMax {
			t.Fatalf("randomBearerTokenRefreshJitter() = %s, want [0, %s]", got, bearerTokenRefreshJitterMax)
		}
	}
}

func TestBearerTokenCredsRefreshJitter(t *testing.T) {
	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	expiresAt := now.Add(time.Hour)
	requestToken := func(context.Context) (*authv1.TokenRequest, error) {
		return tokenResponse("token", expiresAt), nil
	}

	var refreshAt []time.Time
	for _, jitter := range []time.Duration{time.Second, 9 * time.Second} {
		creds, err := newBearerTokenCreds(
			context.Background(),
			requestToken,
			func() time.Time { return now },
			func() time.Duration { return jitter },
		)
		if err != nil {
			t.Fatalf("newBearerTokenCreds(jitter=%s): %v", jitter, err)
		}
		refreshAt = append(refreshAt, creds.state.refreshAt)
		want := expiresAt.Add(-time.Hour/bearerTokenRefreshDivisor - jitter)
		if got := creds.state.refreshAt; !got.Equal(want) {
			t.Errorf("refreshAt with jitter %s = %s, want %s", jitter, got, want)
		}
	}
	if refreshAt[0].Equal(refreshAt[1]) {
		t.Errorf("refreshAt = %v for distinct per-client jitter", refreshAt)
	}
}

func TestNewBearerTokenStateTiming(t *testing.T) {
	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	expiresAt := now.Add(time.Hour)
	jitter := bearerTokenRefreshJitterMax
	state, err := newBearerTokenState(tokenResponse("token", expiresAt), now, jitter)
	if err != nil {
		t.Fatalf("newBearerTokenState: %v", err)
	}

	if got, want := state.expiresAt, expiresAt; !got.Equal(want) {
		t.Errorf("expiresAt = %s, want %s", got, want)
	}
	if got, want := state.usableUntil, expiresAt.Add(-bearerTokenExpirySafetyMargin); !got.Equal(want) {
		t.Errorf("usableUntil = %s, want %s", got, want)
	}
	if got, want := state.refreshAt, expiresAt.Add(-time.Hour/bearerTokenRefreshDivisor-jitter); !got.Equal(want) {
		t.Errorf("refreshAt = %s, want %s", got, want)
	}

	shortExpiry := now.Add(time.Minute)
	state, err = newBearerTokenState(tokenResponse("short-token", shortExpiry), now, 0)
	if err != nil {
		t.Fatalf("newBearerTokenState(short lifetime): %v", err)
	}
	if got, want := state.refreshAt, shortExpiry.Add(-bearerTokenExpirySafetyMargin); !got.Equal(want) {
		t.Errorf("short-lifetime refreshAt = %s, want %s", got, want)
	}
}

func TestNewBearerTokenStateRejectsInvalidResponse(t *testing.T) {
	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		response *authv1.TokenRequest
		jitter   time.Duration
	}{
		{name: "nil response"},
		{name: "empty token", response: tokenResponse("", now.Add(time.Hour))},
		{name: "missing expiration", response: &authv1.TokenRequest{Status: authv1.TokenRequestStatus{Token: "token"}}},
		{name: "expired response", response: tokenResponse("token", now)},
		{name: "expires within safety margin", response: tokenResponse("token", now.Add(bearerTokenExpirySafetyMargin))},
		{name: "negative refresh jitter", response: tokenResponse("token", now.Add(time.Hour)), jitter: -time.Nanosecond},
		{name: "excessive refresh jitter", response: tokenResponse("token", now.Add(time.Hour)), jitter: bearerTokenRefreshJitterMax + time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newBearerTokenState(tc.response, now, tc.jitter); err == nil {
				t.Fatal("newBearerTokenState: got nil error")
			}
		})
	}
}
