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
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

const (
	bearerTokenNamespace      = "ate-system"
	bearerTokenServiceAccount = "ate-client"
	bearerTokenTTL            = time.Hour

	// Refresh after 80% of the remaining lifetime, matching kubelet's threshold.
	bearerTokenRefreshDivisor = 5

	// Advance normal refreshes to reduce synchronized TokenRequests across clients.
	bearerTokenRefreshJitterMax = 10 * time.Second

	// Stop using tokens before issuer expiry to cover RPC transit and clock skew.
	bearerTokenExpirySafetyMargin = 30 * time.Second

	// Bound shared refresh attempts because callers with an unusable token wait for completion.
	bearerTokenRefreshTimeout = 10 * time.Second

	// Failed refreshes use base delays of 1s, 2s, 4s, 8s, 16s, then 30s.
	// Jitter reduces synchronized retries across clients.
	bearerTokenBackoffInitial = time.Second
	bearerTokenBackoffCap     = 30 * time.Second
	bearerTokenBackoffSteps   = 6
	bearerTokenBackoffFactor  = 2.0
	bearerTokenBackoffJitter  = 0.2
)

func randomBearerTokenRefreshJitter() time.Duration {
	return time.Duration(rand.Int64N(int64(bearerTokenRefreshJitterMax) + 1))
}

func newBearerTokenBackoff(jitter float64) wait.Backoff {
	return wait.Backoff{
		Duration: bearerTokenBackoffInitial,
		Factor:   bearerTokenBackoffFactor,
		Jitter:   jitter,
		Steps:    bearerTokenBackoffSteps,
		Cap:      bearerTokenBackoffCap,
	}
}

type tokenRequestFunc func(context.Context) (*authv1.TokenRequest, error)

type bearerTokenState struct {
	token       string
	expiresAt   time.Time
	usableUntil time.Time
	refreshAt   time.Time
}

// bearerTokenCreds serves a usable token while refreshing it in the background.
type bearerTokenCreds struct {
	requestToken   tokenRequestFunc
	now            func() time.Time
	refreshJitter  func() time.Duration
	refreshTimeout time.Duration

	mu          sync.Mutex
	state       bearerTokenState
	refreshDone chan struct{} // in-flight refresh; nil when idle
	retryAt     time.Time
	refreshErr  error
	backoff     wait.Backoff
}

// bearerTokenDialOption eagerly obtains a token and installs refreshing per-RPC credentials.
func bearerTokenDialOption(ctx context.Context, clientset kubernetes.Interface) (grpc.DialOption, error) {
	requestToken := func(ctx context.Context) (*authv1.TokenRequest, error) {
		return requestBearerToken(ctx, clientset)
	}
	creds, err := newBearerTokenCreds(ctx, requestToken, time.Now, randomBearerTokenRefreshJitter)
	if err != nil {
		return nil, err
	}
	return grpc.WithPerRPCCredentials(creds), nil
}

func requestBearerToken(ctx context.Context, clientset kubernetes.Interface) (*authv1.TokenRequest, error) {
	expirationSeconds := int64(bearerTokenTTL / time.Second)
	request := &authv1.TokenRequest{
		Spec: authv1.TokenRequestSpec{
			Audiences:         []string{apiServerName},
			ExpirationSeconds: &expirationSeconds,
		},
	}
	return clientset.CoreV1().ServiceAccounts(bearerTokenNamespace).CreateToken(
		ctx,
		bearerTokenServiceAccount,
		request,
		metav1.CreateOptions{},
	)
}

func newBearerTokenCreds(
	ctx context.Context,
	requestToken tokenRequestFunc,
	now func() time.Time,
	refreshJitter func() time.Duration,
) (*bearerTokenCreds, error) {
	creds := &bearerTokenCreds{
		requestToken:   requestToken,
		now:            now,
		refreshJitter:  refreshJitter,
		refreshTimeout: bearerTokenRefreshTimeout,
		backoff:        newBearerTokenBackoff(bearerTokenBackoffJitter),
	}

	// Initial retrieval is not shared, so preserve caller cancellation.
	initialCtx, cancel := context.WithTimeout(ctx, creds.refreshTimeout)
	defer cancel()
	state, err := creds.retrieve(initialCtx)
	if err != nil {
		return nil, err
	}
	creds.state = state
	return creds, nil
}

func (c *bearerTokenCreds) retrieve(ctx context.Context) (bearerTokenState, error) {
	response, err := c.requestToken(ctx)
	if err != nil {
		return bearerTokenState{}, fmt.Errorf("requesting ateapi bearer token: %w", err)
	}
	state, err := newBearerTokenState(response, c.now(), c.refreshJitter())
	if err != nil {
		return bearerTokenState{}, fmt.Errorf("invalid ateapi bearer token response: %w", err)
	}
	return state, nil
}

func (c *bearerTokenCreds) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := c.token(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

func (c *bearerTokenCreds) RequireTransportSecurity() bool { return true }

func (c *bearerTokenCreds) token(ctx context.Context) (string, error) {
	for {
		c.mu.Lock()
		now := c.now()
		valid := c.state.token != "" && now.Before(c.state.usableUntil)
		if valid && now.Before(c.state.refreshAt) {
			token := c.state.token
			c.mu.Unlock()
			return token, nil
		}

		// During backoff, use only a cached token outside the safety margin.
		if now.Before(c.retryAt) {
			if valid {
				token := c.state.token
				c.mu.Unlock()
				return token, nil
			}
			err := c.refreshErr
			c.mu.Unlock()
			return "", err
		}

		if valid {
			if c.refreshDone == nil {
				c.startRefreshLocked(ctx)
			}
			token := c.state.token
			c.mu.Unlock()
			return token, nil
		}

		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return "", fmt.Errorf("waiting to refresh ateapi bearer token: %w", err)
		}

		var done <-chan struct{} = c.refreshDone
		if done == nil {
			done = c.startRefreshLocked(ctx)
		}
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("waiting to refresh ateapi bearer token: %w", ctx.Err())
		case <-done:
		}
	}
}

// startRefreshLocked starts a caller-detached refresh shared by all callers.
func (c *bearerTokenCreds) startRefreshLocked(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	c.refreshDone = done

	refreshCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.refreshTimeout)
	go func() {
		defer cancel()
		next, err := c.retrieve(refreshCtx)
		c.finishRefresh(done, next, err)
	}()

	return done
}

func (c *bearerTokenCreds) finishRefresh(done chan struct{}, next bearerTokenState, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err == nil {
		c.state = next
		c.retryAt = time.Time{}
		c.refreshErr = nil
		c.backoff = newBearerTokenBackoff(c.backoff.Jitter)
	} else {
		now := c.now()
		retryAt := now.Add(c.backoff.Step())
		if now.Before(c.state.usableUntil) && retryAt.After(c.state.usableUntil) {
			retryAt = c.state.usableUntil
		}
		c.retryAt = retryAt
		c.refreshErr = err
	}
	c.refreshDone = nil
	close(done)
}

func newBearerTokenState(
	response *authv1.TokenRequest,
	now time.Time,
	refreshJitter time.Duration,
) (bearerTokenState, error) {
	if response == nil {
		return bearerTokenState{}, fmt.Errorf("token response was nil")
	}
	if response.Status.Token == "" {
		return bearerTokenState{}, fmt.Errorf("token response was empty")
	}

	expiresAt := response.Status.ExpirationTimestamp.Time
	if expiresAt.IsZero() {
		return bearerTokenState{}, fmt.Errorf("token response had no expiration timestamp")
	}
	if !now.Before(expiresAt) {
		return bearerTokenState{}, fmt.Errorf("token response expired at %s", expiresAt.Format(time.RFC3339))
	}

	lifetime := expiresAt.Sub(now)
	if lifetime <= bearerTokenExpirySafetyMargin {
		return bearerTokenState{}, fmt.Errorf("token response expires too soon at %s", expiresAt.Format(time.RFC3339))
	}
	if refreshJitter < 0 || refreshJitter > bearerTokenRefreshJitterMax {
		return bearerTokenState{}, fmt.Errorf("refresh jitter %s is outside [0, %s]", refreshJitter, bearerTokenRefreshJitterMax)
	}

	usableUntil := expiresAt.Add(-bearerTokenExpirySafetyMargin)
	refreshAt := expiresAt.Add(-lifetime/bearerTokenRefreshDivisor - refreshJitter)
	if refreshAt.After(usableUntil) {
		refreshAt = usableUntil
	}
	return bearerTokenState{
		token:       response.Status.Token,
		expiresAt:   expiresAt,
		usableUntil: usableUntil,
		refreshAt:   refreshAt,
	}, nil
}
