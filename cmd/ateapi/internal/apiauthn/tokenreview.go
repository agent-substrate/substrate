// Copyright 2026 The Agent Substrate Authors
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

package apiauthn

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/oidcjwt"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/cache"
	authenticationv1client "k8s.io/client-go/kubernetes/typed/authentication/v1"
	"k8s.io/utils/lru"
)

// These bound the load reviews put on the Kubernetes API server. They are
// not measured capacity.
const (
	// reviewSuccessTTL is how long an accepted review is reused. It adds to
	// the time a deleted ServiceAccount or bound object takes to revoke its
	// tokens, on top of the API server's own token cache, which keeps a
	// success for the same time.
	reviewSuccessTTL = 10 * time.Second
	// reviewFailureTTL bounds how often a client that retries a rejected
	// token causes a review. A cached failure can only delay a valid token,
	// such as one for a ServiceAccount the API server has not seen yet.
	reviewFailureTTL = 2 * time.Second
	reviewCacheSize  = 4096
	reviewTimeout    = 5 * time.Second

	// Anyone who can create tokens in a namespace can make every token a
	// cache miss, so reviews are limited per ServiceAccount namespace, and
	// a flood from one namespace fails only that namespace's uncached
	// tokens. The namespace comes from the verified subject, so a caller
	// can't pick it.
	namespaceReviewQPS   = 10
	namespaceReviewBurst = 50
	maxReviewNamespaces  = 1024
	// maxInFlightReviews bounds concurrent reviews while the API server is
	// slow. A caller who can create tokens in many namespaces can still
	// fill it, and the API server's priority and fairness sees every review
	// as ate-api-server's, so it can't tell tenants apart either.
	maxInFlightReviews = 64
)

// serviceAccountPrefix starts the subject of a Kubernetes ServiceAccount
// token: system:serviceaccount:<namespace>:<name>.
const serviceAccountPrefix = "system:serviceaccount:"

var (
	errInvalidToken = status.Error(codes.Unauthenticated, "invalid bearer token")
	errCannotVerify = status.Error(codes.Unavailable, "cannot verify token, retry later")
)

// TokenReviewer checks tokens from the cluster's own ServiceAccount issuer
// with the Kubernetes TokenReview API. The API server rejects a token whose
// ServiceAccount, or the pod, Secret or node it is bound to, was deleted or
// replaced.
type TokenReviewer struct {
	issuer   string
	client   authenticationv1client.TokenReviewInterface
	results  *cache.LRUExpireCache
	flights  singleflight.Group
	inFlight chan struct{}

	mu       sync.Mutex
	limiters *lru.Cache

	limitedLog rate.Sometimes
	failedLog  rate.Sometimes
}

// NewTokenReviewer returns a TokenReviewer for tokens whose iss is issuer.
func NewTokenReviewer(client authenticationv1client.TokenReviewInterface, issuer string) *TokenReviewer {
	return &TokenReviewer{
		issuer:     issuer,
		client:     client,
		results:    cache.NewLRUExpireCache(reviewCacheSize),
		inFlight:   make(chan struct{}, maxInFlightReviews),
		limiters:   lru.New(maxReviewNamespaces),
		limitedLog: rate.Sometimes{Interval: time.Minute},
		failedLog:  rate.Sometimes{Interval: time.Minute},
	}
}

// TokenFileIssuer returns the iss claim of the token in path without
// verifying it. The API server mints ate-api-server's own token, so its
// issuer is the one the API server signs ServiceAccount tokens with.
func TokenFileIssuer(path string) (string, error) {
	token, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	issuer, err := unverifiedIssuer(strings.TrimSpace(string(token)))
	if err != nil {
		return "", fmt.Errorf("parse token in %s: %w", path, err)
	}
	return issuer, nil
}

// review returns nil when the API server authenticates token as the
// ServiceAccount in claims for audiences. claims must come from verifying
// token. It returns Unauthenticated when the API server rejects token, and
// Unavailable when it can't tell.
func (r *TokenReviewer) review(ctx context.Context, token string, claims *oidcjwt.Claims, audiences []string) error {
	nsName, ok := strings.CutPrefix(claims.Subject, serviceAccountPrefix)
	namespace, _, _ := strings.Cut(nsName, ":")
	if !ok || namespace == "" {
		slog.DebugContext(ctx, "Token from the cluster's ServiceAccount issuer has no ServiceAccount subject", slog.String("subject", claims.Subject))
		return errInvalidToken
	}
	key := sha256.Sum256([]byte(token))
	if accepted, ok := r.results.Get(key); ok {
		return verdict(accepted.(bool))
	}
	results := r.flights.DoChan(string(key[:]), func() (any, error) {
		// The review outlives a caller that gives up, so the callers
		// sharing it still get its result.
		return r.create(context.WithoutCancel(ctx), key, token, namespace, claims, audiences)
	})
	select {
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case res := <-results:
		if res.Err != nil {
			return res.Err
		}
		return verdict(res.Val.(bool))
	}
}

// create runs one TokenReview and caches its verdict. Errors are not cached.
func (r *TokenReviewer) create(ctx context.Context, key [sha256.Size]byte, token, namespace string, claims *oidcjwt.Claims, audiences []string) (bool, error) {
	select {
	case r.inFlight <- struct{}{}:
		defer func() { <-r.inFlight }()
	default:
		r.limitedLog.Do(func() {
			slog.WarnContext(ctx, "Too many TokenReviews in flight, failing tokens with Unavailable", slog.String("namespace", namespace))
		})
		return false, errCannotVerify
	}
	if !r.limiter(namespace).Allow() {
		r.limitedLog.Do(func() {
			slog.WarnContext(ctx, "TokenReviews over the namespace's rate limit, failing tokens with Unavailable", slog.String("namespace", namespace))
		})
		return false, errCannotVerify
	}

	ctx, cancel := context.WithTimeout(ctx, reviewTimeout)
	defer cancel()
	tr, err := r.client.Create(ctx, &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{Token: token, Audiences: audiences},
	}, metav1.CreateOptions{})
	if err != nil {
		r.failedLog.Do(func() {
			slog.WarnContext(ctx, "TokenReview failed, failing tokens with Unavailable", slog.String("namespace", namespace), slog.Any("err", err))
		})
		return false, errCannotVerify
	}
	// Another authenticator in the API server could claim the token, so the
	// user must be the ServiceAccount the token names.
	user := tr.Status.User
	accepted := tr.Status.Authenticated && user.Username == claims.Subject && user.UID == claims.ServiceAccountUID
	if !accepted {
		slog.DebugContext(ctx, "TokenReview rejected token", slog.String("subject", claims.Subject), slog.String("error", tr.Status.Error))
		r.results.Add(key, false, reviewFailureTTL)
		return false, nil
	}
	r.results.Add(key, true, reviewSuccessTTL)
	return true, nil
}

// limiter returns the review rate limiter of namespace.
func (r *TokenReviewer) limiter(namespace string) *rate.Limiter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l, ok := r.limiters.Get(namespace); ok {
		return l.(*rate.Limiter)
	}
	l := rate.NewLimiter(namespaceReviewQPS, namespaceReviewBurst)
	r.limiters.Add(namespace, l)
	return l
}

func verdict(accepted bool) error {
	if !accepted {
		return errInvalidToken
	}
	return nil
}
