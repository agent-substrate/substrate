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
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"go.ate.dev/substrate/internal/oidcjwt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	clusterIssuer = "https://cluster.example"
	otherIssuer   = "https://other-cluster.example"
	apiAudience   = "api.ate-system.svc"
)

// fakeCluster stands in for the Kubernetes API server. It mints tokens and
// answers TokenReviews for them as kube-apiserver does: a token is valid
// for the review's audiences while its ServiceAccount, and the pod it is
// bound to, exist with the UIDs in its claims.
type fakeCluster struct {
	mu              sync.Mutex
	tokens          map[string]*oidcjwt.Claims
	serviceAccounts map[string]string
	pods            map[string]string
	nextID          int
	reviews         int
	audiences       [][]string
	// err fails reviews while set.
	err error
	// block holds reviews until it is closed.
	block chan struct{}
	// user, when set, replaces the user an authenticated review returns.
	user *authenticationv1.UserInfo
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{tokens: map[string]*oidcjwt.Claims{}, serviceAccounts: map[string]string{}, pods: map[string]string{}}
}

func (c *fakeCluster) uid() string {
	c.nextID++
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", c.nextID)
}

func (c *fakeCluster) createServiceAccount(namespace, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.serviceAccounts[namespace+"/"+name] = c.uid()
}

func (c *fakeCluster) deleteServiceAccount(namespace, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.serviceAccounts, namespace+"/"+name)
}

func (c *fakeCluster) createPod(namespace, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pods[namespace+"/"+name] = c.uid()
}

func (c *fakeCluster) deletePod(namespace, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pods, namespace+"/"+name)
}

// mint returns a token from issuer for subject. A subject naming a
// ServiceAccount gets its current UID, and pod, when set, binds the token to
// that pod in the ServiceAccount's namespace.
func (c *fakeCluster) mint(issuer, subject, pod string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	payload := fmt.Sprintf(`{"iss":%q,"jti":"%d"}`, issuer, c.nextID)
	token := "e30." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".c2ln"
	claims := &oidcjwt.Claims{Issuer: issuer, Subject: subject, Audiences: []string{apiAudience}}
	nsName, isServiceAccount := strings.CutPrefix(subject, serviceAccountPrefix)
	namespace, name, _ := strings.Cut(nsName, ":")
	if isServiceAccount {
		claims.Namespace = namespace
		claims.ServiceAccountName = name
		claims.ServiceAccountUID = c.serviceAccounts[namespace+"/"+name]
	}
	if pod != "" {
		claims.PodName = pod
		claims.PodUID = c.pods[namespace+"/"+pod]
	}
	c.tokens[token] = claims
	return token
}

// verify is the offline check a JWTProvider for issuer runs.
func (c *fakeCluster) verify(issuer string) func(context.Context, string) (*oidcjwt.Claims, error) {
	return func(_ context.Context, token string) (*oidcjwt.Claims, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		claims, ok := c.tokens[token]
		if !ok || claims.Issuer != issuer {
			return nil, fmt.Errorf("bad token")
		}
		return claims, nil
	}
}

// Create answers a TokenReview. It implements TokenReviewInterface.
func (c *fakeCluster) Create(ctx context.Context, tr *authenticationv1.TokenReview, _ metav1.CreateOptions) (*authenticationv1.TokenReview, error) {
	c.mu.Lock()
	c.reviews++
	c.audiences = append(c.audiences, tr.Spec.Audiences)
	block := c.block
	c.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	out := &authenticationv1.TokenReview{Spec: tr.Spec}
	claims, ok := c.tokens[tr.Spec.Token]
	switch {
	case !ok || claims.Issuer != clusterIssuer:
		out.Status.Error = "invalid token"
	case !slices.ContainsFunc(tr.Spec.Audiences, func(a string) bool { return slices.Contains(claims.Audiences, a) }):
		out.Status.Error = "token audiences are invalid"
	case c.serviceAccounts[claims.Namespace+"/"+claims.ServiceAccountName] != claims.ServiceAccountUID:
		out.Status.Error = "service account deleted or replaced"
	case claims.PodName != "" && c.pods[claims.Namespace+"/"+claims.PodName] != claims.PodUID:
		out.Status.Error = "pod deleted or replaced"
	default:
		out.Status.Authenticated = true
		out.Status.User = authenticationv1.UserInfo{Username: claims.Subject, UID: claims.ServiceAccountUID}
		if c.user != nil {
			out.Status.User = *c.user
		}
	}
	return out, nil
}

func (c *fakeCluster) reviewCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reviews
}

// clusterReviewer returns a TokenReviewer for clusterIssuer that sends
// reviews to c. With a nil c, every review is rejected.
func clusterReviewer(c *fakeCluster) *TokenReviewer {
	if c == nil {
		c = newFakeCluster()
	}
	return NewTokenReviewer(c, clusterIssuer)
}

// authenticator returns an authenticator trusting clusterIssuer and
// otherIssuer, with c as the API server of clusterIssuer.
func (c *fakeCluster) authenticator() jwtServerAuthenticator {
	return jwtServerAuthenticator{
		providers: []JWTProvider{
			{Name: "kubernetes", Issuer: clusterIssuer, Audiences: []string{apiAudience}, Verify: c.verify(clusterIssuer)},
			{Name: "other-cluster", Issuer: otherIssuer, Audiences: []string{apiAudience}, Verify: c.verify(otherIssuer)},
		},
		tokenReviewer: clusterReviewer(c),
	}
}

func authCode(ctx context.Context, auth jwtServerAuthenticator, token string) codes.Code {
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
	_, err := auth.authenticate(ctx)
	return status.Code(err)
}

func TestTokenReview(t *testing.T) {
	const (
		zed    = "system:serviceaccount:team-a:zed"
		amy    = "system:serviceaccount:team-b:amy"
		person = "alice@example.com"
	)
	tests := []struct {
		name string
		// run makes calls against c, through auth, checking each code.
		run         func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(token string, want codes.Code))
		wantReviews int
	}{
		{
			name: "current ServiceAccount accepted",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				expect(c.mint(clusterIssuer, zed, ""), codes.OK)
				if want := [][]string{{apiAudience}}; !slices.EqualFunc(c.audiences, want, slices.Equal) {
					t.Errorf("review audiences = %v, want %v", c.audiences, want)
				}
			},
			wantReviews: 1,
		},
		{
			name: "deleted ServiceAccount rejected",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				token := c.mint(clusterIssuer, zed, "")
				c.deleteServiceAccount("team-a", "zed")
				expect(token, codes.Unauthenticated)
			},
			wantReviews: 1,
		},
		{
			name: "recreated ServiceAccount: old token rejected, new token accepted",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				old := c.mint(clusterIssuer, zed, "")
				c.deleteServiceAccount("team-a", "zed")
				c.createServiceAccount("team-a", "zed")
				expect(old, codes.Unauthenticated)
				expect(c.mint(clusterIssuer, zed, ""), codes.OK)
			},
			wantReviews: 2,
		},
		{
			name: "pod-bound token rejected once the pod is deleted and the success expires",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				c.createPod("team-a", "runner")
				token := c.mint(clusterIssuer, zed, "runner")
				expect(token, codes.OK)
				c.deletePod("team-a", "runner")
				expect(token, codes.OK)
				time.Sleep(reviewSuccessTTL + time.Nanosecond)
				expect(token, codes.Unauthenticated)
			},
			wantReviews: 2,
		},
		{
			name: "review for another user rejected",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				token := c.mint(clusterIssuer, zed, "")
				c.user = &authenticationv1.UserInfo{Username: zed, UID: "00000000-0000-4000-8000-999999999999"}
				expect(token, codes.Unauthenticated)
				c.user = &authenticationv1.UserInfo{Username: amy, UID: c.tokens[token].ServiceAccountUID}
				time.Sleep(reviewFailureTTL + time.Nanosecond)
				expect(token, codes.Unauthenticated)
			},
			wantReviews: 2,
		},
		{
			name: "other cluster's ServiceAccount token accepted without review",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				token := c.mint(otherIssuer, "system:serviceaccount:team-a:ghost", "")
				expect(token, codes.OK)
			},
		},
		{
			name: "non-ServiceAccount subject from another issuer accepted without review",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				expect(c.mint(otherIssuer, person, ""), codes.OK)
			},
		},
		{
			name: "cluster token with a non-ServiceAccount subject rejected without review",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				expect(c.mint(clusterIssuer, person, ""), codes.Unauthenticated)
				expect(c.mint(clusterIssuer, "system:serviceaccount::zed", ""), codes.Unauthenticated)
			},
		},
		{
			name: "success reused until it expires",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				token := c.mint(clusterIssuer, zed, "")
				expect(token, codes.OK)
				time.Sleep(reviewSuccessTTL)
				expect(token, codes.OK)
				if got := c.reviewCount(); got != 1 {
					t.Errorf("reviews before the success expired = %d, want 1", got)
				}
				time.Sleep(time.Nanosecond)
				expect(token, codes.OK)
			},
			wantReviews: 2,
		},
		{
			name: "failure reused until it expires",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				token := c.mint(clusterIssuer, zed, "")
				c.deleteServiceAccount("team-a", "zed")
				expect(token, codes.Unauthenticated)
				time.Sleep(reviewFailureTTL)
				expect(token, codes.Unauthenticated)
				if got := c.reviewCount(); got != 1 {
					t.Errorf("reviews before the failure expired = %d, want 1", got)
				}
				time.Sleep(time.Nanosecond)
				expect(token, codes.Unauthenticated)
			},
			wantReviews: 2,
		},
		{
			name: "API server error is Unavailable and not reused",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				token := c.mint(clusterIssuer, zed, "")
				c.err = apierrors.NewForbidden(schema.GroupResource{Group: "authentication.k8s.io", Resource: "tokenreviews"}, "", fmt.Errorf("denied"))
				expect(token, codes.Unavailable)
				c.err = apierrors.NewInternalError(fmt.Errorf("etcd down"))
				expect(token, codes.Unavailable)
				c.err = nil
				expect(token, codes.OK)
			},
			wantReviews: 3,
		},
		{
			name: "namespace over its rate limit is Unavailable, other namespaces are not",
			run: func(t *testing.T, c *fakeCluster, auth jwtServerAuthenticator, expect func(string, codes.Code)) {
				for range namespaceReviewBurst {
					expect(c.mint(clusterIssuer, zed, ""), codes.OK)
				}
				expect(c.mint(clusterIssuer, zed, ""), codes.Unavailable)
				expect(c.mint(clusterIssuer, amy, ""), codes.OK)
				time.Sleep(time.Second / namespaceReviewQPS)
				expect(c.mint(clusterIssuer, zed, ""), codes.OK)
			},
			wantReviews: namespaceReviewBurst + 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newFakeCluster()
				c.createServiceAccount("team-a", "zed")
				c.createServiceAccount("team-b", "amy")
				auth := c.authenticator()
				expect := func(token string, want codes.Code) {
					t.Helper()
					if got := authCode(t.Context(), auth, token); got != want {
						t.Errorf("authenticate = %v, want %v", got, want)
					}
				}
				tt.run(t, c, auth, expect)
				if got := c.reviewCount(); got != tt.wantReviews {
					t.Errorf("reviews = %d, want %d", got, tt.wantReviews)
				}
			})
		})
	}
}

func TestTokenReviewInFlightLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFakeCluster()
		c.block = make(chan struct{})
		auth := c.authenticator()
		var tokens []string
		for i := range maxInFlightReviews + 1 {
			namespace := fmt.Sprintf("ns-%d", i)
			c.createServiceAccount(namespace, "zed")
			tokens = append(tokens, c.mint(clusterIssuer, "system:serviceaccount:"+namespace+":zed", ""))
		}

		held := make([]codes.Code, maxInFlightReviews)
		var wg sync.WaitGroup
		for i := range maxInFlightReviews {
			wg.Go(func() { held[i] = authCode(t.Context(), auth, tokens[i]) })
		}
		synctest.Wait()
		last := tokens[maxInFlightReviews]
		if got := authCode(t.Context(), auth, last); got != codes.Unavailable {
			t.Errorf("authenticate with every review in flight = %v, want Unavailable", got)
		}
		close(c.block)
		wg.Wait()
		for i, got := range held {
			if got != codes.OK {
				t.Errorf("held review %d = %v, want OK", i, got)
			}
		}
		if got := authCode(t.Context(), auth, last); got != codes.OK {
			t.Errorf("authenticate after reviews finished = %v, want OK", got)
		}
	})
}

func TestTokenReviewSharedFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newFakeCluster()
		c.createServiceAccount("team-a", "zed")
		c.block = make(chan struct{})
		auth := c.authenticator()
		token := c.mint(clusterIssuer, "system:serviceaccount:team-a:zed", "")

		canceledCtx, cancel := context.WithCancel(t.Context())
		var canceled codes.Code
		var wg sync.WaitGroup
		wg.Go(func() { canceled = authCode(canceledCtx, auth, token) })
		waiting := make([]codes.Code, 3)
		for i := range waiting {
			wg.Go(func() { waiting[i] = authCode(t.Context(), auth, token) })
		}
		synctest.Wait()
		if got := c.reviewCount(); got != 1 {
			t.Errorf("reviews for concurrent calls = %d, want 1", got)
		}

		cancel()
		synctest.Wait()
		if canceled != codes.Canceled {
			t.Errorf("canceled caller = %v, want Canceled", canceled)
		}
		close(c.block)
		wg.Wait()
		for i, got := range waiting {
			if got != codes.OK {
				t.Errorf("caller %d = %v, want OK", i, got)
			}
		}
		if got := c.reviewCount(); got != 1 {
			t.Errorf("reviews = %d, want 1", got)
		}
	})
}

func TestTokenFileIssuer(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	tests := []struct {
		name    string
		path    string
		want    string
		wantErr bool
	}{
		// {"iss":"https://cluster.example"}
		{name: "token", path: write("token", "e30.eyJpc3MiOiJodHRwczovL2NsdXN0ZXIuZXhhbXBsZSJ9.c2ln\n"), want: clusterIssuer},
		{name: "missing file", path: filepath.Join(dir, "missing"), wantErr: true},
		{name: "not a JWT", path: write("garbage", "not-a-jwt"), wantErr: true},
		// {"sub":"zed"}
		{name: "no issuer", path: write("no-iss", "e30.eyJzdWIiOiJ6ZWQifQ.c2ln"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TokenFileIssuer(tt.path)
			if (err != nil) != tt.wantErr {
				t.Fatalf("TokenFileIssuer() err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("TokenFileIssuer() = %q, want %q", got, tt.want)
			}
		})
	}
}
