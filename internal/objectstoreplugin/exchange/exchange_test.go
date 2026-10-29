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

package exchange

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/objectstoreplugin/actorauthz"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/objectstoreplugin/credential"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
	"go.ate.dev/substrate/internal/resources"
)

// The checks behind the exchange are tested in actorauthz. These tests cover
// what the exchange adds: the issuer call, its errors and the response.

const uid = "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"

// fakeAuthorizer returns a fixed decision or error and records the JWT.
type fakeAuthorizer struct {
	decision *actorauthz.Decision
	err      error
	jwt      string
}

func (f *fakeAuthorizer) Authorize(_ context.Context, jwt string) (*actorauthz.Decision, error) {
	f.jwt = jwt
	return f.decision, f.err
}

// fakeIssuer records what it was asked to mint.
type fakeIssuer struct {
	err    error
	calls  int
	uid    string
	grants []actorscope.Grant
	force  bool
	expiry time.Time
}

func (f *fakeIssuer) Issue(_ context.Context, actorUID string, grants []actorscope.Grant, force bool) (credential.Token, error) {
	f.calls++
	f.uid, f.grants, f.force = actorUID, grants, force
	if f.err != nil {
		return credential.Token{}, f.err
	}
	return credential.Token{AccessToken: "downscoped", Expiry: f.expiry}, nil
}

// allowed is a decision for the actor uid with one read-write grant.
func allowed(t *testing.T) *actorauthz.Decision {
	t.Helper()
	p, err := resources.ParseStoragePrefix("gs://snaps/root/atespaces/team-a/actors/" + uid)
	if err != nil {
		t.Fatal(err)
	}
	return &actorauthz.Decision{ActorUID: uid, Grants: []actorscope.Grant{{Prefix: p, Access: actorscope.ReadWrite}}}
}

func TestExchangeActorJwt(t *testing.T) {
	expiry := time.Now().Add(50 * time.Minute).Truncate(time.Second)
	decision := allowed(t)
	authorizer := &fakeAuthorizer{decision: decision}
	issuer := &fakeIssuer{expiry: expiry}
	srv := New(authorizer, issuer)

	for _, force := range []bool{false, true} {
		resp, err := srv.ExchangeActorJwt(t.Context(), &exchangepb.ExchangeActorJwtRequest{ActorJwt: "jwt", Force: force})
		if err != nil {
			t.Fatalf("force=%v: %v", force, err)
		}
		if authorizer.jwt != "jwt" {
			t.Errorf("force=%v: authorizer got jwt %q, want %q", force, authorizer.jwt, "jwt")
		}
		if resp.GetAccessToken() != "downscoped" || !resp.GetExpireTime().AsTime().Equal(expiry) {
			t.Errorf("force=%v: response = %v", force, resp)
		}
		if issuer.uid != uid || issuer.force != force || !slices.Equal(issuer.grants, decision.Grants) {
			t.Errorf("force=%v: issuer asked for uid=%s grants=%v force=%v", force, issuer.uid, issuer.grants, issuer.force)
		}
	}
	if issuer.calls != 2 {
		t.Errorf("issuer calls = %d, want 2", issuer.calls)
	}
}

func TestExchangeActorJwt_Errors(t *testing.T) {
	for _, tc := range []struct {
		name          string
		authorizerErr error
		issuerErr     error
		want          codes.Code
	}{
		{name: "authorizer refuses", authorizerErr: status.Error(codes.PermissionDenied, "not on this node"), want: codes.PermissionDenied},
		{name: "issuer failing", issuerErr: errors.New("STS 503"), want: codes.Unavailable},
		{name: "issuer cannot express the grants", issuerErr: status.Error(codes.Internal, "bad boundary"), want: codes.Internal},
		{name: "caller gave up at STS", issuerErr: fmt.Errorf("Post sts: %w", context.Canceled), want: codes.Canceled},
		{name: "caller's deadline passed at STS", issuerErr: fmt.Errorf("Post sts: %w", context.DeadlineExceeded), want: codes.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authorizer := &fakeAuthorizer{err: tc.authorizerErr}
			if tc.authorizerErr == nil {
				authorizer.decision = allowed(t)
			}
			issuer := &fakeIssuer{err: tc.issuerErr}
			resp, err := New(authorizer, issuer).ExchangeActorJwt(t.Context(), &exchangepb.ExchangeActorJwtRequest{ActorJwt: "jwt"})
			if resp != nil {
				t.Errorf("response = %v, want nil", resp)
			}
			if status.Code(err) != tc.want {
				t.Errorf("code = %v (%v), want %v", status.Code(err), err, tc.want)
			}
			wantCalls := 0
			if tc.authorizerErr == nil {
				wantCalls = 1
			}
			if issuer.calls != wantCalls {
				t.Errorf("issuer was called %d times, want %d", issuer.calls, wantCalls)
			}
		})
	}
}
