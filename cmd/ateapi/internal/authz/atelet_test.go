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

package authz

import (
	"context"
	"crypto/x509"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/principal"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// fakeActorLocator hosts each Actor in hosted on the node it maps to, and
// records every query.
type fakeActorLocator struct {
	hosted  map[resources.ActorRef]string
	err     error
	queries []actorNodeQuery
}

type actorNodeQuery struct {
	actor resources.ActorRef
	node  string
}

func (f *fakeActorLocator) ActorHostedOnNode(_ context.Context, actor resources.ActorRef, node string) (bool, error) {
	f.queries = append(f.queries, actorNodeQuery{actor: actor, node: node})
	if f.err != nil {
		return false, f.err
	}
	hostingNode, ok := f.hosted[actor]
	return ok && hostingNode == node, nil
}

// TestUnaryServerInterceptor_AteletOnNode covers atelet calling an RPC whose
// rule declares ateletOnNode: the node in its certificate must host the Actor
// the request names, and OpenFGA is not consulted. Everyone else, and atelet
// on any other RPC, still goes through OpenFGA. The Authorizer here has no
// OpenFGA server, so reaching OpenFGA shows up as Internal.
func TestUnaryServerInterceptor_AteletOnNode(t *testing.T) {
	ateletID := installdefaults.SPIFFEID(installdefaults.SystemNamespace, installdefaults.AteletServiceAccount)
	authorizer := &Authorizer{}

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "runner"}
	actor := &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name}
	getEgress := &ateapipb.GetActorEgressPolicyRequest{Actor: actor, Name: resources.SingletonName}
	const (
		getEgressMethod    = ateapipb.Control_GetActorEgressPolicy_FullMethodName
		updateEgressMethod = ateapipb.Control_UpdateActorEgressPolicy_FullMethodName
	)

	// callerCtx is a request from id, authenticated by kind, over a connection
	// presenting cert (none if nil).
	callerCtx := func(id, kind string, cert *x509.Certificate) context.Context {
		return principal.InjectContext(ateletauthtest.ContextWith(cert), principal.PrincipalInfo{ID: id, Kind: kind})
	}
	ateletOn := func(node string) context.Context {
		return callerCtx(ateletID, principal.KindMTLS, ateletauthtest.CertOn(t, node))
	}

	tests := []struct {
		name    string
		ctx     context.Context
		enforce bool
		method  string
		req     any
		locator *fakeActorLocator
		// noLocator leaves the AteletCheck without an ActorLocator.
		noLocator bool
		// zeroCheck gives the interceptor the zero AteletCheck.
		zeroCheck   bool
		wantCode    codes.Code
		wantQueries []actorNodeQuery
	}{
		{
			name:        "atelet on the hosting node",
			ctx:         ateletOn("node-1"),
			enforce:     true,
			method:      getEgressMethod,
			req:         getEgress,
			wantCode:    codes.OK,
			wantQueries: []actorNodeQuery{{actor: actorRef, node: "node-1"}},
		},
		{
			// The same answer whether the Actor is elsewhere or nowhere.
			name:        "atelet on another node",
			ctx:         ateletOn("node-2"),
			enforce:     true,
			method:      getEgressMethod,
			req:         getEgress,
			wantCode:    codes.NotFound,
			wantQueries: []actorNodeQuery{{actor: actorRef, node: "node-2"}},
		},
		{
			name:        "lookup fails",
			ctx:         ateletOn("node-1"),
			enforce:     true,
			method:      getEgressMethod,
			req:         getEgress,
			locator:     &fakeActorLocator{err: apierror.Unavailable("store unavailable")},
			wantCode:    codes.Unavailable,
			wantQueries: []actorNodeQuery{{actor: actorRef, node: "node-1"}},
		},
		{
			name:     "request names no actor",
			ctx:      ateletOn("node-1"),
			enforce:  true,
			method:   getEgressMethod,
			req:      &ateapipb.GetActorEgressPolicyRequest{Name: resources.SingletonName},
			wantCode: codes.InvalidArgument,
		},
		{
			name:      "no ActorLocator configured",
			ctx:       ateletOn("node-1"),
			enforce:   true,
			method:    getEgressMethod,
			req:       getEgress,
			noLocator: true,
			wantCode:  codes.Internal,
		},
		{
			name:      "zero AteletCheck sends atelet to OpenFGA",
			ctx:       ateletOn("node-1"),
			enforce:   true,
			method:    getEgressMethod,
			req:       getEgress,
			zeroCheck: true,
			wantCode:  codes.Internal,
		},
		{
			// The node comes from the certificate, so without one the request
			// is refused rather than trusted.
			name:     "atelet principal without a certificate",
			ctx:      callerCtx(ateletID, principal.KindMTLS, nil),
			enforce:  true,
			method:   getEgressMethod,
			req:      getEgress,
			wantCode: codes.Unauthenticated,
		},
		{
			name:     "atelet principal presenting another identity's certificate",
			ctx:      callerCtx(ateletID, principal.KindMTLS, ateletauthtest.Cert(t, "/ns/ate-system/sa/ate-worker", ateletauthtest.PodIdentityOn("node-1"))),
			enforce:  true,
			method:   getEgressMethod,
			req:      getEgress,
			wantCode: codes.PermissionDenied,
		},
		{
			// A bearer token whose subject is atelet's SPIFFE ID is not atelet.
			name:     "jwt with atelet ID goes to OpenFGA",
			ctx:      callerCtx(ateletID, principal.KindJWT, nil),
			enforce:  true,
			method:   getEgressMethod,
			req:      getEgress,
			wantCode: codes.Internal,
		},
		{
			name:     "other caller goes to OpenFGA",
			ctx:      callerCtx("spiffe://cluster.local/ns/ate-system/sa/atenet-egress", principal.KindMTLS, nil),
			enforce:  true,
			method:   getEgressMethod,
			req:      getEgress,
			wantCode: codes.Internal,
		},
		{
			name:     "atelet on an RPC without ateletOnNode goes to OpenFGA",
			ctx:      ateletOn("node-1"),
			enforce:  true,
			method:   updateEgressMethod,
			req:      &ateapipb.UpdateActorEgressPolicyRequest{Actor: actor},
			wantCode: codes.Internal,
		},
		{
			name:     "enforcement off",
			ctx:      ateletOn("node-2"),
			enforce:  false,
			method:   getEgressMethod,
			req:      getEgress,
			wantCode: codes.OK,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			locator := tc.locator
			if locator == nil {
				locator = &fakeActorLocator{hosted: map[resources.ActorRef]string{actorRef: "node-1"}}
			}
			ateletCheck := AteletCheck{SPIFFEID: ateletID, ActorLocator: locator}
			if tc.noLocator {
				ateletCheck.ActorLocator = nil
			}
			if tc.zeroCheck {
				ateletCheck = AteletCheck{}
			}
			interceptor := UnaryServerInterceptor(authorizer, tc.enforce, ateletCheck)

			handlerCalled := false
			_, err := interceptor(tc.ctx, tc.req, &grpc.UnaryServerInfo{FullMethod: tc.method}, func(context.Context, any) (any, error) {
				handlerCalled = true
				return "ok", nil
			})
			if got := apierror.Code(err); got != tc.wantCode {
				t.Fatalf("code = %v (err %v), want %v", got, err, tc.wantCode)
			}
			if wantHandler := tc.wantCode == codes.OK; handlerCalled != wantHandler {
				t.Errorf("handlerCalled = %v, want %v", handlerCalled, wantHandler)
			}
			if diff := cmp.Diff(tc.wantQueries, locator.queries, cmp.AllowUnexported(actorNodeQuery{})); diff != "" {
				t.Errorf("ActorLocator queries mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
