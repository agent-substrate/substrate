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

package inner

import (
	"context"
	"strings"
	"testing"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"
)

// policyFunc adapts a function to Policy, so a test can state its policy inline.
type policyFunc func(context.Context, *Request) CalloutResult

func (f policyFunc) Evaluate(ctx context.Context, req *Request) CalloutResult { return f(ctx, req) }

// isImmediate reports whether the response is Envoy answering the request
// itself, which is what a denial looks like on the wire.
func isImmediate(resp *extprocv3.ProcessingResponse) bool {
	_, ok := resp.GetResponse().(*extprocv3.ProcessingResponse_ImmediateResponse)
	return ok
}

// authorizeWith runs one request through a server built on the given policy.
func authorizeWith(t *testing.T, policy Policy, actorURI string, headers map[string]string) *extprocv3.ProcessingResponse {
	t.Helper()
	attrs := map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": {Fields: map[string]*structpb.Value{
			actorAttribute: structpb.NewStringValue(actorURI),
		}},
	}
	return NewServer(policy).authorize(t.Context(), attrs, rawHeaders(headers).GetRequestHeaders())
}

// The server must route the decision it was given, not one of its own.
func TestServerHonorsTheConfiguredPolicy(t *testing.T) {
	deny := policyFunc(func(context.Context, *Request) CalloutResult {
		return Deny("test policy denies everything")
	})
	if resp := authorizeWith(t, deny, "", map[string]string{":authority": "example.com"}); !isImmediate(resp) {
		t.Errorf("a denying policy produced %T, want an immediate response", resp.GetResponse())
	}

	if resp := authorizeWith(t, AllowAll{}, "", map[string]string{":authority": "example.com"}); isImmediate(resp) {
		t.Error("AllowAll produced an immediate response, want the request passed upstream")
	}
}

// A Server with no policy is a wiring mistake, and the way a wiring mistake in
// an authorization checkpoint must present is as refused traffic. Passing
// everything would leave the gateway unpoliced with nothing on fire to say so.
func TestNilPolicyDeniesEverything(t *testing.T) {
	resp := authorizeWith(t, nil, "", map[string]string{":authority": "example.com"})
	if !isImmediate(resp) {
		t.Errorf("a server with no policy produced %T, want a denial", resp.GetResponse())
	}
}

// What a policy is handed is the contract this package exists to provide: the
// decrypted request, plus the identity that arrived out of band as filter
// state. Both have to be there, already parsed, before Evaluate is called.
func TestPolicySeesTheRequestAndTheSplitActor(t *testing.T) {
	const uri = "spiffe://substrate-actor.local/atespace/demo/actor/egress-demo"

	var got *Request
	record := policyFunc(func(_ context.Context, req *Request) CalloutResult {
		got = req
		return Allow()
	})
	authorizeWith(t, record, uri, map[string]string{
		":authority": "Example.COM:443",
		":method":    "GET",
		":path":      "/v1/models",
		":scheme":    "https",
	})

	if got == nil {
		t.Fatal("the policy was never called")
	}
	for _, tc := range []struct{ field, got, want string }{
		{"Method", got.Method, "GET"},
		{"Authority", got.Authority, "Example.COM:443"},
		{"Path", got.Path, "/v1/models"},
		{"Scheme", got.Scheme, "https"},
		{"Host()", got.Host(), "example.com"},
		{"ActorSpiffe", got.ActorSpiffe, uri},
		{"Atespace", got.Atespace, "demo"},
		{"ActorName", got.ActorName, "egress-demo"},
	} {
		if tc.got != tc.want {
			t.Errorf("Request.%s = %q, want %q", tc.field, tc.got, tc.want)
		}
	}
}

// The credential names belong in the allow log; the values never do. The
// LogValuer is what keeps that true at every call site, so it is tested
// directly rather than through the log text.
func TestLoggedCredentialsRenderNamesOnly(t *testing.T) {
	value := loggedCredentials{
		{Key: "authorization", Value: "Bearer s3cret"},
		{Key: "x-api-key", Value: "k3y"},
	}.LogValue().String()

	for _, name := range []string{"authorization", "x-api-key"} {
		if !strings.Contains(value, name) {
			t.Errorf("credential log value %q is missing the name %s", value, name)
		}
	}
	for _, secret := range []string{"s3cret", "k3y"} {
		if strings.Contains(value, secret) {
			t.Errorf("a credential value reached the log: %q", value)
		}
	}
}
