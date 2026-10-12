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

package egress

import (
	"cmp"
	"context"
	"errors"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// credentialInjectionPolicySample injects "authorization: Bearer <secret>" from
// ate-secret://k8s/default/token, so the provider name under test is "k8s".
const injectionProviderName = "k8s"

// fakeProvider is a stub CredentialProviderClient recording the last request.
type fakeProvider struct {
	resp *credproviderpb.FetchSecretResponse
	err  error
	got  *credproviderpb.FetchSecretRequest
}

func (f *fakeProvider) FetchSecret(_ context.Context, req *credproviderpb.FetchSecretRequest, _ ...grpc.CallOption) (*credproviderpb.FetchSecretResponse, error) {
	f.got = req
	return f.resp, f.err
}

// bearerTokenResponse is a FetchSecretResponse carrying a bearer-token
// credential as its opaque secret bytes.
func bearerTokenResponse(token string) *credproviderpb.FetchSecretResponse {
	return &credproviderpb.FetchSecretResponse{OpaqueBytes: []byte(token)}
}

func injectionHandlerFor(policy *ateapipb.EgressPolicy, provider credproviderpb.CredentialProviderClient, providerName string) *Handler {
	return New(&egressMockClient{actor: runningActor(), policy: policy}, nil, 0, provider, providerName)
}

// withAuthorization adds the placeholder authorization header the sample
// policy replaces, as the actor would send it.
func withAuthorization(md *extproc.RequestMetadata) *extproc.RequestMetadata {
	md.Headers["authorization"] = "Bearer placeholder"
	return md
}

// Both HTTP and HTTPS resolve the credential using the actor's SPIFFE identity
// and overwrite the header the request carries.
func TestInjectionOnRequestLegs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		leg    string
		policy *ateapipb.EgressPolicy
	}{
		{name: "https", leg: extproc.EgressTLSMITMFilterChainName, policy: credentialInjectionPolicySample("api.example.com")},
		{name: "http", leg: extproc.EgressCleartextFilterChainName, policy: cleartextInjectionPolicy("api.example.com")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &fakeProvider{resp: bearerTokenResponse("s3cr3t\n")}
			h := injectionHandlerFor(tc.policy, provider, injectionProviderName)

			res, err := h.HandleRequestHeaders(context.Background(),
				withAuthorization(innerMetadata(tc.leg, "GET", "api.example.com", nil)))
			if err != nil {
				t.Fatalf("HandleRequestHeaders: %v", err)
			}

			setHeaders := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders()
			if len(setHeaders) != 1 {
				t.Fatalf("got %d header mutations, want 1", len(setHeaders))
			}
			h0 := setHeaders[0]
			if got := h0.GetHeader().GetKey(); got != "authorization" {
				t.Errorf("header key = %q, want authorization", got)
			}
			if got := string(h0.GetHeader().GetRawValue()); got != "Bearer s3cr3t" {
				t.Errorf("header value = %q, want %q (trailing newline trimmed)", got, "Bearer s3cr3t")
			}
			if h0.GetAppendAction() != corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD {
				t.Errorf("append action = %v, want OVERWRITE_IF_EXISTS_OR_ADD", h0.GetAppendAction())
			}
			if got := provider.got.GetUri(); got != "ate-secret://k8s/default/token" {
				t.Errorf("provider URI = %q", got)
			}
			wantActorSPIFFEID := "spiffe://substrate-actor.local/actor/default/my-actor"
			if got := provider.got.GetActorSpiffeId(); got != wantActorSPIFFEID {
				t.Errorf("actor identity = %q, want %q", got, wantActorSPIFFEID)
			}
		})
	}
}

// A request that does not carry the header a rule replaces goes out unchanged:
// nothing is fetched, added, or denied, even where injecting would fail.
func TestInjectionSkippedWithoutHeader(t *testing.T) {
	jwtPolicy := &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{Https: &ateapipb.HTTPSRule{
		Hostnames: []string{"api.example.com"},
		Effects: &ateapipb.HttpRuleEffects{ReplaceHeaders: []*ateapipb.CredentialHeader{{
			Header:   "authorization",
			ActorJwt: &ateapipb.ActorJWTSource{Audiences: []string{"https://api.example.com"}},
		}}},
	}}}}
	for _, tc := range []struct {
		name     string
		policy   *ateapipb.EgressPolicy
		provider *fakeProvider // nil means no provider configured
	}{
		{name: "provider configured", policy: credentialInjectionPolicySample("api.example.com"), provider: &fakeProvider{resp: bearerTokenResponse("s3cr3t")}},
		{name: "no provider configured", policy: credentialInjectionPolicySample("api.example.com")},
		{name: "actor JWT", policy: jwtPolicy, provider: &fakeProvider{resp: bearerTokenResponse("s3cr3t")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var provider credproviderpb.CredentialProviderClient
			if tc.provider != nil {
				provider = tc.provider
			}
			h := injectionHandlerFor(tc.policy, provider, injectionProviderName)
			res, err := h.HandleRequestHeaders(context.Background(),
				innerMetadata(extproc.EgressTLSMITMFilterChainName, "GET", "api.example.com", nil))
			if err != nil {
				t.Fatalf("HandleRequestHeaders: %v", err)
			}
			if got := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders(); len(got) != 0 {
				t.Errorf("got %d injected headers, want 0", len(got))
			}
			if tc.provider != nil && tc.provider.got != nil {
				t.Errorf("provider was asked for %v, want no fetch", tc.provider.got)
			}
		})
	}
}

// Of a rule's replacements, only those whose header the request carries are
// applied, matching the header name case-insensitively.
func TestInjectionReplacesOnlyCarriedHeaders(t *testing.T) {
	policy := &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{Https: &ateapipb.HTTPSRule{
		Hostnames: []string{"api.example.com"},
		Effects: &ateapipb.HttpRuleEffects{ReplaceHeaders: []*ateapipb.CredentialHeader{
			{Header: "authorization", Prefix: "Bearer ", CredentialUri: "ate-secret://k8s/default/token"},
			{Header: "X-Api-Key", CredentialUri: "ate-secret://k8s/default/api-key"},
		}},
	}}}}
	provider := &fakeProvider{resp: bearerTokenResponse("s3cr3t")}
	h := injectionHandlerFor(policy, provider, injectionProviderName)
	md := innerMetadata(extproc.EgressTLSMITMFilterChainName, "GET", "api.example.com", nil)
	md.Headers["x-api-key"] = "placeholder"

	res, err := h.HandleRequestHeaders(context.Background(), md)
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	setHeaders := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders()
	if len(setHeaders) != 1 {
		t.Fatalf("got %d header mutations, want only the carried X-Api-Key", len(setHeaders))
	}
	if got := setHeaders[0].GetHeader().GetKey(); got != "X-Api-Key" {
		t.Errorf("header key = %q, want X-Api-Key", got)
	}
	if got := provider.got.GetUri(); got != "ate-secret://k8s/default/api-key" {
		t.Errorf("provider URI = %q, want the X-Api-Key credential", got)
	}
}

// A failure to produce the promised credential denies the
// request rather than forwarding it without the credential.
func TestInjectionDenials(t *testing.T) {
	tests := []struct {
		name         string
		provider     credproviderpb.CredentialProviderClient
		providerName string
		want         envoy_type.StatusCode
	}{
		{
			name:         "no provider configured is refused",
			providerName: injectionProviderName,
			want:         envoy_type.StatusCode_InternalServerError,
		},
		{
			name:         "credential URI for another provider is refused",
			provider:     &fakeProvider{resp: bearerTokenResponse("s3cr3t")},
			providerName: "vault", // policy URI is ate-secret://k8s/...
			want:         envoy_type.StatusCode_InternalServerError,
		},
		{
			// A transient provider failure is retryable.
			name:         "provider unavailable fails closed as retryable",
			provider:     &fakeProvider{err: status.Error(codes.Unavailable, "provider down")},
			providerName: injectionProviderName,
			want:         envoy_type.StatusCode_ServiceUnavailable,
		},
		{
			// A secret the provider does not hold cannot appear on retry.
			name:         "secret not found denies as non-retryable",
			provider:     &fakeProvider{err: status.Error(codes.NotFound, "no such secret")},
			providerName: injectionProviderName,
			want:         envoy_type.StatusCode_Forbidden,
		},
		{
			name:         "provider refuses the actor denies as non-retryable",
			provider:     &fakeProvider{err: status.Error(codes.PermissionDenied, "atespace not allowed")},
			providerName: injectionProviderName,
			want:         envoy_type.StatusCode_Forbidden,
		},
		{
			// An unclassified error denies rather than inviting retries.
			name:         "unexpected provider error denies",
			provider:     &fakeProvider{err: errors.New("provider down")},
			providerName: injectionProviderName,
			want:         envoy_type.StatusCode_Forbidden,
		},
		{
			name:         "empty secret fails closed",
			provider:     &fakeProvider{resp: bearerTokenResponse("")},
			providerName: injectionProviderName,
			want:         envoy_type.StatusCode_ServiceUnavailable,
		},
	}
	for _, leg := range []string{extproc.EgressTLSMITMFilterChainName, extproc.EgressCleartextFilterChainName} {
		t.Run(leg, func(t *testing.T) {
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					policy := credentialInjectionPolicySample("api.example.com")
					if leg == extproc.EgressCleartextFilterChainName {
						policy = cleartextInjectionPolicy("api.example.com")
					}
					h := injectionHandlerFor(policy, tc.provider, tc.providerName)
					_, err := h.HandleRequestHeaders(context.Background(),
						withAuthorization(innerMetadata(leg, "GET", "api.example.com", nil)))
					wantStatus(t, err, tc.want)
				})
			}
		})
	}
}

// actorJWTHeader replaces header with an actor JWT bound to api.example.com.
func actorJWTHeader(header string) *ateapipb.CredentialHeader {
	return &ateapipb.CredentialHeader{
		Header:   header,
		Prefix:   "Bearer ",
		ActorJwt: &ateapipb.ActorJWTSource{Audiences: []string{"https://api.example.com"}, ExpirationSeconds: 900},
	}
}

// replaceHeadersPolicy is a rule for api.example.com on leg's protocol (https
// for the MITM leg, http for the cleartext one) that replaces headers with
// entries.
func replaceHeadersPolicy(leg string, entries ...*ateapipb.CredentialHeader) *ateapipb.EgressPolicy {
	hosts := []string{"api.example.com"}
	effects := &ateapipb.HttpRuleEffects{ReplaceHeaders: entries}
	if leg == extproc.EgressCleartextFilterChainName {
		return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{Http: &ateapipb.HTTPRule{Hostnames: hosts, Effects: effects}}}}
	}
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{Https: &ateapipb.HTTPSRule{Hostnames: hosts, Effects: effects}}}}
}

var requestLegs = []string{extproc.EgressTLSMITMFilterChainName, extproc.EgressCleartextFilterChainName}

// An actor JWT is injected with no credential provider configured. The
// rule's credential_uri entry is for a header the request does not carry, so
// the missing provider does not deny it.
func TestActorJWTInjection(t *testing.T) {
	for _, leg := range requestLegs {
		t.Run(leg, func(t *testing.T) {
			client := &egressMockClient{actor: runningActor(), policy: replaceHeadersPolicy(leg,
				actorJWTHeader("authorization"),
				&ateapipb.CredentialHeader{Header: "x-api-key", CredentialUri: "ate-secret://k8s/default/token"},
			)}
			h := New(client, nil, 0, nil, "")

			res, err := h.HandleRequestHeaders(context.Background(),
				withAuthorization(innerMetadata(leg, "GET", "api.example.com", nil)))
			if err != nil {
				t.Fatalf("HandleRequestHeaders: %v", err)
			}
			setHeaders := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders()
			if len(setHeaders) != 1 {
				t.Fatalf("got %d header mutations, want only the actor JWT", len(setHeaders))
			}
			h0 := setHeaders[0]
			if got := h0.GetHeader().GetKey(); got != "authorization" {
				t.Errorf("header key = %q, want authorization", got)
			}
			if got := string(h0.GetHeader().GetRawValue()); got != "Bearer jwt-1" {
				t.Errorf("header value = %q, want %q", got, "Bearer jwt-1")
			}
			if h0.GetAppendAction() != corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD {
				t.Errorf("append action = %v, want OVERWRITE_IF_EXISTS_OR_ADD", h0.GetAppendAction())
			}
			want := &ateapipb.MintActorJWTRequest{
				Actor:             &ateapipb.ObjectRef{Atespace: testEgressAtespace, Name: testEgressActor},
				Audiences:         []string{"https://api.example.com"},
				ExpirationSeconds: 900,
			}
			if got := client.lastMint.Load(); !proto.Equal(got, want) {
				t.Errorf("MintActorJWT request = %v, want %v", got, want)
			}
		})
	}
}

func TestActorJWTInjectionSkippedWithoutHeader(t *testing.T) {
	client := &egressMockClient{actor: runningActor(), policy: replaceHeadersPolicy(extproc.EgressTLSMITMFilterChainName, actorJWTHeader("authorization"))}
	h := New(client, nil, 0, nil, "")
	res, err := h.HandleRequestHeaders(context.Background(),
		innerMetadata(extproc.EgressTLSMITMFilterChainName, "GET", "api.example.com", nil))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if got := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders(); len(got) != 0 {
		t.Errorf("got %d injected headers, want 0", len(got))
	}
	if calls := client.mintCalls.Load(); calls != 0 {
		t.Errorf("MintActorJWT calls = %d, want 0", calls)
	}
}

func TestActorJWTInjectionDenials(t *testing.T) {
	tests := []struct {
		name    string
		header  string // authorization unless set
		mintErr error
		want    envoy_type.StatusCode
	}{
		{name: "actor deleted", mintErr: status.Error(codes.NotFound, "actor not found"), want: envoy_type.StatusCode_Forbidden},
		{name: "ateapi unavailable", mintErr: status.Error(codes.Unavailable, "ateapi is down"), want: envoy_type.StatusCode_ServiceUnavailable},
		{name: "mint timed out", mintErr: status.Error(codes.DeadlineExceeded, "deadline exceeded"), want: envoy_type.StatusCode_ServiceUnavailable},
		{name: "gateway may not mint", mintErr: status.Error(codes.PermissionDenied, "denied"), want: envoy_type.StatusCode_InternalServerError},
		{name: "mint request rejected", mintErr: status.Error(codes.InvalidArgument, "bad lifetime"), want: envoy_type.StatusCode_InternalServerError},
		{name: "system header", header: ":path", want: envoy_type.StatusCode_InternalServerError},
	}
	for _, leg := range requestLegs {
		t.Run(leg, func(t *testing.T) {
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					header := cmp.Or(tc.header, "authorization")
					mock := &egressMockClient{
						actor:   runningActor(),
						policy:  replaceHeadersPolicy(leg, actorJWTHeader(header)),
						mintErr: tc.mintErr,
					}
					h := New(mock, nil, 0, nil, "")
					md := innerMetadata(leg, "GET", "api.example.com", nil)
					md.Headers[header] = "placeholder"
					_, err := h.HandleRequestHeaders(context.Background(), md)
					wantStatus(t, err, tc.want)
				})
			}
		})
	}
}
