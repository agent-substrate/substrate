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
	"context"
	"errors"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// credentialInjectionPolicySample injects "authorization: Bearer <secret>" from
// substrate-secret://k8s/default/token, so the provider class under test is "k8s".
const injectionProviderClass = "k8s"

// fakeProvider is a stub CredentialProviderClient recording the last request.
type fakeProvider struct {
	resp *credproviderpb.RequestSecretResponse
	err  error
	got  *credproviderpb.RequestSecretRequest
}

func (f *fakeProvider) RequestSecret(_ context.Context, req *credproviderpb.RequestSecretRequest, _ ...grpc.CallOption) (*credproviderpb.RequestSecretResponse, error) {
	f.got = req
	return f.resp, f.err
}

// bearerTokenResponse is a RequestSecretResponse carrying a bearer-token
// credential.
func bearerTokenResponse(token string) *credproviderpb.RequestSecretResponse {
	return &credproviderpb.RequestSecretResponse{BearerToken: []byte(token)}
}

// injectionHandler builds a handler whose actor's policy injects a credential
// for api.example.com, with provider as the credential provider (nil leaves
// injection off).
func injectionHandler(provider credproviderpb.CredentialProviderClient, providerClass string) *Handler {
	return New(&egressMockClient{actor: runningActor(), policy: credentialInjectionPolicySample("api.example.com")}, nil, 0, provider, providerClass)
}

// On the TLS-terminated MITM leg an allowed rule's credential is resolved and
// injected as an overwriting header, and the provider is asked for the policy's
// URI with the actor's SPIFFE identity as context.
func TestInjectionOnTLSLeg(t *testing.T) {
	provider := &fakeProvider{resp: bearerTokenResponse("s3cr3t\n")}
	h := injectionHandler(provider, injectionProviderClass)

	res, err := h.HandleRequestHeaders(context.Background(),
		innerMetadata(extproc.EgressTLSMITMFilterChainName, "GET", "api.example.com", nil))
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
	if got := provider.got.GetUri(); got != "substrate-secret://k8s/default/token" {
		t.Errorf("provider URI = %q", got)
	}
	if got := provider.got.GetContext().GetActorIdentity(); got != testActorSPIFFEID {
		t.Errorf("actor identity = %q, want %q", got, testActorSPIFFEID)
	}
}

// When injection cannot be performed — a cleartext leg, or no provider
// configured — the request is allowed through with no header added, and any
// provider is never dialed, because the secret must not go out over cleartext or
// block egress the policy allowed.
func TestInjectionSkippedAndPassedThrough(t *testing.T) {
	tests := []struct {
		name     string
		provider *fakeProvider // nil means no provider configured
		leg      string
	}{
		{
			name:     "cleartext leg skips injection",
			provider: &fakeProvider{resp: bearerTokenResponse("s3cr3t")},
			leg:      extproc.EgressCleartextFilterChainName,
		},
		{
			name:     "no provider configured skips injection",
			provider: nil,
			leg:      extproc.EgressTLSMITMFilterChainName,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var h *Handler
			if tc.provider == nil {
				h = injectionHandler(nil, injectionProviderClass)
			} else {
				h = injectionHandler(tc.provider, injectionProviderClass)
			}
			res, err := h.HandleRequestHeaders(context.Background(),
				innerMetadata(tc.leg, "GET", "api.example.com", nil))
			if err != nil {
				t.Fatalf("HandleRequestHeaders: %v", err)
			}
			if got := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders(); len(got) != 0 {
				t.Errorf("got %d injected headers, want 0 (injection should be skipped)", len(got))
			}
			if tc.provider != nil && tc.provider.got != nil {
				t.Error("provider was dialed; injection should be skipped without a callout")
			}
		})
	}
}

// Once injection is attempted on the TLS leg with a provider present, a failure
// to produce the promised credential fails closed rather than forwarding the
// request without it.
func TestInjectionDenials(t *testing.T) {
	tests := []struct {
		name          string
		provider      *fakeProvider
		providerClass string
		leg           string
		want          envoy_type.StatusCode
	}{
		{
			name:          "wrong provider class is refused",
			provider:      &fakeProvider{resp: bearerTokenResponse("s3cr3t")},
			providerClass: "vault", // policy URI is substrate-secret://k8s/...
			leg:           extproc.EgressTLSMITMFilterChainName,
			want:          envoy_type.StatusCode_InternalServerError,
		},
		{
			name:          "provider failure fails closed",
			provider:      &fakeProvider{err: errors.New("provider down")},
			providerClass: injectionProviderClass,
			leg:           extproc.EgressTLSMITMFilterChainName,
			want:          envoy_type.StatusCode_ServiceUnavailable,
		},
		{
			name:          "empty secret fails closed",
			provider:      &fakeProvider{resp: bearerTokenResponse("")},
			providerClass: injectionProviderClass,
			leg:           extproc.EgressTLSMITMFilterChainName,
			want:          envoy_type.StatusCode_ServiceUnavailable,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := injectionHandler(tc.provider, tc.providerClass)
			_, err := h.HandleRequestHeaders(context.Background(),
				innerMetadata(tc.leg, "GET", "api.example.com", nil))
			wantStatus(t, err, tc.want)
		})
	}
}
