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
	"net"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	// unreachable leaves the call's peer unset, as gRPC does for a call that
	// never got a stream to the provider.
	unreachable bool
	got         *credproviderpb.FetchSecretRequest
}

func (f *fakeProvider) FetchSecret(_ context.Context, req *credproviderpb.FetchSecretRequest, opts ...grpc.CallOption) (*credproviderpb.FetchSecretResponse, error) {
	f.got = req
	if !f.unreachable {
		for _, opt := range opts {
			if p, ok := opt.(grpc.PeerCallOption); ok {
				p.PeerAddr.Addr = &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 50051}
			}
		}
	}
	return f.resp, f.err
}

// bearerTokenResponse is a FetchSecretResponse carrying a bearer-token
// credential as its opaque secret bytes.
func bearerTokenResponse(token string) *credproviderpb.FetchSecretResponse {
	return &credproviderpb.FetchSecretResponse{OpaqueBytes: []byte(token)}
}

// injectionHandler builds a handler whose actor's policy injects a credential
// for HTTPS to api.example.com, with provider as the credential provider (nil
// configures none).
func injectionHandler(provider credproviderpb.CredentialProviderClient, providerName string) *Handler {
	return injectionHandlerFor(credentialInjectionPolicySample("api.example.com"), provider, providerName)
}

func injectionHandlerFor(policy *ateapipb.EgressPolicy, provider credproviderpb.CredentialProviderClient, providerName string) *Handler {
	return New(&egressMockClient{actor: runningActor(), policy: policy}, nil, 0, provider, providerName)
}

// On the TLS-terminated MITM leg an allowed rule's credential is resolved and
// injected as an overwriting header, and the provider is asked for the policy's
// URI with the actor's SPIFFE identity as context.
func TestInjectionOnTLSLeg(t *testing.T) {
	provider := &fakeProvider{resp: bearerTokenResponse("s3cr3t\n")}
	h := injectionHandler(provider, injectionProviderName)

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
	if got := provider.got.GetUri(); got != "ate-secret://k8s/default/token" {
		t.Errorf("provider URI = %q", got)
	}
	wantActorSPIFFEID := "spiffe://substrate-actor.local/actor/default/my-actor"
	if got := provider.got.GetActorSpiffeId(); got != wantActorSPIFFEID {
		t.Errorf("actor identity = %q, want %q", got, wantActorSPIFFEID)
	}
}

// On a cleartext leg the request is allowed through with no header added, and
// the provider is never dialed, because the secret must not go out over
// cleartext.
func TestInjectionSkippedOnCleartextLeg(t *testing.T) {
	provider := &fakeProvider{resp: bearerTokenResponse("s3cr3t")}
	h := injectionHandlerFor(cleartextInjectionPolicy("api.example.com"), provider, injectionProviderName)
	res, err := h.HandleRequestHeaders(context.Background(),
		innerMetadata(extproc.EgressCleartextFilterChainName, "GET", "api.example.com", nil))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if got := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders(); len(got) != 0 {
		t.Errorf("got %d injected headers, want 0 (injection should be skipped)", len(got))
	}
	if provider.got != nil {
		t.Error("provider was dialed; injection should be skipped without a callout")
	}
}

// On the TLS leg, a failure to produce the promised credential fails closed
// rather than forwarding the request without it.
func TestInjectionDenials(t *testing.T) {
	tests := []struct {
		name         string
		provider     *fakeProvider // nil means no provider configured
		providerName string
		leg          string
		want         envoy_type.StatusCode
		wantBody     string // empty means deniedBody
	}{
		{
			// The gateway's misconfiguration, so the body says so.
			name:         "no provider configured",
			providerName: injectionProviderName,
			leg:          extproc.EgressTLSMITMFilterChainName,
			want:         envoy_type.StatusCode_InternalServerError,
			wantBody:     noProviderBody,
		},
		{
			name:         "unreachable provider",
			provider:     &fakeProvider{err: status.Error(codes.Unavailable, "connection refused"), unreachable: true},
			providerName: injectionProviderName,
			leg:          extproc.EgressTLSMITMFilterChainName,
			want:         envoy_type.StatusCode_InternalServerError,
			wantBody:     providerUnavailableBody,
		},
		{
			name:         "credential URI for another provider is refused",
			provider:     &fakeProvider{resp: bearerTokenResponse("s3cr3t")},
			providerName: "vault", // policy URI is ate-secret://k8s/...
			leg:          extproc.EgressTLSMITMFilterChainName,
			want:         envoy_type.StatusCode_InternalServerError,
		},
		{
			// A provider that answers with a transient failure of its own, such
			// as an outage of its secret store, is retryable.
			name:         "provider answers unavailable fails closed as retryable",
			provider:     &fakeProvider{err: status.Error(codes.Unavailable, "secret store down")},
			providerName: injectionProviderName,
			leg:          extproc.EgressTLSMITMFilterChainName,
			want:         envoy_type.StatusCode_ServiceUnavailable,
		},
		{
			// A secret the provider does not hold cannot appear on retry.
			name:         "secret not found denies as non-retryable",
			provider:     &fakeProvider{err: status.Error(codes.NotFound, "no such secret")},
			providerName: injectionProviderName,
			leg:          extproc.EgressTLSMITMFilterChainName,
			want:         envoy_type.StatusCode_Forbidden,
		},
		{
			name:         "provider refuses the actor denies as non-retryable",
			provider:     &fakeProvider{err: status.Error(codes.PermissionDenied, "atespace not allowed")},
			providerName: injectionProviderName,
			leg:          extproc.EgressTLSMITMFilterChainName,
			want:         envoy_type.StatusCode_Forbidden,
		},
		{
			// An unclassified error denies rather than inviting retries.
			name:         "unexpected provider error denies",
			provider:     &fakeProvider{err: errors.New("provider down")},
			providerName: injectionProviderName,
			leg:          extproc.EgressTLSMITMFilterChainName,
			want:         envoy_type.StatusCode_Forbidden,
		},
		{
			name:         "empty secret fails closed",
			provider:     &fakeProvider{resp: bearerTokenResponse("")},
			providerName: injectionProviderName,
			leg:          extproc.EgressTLSMITMFilterChainName,
			want:         envoy_type.StatusCode_ServiceUnavailable,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var h *Handler
			if tc.provider == nil {
				h = injectionHandler(nil, tc.providerName)
			} else {
				h = injectionHandler(tc.provider, tc.providerName)
			}
			_, err := h.HandleRequestHeaders(context.Background(),
				innerMetadata(tc.leg, "GET", "api.example.com", nil))
			wantStatus(t, err, tc.want)
			wantBody := tc.wantBody
			if wantBody == "" {
				wantBody = deniedBody
			}
			wantDenialBody(t, err, wantBody)
		})
	}
}

// The reached/unreached split rests on how grpc-go fills in the peer, so pin it
// against a real client: a provider nobody serves is a 500 naming the provider,
// while a provider that answers Unavailable itself stays a retryable 503.
func TestInjectionProviderReachability(t *testing.T) {
	t.Run("nothing listening", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		addr := lis.Addr().String()
		lis.Close()

		err = fetchThroughProvider(t, addr)
		wantStatus(t, err, envoy_type.StatusCode_InternalServerError)
		wantDenialBody(t, err, providerUnavailableBody)
	})

	t.Run("provider answers unavailable", func(t *testing.T) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		srv := grpc.NewServer()
		credproviderpb.RegisterCredentialProviderServer(srv, unavailableProvider{})
		go srv.Serve(lis)
		t.Cleanup(srv.Stop)

		err = fetchThroughProvider(t, lis.Addr().String())
		wantStatus(t, err, envoy_type.StatusCode_ServiceUnavailable)
		wantDenialBody(t, err, deniedBody)
	})
}

// unavailableProvider is a credential provider whose secret store is down.
type unavailableProvider struct {
	credproviderpb.UnimplementedCredentialProviderServer
}

func (unavailableProvider) FetchSecret(context.Context, *credproviderpb.FetchSecretRequest) (*credproviderpb.FetchSecretResponse, error) {
	return nil, status.Error(codes.Unavailable, "secret store down")
}

// fetchThroughProvider runs an injecting request through a handler whose
// provider is dialed at addr, and returns the handler's error.
func fetchThroughProvider(t *testing.T, addr string) error {
	t.Helper()
	conn, err := DialProvider(t.Context(), ProviderDialConfig{Address: addr, Insecure: true})
	if err != nil {
		t.Fatalf("DialProvider: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	h := injectionHandler(credproviderpb.NewCredentialProviderClient(conn), injectionProviderName)
	_, err = h.HandleRequestHeaders(ctx, innerMetadata(extproc.EgressTLSMITMFilterChainName, "GET", "api.example.com", nil))
	return err
}

// wantDenialBody checks the body a denial answers the actor with.
func wantDenialBody(t *testing.T, err error, want string) {
	t.Helper()
	var re *extproc.ReqError
	if !errors.As(err, &re) {
		t.Fatalf("error %v is not a *extproc.ReqError", err)
	}
	if re.Msg != want {
		t.Errorf("body = %q, want %q", re.Msg, want)
	}
}

func TestActorJWTInjectionNotImplemented(t *testing.T) {
	effects := &ateapipb.HttpRuleEffects{ReplaceHeaders: []*ateapipb.CredentialHeader{{
		Header:   "authorization",
		Prefix:   "Bearer ",
		ActorJwt: &ateapipb.ActorJWTSource{Audiences: []string{"https://api.example.com"}, ExpirationSeconds: 900},
	}}}
	httpsPolicy := &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{Https: &ateapipb.HTTPSRule{Hostnames: []string{"api.example.com"}, Effects: effects}}}}
	httpPolicy := &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{Http: &ateapipb.HTTPRule{Hostnames: []string{"api.example.com"}, Effects: effects}}}}
	tests := []struct {
		name     string
		leg      string
		policy   *ateapipb.EgressPolicy
		provider *fakeProvider // nil means no provider configured
		wantDeny bool
	}{{
		name:     "https rule",
		leg:      extproc.EgressTLSMITMFilterChainName,
		policy:   httpsPolicy,
		provider: &fakeProvider{resp: bearerTokenResponse("s3cr3t")},
		wantDeny: true,
	}, {
		name:     "https rule without a provider",
		leg:      extproc.EgressTLSMITMFilterChainName,
		policy:   httpsPolicy,
		wantDeny: true,
	}, {
		name:     "http rule skips injection",
		leg:      extproc.EgressCleartextFilterChainName,
		policy:   httpPolicy,
		provider: &fakeProvider{resp: bearerTokenResponse("s3cr3t")},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var h *Handler
			if tc.provider == nil {
				h = injectionHandlerFor(tc.policy, nil, injectionProviderName)
			} else {
				h = injectionHandlerFor(tc.policy, tc.provider, injectionProviderName)
			}
			res, err := h.HandleRequestHeaders(context.Background(),
				innerMetadata(tc.leg, "GET", "api.example.com", nil))
			if tc.wantDeny {
				wantStatus(t, err, envoy_type.StatusCode_NotImplemented)
			} else if err != nil {
				t.Fatalf("HandleRequestHeaders: %v", err)
			} else if got := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders(); len(got) != 0 {
				t.Errorf("got %d injected headers, want 0", len(got))
			}
			if tc.provider != nil && tc.provider.got != nil {
				t.Errorf("provider was asked for %v, want no fetch", tc.provider.got)
			}
		})
	}
}
