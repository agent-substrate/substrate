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

package egressinject

import (
	"context"
	"errors"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

// fakeProvider is a stub CredentialProviderClient recording the last request.
type fakeProvider struct {
	resp   *credproviderpb.RequestSecretResponse
	err    error
	gotReq *credproviderpb.RequestSecretRequest
}

func (f *fakeProvider) RequestSecret(_ context.Context, req *credproviderpb.RequestSecretRequest, _ ...grpc.CallOption) (*credproviderpb.RequestSecretResponse, error) {
	f.gotReq = req
	return f.resp, f.err
}

// fakePolicyClient is a stub ateapi policy client: it returns the policy keyed
// by the requested actor, NotFound when the actor is absent, or a fixed error.
type fakePolicyClient struct {
	policies map[string]*ateapipb.EgressPolicy // keyed by "atespace/actor"
	err      error
	gotReq   *ateapipb.GetActorEgressPolicyRequest
}

func (f *fakePolicyClient) GetActorEgressPolicy(_ context.Context, in *ateapipb.GetActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	f.gotReq = in
	if f.err != nil {
		return nil, f.err
	}
	p, ok := f.policies[in.GetActor().GetAtespace()+"/"+in.GetActor().GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no egress policy")
	}
	return p, nil
}

// sampleAPIClient serves the sample policy for team-a/my-actor.
func sampleAPIClient() *fakePolicyClient {
	return &fakePolicyClient{policies: map[string]*ateapipb.EgressPolicy{
		"team-a/my-actor": sampleEgressPolicy(),
	}}
}

const testActorURI = "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor"

// testProviderName is the provider class the handler tests configure; the sample
// policy's credential URIs are all of this class.
const testProviderName = "kubernetes.io"

func metadataFor(t *testing.T, identity, host string) *extproc.RequestMetadata {
	t.Helper()
	// Default to https so the HTTPS path is what unscoped tests exercise.
	return metadataForScheme(t, identity, host, "https")
}

func metadataForScheme(t *testing.T, identity, host, scheme string) *extproc.RequestMetadata {
	t.Helper()
	st, err := structpb.NewStruct(map[string]any{actorIdentityAttribute: identity})
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	md := &extproc.RequestMetadata{
		Host:       host,
		Attributes: map[string]*structpb.Struct{"efp": st},
	}
	if scheme != "" {
		md.Headers = map[string]string{schemeHeader: scheme}
	}
	return md
}

func TestHandleRequestHeadersInjects(t *testing.T) {
	provider := &fakeProvider{resp: &credproviderpb.RequestSecretResponse{Secret: []byte("s3cr3t")}}
	h := New(sampleAPIClient(), provider, testProviderName)

	res, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com:443"))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}

	setHeaders := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders()
	if len(setHeaders) != 1 {
		t.Fatalf("got %d header mutations, want 1", len(setHeaders))
	}
	h0 := setHeaders[0]
	if got := h0.GetHeader().GetKey(); got != "Authorization" {
		t.Errorf("header key = %q, want Authorization", got)
	}
	if got := string(h0.GetHeader().GetRawValue()); got != "Bearer s3cr3t" {
		t.Errorf("header value = %q, want %q", got, "Bearer s3cr3t")
	}
	if h0.GetAppendAction() != corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD {
		t.Errorf("append action = %v, want OVERWRITE_IF_EXISTS_OR_ADD", h0.GetAppendAction())
	}

	// The provider was asked for the policy's URI with the attested context.
	if got := provider.gotReq.GetUri(); got != "substrate-secret://kubernetes.io/team-secrets/ns1/example-api" {
		t.Errorf("provider URI = %q", got)
	}
	if got := provider.gotReq.GetContext().GetActorIdentity(); got != testActorURI {
		t.Errorf("actor identity = %q", got)
	}
}

func TestHandleRequestHeadersFetchesPolicyForActor(t *testing.T) {
	api := sampleAPIClient()
	h := New(api, &fakeProvider{resp: &credproviderpb.RequestSecretResponse{Secret: []byte("s3cr3t")}}, testProviderName)

	if _, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com")); err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if got := api.gotReq.GetActor(); got.GetAtespace() != "team-a" || got.GetName() != "my-actor" {
		t.Errorf("GetActorEgressPolicy actor = %+v, want team-a/my-actor", got)
	}
}

func TestHandleRequestHeadersCleartextDenied(t *testing.T) {
	provider := &fakeProvider{resp: &credproviderpb.RequestSecretResponse{Secret: []byte("s3cr3t")}}
	h := New(sampleAPIClient(), provider, testProviderName)

	// A matched rule carries an injection; the API has no cleartext opt-in, so
	// an http request is always refused before the credential is fetched.
	_, err := h.HandleRequestHeaders(context.Background(), metadataForScheme(t, testActorURI, "api.example.com", "http"))
	assertReqErrCode(t, err, envoy_type.StatusCode_Forbidden)
	if provider.gotReq != nil {
		t.Error("provider was called for a cleartext request that should have been refused")
	}
}

func TestHandleRequestHeadersMissingSchemeDenied(t *testing.T) {
	provider := &fakeProvider{resp: &credproviderpb.RequestSecretResponse{Secret: []byte("s3cr3t")}}
	h := New(sampleAPIClient(), provider, testProviderName)

	// An absent scheme must fail closed, not be treated as https.
	_, err := h.HandleRequestHeaders(context.Background(), metadataForScheme(t, testActorURI, "api.example.com", ""))
	assertReqErrCode(t, err, envoy_type.StatusCode_Forbidden)
	if provider.gotReq != nil {
		t.Error("provider was called for a request with no scheme")
	}
}

func TestHandleRequestHeadersHTTPSInjects(t *testing.T) {
	provider := &fakeProvider{resp: &credproviderpb.RequestSecretResponse{Secret: []byte("s3cr3t")}}
	h := New(sampleAPIClient(), provider, testProviderName)

	res, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if setHeaders := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders(); len(setHeaders) != 1 {
		t.Fatalf("got %d header mutations, want 1", len(setHeaders))
	}
}

func TestHandleRequestHeadersMatchedNoInjectionPassesThrough(t *testing.T) {
	// A rule matches but injects nothing: the request passes through unchanged,
	// over cleartext too (no secret leaves the pod).
	api := &fakePolicyClient{policies: map[string]*ateapipb.EgressPolicy{
		"team-a/my-actor": {Rules: []*ateapipb.EgressRule{{
			Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}},
		}}},
	}}
	provider := &fakeProvider{}
	h := New(api, provider, testProviderName)

	res, err := h.HandleRequestHeaders(context.Background(), metadataForScheme(t, testActorURI, "api.example.com", "http"))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if muts := res.Response.GetResponse().GetHeaderMutation(); muts != nil {
		t.Errorf("got header mutation %+v, want none", muts)
	}
	if provider.gotReq != nil {
		t.Error("provider was called for a rule with no injection")
	}
}

func TestHandleRequestHeadersNoMatchPassesThrough(t *testing.T) {
	// No rule matches the host: the injector makes no allow/deny decision, so the
	// request passes through unchanged with nothing injected.
	provider := &fakeProvider{}
	h := New(sampleAPIClient(), provider, testProviderName)

	res, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "other.example.com"))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if muts := res.Response.GetResponse().GetHeaderMutation(); muts != nil {
		t.Errorf("got header mutation %+v, want none", muts)
	}
	if provider.gotReq != nil {
		t.Error("provider was called on a non-matching request")
	}
}

func TestHandleRequestHeadersNoPolicyForActorPassesThrough(t *testing.T) {
	// The API has no policy for this actor (NotFound): nothing to inject, so the
	// request passes through unchanged.
	provider := &fakeProvider{}
	h := New(&fakePolicyClient{}, provider, testProviderName)

	res, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if muts := res.Response.GetResponse().GetHeaderMutation(); muts != nil {
		t.Errorf("got header mutation %+v, want none", muts)
	}
	if provider.gotReq != nil {
		t.Error("provider was called though the actor has no egress policy")
	}
}

func TestHandleRequestHeadersPolicyFetchFailsClosed(t *testing.T) {
	provider := &fakeProvider{}
	// A transport-level failure (not NotFound) must fail closed: we cannot tell
	// whether a credential was required, so the request must not proceed.
	h := New(&fakePolicyClient{err: status.Error(codes.Unavailable, "ateapi down")}, provider, testProviderName)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_ServiceUnavailable)
	if provider.gotReq != nil {
		t.Error("provider was called though the egress policy could not be fetched")
	}
}

func TestHandleRequestHeadersProviderFailsClosed(t *testing.T) {
	provider := &fakeProvider{err: errors.New("provider down")}
	h := New(sampleAPIClient(), provider, testProviderName)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_ServiceUnavailable)
}

func TestHandleRequestHeadersEmptySecretFailsClosed(t *testing.T) {
	// An empty credential must not go upstream as a bare "Bearer ".
	provider := &fakeProvider{resp: &credproviderpb.RequestSecretResponse{Secret: []byte("")}}
	h := New(sampleAPIClient(), provider, testProviderName)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_ServiceUnavailable)
}

func TestHandleRequestHeadersTrailingNewlineTrimmed(t *testing.T) {
	// A Secret created from a file commonly carries a trailing newline; it must
	// not end up in the header value (Envoy would reject it).
	provider := &fakeProvider{resp: &credproviderpb.RequestSecretResponse{Secret: []byte("s3cr3t\n")}}
	h := New(sampleAPIClient(), provider, testProviderName)

	res, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	setHeaders := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders()
	if len(setHeaders) != 1 || string(setHeaders[0].GetHeader().GetRawValue()) != "Bearer s3cr3t" {
		t.Fatalf("header value = %q, want %q", string(setHeaders[0].GetHeader().GetRawValue()), "Bearer s3cr3t")
	}
}

func TestHandleRequestHeadersControlCharSecretFailsClosed(t *testing.T) {
	// An embedded CR/LF would enable header injection; fail closed.
	provider := &fakeProvider{resp: &credproviderpb.RequestSecretResponse{Secret: []byte("s3\r\ncr3t")}}
	h := New(sampleAPIClient(), provider, testProviderName)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_ServiceUnavailable)
}

func TestHandleRequestHeadersWrongProviderClassFailsClosed(t *testing.T) {
	// The policy's credential URI targets a provider class this injector does not
	// serve; it must fail closed rather than dial the wrong provider.
	provider := &fakeProvider{resp: &credproviderpb.RequestSecretResponse{Secret: []byte("s3cr3t")}}
	api := &fakePolicyClient{policies: map[string]*ateapipb.EgressPolicy{
		"team-a/my-actor": {Rules: []*ateapipb.EgressRule{{
			Hostnames: &ateapipb.HostnameRule{
				Patterns: []string{"api.example.com"},
				Effects: &ateapipb.EgressRuleEffects{InjectStaticHeaders: []*ateapipb.CredentialHeaderInjection{{
					Header: "Authorization", Prefix: "Bearer ", CredentialUri: "substrate-secret://vault.hashicorp.com/p/ns/s",
				}}},
			},
		}}},
	}}
	h := New(api, provider, testProviderName)

	_, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, testActorURI, "api.example.com"))
	assertReqErrCode(t, err, envoy_type.StatusCode_InternalServerError)
	if provider.gotReq != nil {
		t.Error("provider was called for a URI of an unserved class")
	}
}

func TestSanitizeSecret(t *testing.T) {
	tests := []struct {
		name    string
		in      []byte
		want    string
		wantErr bool
	}{
		{name: "plain", in: []byte("tok"), want: "tok"},
		{name: "trailing newline trimmed", in: []byte("tok\n"), want: "tok"},
		{name: "trailing crlf trimmed", in: []byte("tok\r\n"), want: "tok"},
		{name: "empty", in: []byte(""), wantErr: true},
		{name: "only newline", in: []byte("\n"), wantErr: true},
		{name: "embedded lf", in: []byte("to\nk"), wantErr: true},
		{name: "embedded cr", in: []byte("to\rk"), wantErr: true},
		{name: "embedded tab", in: []byte("to\tk"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sanitizeSecret(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("sanitizeSecret(%q) = %q, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("sanitizeSecret(%q) unexpected error: %v", tc.in, err)
			}
			if string(got) != tc.want {
				t.Errorf("sanitizeSecret(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestHandleRequestHeadersBadIdentityPassesThrough(t *testing.T) {
	// An unusable identity means no policy can be fetched, so there is nothing to
	// inject and the request passes through unchanged.
	provider := &fakeProvider{}
	h := New(sampleAPIClient(), provider, testProviderName)

	res, err := h.HandleRequestHeaders(context.Background(), metadataFor(t, "not-a-spiffe-uri", "api.example.com"))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if muts := res.Response.GetResponse().GetHeaderMutation(); muts != nil {
		t.Errorf("got header mutation %+v, want none", muts)
	}
	if provider.gotReq != nil {
		t.Error("provider was called despite an unusable identity")
	}
}

func assertReqErrCode(t *testing.T, err error, want envoy_type.StatusCode) {
	t.Helper()
	var reqErr *extproc.ReqError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error %v is not a *extproc.ReqError", err)
	}
	if reqErr.StatusCode != int(want) {
		t.Errorf("status code = %d, want %d", reqErr.StatusCode, int(want))
	}
}
