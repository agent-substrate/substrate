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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/api/credprovider/v1"
	"go.ate.dev/substrate/cmd/atenet/internal/router/egress/tokenexchange"
	"go.ate.dev/substrate/cmd/atenet/internal/router/extproc"
)

// credentialInjectionPolicySample injects "authorization: Bearer <secret>" from
// ate-secret://k8s/default/token, so the provider name under test is "k8s".
const injectionProviderName = "k8s"

// fakeProvider is a stub CredentialProviderClient recording the last request.
type fakeProvider struct {
	resp *credprovider.FetchSecretResponse
	err  error
	got  *credprovider.FetchSecretRequest
}

func (f *fakeProvider) FetchSecret(_ context.Context, req *credprovider.FetchSecretRequest, _ ...grpc.CallOption) (*credprovider.FetchSecretResponse, error) {
	f.got = req
	return f.resp, f.err
}

// bearerTokenResponse is a FetchSecretResponse carrying a bearer-token
// credential as its opaque secret bytes.
func bearerTokenResponse(token string) *credprovider.FetchSecretResponse {
	return &credprovider.FetchSecretResponse{OpaqueBytes: []byte(token)}
}

func injectionHandlerFor(policy *ateapi.EgressPolicy, provider credprovider.CredentialProviderClient, providerName string) *Handler {
	return New(&egressMockClient{actor: runningActor(), policy: policy}, nil, 0, provider, providerName, nil)
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
		policy *ateapi.EgressPolicy
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
	jwtPolicy := &ateapi.EgressPolicy{Rules: []*ateapi.EgressRule{{Https: &ateapi.HttpsRule{
		Hostnames: []string{"api.example.com"},
		Effects: &ateapi.HttpRuleEffects{ReplaceHeaders: []*ateapi.CredentialHeader{{
			Header:   "authorization",
			ActorJwt: &ateapi.ActorJwtSource{Audiences: []string{"https://api.example.com"}},
		}}},
	}}}}
	for _, tc := range []struct {
		name     string
		policy   *ateapi.EgressPolicy
		provider *fakeProvider // nil means no provider configured
	}{
		{name: "provider configured", policy: credentialInjectionPolicySample("api.example.com"), provider: &fakeProvider{resp: bearerTokenResponse("s3cr3t")}},
		{name: "no provider configured", policy: credentialInjectionPolicySample("api.example.com")},
		{name: "actor JWT", policy: jwtPolicy, provider: &fakeProvider{resp: bearerTokenResponse("s3cr3t")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var provider credprovider.CredentialProviderClient
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
	policy := &ateapi.EgressPolicy{Rules: []*ateapi.EgressRule{{Https: &ateapi.HttpsRule{
		Hostnames: []string{"api.example.com"},
		Effects: &ateapi.HttpRuleEffects{ReplaceHeaders: []*ateapi.CredentialHeader{
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
		provider     credprovider.CredentialProviderClient
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

// blockingProvider never answers, like a provider stuck dialing
type blockingProvider struct{}

func (blockingProvider) FetchSecret(ctx context.Context, _ *credprovider.FetchSecretRequest, _ ...grpc.CallOption) (*credprovider.FetchSecretResponse, error) {
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

// a hung provider must be a retryable 503 before Envoy's ext_proc
// message_timeout turns it into a 504
func TestInjectionProviderHangs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := injectionHandlerFor(credentialInjectionPolicySample("api.example.com"), blockingProvider{}, injectionProviderName)
		start := time.Now()
		_, err := h.HandleRequestHeaders(context.Background(),
			withAuthorization(innerMetadata(extproc.EgressTLSMITMFilterChainName, "GET", "api.example.com", nil)))
		wantStatus(t, err, envoy_type.StatusCode_ServiceUnavailable)
		if took := time.Since(start); took >= 5*time.Second {
			t.Errorf("denied after %v, want under the 5s ext_proc message_timeout", took)
		}
	})
}

// actorJWTHeader replaces header with an actor JWT bound to api.example.com.
func actorJWTHeader(header string) *ateapi.CredentialHeader {
	return &ateapi.CredentialHeader{
		Header:   header,
		Prefix:   "Bearer ",
		ActorJwt: &ateapi.ActorJwtSource{Audiences: []string{"https://api.example.com"}, ExpirationSeconds: 900},
	}
}

// replaceHeadersPolicy is a rule for api.example.com on leg's protocol (https
// for the MITM leg, http for the cleartext one) that replaces headers with
// entries.
func replaceHeadersPolicy(leg string, entries ...*ateapi.CredentialHeader) *ateapi.EgressPolicy {
	hosts := []string{"api.example.com"}
	effects := &ateapi.HttpRuleEffects{ReplaceHeaders: entries}
	if leg == extproc.EgressCleartextFilterChainName {
		return &ateapi.EgressPolicy{Rules: []*ateapi.EgressRule{{Http: &ateapi.HttpRule{Hostnames: hosts, Effects: effects}}}}
	}
	return &ateapi.EgressPolicy{Rules: []*ateapi.EgressRule{{Https: &ateapi.HttpsRule{Hostnames: hosts, Effects: effects}}}}
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
				&ateapi.CredentialHeader{Header: "x-api-key", CredentialUri: "ate-secret://k8s/default/token"},
			)}
			h := New(client, nil, 0, nil, "", nil)

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
			want := &ateapi.MintActorJwtRequest{
				Actor:             &ateapi.ObjectRef{Atespace: testEgressAtespace, Name: testEgressActor},
				Audiences:         []string{"https://api.example.com"},
				ExpirationSeconds: 900,
			}
			if got := client.lastMint.Load(); !proto.Equal(got, want) {
				t.Errorf("MintActorJwt request = %v, want %v", got, want)
			}
		})
	}
}

func TestActorJWTInjectionSkippedWithoutHeader(t *testing.T) {
	client := &egressMockClient{actor: runningActor(), policy: replaceHeadersPolicy(extproc.EgressTLSMITMFilterChainName, actorJWTHeader("authorization"))}
	h := New(client, nil, 0, nil, "", nil)
	res, err := h.HandleRequestHeaders(context.Background(),
		innerMetadata(extproc.EgressTLSMITMFilterChainName, "GET", "api.example.com", nil))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if got := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders(); len(got) != 0 {
		t.Errorf("got %d injected headers, want 0", len(got))
	}
	if calls := client.mintCalls.Load(); calls != 0 {
		t.Errorf("MintActorJwt calls = %d, want 0", calls)
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
					h := New(mock, nil, 0, nil, "", nil)
					md := innerMetadata(leg, "GET", "api.example.com", nil)
					md.Headers[header] = "placeholder"
					_, err := h.HandleRequestHeaders(context.Background(), md)
					wantStatus(t, err, tc.want)
				})
			}
		})
	}
}

// tokenEndpoint starts a TLS token endpoint at localhost that answers every
// exchange with status and body. It returns the endpoint's token URL, a client
// that trusts it, and the number of exchanges it has served.
func tokenEndpoint(t *testing.T, status int, body string) (string, *tokenexchange.Client, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		DNSNames:              []string{"localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := tokenexchange.New(caFile)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return "https://localhost:" + u.Port() + "/token", client, &calls
}

func tokenExchangeHeader(header, tokenURL string) *ateapi.CredentialHeader {
	return &ateapi.CredentialHeader{
		Header:        header,
		Prefix:        "Bearer ",
		TokenExchange: &ateapi.TokenExchangeSource{TokenUrl: tokenURL, Audience: "sts.example.com"},
	}
}

// tokenExchangePolicy is replaceHeadersPolicy plus an https rule that allows
// tokenURL's host and port.
func tokenExchangePolicy(t *testing.T, leg, tokenURL string, entries ...*ateapi.CredentialHeader) *ateapi.EgressPolicy {
	t.Helper()
	u, err := url.Parse(tokenURL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	policy := replaceHeadersPolicy(leg, entries...)
	policy.Rules = append(policy.Rules, &ateapi.EgressRule{Https: &ateapi.HttpsRule{
		Hostnames: []string{u.Hostname()},
		Ports:     &ateapi.Ports{Numbers: []int32{int32(port)}},
	}})
	return policy
}

func TestTokenExchangeInjection(t *testing.T) {
	for _, leg := range requestLegs {
		t.Run(leg, func(t *testing.T) {
			tokenURL, tokenClient, _ := tokenEndpoint(t, http.StatusOK, `{"access_token":"exchanged","expires_in":900}`)
			client := &egressMockClient{actor: runningActor(), policy: tokenExchangePolicy(t, leg, tokenURL, tokenExchangeHeader("authorization", tokenURL))}
			h := New(client, nil, 0, nil, "", tokenClient)

			res, err := h.HandleRequestHeaders(context.Background(),
				withAuthorization(innerMetadata(leg, "GET", "api.example.com", nil)))
			if err != nil {
				t.Fatalf("HandleRequestHeaders: %v", err)
			}
			setHeaders := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders()
			if len(setHeaders) != 1 {
				t.Fatalf("got %d header mutations, want 1", len(setHeaders))
			}
			if got := string(setHeaders[0].GetHeader().GetRawValue()); got != "Bearer exchanged" {
				t.Errorf("header value = %q, want %q", got, "Bearer exchanged")
			}
			if got := setHeaders[0].GetAppendAction(); got != corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD {
				t.Errorf("append action = %v, want OVERWRITE_IF_EXISTS_OR_ADD", got)
			}
			want := &ateapi.MintActorJwtRequest{
				Actor:             &ateapi.ObjectRef{Atespace: testEgressAtespace, Name: testEgressActor},
				Audiences:         []string{"sts.example.com"},
				ExpirationSeconds: 300,
			}
			if got := client.lastMint.Load(); !proto.Equal(got, want) {
				t.Errorf("MintActorJwt request = %v, want %v", got, want)
			}
		})
	}
}

func TestTokenExchangeSkippedWithoutHeader(t *testing.T) {
	tokenURL, tokenClient, exchanges := tokenEndpoint(t, http.StatusOK, `{"access_token":"exchanged"}`)
	client := &egressMockClient{actor: runningActor(), policy: tokenExchangePolicy(t, extproc.EgressTLSMITMFilterChainName, tokenURL, tokenExchangeHeader("authorization", tokenURL))}
	h := New(client, nil, 0, nil, "", tokenClient)
	res, err := h.HandleRequestHeaders(context.Background(),
		innerMetadata(extproc.EgressTLSMITMFilterChainName, "GET", "api.example.com", nil))
	if err != nil {
		t.Fatalf("HandleRequestHeaders: %v", err)
	}
	if got := res.Response.GetResponse().GetHeaderMutation().GetSetHeaders(); len(got) != 0 {
		t.Errorf("got %d injected headers, want 0", len(got))
	}
	if n := exchanges.Load(); n != 0 {
		t.Errorf("%d exchanges, want 0", n)
	}
}

func TestTokenExchangeDenials(t *testing.T) {
	const issued = `{"access_token":"exchanged"}`
	tests := []struct {
		name       string
		status     int
		body       string
		mintErr    error
		notAllowed bool // no rule allows the token URL
		want       envoy_type.StatusCode
	}{
		{name: "token URL no rule allows", status: http.StatusOK, body: issued, notAllowed: true, want: envoy_type.StatusCode_InternalServerError},
		{name: "endpoint refuses the actor", status: http.StatusBadRequest, body: `{"error":"invalid_grant"}`, want: envoy_type.StatusCode_Forbidden},
		{name: "endpoint unavailable", status: http.StatusServiceUnavailable, want: envoy_type.StatusCode_ServiceUnavailable},
		{name: "endpoint rejects the request", status: http.StatusBadRequest, body: `{"error":"invalid_request"}`, want: envoy_type.StatusCode_InternalServerError},
		{name: "actor deleted", status: http.StatusOK, body: issued, mintErr: status.Error(codes.NotFound, "actor not found"), want: envoy_type.StatusCode_Forbidden},
		{name: "ateapi unavailable", status: http.StatusOK, body: issued, mintErr: status.Error(codes.Unavailable, "ateapi is down"), want: envoy_type.StatusCode_ServiceUnavailable},
		{name: "gateway may not mint", status: http.StatusOK, body: issued, mintErr: status.Error(codes.PermissionDenied, "denied"), want: envoy_type.StatusCode_InternalServerError},
	}
	for _, leg := range requestLegs {
		t.Run(leg, func(t *testing.T) {
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					tokenURL, tokenClient, exchanges := tokenEndpoint(t, tc.status, tc.body)
					entry := tokenExchangeHeader("authorization", tokenURL)
					policy := tokenExchangePolicy(t, leg, tokenURL, entry)
					if tc.notAllowed {
						policy = replaceHeadersPolicy(leg, entry)
					}
					h := New(&egressMockClient{actor: runningActor(), policy: policy, mintErr: tc.mintErr}, nil, 0, nil, "", tokenClient)
					_, err := h.HandleRequestHeaders(context.Background(),
						withAuthorization(innerMetadata(leg, "GET", "api.example.com", nil)))
					wantStatus(t, err, tc.want)
					if tc.notAllowed && exchanges.Load() != 0 {
						t.Errorf("%d exchanges at a token URL no rule allows, want 0", exchanges.Load())
					}
				})
			}
		})
	}
}
