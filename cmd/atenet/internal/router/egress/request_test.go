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
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	// testActorSPIFFEID is the identity filter state the CONNECT chain shares.
	testActorSPIFFEID = "spiffe://substrate-actor.local/atespace/default/actor/my-actor"
	// testOriginalDst is the IP:port the test actor's kernel dialed.
	testOriginalDst = "93.184.216.34:443"
)

func ipBlocksPolicy(cidrs ...string) *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
		IpBlocks: &ateapipb.IPBlockRule{Cidrs: cidrs},
	}}}
}

func credentialInjectionPolicySample(pattern string) *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
		Hostnames: &ateapipb.HostnameRule{
			Patterns: []string{pattern},
			Effects: &ateapipb.EgressRuleEffects{InjectStaticHeaders: []*ateapipb.CredentialHeaderInjection{{
				Header: "authorization", Prefix: "Bearer ", CredentialUri: "substrate-secret://k8s/default/token",
			}}},
		},
	}}}
}

// policyHandler builds a Handler for an actor whose policy is policy (nil
// means none) with the cache disabled, so each callout sees the mock as is.
func policyHandler(policy *ateapipb.EgressPolicy) *Handler {
	return New(&egressMockClient{actor: runningActor(), policy: policy}, nil, 0)
}

// innerMetadata builds an inner chain's callout: pseudo-headers plus the
// attributes that chain requests. attrs overrides the defaults; an empty value
// deletes one.
func innerMetadata(leg, method, authority string, attrs map[string]string) *extproc.RequestMetadata {
	fields := map[string]string{
		extproc.FilterChainNameAttribute:          leg,
		extproc.ActorIdentityFilterStateAttribute: testActorSPIFFEID,
		extproc.OriginalDstAttribute:              testOriginalDst,
	}
	for k, v := range attrs {
		if v == "" {
			delete(fields, k)
			continue
		}
		fields[k] = v
	}
	values := map[string]*structpb.Value{}
	for k, v := range fields {
		values[k] = structpb.NewStringValue(v)
	}
	return extproc.NewRequestMetadata([]*corev3.HeaderValue{
		{Key: ":method", RawValue: []byte(method)},
		{Key: ":authority", RawValue: []byte(authority)},
		{Key: ":path", RawValue: []byte("/v1/things?secret=1")},
	}, map[string]*structpb.Struct{"envoy.filters.http.ext_proc": {Fields: values}})
}

func requestMetadata(authority string) *extproc.RequestMetadata {
	return innerMetadata(extproc.EgressCleartextFilterChainName, "GET", authority, nil)
}

func wantAllowed(t *testing.T, res extproc.Result, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("HandleRequestHeaders() error = %v, want an allow", err)
	}
	if res.Response == nil {
		t.Fatal("HandleRequestHeaders() allowed without a response")
	}
}

func TestHandleRequestHeadersRefusesUnknownFilterChain(t *testing.T) {
	h := policyHandler(allowAllPolicy())
	_, err := h.HandleRequestHeaders(context.Background(), innerMetadata("some_other_chain", "GET", "example.com", nil))
	wantStatus(t, err, envoy_type.StatusCode_NotFound)
}

// The request legs authorize the request's own Host on every callout: that is
// the name the gateway resolves and dials.
func TestRequestLegAuthorizesHost(t *testing.T) {
	tests := []struct {
		name      string
		policy    *ateapipb.EgressPolicy
		authority string
		want      envoy_type.StatusCode // 0 means allowed
	}{
		{name: "exact hostname", policy: hostnamesPolicy("api.example.com"), authority: "api.example.com"},
		{name: "hostname with port", policy: hostnamesPolicy("api.example.com"), authority: "api.example.com:8443"},
		{name: "hostname case folded", policy: hostnamesPolicy("api.example.com"), authority: "API.Example.com"},
		{name: "hostname with trailing dot", policy: hostnamesPolicy("api.example.com"), authority: "api.example.com."},
		{name: "wildcard hostname", policy: hostnamesPolicy("*.example.com"), authority: "api.example.com"},
		{name: "all rule", policy: allowAllPolicy(), authority: "anything.example"},
		{name: "ip literal host in ip block", policy: ipBlocksPolicy("203.0.113.0/24"), authority: "203.0.113.9:8080"},
		{name: "ipv6 literal host in ip block", policy: ipBlocksPolicy("2001:db8::/32"), authority: "[2001:db8::7]:443"},
		{name: "other hostname", policy: hostnamesPolicy("api.example.com"), authority: "evil.example", want: envoy_type.StatusCode_Forbidden},
		{name: "wildcard does not match the apex", policy: hostnamesPolicy("*.example.com"), authority: "example.com", want: envoy_type.StatusCode_Forbidden},
		{name: "wildcard does not match two labels", policy: hostnamesPolicy("*.example.com"), authority: "a.b.example.com", want: envoy_type.StatusCode_Forbidden},
		{name: "ip literal host with hostname policy", policy: hostnamesPolicy("api.example.com"), authority: "93.184.216.34", want: envoy_type.StatusCode_Forbidden},
		// The tunnel's original destination is inside the block, but the
		// request names a hostname, and that hostname is what gets dialed.
		{name: "hostname host with ip block policy ignores the original destination", policy: ipBlocksPolicy("93.184.216.0/24"), authority: "evil.example", want: envoy_type.StatusCode_Forbidden},
		{name: "ip literal host outside the block", policy: ipBlocksPolicy("203.0.113.0/24"), authority: "198.51.100.1", want: envoy_type.StatusCode_Forbidden},
		{name: "unparseable host", policy: allowAllPolicy(), authority: "exa mple.com", want: envoy_type.StatusCode_Forbidden},
		{name: "empty host", policy: allowAllPolicy(), authority: "", want: envoy_type.StatusCode_Forbidden},
		{name: "matching rule requires injection", policy: credentialInjectionPolicySample("api.example.com"), authority: "api.example.com", want: envoy_type.StatusCode_NotImplemented},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := policyHandler(tc.policy)
			res, err := h.HandleRequestHeaders(context.Background(), requestMetadata(tc.authority))
			if tc.want == 0 {
				wantAllowed(t, res, err)
				return
			}
			wantStatus(t, err, tc.want)
		})
	}
}

func TestRequestLegServesBothDecryptedChains(t *testing.T) {
	h := policyHandler(hostnamesPolicy("api.example.com"))
	for _, leg := range []string{extproc.EgressCleartextFilterChainName, extproc.EgressTLSMITMFilterChainName} {
		res, err := h.HandleRequestHeaders(context.Background(), innerMetadata(leg, "POST", "api.example.com", nil))
		wantAllowed(t, res, err)
	}
}

// Without the identity the outer chain shares, a request cannot be attributed
// to any actor and is refused.
func TestRequestLegRequiresIdentity(t *testing.T) {
	h := policyHandler(allowAllPolicy())
	for name, identity := range map[string]string{
		"absent":               "",
		"not a spiffe id":      "api.example.com",
		"another trust domain": "spiffe://cluster.local/ns/default/sa/thing",
		"truncated":            "spiffe://substrate-actor.local/atespace/default",
	} {
		t.Run(name, func(t *testing.T) {
			md := innerMetadata(extproc.EgressCleartextFilterChainName, "GET", "api.example.com",
				map[string]string{extproc.ActorIdentityFilterStateAttribute: identity})
			_, err := h.HandleRequestHeaders(context.Background(), md)
			wantStatus(t, err, envoy_type.StatusCode_Forbidden)
		})
	}
}

func TestRequestLegPolicyLookup(t *testing.T) {
	tests := []struct {
		name   string
		client *egressMockClient
		want   envoy_type.StatusCode
	}{
		{name: "no policy", client: &egressMockClient{}, want: envoy_type.StatusCode_Forbidden},
		{name: "policy with no rules", client: &egressMockClient{policy: &ateapipb.EgressPolicy{}}, want: envoy_type.StatusCode_Forbidden},
		{name: "control plane unavailable", client: &egressMockClient{policyErr: status.Error(codes.Unavailable, "down")}, want: envoy_type.StatusCode_ServiceUnavailable},
		{name: "control plane refuses", client: &egressMockClient{policyErr: status.Error(codes.PermissionDenied, "no")}, want: envoy_type.StatusCode_ServiceUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := New(tc.client, nil, 0)
			_, err := h.HandleRequestHeaders(context.Background(), requestMetadata("api.example.com"))
			wantStatus(t, err, tc.want)
		})
	}
}

// The CONNECT leg refuses to open a tunnel for an actor with nothing that
// could be allowed through it, and warms the cache for the requests inside.
func TestConnectLegRequiresAPolicy(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, actorCertOptions{})

	tests := []struct {
		name   string
		client *egressMockClient
		want   envoy_type.StatusCode // 0 means allowed
	}{
		{name: "policy present", client: &egressMockClient{actor: runningActor(), policy: allowAllPolicy()}},
		{name: "hostname-only policy still opens the tunnel", client: &egressMockClient{actor: runningActor(), policy: hostnamesPolicy("api.example.com")}},
		{name: "no policy", client: &egressMockClient{actor: runningActor()}, want: envoy_type.StatusCode_Forbidden},
		{name: "policy with no rules", client: &egressMockClient{actor: runningActor(), policy: &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{}}}, want: envoy_type.StatusCode_Forbidden},
		{name: "control plane unavailable", client: &egressMockClient{actor: runningActor(), policyErr: status.Error(codes.Unavailable, "down")}, want: envoy_type.StatusCode_ServiceUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := New(tc.client, ca.roots(), DefaultPolicyCacheTTL)
			res, err := h.HandleRequestHeaders(context.Background(), egressMetadata(xfccHeader(leaf)))
			if tc.want == 0 {
				wantAllowed(t, res, err)
				if calls := tc.client.policyCalls.Load(); calls != 1 {
					t.Errorf("GetActorEgressPolicy calls = %d, want 1", calls)
				}
				// The CONNECT warmed the cache: the first request inside the
				// tunnel costs no control-plane call.
				if _, err := h.HandleRequestHeaders(context.Background(), requestMetadata("api.example.com")); err != nil {
					t.Fatalf("request inside the tunnel: %v", err)
				}
				if calls := tc.client.policyCalls.Load(); calls != 1 {
					t.Errorf("GetActorEgressPolicy calls after the first request = %d, want 1", calls)
				}
				return
			}
			wantStatus(t, err, tc.want)
		})
	}
}

// The request legs police :authority because that is what the gateway dials;
// a Host header that disagrees with it is refused rather than trusted either way.
func TestRequestLegRefusesAuthorityHostMismatch(t *testing.T) {
	h := policyHandler(hostnamesPolicy("api.example.com"))
	md := requestMetadata("api.example.com")
	md.Headers["host"] = "evil.example"
	_, err := h.HandleRequestHeaders(context.Background(), md)
	wantStatus(t, err, envoy_type.StatusCode_Forbidden)

	// The same name spelled differently is not a disagreement.
	md = requestMetadata("api.example.com")
	md.Headers["host"] = "API.example.com."
	res, err := h.HandleRequestHeaders(context.Background(), md)
	wantAllowed(t, res, err)
}

// A denial's body is fixed; the reason stays in the log.
func TestDenialBodyIsUniform(t *testing.T) {
	h := policyHandler(hostnamesPolicy("api.example.com"))
	for _, md := range []*extproc.RequestMetadata{
		requestMetadata("evil.example"),
		requestMetadata("exa mple.com"),
		innerMetadata(extproc.EgressTLSMITMFilterChainName, "GET", "evil.example", nil),
	} {
		_, err := h.HandleRequestHeaders(context.Background(), md)
		if err == nil || err.Error() != deniedBody {
			t.Errorf("denial body = %v, want %q", err, deniedBody)
		}
	}
}

// A caller that gives up mid-fetch is neither a denial nor an outage.
func TestCanceledCallerIsNotAPolicyFailure(t *testing.T) {
	client := &egressMockClient{policy: allowAllPolicy(), policyGate: make(chan struct{})}
	h := New(client, nil, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := h.HandleRequestHeaders(ctx, requestMetadata("example.com"))
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for client.policyCalls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no fetch started")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	err := <-done
	close(client.policyGate)
	wantStatus(t, err, envoy_type.StatusCode_RequestTimeout)
}
