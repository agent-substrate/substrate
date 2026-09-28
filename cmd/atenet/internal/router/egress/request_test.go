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
	"maps"
	"net"
	"strconv"
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
	testActorSPIFFEID = "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor"
	// testOriginalDst is the IP:port the test actor's kernel dialed.
	testOriginalDst = "93.184.216.34:443"
)

func cidrsPolicy(cidrs ...string) *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
		Cidrs: &ateapipb.CIDRRule{Cidrs: cidrs},
	}}}
}

func credentialInjectionPolicySample(pattern string) *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
		Hostnames: &ateapipb.HostnameRule{
			Patterns: []string{pattern},
			Effects: &ateapipb.EgressRuleEffects{InjectStaticHeaders: []*ateapipb.CredentialHeaderInjection{{
				Header: "authorization", Prefix: "Bearer ", CredentialUri: "ate-secret://k8s/default/token",
			}}},
		},
	}}}
}

// policyHandler builds a Handler for an actor whose policy is policy (nil
// means none) with the cache disabled, so each callout sees the mock as is.
func policyHandler(policy *ateapipb.EgressPolicy) *Handler {
	return New(&egressMockClient{actor: runningActor(), policy: policy}, nil, 0, nil, "")
}

// innerMetadata builds an inner chain's callout: pseudo-headers plus the
// attributes that chain requests. attrs overrides the defaults; an empty value
// deletes one.
func innerMetadata(leg, method, authority string, attrs map[string]string) *extproc.RequestMetadata {
	fields := map[string]string{
		extproc.FilterChainNameAttribute:          leg,
		extproc.ActorIdentityFilterStateAttribute: testActorSPIFFEID,
	}
	maps.Copy(fields, dialedAttributes(testOriginalDst))
	for k, v := range attrs {
		if v == "" {
			delete(fields, k)
			continue
		}
		fields[k] = v
	}
	values := map[string]*structpb.Value{}
	for k, v := range fields {
		// Envoy sends the port field as a number.
		if n, err := strconv.Atoi(v); err == nil && k == extproc.OriginalDstPortAttribute {
			values[k] = structpb.NewNumberValue(float64(n))
			continue
		}
		values[k] = structpb.NewStringValue(v)
	}
	return extproc.NewRequestMetadata([]*corev3.HeaderValue{
		{Key: ":method", RawValue: []byte(method)},
		{Key: ":authority", RawValue: []byte(authority)},
		{Key: ":path", RawValue: []byte("/v1/things?secret=1")},
	}, map[string]*structpb.Struct{"envoy.filters.http.ext_proc": {Fields: values}})
}

// dialedAttributes is what Envoy sends for the ORIGINAL_DST filter state
// holding hostport: the address and the port as separate fields. Something
// that is not host:port at all goes in the address field as is.
func dialedAttributes(hostport string) map[string]string {
	ip, port, err := net.SplitHostPort(hostport)
	if err != nil {
		ip, port = hostport, ""
	}
	return map[string]string{extproc.OriginalDstIPAttribute: ip, extproc.OriginalDstPortAttribute: port}
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

// combined is one policy with the rules of policies, in order.
func combined(policies ...*ateapipb.EgressPolicy) *ateapipb.EgressPolicy {
	out := &ateapipb.EgressPolicy{}
	for _, p := range policies {
		out.Rules = append(out.Rules, p.Rules...)
	}
	return out
}

// dialOf reads where an allowed request was sent, or "" when the answer said
// nothing.
func dialOf(res extproc.Result) string {
	return res.DynamicMetadata.GetFields()[extproc.EgressMetadataNamespace].GetStructValue().GetFields()[extproc.EgressDialKey].GetStringValue()
}

// wantDial checks an allowed request's answer: the dial the routes match on,
// with the route cache cleared so Envoy matches again.
func wantDial(t *testing.T, res extproc.Result, err error, want string) {
	t.Helper()
	wantAllowed(t, res, err)
	if !res.Response.GetResponse().GetClearRouteCache() {
		t.Error("allowed without clearing the route cache; Envoy would keep the route it picked before ext_proc ran")
	}
	if res.Response.GetResponse().GetHeaderMutation() == nil {
		t.Error("allowed without a header mutation; ext_proc ignores clear_route_cache without one")
	}
	if got := dialOf(res); got != want {
		t.Errorf("dial = %q, want %q", got, want)
	}
}

// The request legs walk the rules once, in order, over the Host the request
// named and the address the actor dialed (testOriginalDst unless a case says
// otherwise). The first match decides, and the answer says whether the name or
// the dialed address is what gets dialed.
func TestRequestLegDecidesHostAndDialedAddress(t *testing.T) {
	nameThenAddress := combined(hostnamesPolicy("api.example.com"), cidrsPolicy("93.184.216.0/24"))
	addressThenName := combined(cidrsPolicy("93.184.216.0/24"), hostnamesPolicy("api.example.com"))
	tests := []struct {
		name      string
		policy    *ateapipb.EgressPolicy
		authority string
		dialed    string                // overrides testOriginalDst
		noDialed  bool                  // the CONNECT leg allowed nothing by address, so there is none
		want      envoy_type.StatusCode // 0 means allowed
		dial      string
	}{
		{name: "exact hostname", policy: hostnamesPolicy("api.example.com"), authority: "api.example.com", dial: extproc.EgressDialName},
		{name: "hostname with port", policy: hostnamesPolicy("api.example.com"), authority: "api.example.com:8443", dial: extproc.EgressDialName},
		{name: "hostname case folded", policy: hostnamesPolicy("api.example.com"), authority: "API.Example.com", dial: extproc.EgressDialName},
		{name: "hostname with trailing dot", policy: hostnamesPolicy("api.example.com"), authority: "api.example.com.", dial: extproc.EgressDialName},
		{name: "wildcard hostname", policy: hostnamesPolicy("*.example.com"), authority: "api.example.com", dial: extproc.EgressDialName},
		{name: "hostname with no dialed address", policy: hostnamesPolicy("api.example.com"), authority: "api.example.com", noDialed: true, dial: extproc.EgressDialName},
		{name: "all rule goes to the dialed address", policy: allowAllPolicy(), authority: "anything.example", dial: extproc.EgressDialAddress},
		{name: "dialed address in a cidr, request by name", policy: cidrsPolicy("93.184.216.0/24"), authority: "evil.example", dial: extproc.EgressDialAddress},
		{name: "dialed address in a cidr, request by ip literal", policy: cidrsPolicy("93.184.216.0/24"), authority: "203.0.113.9:8080", dial: extproc.EgressDialAddress},
		{name: "ipv6 dialed address in a cidr", policy: cidrsPolicy("2001:db8::/32"), authority: "api.example.com", dialed: "[2001:db8::7]:443", dial: extproc.EgressDialAddress},
		{name: "address rule first wins over a matching hostname rule", policy: addressThenName, authority: "api.example.com", dial: extproc.EgressDialAddress},
		{name: "hostname rule first wins over a matching address rule", policy: nameThenAddress, authority: "api.example.com", dial: extproc.EgressDialName},
		{name: "address rule first that does not match falls through to the name", policy: addressThenName, authority: "api.example.com", dialed: "198.51.100.1:443", dial: extproc.EgressDialName},
		{name: "other hostname", policy: hostnamesPolicy("api.example.com"), authority: "evil.example", want: envoy_type.StatusCode_Forbidden},
		{name: "wildcard does not match the apex", policy: hostnamesPolicy("*.example.com"), authority: "example.com", want: envoy_type.StatusCode_Forbidden},
		{name: "wildcard does not match two labels", policy: hostnamesPolicy("*.example.com"), authority: "a.b.example.com", want: envoy_type.StatusCode_Forbidden},
		{name: "ip literal host with hostname policy", policy: hostnamesPolicy("api.example.com"), authority: "93.184.216.34", want: envoy_type.StatusCode_Forbidden},
		// The Host literal is inside the block but the actor dialed elsewhere;
		// the dialed address is what an address rule checks.
		{name: "host literal is not the dialed address", policy: cidrsPolicy("203.0.113.0/24"), authority: "203.0.113.9", want: envoy_type.StatusCode_Forbidden},
		{name: "dialed address outside the cidr", policy: cidrsPolicy("203.0.113.0/24"), authority: "api.example.com", want: envoy_type.StatusCode_Forbidden},
		{name: "no dialed address with an address-only policy", policy: cidrsPolicy("93.184.216.0/24"), authority: "api.example.com", noDialed: true, want: envoy_type.StatusCode_Forbidden},
		{name: "unparseable dialed address", policy: allowAllPolicy(), authority: "api.example.com", dialed: "not an address:443", want: envoy_type.StatusCode_Forbidden},
		{name: "name in the dialed address field", policy: allowAllPolicy(), authority: "api.example.com", dialed: "example.com:443", want: envoy_type.StatusCode_Forbidden},
		{name: "dialed address without a port", policy: allowAllPolicy(), authority: "api.example.com", dialed: "93.184.216.34", want: envoy_type.StatusCode_Forbidden},
		{name: "unparseable host", policy: allowAllPolicy(), authority: "exa mple.com", want: envoy_type.StatusCode_Forbidden},
		{name: "empty host", policy: allowAllPolicy(), authority: "", want: envoy_type.StatusCode_Forbidden},
		// On the cleartext leg a rule that requires injection is let through
		// without the credential, not denied: the secret is never re-originated in
		// the clear, and blocking allowed egress is worse than an unauthenticated
		// request. See the dedicated injection tests for the TLS leg.
		{name: "cleartext rule requires injection passes through uninjected", policy: credentialInjectionPolicySample("api.example.com"), authority: "api.example.com", dial: extproc.EgressDialName},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attrs := map[string]string{}
			if tc.dialed != "" {
				maps.Copy(attrs, dialedAttributes(tc.dialed))
			}
			if tc.noDialed {
				attrs[extproc.OriginalDstIPAttribute] = ""
				attrs[extproc.OriginalDstPortAttribute] = ""
			}
			h := policyHandler(tc.policy)
			res, err := h.HandleRequestHeaders(context.Background(), innerMetadata(extproc.EgressCleartextFilterChainName, "GET", tc.authority, attrs))
			if tc.want == 0 {
				wantDial(t, res, err, tc.dial)
				return
			}
			wantStatus(t, err, tc.want)
		})
	}
}

func TestRequestLegServesBothDecryptedChains(t *testing.T) {
	h := policyHandler(combined(hostnamesPolicy("api.example.com"), cidrsPolicy("93.184.216.0/24")))
	for _, leg := range []string{extproc.EgressCleartextFilterChainName, extproc.EgressTLSMITMFilterChainName} {
		res, err := h.HandleRequestHeaders(context.Background(), innerMetadata(leg, "POST", "api.example.com", nil))
		wantDial(t, res, err, extproc.EgressDialName)
		res, err = h.HandleRequestHeaders(context.Background(), innerMetadata(leg, "POST", "other.example", nil))
		wantDial(t, res, err, extproc.EgressDialAddress)
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
			h := New(tc.client, nil, 0, nil, "")
			_, err := h.HandleRequestHeaders(context.Background(), requestMetadata("api.example.com"))
			wantStatus(t, err, tc.want)
		})
	}
}

// The CONNECT leg refuses to open a tunnel for an actor with nothing that
// could be allowed through it, and warms the cache for the requests inside.
func TestConnectLegRequiresAPolicy(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})

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
			h := New(tc.client, ca.roots(), DefaultPolicyCacheTTL, nil, "")
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

// The request legs police :authority because that is what a by-name dial
// resolves; a Host header that disagrees with it is refused rather than
// trusted either way.
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
	h := New(client, nil, 0, nil, "")
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
