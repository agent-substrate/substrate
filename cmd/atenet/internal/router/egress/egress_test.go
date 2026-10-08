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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"path"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/egresspolicy"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	testEgressAtespace = "default"
	testEgressActor    = "my-actor"
	testEgressActorUID = "1b4e28ba-2fa1-11d2-883f-0016d3cca427"
)

// testCA is a throwaway CA standing in for the actor-identity CA.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T, commonName string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}
	return &testCA{cert: cert, key: key}
}

func (ca *testCA) roots() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}

// actorCertOptions mutates the leaf template so each test can break exactly one
// property of an otherwise-valid actor certificate.
type actorCertOptions struct {
	mutate func(*x509.Certificate)
}

// issueActorCert mints a leaf off ca, mirroring what ateapi's actoridentity
// service produces.
func (ca *testCA) issueActorCert(t *testing.T, spiffeURI string, opts actorCertOptions) *x509.Certificate {
	t.Helper()
	cert, err := x509.ParseCertificate(ca.issueActorCertDER(t, spiffeURI, opts))
	if err != nil {
		t.Fatalf("parsing leaf certificate: %v", err)
	}
	return cert
}

func (ca *testCA) issueActorCertDER(t *testing.T, spiffeURI string, opts actorCertOptions) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating leaf key: %v", err)
	}

	spiffeParsed, err := url.Parse(spiffeURI)
	if err != nil {
		t.Fatalf("parsing SPIFFE URI: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: testEgressActor},
		URIs:                  []*url.URL{spiffeParsed},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}
	if opts.mutate != nil {
		opts.mutate(template)
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("signing leaf certificate: %v", err)
	}
	return der
}

// encodedCertificateChain is retained as a helper name for the test cases; it now renders
// the URL-encoded PEM chain Envoy forwards as trusted dynamic metadata.
func encodedCertificateChain(chain ...*x509.Certificate) string {
	der := make([][]byte, 0, len(chain))
	for _, cert := range chain {
		der = append(der, cert.Raw)
	}
	return encodedCertificateChainDER(der...)
}

func encodedCertificateChainDER(chain ...[]byte) string {
	var buf strings.Builder
	for _, der := range chain {
		_ = pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	}
	return url.PathEscape(buf.String())
}

// egressHandler builds a Handler with an allow-everything policy, so the
// identity tests are about identity alone.
func egressHandler(roots *x509.CertPool, actor *ateapipb.Actor, err error) *Handler {
	return New(&egressMockClient{actor: actor, err: err, policy: allowAllPolicy()}, roots, DefaultPolicyCacheTTL, nil, "", PeerCertificateSourceEnvoy)
}

// egressMockClient is the slice of ateapi the egress handler talks to.
type egressMockClient struct {
	ateapipb.ControlClient
	actor      *ateapipb.Actor
	err        error
	actorCalls atomic.Int32

	// policy is what GetActorEgressPolicy returns; nil answers NotFound.
	// policyErr, when set, is returned instead.
	policy    *ateapipb.EgressPolicy
	policyErr error
	// policyCalls counts GetActorEgressPolicy calls, for the cache tests.
	policyCalls atomic.Int32
	// policyGate, when non-nil, blocks each GetActorEgressPolicy until it is
	// closed, so a test can hold several callers on one fetch.
	policyGate chan struct{}
}

func (m *egressMockClient) GetActor(context.Context, *ateapipb.GetActorRequest, ...grpc.CallOption) (*ateapipb.Actor, error) {
	m.actorCalls.Add(1)
	if m.err != nil {
		return nil, m.err
	}
	return m.actor, nil
}

func (m *egressMockClient) GetActorEgressPolicy(ctx context.Context, _ *ateapipb.GetActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	m.policyCalls.Add(1)
	if m.policyGate != nil {
		select {
		case <-m.policyGate:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	if m.policyErr != nil {
		return nil, m.policyErr
	}
	if m.policy == nil {
		return nil, status.Error(codes.NotFound, "EgressPolicy not found")
	}
	return m.policy, nil
}

// allowAllPolicy allows every name and address: cleartext HTTP on any port
// and HTTPS on 443.
func allowAllPolicy() *ateapipb.EgressPolicy {
	return combined(httpPolicyOnPorts(allPorts(), "*"), httpsPolicy("*"))
}

func httpPolicy(patterns ...string) *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
		Http: &ateapipb.HTTPRule{Hostnames: patterns},
	}}}
}

func httpPolicyOnPorts(ports *ateapipb.Ports, patterns ...string) *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
		Http: &ateapipb.HTTPRule{Hostnames: patterns, Ports: ports},
	}}}
}

func httpsPolicy(patterns ...string) *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
		Https: &ateapipb.HTTPSRule{Hostnames: patterns},
	}}}
}

func httpsPolicyOnPorts(ports *ateapipb.Ports, patterns ...string) *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
		Https: &ateapipb.HTTPSRule{Hostnames: patterns, Ports: ports},
	}}}
}

func passthroughPolicy(ports *ateapipb.Ports, patterns ...string) *ateapipb.EgressPolicy {
	return &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
		TlsPassthrough: &ateapipb.TLSPassthroughRule{Hostnames: patterns, Ports: ports},
	}}}
}

func ports(numbers ...int32) *ateapipb.Ports { return &ateapipb.Ports{Numbers: numbers} }

func allPorts() *ateapipb.Ports { return &ateapipb.Ports{All: &ateapipb.AllPorts{}} }

func runningActor() *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: testEgressAtespace,
			Name:     testEgressActor,
			Uid:      testEgressActorUID,
		},
		Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	}
}

// egressMetadata builds the CONNECT the egress listener hands to ext_proc,
// with the peer certificate and the chain name of the CONNECT leg.
func egressMetadata(encodedChain string) *extproc.RequestMetadata {
	headers := []*corev3.HeaderValue{
		{Key: ":method", RawValue: []byte("CONNECT")},
		{Key: ":authority", RawValue: []byte("93.184.216.34:80")},
	}
	md := extproc.NewRequestMetadata(headers, map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": {
			Fields: map[string]*structpb.Value{
				extproc.FilterChainNameAttribute: structpb.NewStringValue(extproc.EgressFilterChainName),
			},
		},
	})
	if encodedChain != "" {
		md.DynamicMetadata = map[string]*structpb.Struct{extproc.EgressPeerCertificateMetadataNamespace: {
			Fields: map[string]*structpb.Value{extproc.EgressPeerCertificateChainKey: structpb.NewStringValue(encodedChain)},
		}}
	}
	return md
}

func agentgatewayEgressMetadata(certificate string) *extproc.RequestMetadata {
	return extproc.NewRequestMetadata([]*corev3.HeaderValue{
		{Key: ":method", RawValue: []byte("CONNECT")},
		{Key: ":authority", RawValue: []byte("93.184.216.34:80")},
	}, map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": {
			Fields: map[string]*structpb.Value{
				agentgatewayClientCertificateAttribute: structpb.NewStringValue(certificate),
			},
		},
	})
}

func wantStatus(t *testing.T, err error, want envoy_type.StatusCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected a denial with status %d, got none", want)
	}
	var re *extproc.ReqError
	if !errors.As(err, &re) {
		t.Fatalf("error %v is not a *extproc.ReqError", err)
	}
	if re.StatusCode != int(want) {
		t.Errorf("status = %d, want %d (%v)", re.StatusCode, want, err)
	}
}

func TestHandleRequestHeadersAllowsVerifiedActor(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})
	h := egressHandler(ca.roots(), runningActor(), nil)

	res, err := h.HandleRequestHeaders(context.Background(), egressMetadata(encodedCertificateChain(leaf)))
	if err != nil {
		t.Fatalf("HandleRequestHeaders() error = %v, want nil", err)
	}
	if res.Response == nil {
		t.Fatal("HandleRequestHeaders() returned no response")
	}
	// Egress neither resumes an actor nor picks an upstream.
	if res.Resume != "" {
		t.Errorf("resume outcome = %q, want %q", res.Resume, "")
	}
	if res.Target != "" {
		t.Errorf("target = %q, want %q", res.Target, "")
	}
	if got := dialedPortOf(res); got != "80" {
		t.Errorf("dialed port = %q, want %q", got, "80")
	}
}

func TestProductionCertificateConnectAndInnerRequests(t *testing.T) {
	ref := resources.ActorRef{Atespace: "tenant-one", Name: "runner-two"}
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, resources.AteomForActorSPIFFEID(ref).String(), actorCertOptions{mutate: func(c *x509.Certificate) {
		c.ExtraExtensions = nil
		c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}
	}})
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != resources.AteomForActorSPIFFEID(ref).String() {
		t.Fatalf("production leaf URI SANs = %v", leaf.URIs)
	}
	if identity, err := substratex509.ActorIdentityFromCertificate(leaf); err != nil || identity != nil {
		t.Fatalf("production leaf ActorIdentity = %v, %v; want nil, nil", identity, err)
	}

	for _, source := range []PeerCertificateSource{PeerCertificateSourceEnvoy, PeerCertificateSourceAgentgateway} {
		for _, leg := range []string{extproc.EgressCleartextFilterChainName, extproc.EgressTLSMITMFilterChainName} {
			t.Run(string(source)+"/"+leg, func(t *testing.T) {
				mock := &certificateLookupClient{
					egressMockClient: &egressMockClient{actor: actorForRef(ref), policy: combined(httpPolicy("api.example.com"), httpsPolicy("api.example.com"))},
					t:                t, want: ref,
				}
				h := New(mock, ca.roots(), 0, nil, "", source)
				var md *extproc.RequestMetadata
				if source == PeerCertificateSourceEnvoy {
					md = egressMetadata(encodedCertificateChain(leaf))
				} else {
					md = agentgatewayEgressMetadata(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.Raw})))
				}
				md.Host = testDialed(leg)
				md.Headers[":authority"] = md.Host
				res, err := h.HandleRequestHeaders(context.Background(), md)
				wantAllowed(t, res, err)

				attrs := map[string]string{extproc.ActorIdentityFilterStateAttribute: leaf.URIs[0].String()}
				res, err = h.HandleRequestHeaders(context.Background(), innerMetadata(leg, "GET", "api.example.com", attrs))
				wantDial(t, res, err, extproc.EgressDialName)
				_, err = h.HandleRequestHeaders(context.Background(), innerMetadata(leg, "GET", "other.example", attrs))
				wantStatus(t, err, envoy_type.StatusCode_Forbidden)
				if mock.seenActor != 1 || mock.seenPolicy != 3 {
					t.Errorf("lookups = actor %d, policy %d; want 1 actor and 3 policy lookups", mock.seenActor, mock.seenPolicy)
				}
			})
		}
	}
}

func actorForRef(ref resources.ActorRef) *ateapipb.Actor {
	actor := runningActor()
	actor.Metadata.Atespace = ref.Atespace
	actor.Metadata.Name = ref.Name
	return actor
}

type certificateLookupClient struct {
	*egressMockClient
	t          *testing.T
	want       resources.ActorRef
	seenActor  int
	seenPolicy int
}

func (c *certificateLookupClient) check(ref *ateapipb.ObjectRef) {
	c.t.Helper()
	if ref.GetAtespace() != c.want.Atespace || ref.GetName() != c.want.Name {
		c.t.Fatalf("lookup ref = %v, want %v", ref, c.want)
	}
}

func (c *certificateLookupClient) GetActor(ctx context.Context, req *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	c.check(req.GetActor())
	c.seenActor++
	return c.egressMockClient.GetActor(ctx, req, opts...)
}

func (c *certificateLookupClient) GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	c.check(req.GetActor())
	c.seenPolicy++
	return c.egressMockClient.GetActorEgressPolicy(ctx, req, opts...)
}

// dialedPortOf reads the port a CONNECT decision handed back for the
// passthrough chain.
func dialedPortOf(res extproc.Result) string {
	return res.DynamicMetadata.GetFields()[extproc.EgressMetadataNamespace].GetStructValue().GetFields()[extproc.EgressDialedPortKey].GetStringValue()
}

// The CONNECT opens for any policy with rules and returns the https and
// tls_passthrough SNI rules for the dialed port, most specific first.
func TestConnectLegOpensForAnyRules(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})

	mitm := func(patterns ...string) []egresspolicy.SNIRule {
		rules := make([]egresspolicy.SNIRule, len(patterns))
		for i, p := range patterns {
			rules[i] = egresspolicy.SNIRule{Pattern: p, Mode: egresspolicy.SNIModeMITM}
		}
		return rules
	}
	passthrough := func(patterns ...string) []egresspolicy.SNIRule {
		rules := make([]egresspolicy.SNIRule, len(patterns))
		for i, p := range patterns {
			rules[i] = egresspolicy.SNIRule{Pattern: p, Mode: egresspolicy.SNIModePassthrough}
		}
		return rules
	}
	tests := []struct {
		name   string
		policy *ateapipb.EgressPolicy
		// dialed is the CONNECT authority; port 443 unless set.
		dialed string
		want   []egresspolicy.SNIRule
	}{
		{name: "http", policy: httpPolicy("api.example.com")},
		{name: "https", policy: httpsPolicy("api.example.com"), want: mitm("api.example.com")},
		{name: "tls passthrough", policy: passthroughPolicy(ports(443), "*"), want: passthrough("*")},
		{name: "allow all", policy: allowAllPolicy(), want: mitm("*")},
		{
			name: "https and tls passthrough rules, most specific first",
			policy: combined(
				httpsPolicy("*.example.org", "api.example.com"),
				httpPolicy("plain.example.com"),
				passthroughPolicy(ports(443), "foo.bar.com"),
			),
			want: []egresspolicy.SNIRule{
				{Pattern: "api.example.com", Mode: egresspolicy.SNIModeMITM},
				{Pattern: "foo.bar.com", Mode: egresspolicy.SNIModePassthrough},
				{Pattern: "*.example.org", Mode: egresspolicy.SNIModeMITM},
			},
		},
		{name: "https rule on another port", policy: httpsPolicy("api.example.com"), dialed: "93.184.216.34:8443"},
		{name: "https rule on the dialed port", policy: httpsPolicyOnPorts(ports(8443), "api.example.com"), dialed: "93.184.216.34:8443", want: mitm("api.example.com")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := New(&egressMockClient{actor: runningActor(), policy: tc.policy}, ca.roots(), 0, nil, "", PeerCertificateSourceEnvoy)
			md := egressMetadata(encodedCertificateChain(leaf))
			md.Host = "93.184.216.34:443"
			if tc.dialed != "" {
				md.Host = tc.dialed
			}
			md.Headers[":authority"] = md.Host
			res, err := h.HandleRequestHeaders(context.Background(), md)
			if err != nil {
				t.Fatalf("HandleRequestHeaders() error = %v, want the tunnel to open", err)
			}
			_, wantPort, _ := net.SplitHostPort(md.Host)
			if got := dialedPortOf(res); got != wantPort {
				t.Errorf("dialed port = %q, want %q", got, wantPort)
			}
			if got := sniRulesOf(t, res); !slices.Equal(got, tc.want) {
				t.Errorf("SNI rules = %v, want %v", got, tc.want)
			}
		})
	}
}

// sniRulesOf reads the SNI rules from a CONNECT result.
func sniRulesOf(t *testing.T, res extproc.Result) []egresspolicy.SNIRule {
	t.Helper()
	policyStruct := res.DynamicMetadata.GetFields()[extproc.EgressPolicyMetadataNamespace].GetStructValue()
	if policyStruct == nil {
		t.Fatalf("missing %q struct in DynamicMetadata", extproc.EgressPolicyMetadataNamespace)
	}
	listVal := policyStruct.GetFields()[extproc.EgressSNIRulesKey].GetListValue()
	if listVal == nil {
		t.Fatalf("missing %q list in %q DynamicMetadata", extproc.EgressSNIRulesKey, extproc.EgressPolicyMetadataNamespace)
	}
	out := make([]egresspolicy.SNIRule, len(listVal.GetValues()))
	for i, v := range listVal.GetValues() {
		fields := v.GetStructValue().GetFields()
		out[i] = egresspolicy.SNIRule{
			Pattern: fields[extproc.EgressSNIRulePatternKey].GetStringValue(),
			Mode:    egresspolicy.SNIMode(fields[extproc.EgressSNIRuleModeKey].GetStringValue()),
		}
	}
	return out
}

// A callout with no filter chain name gets the same answer: the tunnel opens,
// and an actor without a policy is refused.
func TestConnectLegWithoutRequestLegs(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})
	certificate := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.Raw}))

	h := New(&egressMockClient{actor: runningActor(), policy: httpPolicy("api.example.com")}, ca.roots(), 0, nil, "", PeerCertificateSourceAgentgateway)
	res, err := h.HandleRequestHeaders(context.Background(), agentgatewayEgressMetadata(certificate))
	if err != nil {
		t.Fatalf("HandleRequestHeaders() error = %v, want the tunnel to open", err)
	}
	if got := dialedPortOf(res); got != "80" {
		t.Errorf("dialed port = %q, want %q", got, "80")
	}
	h = New(&egressMockClient{actor: runningActor()}, ca.roots(), 0, nil, "", PeerCertificateSourceAgentgateway)
	_, err = h.HandleRequestHeaders(context.Background(), agentgatewayEgressMetadata(certificate))
	wantStatus(t, err, envoy_type.StatusCode_Forbidden)
}

func TestHandleRequestHeadersAllowsAgentgatewayCertificateAttribute(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})
	h := New(&egressMockClient{actor: runningActor(), policy: allowAllPolicy()}, ca.roots(), DefaultPolicyCacheTTL, nil, "", PeerCertificateSourceAgentgateway)

	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.Raw})
	if _, err := h.HandleRequestHeaders(context.Background(), agentgatewayEgressMetadata(string(certificate))); err != nil {
		t.Fatalf("HandleRequestHeaders() error = %v, want nil", err)
	}
}

// Every way an actor certificate can fail to prove an identity has to end in a
// denial, never in a tunnel.
func TestHandleRequestHeadersRejectsBadCertificates(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	otherCA := newTestCA(t, "some-other-ca")

	tests := []struct {
		name         string
		encodedChain func(t *testing.T) string
		want         envoy_type.StatusCode
	}{
		{
			name:         "no client certificate at all",
			encodedChain: func(*testing.T) string { return "" },
			want:         envoy_type.StatusCode_Forbidden,
		},
		{
			name: "signed by an unknown CA",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(otherCA.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "expired",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.NotBefore = time.Now().Add(-2 * time.Hour)
					c.NotAfter = time.Now().Add(-time.Hour)
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "not yet valid",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.NotBefore = time.Now().Add(time.Hour)
					c.NotAfter = time.Now().Add(2 * time.Hour)
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "no ClientAuth EKU",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "is a CA certificate",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.IsCA = true
					c.KeyUsage |= x509.KeyUsageCertSign
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "no URI SANs",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs = nil
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "multiple URI SANs",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs = append(c.URIs, &url.URL{
						Scheme: "spiffe",
						Host:   "substrate-actor.local",
						Path:   path.Join("ateom-for-actor", testEgressAtespace, "other-actor"),
					})
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "non-spiffe URI scheme",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs = []*url.URL{{
						Scheme: "https",
						Host:   "substrate-actor.local",
						Path:   path.Join("ateom-for-actor", testEgressAtespace, testEgressActor),
					}}
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "wrong trust domain",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs[0].Host = "other.local"
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "empty trust domain",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs = []*url.URL{{
						Scheme: "spiffe",
						Host:   "",
						Path:   "/" + path.Join("ateom-for-actor", testEgressAtespace, testEgressActor),
					}}
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "non-actor SPIFFE URI",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs = []*url.URL{{
						Scheme: "spiffe",
						Host:   "substrate-actor.local",
						Path:   path.Join("pod", testEgressAtespace, testEgressActor),
					}}
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "actor workload SPIFFE URI has the wrong role",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/actor/default/my-actor", actorCertOptions{}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "query in SPIFFE URI",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs[0].RawQuery = "extra=1"
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "userinfo in SPIFFE URI",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs[0].User = url.User("actor")
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "fragment in SPIFFE URI",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs[0].Fragment = "extra"
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "malformed SPIFFE URI path (too few segments)",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs = []*url.URL{{
						Scheme: "spiffe",
						Host:   "substrate-actor.local",
						Path:   path.Join("ateom-for-actor", testEgressAtespace),
					}}
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "malformed SPIFFE URI path (extra segment)",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs = []*url.URL{{
						Scheme: "spiffe",
						Host:   "substrate-actor.local",
						Path:   path.Join("ateom-for-actor", testEgressAtespace, testEgressActor, "extra"),
					}}
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "invalid atespace resource name in SPIFFE URI",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs = []*url.URL{{
						Scheme: "spiffe",
						Host:   "substrate-actor.local",
						Path:   path.Join("ateom-for-actor", "INVALID_ATESPACE", testEgressActor),
					}}
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "invalid actor resource name in SPIFFE URI",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) {
					c.URIs = []*url.URL{{
						Scheme: "spiffe",
						Host:   "substrate-actor.local",
						Path:   path.Join("ateom-for-actor", testEgressAtespace, "INVALID_ACTOR"),
					}}
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			// Two concatenated payloads are not one encoded PEM chain.
			name: "malformed percent encoding",
			encodedChain: func(t *testing.T) string {
				leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})
				return encodedCertificateChain(leaf) + "%zz"
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "malformed metadata payload",
			encodedChain: func(*testing.T) string {
				return `By=spiffe://cluster.local/ns/ate-system/sa/atenet-egress;Hash=abc123`
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "no URI SAN at all",
			encodedChain: func(t *testing.T) string {
				return encodedCertificateChain(ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{mutate: func(c *x509.Certificate) { c.URIs = nil }}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "encoded chain that is not a certificate",
			encodedChain: func(*testing.T) string {
				return `Chain="` + url.PathEscape("-----BEGIN CERTIFICATE-----\nbm90LWEtY2VydA==\n-----END CERTIFICATE-----\n") + `"`
			},
			want: envoy_type.StatusCode_Forbidden,
		},
	}

	for _, tc := range tests {
		for _, source := range []PeerCertificateSource{PeerCertificateSourceEnvoy, PeerCertificateSourceAgentgateway} {
			if source == PeerCertificateSourceAgentgateway && tc.name == "malformed percent encoding" {
				continue
			}
			t.Run(tc.name+"/"+string(source), func(t *testing.T) {
				client := &egressMockClient{actor: runningActor(), policy: allowAllPolicy()}
				h := New(client, ca.roots(), 0, nil, "", source)
				encoded := tc.encodedChain(t)
				var md *extproc.RequestMetadata
				if source == PeerCertificateSourceEnvoy {
					md = egressMetadata(encoded)
				} else {
					certificate, err := url.PathUnescape(encoded)
					if err != nil {
						t.Fatalf("decoding test certificate: %v", err)
					}
					md = agentgatewayEgressMetadata(certificate)
				}
				_, err := h.HandleRequestHeaders(context.Background(), md)
				wantStatus(t, err, tc.want)
				if calls := client.actorCalls.Load(); calls != 0 {
					t.Errorf("GetActor calls = %d, want 0 for invalid certificate identity", calls)
				}
			})
		}
	}
}

func TestConfiguredCertificateSourceDoesNotFallBack(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})
	certificate := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.Raw}))
	validEnvoy := encodedCertificateChain(leaf)
	for _, tc := range []struct {
		name   string
		source PeerCertificateSource
		md     *extproc.RequestMetadata
	}{
		{
			name:   "Envoy source missing despite valid alternate field and XFCC header",
			source: PeerCertificateSourceEnvoy,
			md: func() *extproc.RequestMetadata {
				md := egressMetadata("")
				md.Headers["x-forwarded-client-cert"] = validEnvoy
				md.Attributes["envoy.filters.http.ext_proc"].Fields[agentgatewayClientCertificateAttribute] = structpb.NewStringValue(certificate)
				return md
			}(),
		},
		{
			name:   "Envoy source malformed despite valid Agentgateway field",
			source: PeerCertificateSourceEnvoy,
			md: func() *extproc.RequestMetadata {
				md := egressMetadata("not-a-chain")
				md.Attributes["envoy.filters.http.ext_proc"].Fields[agentgatewayClientCertificateAttribute] = structpb.NewStringValue(certificate)
				return md
			}(),
		},
		{
			name:   "Agentgateway source missing despite valid Envoy metadata",
			source: PeerCertificateSourceAgentgateway,
			md:     egressMetadata(validEnvoy),
		},
		{
			name:   "Agentgateway source malformed despite valid Envoy metadata",
			source: PeerCertificateSourceAgentgateway,
			md: func() *extproc.RequestMetadata {
				md := egressMetadata(validEnvoy)
				md.Attributes["envoy.filters.http.ext_proc"].Fields[agentgatewayClientCertificateAttribute] = structpb.NewStringValue("not-a-certificate")
				return md
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := New(&egressMockClient{actor: runningActor(), policy: allowAllPolicy()}, ca.roots(), 0, nil, "", tc.source)
			_, err := h.HandleRequestHeaders(context.Background(), tc.md)
			wantStatus(t, err, envoy_type.StatusCode_Forbidden)
		})
	}
}

// The certificate authenticates; these cover what the control plane says about
// the actor it names.
func TestHandleRequestHeadersAuthorization(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")

	tests := []struct {
		name  string
		actor *ateapipb.Actor
		err   error
		want  envoy_type.StatusCode
	}{
		{
			name: "actor is not running",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{
					Atespace: testEgressAtespace,
					Name:     testEgressActor,
					Uid:      testEgressActorUID,
				},
				Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "actor no longer exists",
			err:  status.Error(codes.NotFound, "no such actor"),
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "control plane unreachable",
			err:  status.Error(codes.Unavailable, "ateapi is down"),
			want: envoy_type.StatusCode_ServiceUnavailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := egressHandler(ca.roots(), tc.actor, tc.err)
			leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})
			_, err := h.HandleRequestHeaders(context.Background(), egressMetadata(encodedCertificateChain(leaf)))
			wantStatus(t, err, tc.want)
		})
	}
}

// An ingress-only router has no actor-identity CA. If an egress CONNECT somehow
// reaches it, it must fail closed rather than tunnel unauthenticated traffic.
func TestHandleRequestHeadersWithoutConfiguredCA(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})
	h := egressHandler(nil, runningActor(), nil)

	_, err := h.HandleRequestHeaders(context.Background(), egressMetadata(encodedCertificateChain(leaf)))
	wantStatus(t, err, envoy_type.StatusCode_ServiceUnavailable)
}

// atunnel always sends the address the actor's kernel dialed; a name here is
// not a tunnel this gateway can carry, and is refused where it can still say so.
func TestHandleRequestHeadersRejectsNonAddressAuthority(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})
	h := egressHandler(ca.roots(), runningActor(), nil)

	for _, authority := range []string{"example.com:443", "93.184.216.34", ""} {
		md := egressMetadata(encodedCertificateChain(leaf))
		md.Host = authority
		md.Headers[":authority"] = authority
		_, err := h.HandleRequestHeaders(context.Background(), md)
		wantStatus(t, err, envoy_type.StatusCode_Forbidden)
	}
}

func TestHandleRequestHeadersRejectsNonConnect(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})
	h := egressHandler(ca.roots(), runningActor(), nil)

	md := egressMetadata(encodedCertificateChain(leaf))
	md.Method = "GET"
	md.Headers[":method"] = "GET"

	_, err := h.HandleRequestHeaders(context.Background(), md)
	wantStatus(t, err, envoy_type.StatusCode_MethodNotAllowed)
}

// PEM bodies routinely contain '+'. Decoding the header as a query string would
// turn those into spaces and corrupt the DER, so pin the round trip.
func TestEncodedCertificateChainPreservesPlusInPEM(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	// Serials differ per certificate, so mint until one encodes with a '+'.
	var leaf *x509.Certificate
	for i := 0; i < 50; i++ {
		candidate := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})
		if strings.Contains(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: candidate.Raw})), "+") {
			leaf = candidate
			break
		}
	}
	if leaf == nil {
		t.Skip("no certificate with a '+' in its PEM body after 50 attempts")
	}

	chain, err := parseEncodedCertificateChain(encodedCertificateChain(leaf))
	if err != nil {
		t.Fatalf("parseEncodedCertificateChain() error = %v", err)
	}
	if len(chain) != 1 || !chain[0].Equal(leaf) {
		t.Fatalf("parseEncodedCertificateChain() did not round-trip the certificate")
	}
}

func TestEncodedCertificateChainIncludesIntermediates(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/my-actor", actorCertOptions{})

	chain, err := parseEncodedCertificateChain(encodedCertificateChain(leaf, ca.cert))
	if err != nil {
		t.Fatalf("parseEncodedCertificateChain() error = %v", err)
	}
	if len(chain) != 2 {
		t.Fatalf("parseEncodedCertificateChain() returned %d certificates, want 2", len(chain))
	}
	if !chain[0].Equal(leaf) {
		t.Error("parseEncodedCertificateChain() did not return the leaf first")
	}
}

func parseEncodedCertificateChain(encoded string) ([]*x509.Certificate, error) {
	chainPEM, err := url.PathUnescape(encoded)
	if err != nil {
		return nil, err
	}
	return parseCertificateChainPEM([]byte(chainPEM))
}
