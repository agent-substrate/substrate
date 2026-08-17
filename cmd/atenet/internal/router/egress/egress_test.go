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
	"encoding/asn1"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
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

// oidActorIdentity mirrors the unexported OID substratex509 encodes the
// ActorIdentity extension under: the Substrate PEN arc, sub-identifier 2.
var oidActorIdentity = append(append(asn1.ObjectIdentifier{}, substratex509.GoogleSubstratePEN...), 2)

// addActorIdentityUnchecked encodes identity into template the way
// substratex509.AddActorIdentityToCertificate does, minus its validation, so
// tests can mint the malformed identities a real CA would refuse to produce and
// confirm the gateway rejects them anyway.
func addActorIdentityUnchecked(identity *substratex509.ActorIdentity, template *x509.Certificate) error {
	value, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	template.ExtraExtensions = append(template.ExtraExtensions, pkix.Extension{Id: oidActorIdentity, Value: value})
	return nil
}

// actorCertOptions mutates the leaf template so each test can break exactly one
// property of an otherwise-valid actor certificate.
type actorCertOptions struct {
	identity      *substratex509.ActorIdentity
	extraIdentity *substratex509.ActorIdentity
	mutate        func(*x509.Certificate)
}

// issueActorCert mints a leaf off ca, mirroring what ateapi's actoridentity
// service produces.
func (ca *testCA) issueActorCert(t *testing.T, opts actorCertOptions) *x509.Certificate {
	t.Helper()
	cert, err := x509.ParseCertificate(ca.issueActorCertDER(t, opts))
	if err != nil {
		t.Fatalf("parsing leaf certificate: %v", err)
	}
	return cert
}

// issueActorCertDER is issueActorCert without the parse, for the certificates
// crypto/x509 itself refuses to read back.
func (ca *testCA) issueActorCertDER(t *testing.T, opts actorCertOptions) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating leaf key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: testEgressActor},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}
	identity := opts.identity
	if identity == nil {
		identity = &substratex509.ActorIdentity{
			Atespace:  testEgressAtespace,
			ActorName: testEgressActor,
			ActorUid:  testEgressActorUID,
			Purpose:   substratex509.ActorIdentityPurposeAtunnel,
		}
	}
	// AddActorIdentityToCertificate validates its input, so identities a real CA
	// would refuse to mint are encoded directly.
	if err := addActorIdentityUnchecked(identity, template); err != nil {
		t.Fatalf("adding ActorIdentity extension: %v", err)
	}
	if opts.extraIdentity != nil {
		if err := addActorIdentityUnchecked(opts.extraIdentity, template); err != nil {
			t.Fatalf("adding second ActorIdentity extension: %v", err)
		}
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

// xfccHeader renders chain the way Envoy's SANITIZE_SET +
// set_current_client_cert_details{chain: true} does.
func xfccHeader(chain ...*x509.Certificate) string {
	der := make([][]byte, 0, len(chain))
	for _, cert := range chain {
		der = append(der, cert.Raw)
	}
	return xfccHeaderDER(der...)
}

func xfccHeaderDER(chain ...[]byte) string {
	var buf strings.Builder
	for _, der := range chain {
		_ = pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	}
	return fmt.Sprintf(`By=spiffe://cluster.local/ns/ate-system/sa/atenet-egress;Hash=abc123;Chain="%s"`,
		url.PathEscape(buf.String()))
}

// egressHandler builds a Handler whose GetActor returns actor/err.
func egressHandler(roots *x509.CertPool, actor *ateapipb.Actor, err error) *Handler {
	return New(&egressMockClient{actor: actor, err: err}, roots)
}

type egressMockClient struct {
	ateapipb.ControlClient
	actor *ateapipb.Actor
	err   error
}

func (m *egressMockClient) GetActor(context.Context, *ateapipb.GetActorRequest, ...grpc.CallOption) (*ateapipb.Actor, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.actor, nil
}

func runningActor() *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: testEgressAtespace,
			Name:     testEgressActor,
			Uid:      testEgressActorUID,
		},
		Status: ateapipb.Actor_STATUS_RUNNING,
	}
}

// egressMetadata builds the CONNECT the egress listener hands to ext_proc.
func egressMetadata(xfcc string) *extproc.RequestMetadata {
	headers := []*corev3.HeaderValue{
		{Key: ":method", RawValue: []byte("CONNECT")},
		{Key: ":authority", RawValue: []byte("93.184.216.34:80")},
	}
	if xfcc != "" {
		headers = append(headers, &corev3.HeaderValue{Key: forwardedClientCertHeader, RawValue: []byte(xfcc)})
	}
	return extproc.NewRequestMetadata(headers, nil)
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
	leaf := ca.issueActorCert(t, actorCertOptions{})
	h := egressHandler(ca.roots(), runningActor(), nil)

	res, err := h.HandleRequestHeaders(context.Background(), egressMetadata(xfccHeader(leaf)))
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
}

// Every way an actor certificate can fail to prove an identity has to end in a
// denial, never in a tunnel.
func TestHandleRequestHeadersRejectsBadCertificates(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	otherCA := newTestCA(t, "some-other-ca")

	tests := []struct {
		name string
		xfcc func(t *testing.T) string
		want envoy_type.StatusCode
	}{
		{
			name: "no client certificate at all",
			xfcc: func(*testing.T) string { return "" },
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "signed by an unknown CA",
			xfcc: func(t *testing.T) string {
				return xfccHeader(otherCA.issueActorCert(t, actorCertOptions{}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "expired",
			xfcc: func(t *testing.T) string {
				return xfccHeader(ca.issueActorCert(t, actorCertOptions{mutate: func(c *x509.Certificate) {
					c.NotBefore = time.Now().Add(-2 * time.Hour)
					c.NotAfter = time.Now().Add(-time.Hour)
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "not yet valid",
			xfcc: func(t *testing.T) string {
				return xfccHeader(ca.issueActorCert(t, actorCertOptions{mutate: func(c *x509.Certificate) {
					c.NotBefore = time.Now().Add(time.Hour)
					c.NotAfter = time.Now().Add(2 * time.Hour)
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "no ClientAuth EKU",
			xfcc: func(t *testing.T) string {
				return xfccHeader(ca.issueActorCert(t, actorCertOptions{mutate: func(c *x509.Certificate) {
					c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			// An empty EKU means "any usage" to crypto/x509 and would pass
			// VerifyOptions.KeyUsages; the explicit ClientAuth check is what
			// catches it.
			name: "empty EKU",
			xfcc: func(t *testing.T) string {
				return xfccHeader(ca.issueActorCert(t, actorCertOptions{mutate: func(c *x509.Certificate) {
					c.ExtKeyUsage = nil
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "is a CA certificate",
			xfcc: func(t *testing.T) string {
				return xfccHeader(ca.issueActorCert(t, actorCertOptions{mutate: func(c *x509.Certificate) {
					c.IsCA = true
					c.KeyUsage |= x509.KeyUsageCertSign
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "no ActorIdentity extension",
			xfcc: func(t *testing.T) string {
				return xfccHeader(ca.issueActorCert(t, actorCertOptions{mutate: func(c *x509.Certificate) {
					c.ExtraExtensions = nil
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			// Stays in DER: crypto/x509 refuses to parse a certificate with a
			// duplicate extension OID at all, so the helper cannot hand back an
			// *x509.Certificate here. That refusal is the first of the two
			// layers guarding "exactly one ActorIdentity" — this case proves the
			// handler denies rather than panics when the parse fails, and
			// substratex509 rejects a second copy if a parser ever allowed one.
			name: "two ActorIdentity extensions",
			xfcc: func(t *testing.T) string {
				return xfccHeaderDER(ca.issueActorCertDER(t, actorCertOptions{
					extraIdentity: &substratex509.ActorIdentity{
						Atespace:  testEgressAtespace,
						ActorName: "a-different-actor",
						ActorUid:  "8f14e45f-ceea-467a-9575-25a0d5d5e4b0",
						Purpose:   substratex509.ActorIdentityPurposeAtunnel,
					},
				}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "generic purpose",
			xfcc: func(t *testing.T) string {
				return xfccHeader(ca.issueActorCert(t, actorCertOptions{identity: &substratex509.ActorIdentity{
					Atespace:  testEgressAtespace,
					ActorName: testEgressActor,
					ActorUid:  testEgressActorUID,
					Purpose:   "generic",
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "missing purpose",
			xfcc: func(t *testing.T) string {
				return xfccHeader(ca.issueActorCert(t, actorCertOptions{identity: &substratex509.ActorIdentity{
					Atespace:  testEgressAtespace,
					ActorName: testEgressActor,
					ActorUid:  testEgressActorUID,
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "empty atespace",
			xfcc: func(t *testing.T) string {
				return xfccHeader(ca.issueActorCert(t, actorCertOptions{identity: &substratex509.ActorIdentity{
					ActorName: testEgressActor,
					ActorUid:  testEgressActorUID,
					Purpose:   substratex509.ActorIdentityPurposeAtunnel,
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "empty actor name",
			xfcc: func(t *testing.T) string {
				return xfccHeader(ca.issueActorCert(t, actorCertOptions{identity: &substratex509.ActorIdentity{
					Atespace: testEgressAtespace,
					ActorUid: testEgressActorUID,
					Purpose:  substratex509.ActorIdentityPurposeAtunnel,
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "empty actor UID",
			xfcc: func(t *testing.T) string {
				return xfccHeader(ca.issueActorCert(t, actorCertOptions{identity: &substratex509.ActorIdentity{
					Atespace:  testEgressAtespace,
					ActorName: testEgressActor,
					Purpose:   substratex509.ActorIdentityPurposeAtunnel,
				}}))
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			// SANITIZE_SET makes Envoy the only writer of the header, so two
			// elements means we can no longer tell which one is our peer.
			name: "two XFCC elements",
			xfcc: func(t *testing.T) string {
				leaf := ca.issueActorCert(t, actorCertOptions{})
				return xfccHeader(leaf) + "," + xfccHeader(leaf)
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "XFCC without a Chain value",
			xfcc: func(*testing.T) string {
				return `By=spiffe://cluster.local/ns/ate-system/sa/atenet-egress;Hash=abc123`
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "XFCC Chain that is not a certificate",
			xfcc: func(*testing.T) string {
				return `Chain="` + url.PathEscape("-----BEGIN CERTIFICATE-----\nbm90LWEtY2VydA==\n-----END CERTIFICATE-----\n") + `"`
			},
			want: envoy_type.StatusCode_Forbidden,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := egressHandler(ca.roots(), runningActor(), nil)
			_, err := h.HandleRequestHeaders(context.Background(), egressMetadata(tc.xfcc(t)))
			wantStatus(t, err, tc.want)
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
			// The actor was deleted and recreated under the same name: the
			// certificate is still cryptographically valid but names a UID that
			// no longer exists, and must not carry over to the successor.
			name: "actor UID does not match the certificate",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{
					Atespace: testEgressAtespace,
					Name:     testEgressActor,
					Uid:      "d41d8cd9-8f00-4204-a980-0998ecf8427e",
				},
				Status: ateapipb.Actor_STATUS_RUNNING,
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "actor has no UID",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: testEgressAtespace, Name: testEgressActor},
				Status:   ateapipb.Actor_STATUS_RUNNING,
			},
			want: envoy_type.StatusCode_Forbidden,
		},
		{
			name: "actor is not running",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{
					Atespace: testEgressAtespace,
					Name:     testEgressActor,
					Uid:      testEgressActorUID,
				},
				Status: ateapipb.Actor_STATUS_SUSPENDED,
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
			leaf := ca.issueActorCert(t, actorCertOptions{})
			_, err := h.HandleRequestHeaders(context.Background(), egressMetadata(xfccHeader(leaf)))
			wantStatus(t, err, tc.want)
		})
	}
}

// An ingress-only router has no actor-identity CA. If an egress CONNECT somehow
// reaches it, it must fail closed rather than tunnel unauthenticated traffic.
func TestHandleRequestHeadersWithoutConfiguredCA(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, actorCertOptions{})
	h := egressHandler(nil, runningActor(), nil)

	_, err := h.HandleRequestHeaders(context.Background(), egressMetadata(xfccHeader(leaf)))
	wantStatus(t, err, envoy_type.StatusCode_ServiceUnavailable)
}

func TestHandleRequestHeadersRejectsNonConnect(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, actorCertOptions{})
	h := egressHandler(ca.roots(), runningActor(), nil)

	md := egressMetadata(xfccHeader(leaf))
	md.Method = "GET"
	md.Headers[":method"] = "GET"

	_, err := h.HandleRequestHeaders(context.Background(), md)
	wantStatus(t, err, envoy_type.StatusCode_MethodNotAllowed)
}

// PEM bodies routinely contain '+'. Decoding the header as a query string would
// turn those into spaces and corrupt the DER, so pin the round trip.
func TestParseXFCCChainPreservesPlusInPEM(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	// Serials differ per certificate, so mint until one encodes with a '+'.
	var leaf *x509.Certificate
	for i := 0; i < 50; i++ {
		candidate := ca.issueActorCert(t, actorCertOptions{})
		if strings.Contains(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: candidate.Raw})), "+") {
			leaf = candidate
			break
		}
	}
	if leaf == nil {
		t.Skip("no certificate with a '+' in its PEM body after 50 attempts")
	}

	chain, err := parseXFCCChain(xfccHeader(leaf))
	if err != nil {
		t.Fatalf("parseXFCCChain() error = %v", err)
	}
	if len(chain) != 1 || !chain[0].Equal(leaf) {
		t.Fatalf("parseXFCCChain() did not round-trip the certificate")
	}
}

func TestParseXFCCChainIncludesIntermediates(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, actorCertOptions{})

	chain, err := parseXFCCChain(xfccHeader(leaf, ca.cert))
	if err != nil {
		t.Fatalf("parseXFCCChain() error = %v", err)
	}
	if len(chain) != 2 {
		t.Fatalf("parseXFCCChain() returned %d certificates, want 2", len(chain))
	}
	if !chain[0].Equal(leaf) {
		t.Error("parseXFCCChain() did not return the leaf first")
	}
}
