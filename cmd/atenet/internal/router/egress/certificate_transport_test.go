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
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"net/url"
	"testing"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/resources"
)

func TestCertificateTransportRequiresIntermediate(t *testing.T) {
	root := newTestCA(t, "root")
	intermediate := newTestCA(t, "intermediate")
	der, err := x509.CreateCertificate(rand.Reader, intermediate.cert, root.cert, &intermediate.key.PublicKey, root.key)
	if err != nil {
		t.Fatal(err)
	}
	intermediate.cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	leaf := intermediate.issueActorCert(t, testActorSPIFFEID, actorCertOptions{})

	otherRoot := newTestCA(t, "other-root")
	otherIntermediate := newTestCA(t, "other-intermediate")
	der, err = x509.CreateCertificate(rand.Reader, otherIntermediate.cert, otherRoot.cert, &otherIntermediate.key.PublicKey, otherRoot.key)
	if err != nil {
		t.Fatal(err)
	}
	otherIntermediate.cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	otherLeaf := otherIntermediate.issueActorCert(t, testActorSPIFFEID, actorCertOptions{})

	for _, source := range []PeerCertificateSource{PeerCertificateSourceEnvoy, PeerCertificateSourceAgentgateway} {
		t.Run(string(source), func(t *testing.T) {
			client := &egressMockClient{actor: runningActor(), policy: allowAllPolicy()}
			h := New(client, root.roots(), 0, nil, "", source)

			res, err := h.HandleRequestHeaders(context.Background(), certificateTransportMetadata(t, source, leaf, intermediate.cert))
			wantAllowed(t, res, err)
			if client.actorCalls.Load() == 0 {
				t.Fatal("full chain was allowed without reaching actor validation")
			}

			client.actorCalls.Store(0)
			_, err = h.HandleRequestHeaders(context.Background(), certificateTransportMetadata(t, source, leaf))
			wantStatus(t, err, envoy_type.StatusCode_Forbidden)
			if client.actorCalls.Load() != 0 {
				t.Fatalf("leaf-only chain reached actor validation %d times", client.actorCalls.Load())
			}

			_, err = h.HandleRequestHeaders(context.Background(), certificateTransportMetadata(t, source, otherLeaf, otherIntermediate.cert))
			wantStatus(t, err, envoy_type.StatusCode_Forbidden)
		})
	}
}

func TestCertificateTransportIgnoresXFCC(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, testActorSPIFFEID, actorCertOptions{})
	other := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/default/unknown-actor", actorCertOptions{})
	otherPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: other.Raw}))
	xfcc := `Chain="` + url.PathEscape(otherPEM) + `"`

	for _, source := range []PeerCertificateSource{PeerCertificateSourceEnvoy, PeerCertificateSourceAgentgateway} {
		t.Run(string(source), func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				xfcc  string
				allow bool
			}{
				{name: "absent XFCC", allow: true},
				{name: "malformed XFCC", xfcc: "malformed", allow: true},
				{name: "different actor in XFCC", xfcc: xfcc, allow: true},
				{name: "valid XFCC without selected evidence", xfcc: xfcc},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ref := resources.ActorRef{Atespace: testEgressAtespace, Name: testEgressActor}
					client := &certificateLookupClient{
						egressMockClient: &egressMockClient{actor: actorForRef(ref), policy: allowAllPolicy()},
						t:                t,
						want:             ref,
					}
					h := New(client, ca.roots(), 0, nil, "", source)
					var md *extproc.RequestMetadata
					if tc.allow {
						md = certificateTransportMetadata(t, source, leaf)
					} else {
						md = certificateTransportMetadata(t, source)
					}
					if tc.xfcc != "" {
						md.Headers["x-forwarded-client-cert"] = tc.xfcc
					}
					if tc.allow {
						res, err := h.HandleRequestHeaders(context.Background(), md)
						wantAllowed(t, res, err)
						if client.seenActor != 1 || client.seenPolicy != 1 {
							t.Fatalf("lookups = actor %d, policy %d; want one each", client.seenActor, client.seenPolicy)
						}
						return
					}
					_, err := h.HandleRequestHeaders(context.Background(), md)
					wantStatus(t, err, envoy_type.StatusCode_Forbidden)
					if client.seenActor != 0 || client.seenPolicy != 0 {
						t.Fatalf("missing selected evidence reached lookups: actor %d, policy %d", client.seenActor, client.seenPolicy)
					}
				})
			}
		})
	}
}

func certificateTransportMetadata(t *testing.T, source PeerCertificateSource, chain ...*x509.Certificate) *extproc.RequestMetadata {
	t.Helper()
	encoded := encodedCertificateChain(chain...)
	if source == PeerCertificateSourceEnvoy {
		return egressMetadata(encoded)
	}
	certificate, err := url.PathUnescape(encoded)
	if err != nil {
		t.Fatalf("decoding test certificate chain: %v", err)
	}
	return agentgatewayEgressMetadata(certificate)
}
