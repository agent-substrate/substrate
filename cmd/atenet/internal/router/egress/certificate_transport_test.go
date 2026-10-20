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

	"go.ate.dev/substrate/internal/resources"
)

// The filter state carries the leaf alone, so a leaf the root did not sign
// directly cannot chain to it and is refused before any actor lookup.
func TestCertificateTransportRejectsIntermediateIssuedLeaf(t *testing.T) {
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

	client := &egressMockClient{actor: runningActor(), policy: allowAllPolicy()}
	h := New(client, root.roots(), 0, nil, "")
	_, err = h.HandleRequestHeaders(context.Background(), egressMetadata(encodedCertificate(leaf)))
	wantStatus(t, err, envoy_type.StatusCode_Forbidden)
	if calls := client.actorCalls.Load(); calls != 0 {
		t.Fatalf("intermediate-issued leaf reached actor validation %d times", calls)
	}
}

// The gateway no longer forwards x-forwarded-client-cert, so a value an actor
// sends must not authenticate or override the certificate in filter state.
func TestCertificateTransportIgnoresXFCC(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, testActorSPIFFEID, actorCertOptions{})
	other := ca.issueActorCert(t, "spiffe://substrate-actor.local/worker-for-actor/default/unknown-actor", actorCertOptions{})
	otherPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: other.Raw}))
	xfcc := `Chain="` + url.PathEscape(otherPEM) + `"`

	for _, tc := range []struct {
		name  string
		xfcc  string
		allow bool
	}{
		{name: "absent XFCC", allow: true},
		{name: "malformed XFCC", xfcc: "malformed", allow: true},
		{name: "different actor in XFCC", xfcc: xfcc, allow: true},
		{name: "valid XFCC without filter state", xfcc: xfcc},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref := resources.ActorRef{Atespace: testEgressAtespace, Name: testEgressActor}
			client := &certificateLookupClient{
				egressMockClient: &egressMockClient{actor: actorForRef(ref), policy: allowAllPolicy()},
				t:                t,
				want:             ref,
			}
			h := New(client, ca.roots(), 0, nil, "")
			md := egressMetadata("")
			if tc.allow {
				md = egressMetadata(encodedCertificate(leaf))
			}
			if tc.xfcc != "" {
				md.Headers["x-forwarded-client-cert"] = tc.xfcc
			}
			res, err := h.HandleRequestHeaders(context.Background(), md)
			if tc.allow {
				wantAllowed(t, res, err)
				if client.seenActor != 1 || client.seenPolicy != 1 {
					t.Fatalf("lookups = actor %d, policy %d; want one each", client.seenActor, client.seenPolicy)
				}
				return
			}
			wantStatus(t, err, envoy_type.StatusCode_Forbidden)
			if client.seenActor != 0 || client.seenPolicy != 0 {
				t.Fatalf("missing filter state reached lookups: actor %d, policy %d", client.seenActor, client.seenPolicy)
			}
		})
	}
}
