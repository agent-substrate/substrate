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

package egressauthz

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
)

func TestActorCredentialFixtureMatchesProduction(t *testing.T) {
	ca, err := localca.GenerateCA("fixture-root", localca.KeyTypeED25519, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ref := resources.ActorRef{Atespace: "fixture-space", Name: "fixture-actor"}
	wantURI := resources.AteomForActorSPIFFEID(ref).String()
	for _, tc := range []struct {
		name     string
		mint     func(*testing.T, *localca.CA, resources.ActorRef) []byte
		chainLen int
	}{
		{name: "direct", mint: mintActorCredential, chainLen: 1},
		{name: "intermediate", mint: mintActorCredentialWithIntermediate, chainLen: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := tc.mint(t, ca, ref)
			pair, err := tls.X509KeyPair(bundle, bundle)
			if err != nil {
				t.Fatal(err)
			}
			if len(pair.Certificate) != tc.chainLen {
				t.Fatalf("chain length = %d, want %d", len(pair.Certificate), tc.chainLen)
			}
			leaf, err := x509.ParseCertificate(pair.Certificate[0])
			if err != nil {
				t.Fatal(err)
			}
			if len(leaf.URIs) != 1 || leaf.URIs[0].String() != wantURI {
				t.Errorf("URI SANs = %v, want %s", leaf.URIs, wantURI)
			}
			identity, err := substratex509.ActorIdentityFromCertificate(leaf)
			if err != nil || identity != nil {
				t.Errorf("unexpected ActorIdentity: %v, %v", identity, err)
			}
			if leaf.IsCA || leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
				t.Errorf("leaf CA = %t, key usage = %v; want non-CA DigitalSignature", leaf.IsCA, leaf.KeyUsage)
			}
			for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth} {
				found := false
				for _, got := range leaf.ExtKeyUsage {
					found = found || got == usage
				}
				if !found {
					t.Errorf("EKUs = %v, want %v", leaf.ExtKeyUsage, usage)
				}
			}
			roots := x509.NewCertPool()
			roots.AddCert(ca.RootCertificate)
			intermediates := x509.NewCertPool()
			for _, der := range pair.Certificate[1:] {
				cert, err := x509.ParseCertificate(der)
				if err != nil {
					t.Fatal(err)
				}
				intermediates.AddCert(cert)
			}
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, CurrentTime: time.Now()}); err != nil {
				t.Errorf("leaf does not verify to fixture root: %v", err)
			}
		})
	}
}
