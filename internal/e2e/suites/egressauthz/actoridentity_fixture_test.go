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
	"slices"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/substratex509"
)

func TestActorCredentialFixtureMatchesProduction(t *testing.T) {
	ca, err := localca.GenerateCA("fixture-root", localca.KeyTypeED25519, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ref := resources.ActorRef{Atespace: "fixture-space", Name: "fixture-actor"}
	wantURI := resources.WorkerForActorSPIFFEID(ref).String()

	bundle := mintActorCredential(t, ca, ref)
	pair, err := tls.X509KeyPair(bundle, bundle)
	if err != nil {
		t.Fatal(err)
	}
	// The gateway sees the leaf alone, so the root must sign it directly.
	if len(pair.Certificate) != 1 {
		t.Fatalf("chain length = %d, want 1", len(pair.Certificate))
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
		if !slices.Contains(leaf.ExtKeyUsage, usage) {
			t.Errorf("EKUs = %v, want %v", leaf.ExtKeyUsage, usage)
		}
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca.RootCertificate)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, CurrentTime: time.Now()}); err != nil {
		t.Errorf("leaf does not verify to fixture root: %v", err)
	}
}
