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

package certauth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"math/big"
	"testing"
	"testing/synctest"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
)

func testCA(t *testing.T) *localca.CA {
	t.Helper()
	return testRoot(t, "sdsmint test CA", time.Hour)
}

// testRoot builds a pool entry in the shape sdsmint expects to be handed one.
// P-256 rather than substrate's usual Ed25519: the leaves signed under it are
// validated by whatever HTTP client an actor happens to run, and Ed25519 in a
// chain needs OpenSSL 1.1.1+.
func testRoot(t *testing.T, commonName string, lifetime time.Duration) *localca.CA {
	t.Helper()
	ca, err := localca.GenerateCA(localca.GenerateOptions{
		ID:         "mitm",
		CommonName: commonName,
		KeyType:    localca.KeyTypeECDSAP256,
		Lifetime:   lifetime,
	})
	if err != nil {
		t.Fatalf("generating test CA %q: %v", commonName, err)
	}
	return ca
}

// inPool wraps entries the way a mounted pool Secret presents them.
func inPool(t *testing.T, id string, entries ...*localca.CA) *Signer {
	t.Helper()
	signer, err := New(&localca.Pool{CAs: entries}, id)
	if err != nil {
		t.Fatalf("New(%q): %v", id, err)
	}
	return signer
}

// testSigner builds a Signer over a single-entry pool.
func testSigner(t *testing.T, ca *localca.CA) *Signer {
	t.Helper()
	signer, err := New(&localca.Pool{CAs: []*localca.CA{ca}}, "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return signer
}

// oidSubjectAltName is id-ce-subjectAltName, RFC 5280 section 4.2.1.6.
var oidSubjectAltName = asn1.ObjectIdentifier{2, 5, 29, 17}

func hasCriticalSAN(t *testing.T, cert *x509.Certificate) bool {
	t.Helper()
	for _, ext := range cert.Extensions {
		if ext.Id.Equal(oidSubjectAltName) {
			return ext.Critical
		}
	}
	return false
}

// parseChain splits a PEM chain into leaf and root.
func parseChain(t *testing.T, chainPEM []byte) []*x509.Certificate {
	t.Helper()
	var certs []*x509.Certificate
	rest := chainPEM
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("parsing chain: %v", err)
		}
		certs = append(certs, c)
	}
	return certs
}

func TestSignProducesUsableLeaf(t *testing.T) {
	ca := testCA(t)

	minted, err := testSigner(t, ca).Sign("foo.example", 5*time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	chain := parseChain(t, minted.CertChainPEM)
	if len(chain) != 2 {
		t.Fatalf("chain length = %d, want 2 (leaf + CA)", len(chain))
	}
	leaf, root := chain[0], chain[1]

	if got := leaf.DNSNames; len(got) != 1 || got[0] != "foo.example" {
		t.Errorf("leaf DNSNames = %v, want [foo.example]", got)
	}
	// No subject at all: the name lives in the SAN, which is the only place
	// clients read it from. An empty subject obliges the SAN to be critical.
	if got := leaf.Subject.String(); got != "" {
		t.Errorf("leaf subject = %q, want empty", got)
	}
	if !hasCriticalSAN(t, leaf) {
		t.Error("leaf has an empty subject but a non-critical SAN, which RFC 5280 forbids")
	}
	if leaf.SerialNumber == nil || leaf.SerialNumber.Sign() <= 0 {
		t.Errorf("leaf serial = %v, want a positive number", leaf.SerialNumber)
	}
	if leaf.IsCA {
		t.Error("leaf is marked as a CA")
	}
	if leaf.KeyUsage != x509.KeyUsageDigitalSignature {
		t.Errorf("leaf KeyUsage = %v, want DigitalSignature only", leaf.KeyUsage)
	}
	if len(leaf.ExtKeyUsage) != 1 || leaf.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
		t.Errorf("leaf EKU = %v, want [ServerAuth]", leaf.ExtKeyUsage)
	}
	if !root.Equal(ca.RootCertificate) {
		t.Error("chain does not end in the CA certificate")
	}

	// The whole point is that a normal TLS client accepts this, so verify the
	// way one would.
	pool := x509.NewCertPool()
	pool.AddCert(ca.RootCertificate)
	if _, err := leaf.Verify(x509.VerifyOptions{
		DNSName:   "foo.example",
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("leaf does not verify against the CA: %v", err)
	}

	// And the key must actually match the certificate.
	if _, err := tls.X509KeyPair(minted.CertChainPEM, minted.PrivateKeyPEM); err != nil {
		t.Errorf("minted chain/key is not a usable TLS keypair: %v", err)
	}
}

func TestSignIsUniquePerCall(t *testing.T) {
	signer := testSigner(t, testCA(t))

	a, err := signer.Sign("a.example", time.Minute)
	if err != nil {
		t.Fatalf("Sign a: %v", err)
	}
	b, err := signer.Sign("a.example", time.Minute)
	if err != nil {
		t.Fatalf("Sign a again: %v", err)
	}

	// The certificate is new every time, which is what the SDS layer reads as
	// a version and what the audit log names the issuance by.
	if a.Serial == b.Serial {
		t.Error("two mints for the same host reused a serial number")
	}
	// The keypair, by contrast, is shared on purpose. Asserted
	// rather than left implicit, because the saving disappears silently the
	// moment a mint starts generating its own again.
	if string(a.PrivateKeyPEM) != string(b.PrivateKeyPEM) {
		t.Error("two mints generated separate private keys; leaves are meant to share one")
	}
}

func TestSignHonoursTTL(t *testing.T) {
	// On the fake clock nothing advances between the read and the mint, so the
	// deadline is exact rather than a tolerance around real elapsed time.
	synctest.Test(t, func(t *testing.T) {
		ca := testCA(t)
		before := time.Now()

		minted, err := testSigner(t, ca).Sign("ttl.example", 90*time.Second)
		if err != nil {
			t.Fatalf("Sign: %v", err)
		}

		if want := before.Add(90 * time.Second); !minted.NotAfter.Equal(want) {
			t.Errorf("NotAfter = %v, want %v", minted.NotAfter, want)
		}
	})
}

func TestSignIPLiteralGoesInSANIPAddresses(t *testing.T) {
	ca := testCA(t)

	minted, err := testSigner(t, ca).Sign("10.1.2.3", time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	leaf := parseChain(t, minted.CertChainPEM)[0]

	if len(leaf.IPAddresses) != 1 || leaf.IPAddresses[0].String() != "10.1.2.3" {
		t.Errorf("leaf IPAddresses = %v, want [10.1.2.3]", leaf.IPAddresses)
	}
	if len(leaf.DNSNames) != 0 {
		t.Errorf("leaf DNSNames = %v, want empty for an IP literal", leaf.DNSNames)
	}
}

func TestSignRejectsEmptyHost(t *testing.T) {
	if _, err := testSigner(t, testCA(t)).Sign("", time.Minute); err == nil {
		t.Fatal("Sign(\"\") succeeded, want an error")
	}
}

func TestNewRejectsANonCACertificate(t *testing.T) {
	ca := testCA(t)
	// A leaf is not a CA; a pool carrying one must be refused rather than
	// silently produce a signer that emits certificates nothing will chain.
	minted, err := testSigner(t, ca).Sign("leaf.example", time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	leaf := parseChain(t, minted.CertChainPEM)[0]

	if _, err := New(&localca.Pool{CAs: []*localca.CA{
		{ID: "mitm", RootCertificate: leaf, SigningKey: ca.SigningKey},
	}}, ""); err == nil {
		t.Fatal("New accepted a non-CA certificate")
	}
}

func TestNewAcceptsAnUnconstrainedCA(t *testing.T) {
	signer := inPool(t, "", testRoot(t, "wide open", time.Hour))

	if got := signer.Issuer().PermittedDNSDomains; len(got) != 0 {
		t.Errorf("PermittedDNSDomains = %v, want none", got)
	}

	// And it signs. Refusing at load and then failing at the first handshake
	// would be the same outage with a worse error message.
	if _, err := signer.Sign("anything.example", time.Minute); err != nil {
		t.Errorf("Sign under an unconstrained root: %v", err)
	}
}

func TestNewRejectsAMismatchedKey(t *testing.T) {
	a := testRoot(t, "a", time.Hour)
	b := testRoot(t, "b", time.Hour)

	// Signing with the wrong key produces a chain nothing can verify, and the
	// failure otherwise surfaces at the first handshake rather than at load.
	// localca.CA.Validate is what catches it; this pins that New asks.
	mismatched := &localca.CA{ID: "mismatched", RootCertificate: a.RootCertificate, SigningKey: b.SigningKey}
	if _, err := New(&localca.Pool{CAs: []*localca.CA{mismatched}}, ""); err == nil {
		t.Fatal("New accepted a key that does not match the certificate")
	}
}

// A pool entry may carry certificates that climb from its signing certificate
// up to whatever a client is configured to trust. Those have to travel with the
// leaf: a client holding only the top of that chain cannot build a path to a
// leaf issued two levels down, so omitting them fails the handshake.
func TestSignEmitsIntermediatesInTheChain(t *testing.T) {
	anchor := testRoot(t, "anchor", 24*time.Hour)

	// An issuing CA under the anchor, with its own key. This is the shape
	// RootCertificate names: the certificate whose key signs leaves, which here
	// is not the trust anchor.
	issuerKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating issuer key: %v", err)
	}
	issuerTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "issuing CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(12 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	issuerDER, err := x509.CreateCertificate(rand.Reader, issuerTmpl, anchor.RootCertificate, issuerKey.Public(), anchor.SigningKey)
	if err != nil {
		t.Fatalf("signing issuing CA: %v", err)
	}
	issuer, err := x509.ParseCertificate(issuerDER)
	if err != nil {
		t.Fatalf("parsing issuing CA: %v", err)
	}

	signer := testSigner(t, &localca.CA{
		ID:                       "delegated",
		RootCertificate:          issuer,
		SigningKey:               issuerKey,
		IntermediateCertificates: []*x509.Certificate{anchor.RootCertificate},
	})

	minted, err := signer.Sign("host.example", time.Hour)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	chain := parseChain(t, minted.CertChainPEM)
	if len(chain) != 3 {
		t.Fatalf("chain holds %d certificates, want leaf, issuer and anchor", len(chain))
	}
	if got := chain[1].Subject.CommonName; got != "issuing CA" {
		t.Errorf("chain[1] CN = %q, want the issuing CA", got)
	}
	if got := chain[2].Subject.CommonName; got != "anchor" {
		t.Errorf("chain[2] CN = %q, want the anchor", got)
	}

	// The point of carrying them: a client that trusts only the anchor can
	// still build a path to the leaf out of what the handshake delivered.
	roots := x509.NewCertPool()
	roots.AddCert(anchor.RootCertificate)
	intermediates := x509.NewCertPool()
	for _, c := range chain[1:] {
		intermediates.AddCert(c)
	}
	if _, err := chain[0].Verify(x509.VerifyOptions{
		DNSName:       "host.example",
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("verifying the leaf against the anchor alone: %v", err)
	}
}

func TestNewRoundTrip(t *testing.T) {
	entry := testRoot(t, "pooled CA", time.Hour)

	// Through the same serialization podcertcontroller's CAs use.
	poolBytes, err := localca.Marshal(&localca.Pool{CAs: []*localca.CA{entry}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	pool, err := localca.Unmarshal(poolBytes)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	signer, err := New(pool, "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	minted, err := signer.Sign("host.example", time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	roots := x509.NewCertPool()
	roots.AddCert(signer.Issuer())
	if _, err := parseChain(t, minted.CertChainPEM)[0].Verify(x509.VerifyOptions{
		DNSName: "host.example", Roots: roots,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("leaf from a pooled CA does not verify: %v", err)
	}
}

func TestNewSelectsByID(t *testing.T) {
	var entries []*localca.CA
	for _, id := range []string{"first", "second"} {
		entry := testRoot(t, id, time.Hour)
		entry.ID = id
		entries = append(entries, entry)
	}
	pool := &localca.Pool{CAs: entries}

	signer, err := New(pool, "second")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := signer.Issuer().Subject.CommonName; got != "second" {
		t.Errorf("selected CA CN = %q, want %q", got, "second")
	}

	// An empty ID takes the first, which is what a single-CA pool relies on.
	signer, err = New(pool, "")
	if err != nil {
		t.Fatalf("New(''): %v", err)
	}
	if got := signer.Issuer().Subject.CommonName; got != "first" {
		t.Errorf("default CA CN = %q, want %q", got, "first")
	}

	// A typo in --ca-id must not silently fall back to some other CA.
	if _, err := New(pool, "third"); err == nil {
		t.Fatal("New accepted an unknown CA ID")
	}
}

// TestAnchorsCoversTheWholePool is the distinction the two accessors exist to
// make. During a rotation the pool holds the outgoing and incoming CA at once
// while only one of them signs, so a trust store built from Issuer would reject
// every leaf minted the moment --ca-id moved.
func TestAnchorsCoversTheWholePool(t *testing.T) {
	var entries []*localca.CA
	for _, id := range []string{"outgoing", "incoming"} {
		entry := testRoot(t, id, time.Hour)
		entry.ID = id
		entries = append(entries, entry)
	}
	signer := inPool(t, "outgoing", entries...)

	anchors := signer.Anchors()
	if len(anchors) != 2 {
		t.Fatalf("Anchors returned %d certificates, want one per CA in the pool", len(anchors))
	}
	for i, entry := range entries {
		if !anchors[i].Equal(entry.RootCertificate) {
			t.Errorf("anchor %d is not the root of CA %q", i, entry.ID)
		}
	}

	// And the CA not signing is still in there, which is the whole point.
	if anchors[1].Equal(signer.Issuer()) {
		t.Error("the incoming CA was reported as the issuer; only the outgoing one signs")
	}
}

func TestNewRejectsAnEmptyPool(t *testing.T) {
	if _, err := New(&localca.Pool{}, ""); err == nil {
		t.Fatal("New accepted an empty pool")
	}
	if _, err := New(nil, ""); err == nil {
		t.Fatal("New accepted a nil pool")
	}
}

func TestSignClampsLeafLifetimeToTheRoot(t *testing.T) {
	// A leaf outliving its issuer is accepted at handshake time and rejected
	// later, with an error that points at the leaf rather than at the CA.
	ca := testRoot(t, "short-lived CA", 2*time.Minute)
	rootNotAfter := ca.RootCertificate.NotAfter

	minted, err := testSigner(t, ca).Sign("long.example", time.Hour)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if minted.NotAfter.After(rootNotAfter) {
		t.Errorf("leaf NotAfter = %v, outlives its issuer at %v", minted.NotAfter, rootNotAfter)
	}
	if leaf := parseChain(t, minted.CertChainPEM)[0]; leaf.NotAfter.After(rootNotAfter) {
		t.Errorf("encoded leaf NotAfter = %v, outlives its issuer at %v", leaf.NotAfter, rootNotAfter)
	}
}
