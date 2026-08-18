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

package oidcjwt

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testAudience = "ate-api"

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// testIssuer serves the OIDC discovery document and a JWKS built from the keys
// registered on it, standing in for a Kubernetes API server's OIDC endpoints.
type testIssuer struct {
	server            *httptest.Server
	jwks              jwkSetT
	discoveryRequests atomic.Int32
	jwksRequests      atomic.Int32
	jwksFailures      atomic.Int32
}

func newTestIssuer(t *testing.T) *testIssuer {
	t.Helper()
	ti := &testIssuer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		ti.discoveryRequests.Add(1)
		writeJSON(t, w, oidcConfigT{Issuer: ti.issuer(), JWKSURI: ti.server.URL + "/jwks"})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		ti.jwksRequests.Add(1)
		if ti.jwksFailures.Load() > 0 {
			ti.jwksFailures.Add(-1)
			http.Error(w, "temporary failure", http.StatusServiceUnavailable)
			return
		}
		writeJSON(t, w, ti.jwks)
	})
	ti.server = httptest.NewServer(mux)
	t.Cleanup(ti.server.Close)
	return ti
}

func TestVerifierRetriesInitialDiscoveryFailure(t *testing.T) {
	ti := newTestIssuer(t)
	key := testRSAKey(t)
	ti.addRSA("key", &key.PublicKey)
	ti.jwksFailures.Store(1)
	verifier := NewVerifier(ti.issuer(), []string{testAudience}, ti.server.Client())
	token := mintJWT(t, "RS256", "key", key, validClaims(ti.issuer()))
	now := time.Now()

	if _, err := verifier.Verify(t.Context(), token, now); err == nil {
		t.Fatal("first Verify() succeeded, want discovery failure")
	}
	if _, err := verifier.Verify(t.Context(), token, now); err == nil {
		t.Fatal("second Verify() succeeded during retry cooldown")
	}
	if got := ti.jwksRequests.Load(); got != 1 {
		t.Fatalf("JWKS requests during retry cooldown = %d, want 1", got)
	}
	if _, err := verifier.Verify(t.Context(), token, now.Add(jwksRefreshRetryInterval)); err != nil {
		t.Fatalf("Verify() after retry cooldown error = %v", err)
	}
}

func TestVerifierUsesCachedKeyWhenRefreshFails(t *testing.T) {
	ti := newTestIssuer(t)
	key := testRSAKey(t)
	ti.addRSA("key", &key.PublicKey)
	verifier := NewVerifier(ti.issuer(), []string{testAudience}, ti.server.Client())
	token := mintJWT(t, "RS256", "key", key, validClaims(ti.issuer()))
	now := time.Now()

	if _, err := verifier.Verify(t.Context(), token, now); err != nil {
		t.Fatal(err)
	}
	ti.jwksFailures.Store(1)
	if _, err := verifier.Verify(t.Context(), token, now.Add(jwksRefreshInterval)); err != nil {
		t.Fatalf("Verify() with failed refresh error = %v", err)
	}
	if _, err := verifier.Verify(t.Context(), token, now.Add(jwksRefreshInterval+time.Second)); err != nil {
		t.Fatalf("Verify() during retry cooldown error = %v", err)
	}
	if got := ti.jwksRequests.Load(); got != 2 {
		t.Fatalf("JWKS requests after failed refresh = %d, want 2", got)
	}
}

func TestVerifierReturnsKubernetesClaims(t *testing.T) {
	ti := newTestIssuer(t)
	key := testRSAKey(t)
	ti.addRSA("key", &key.PublicKey)
	claims := validClaims(ti.issuer())
	claims["kubernetes.io"] = map[string]any{
		"namespace":      "ate-system",
		"serviceaccount": map[string]string{"name": "atelet", "uid": "sa-uid"},
	}
	token := mintJWT(t, "RS256", "key", key, claims)

	got, err := NewVerifier(ti.issuer(), []string{testAudience}, ti.server.Client()).Verify(t.Context(), token, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got.Namespace != "ate-system" || got.ServiceAccountName != "atelet" {
		t.Fatalf("Kubernetes claims = %+v", got.KubernetesClaims)
	}
}

func TestVerifierCachesAndRefreshesKeys(t *testing.T) {
	ti := newTestIssuer(t)
	key1 := testRSAKey(t)
	ti.addRSA("key-1", &key1.PublicKey)
	verifier := NewVerifier(ti.issuer(), []string{"other", testAudience}, ti.server.Client())
	now := time.Now()

	token1 := mintJWT(t, "RS256", "key-1", key1, validClaims(ti.issuer()))
	for range 2 {
		if _, err := verifier.Verify(t.Context(), token1, now); err != nil {
			t.Fatalf("Verify(key-1) error = %v", err)
		}
	}
	if got := ti.jwksRequests.Load(); got != 1 {
		t.Fatalf("JWKS requests after cached verification = %d, want 1", got)
	}

	key2, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ti.addRSA("key-2", &key2.PublicKey)
	token2 := mintJWT(t, "RS256", "key-2", key2, validClaims(ti.issuer()))
	if _, err := verifier.Verify(t.Context(), token2, now); err != nil {
		t.Fatalf("Verify(key-2) after rotation error = %v", err)
	}
	if got := ti.jwksRequests.Load(); got != 2 {
		t.Fatalf("JWKS requests after unknown key = %d, want 2", got)
	}

	missingToken := mintJWT(t, "RS256", "missing", key2, validClaims(ti.issuer()))
	for range 2 {
		if _, err := verifier.Verify(t.Context(), missingToken, now); err == nil {
			t.Fatal("Verify(missing) succeeded")
		}
	}
	if got := ti.jwksRequests.Load(); got != 2 {
		t.Fatalf("JWKS requests during unknown-key cooldown = %d, want 2", got)
	}

	ti.jwks.Keys = ti.jwks.Keys[1:]
	if _, err := verifier.Verify(t.Context(), token1, now.Add(jwksRefreshInterval)); err == nil {
		t.Fatal("Verify(removed key) succeeded after periodic refresh")
	}
	if got := ti.jwksRequests.Load(); got != 3 {
		t.Fatalf("JWKS requests after periodic refresh = %d, want 3", got)
	}
}

func (ti *testIssuer) issuer() string { return ti.server.URL }

func (ti *testIssuer) addRSA(kid string, pub *rsa.PublicKey) {
	ti.jwks.Keys = append(ti.jwks.Keys, jwkT{
		KeyType: "RSA",
		KeyID:   kid,
		RSAN:    b64url(pub.N.Bytes()),
		RSAE:    b64url(big.NewInt(int64(pub.E)).Bytes()),
	})
}

func (ti *testIssuer) addEC(t *testing.T, kid, crv string, pub *ecdsa.PublicKey) {
	t.Helper()
	// Use the ecdh bridge to read the point rather than the deprecated
	// ecdsa.PublicKey.X/Y fields. Bytes() returns the uncompressed SEC1 encoding
	// (0x04 || X || Y), each coordinate padded to the field size.
	ecdhPub, err := pub.ECDH()
	if err != nil {
		t.Fatalf("converting EC key to ECDH: %v", err)
	}
	raw := ecdhPub.Bytes()
	size := (pub.Curve.Params().BitSize + 7) / 8
	ti.jwks.Keys = append(ti.jwks.Keys, jwkT{
		KeyType:       "EC",
		KeyID:         kid,
		EllipticCurve: crv,
		EllipticX:     b64url(raw[1 : 1+size]),
		EllipticY:     b64url(raw[1+size:]),
	})
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Errorf("encoding test response: %v", err)
	}
}

// validClaims returns a set of claims that Verify should accept for issuer.
func validClaims(issuer string) map[string]any {
	now := time.Now()
	return map[string]any{
		"iss": issuer,
		"sub": "system:serviceaccount:ate-system:atelet",
		"aud": []string{testAudience},
		"exp": now.Add(time.Hour).Unix(),
		"nbf": now.Add(-time.Minute).Unix(),
		"iat": now.Add(-time.Minute).Unix(),
		"jti": "test-jti",
	}
}

// mintJWT signs a compact JWT the way a Kubernetes issuer would: RS* via
// PKCS1v15, ES* via a fixed-width r||s signature (not ASN.1), matching what
// verifySignature expects. A "" kid omits the header field.
func mintJWT(t *testing.T, alg, kid string, priv any, claims map[string]any) string {
	t.Helper()
	header := map[string]string{"alg": alg, "typ": "JWT"}
	if kid != "" {
		header["kid"] = kid
	}
	hb, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshaling header: %v", err)
	}
	cb, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshaling claims: %v", err)
	}
	signingInput := b64url(hb) + "." + b64url(cb)

	var sig []byte
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		digest, hashID := rsaDigest(t, alg, signingInput)
		sig, err = rsa.SignPKCS1v15(rand.Reader, k, hashID, digest)
		if err != nil {
			t.Fatalf("signing RSA: %v", err)
		}
	case *ecdsa.PrivateKey:
		digest := ecdsaDigest(t, alg, signingInput)
		r, s, err := ecdsa.Sign(rand.Reader, k, digest)
		if err != nil {
			t.Fatalf("signing ECDSA: %v", err)
		}
		size := (k.Curve.Params().BitSize + 7) / 8
		sig = make([]byte, 2*size)
		r.FillBytes(sig[:size])
		s.FillBytes(sig[size:])
	default:
		t.Fatalf("unsupported key type %T", priv)
	}
	return signingInput + "." + b64url(sig)
}

func rsaDigest(t *testing.T, alg, input string) ([]byte, crypto.Hash) {
	t.Helper()
	switch alg {
	case "RS256":
		d := sha256.Sum256([]byte(input))
		return d[:], crypto.SHA256
	case "RS384":
		d := sha512.Sum384([]byte(input))
		return d[:], crypto.SHA384
	case "RS512":
		d := sha512.Sum512([]byte(input))
		return d[:], crypto.SHA512
	default:
		t.Fatalf("unsupported RSA alg %q", alg)
		return nil, 0
	}
}

func ecdsaDigest(t *testing.T, alg, input string) []byte {
	t.Helper()
	switch alg {
	case "ES256":
		d := sha256.Sum256([]byte(input))
		return d[:]
	case "ES384":
		d := sha512.Sum384([]byte(input))
		return d[:]
	case "ES512":
		d := sha512.Sum512([]byte(input))
		return d[:]
	default:
		t.Fatalf("unsupported ECDSA alg %q", alg)
		return nil
	}
}

var (
	rsaKeyOnce sync.Once
	rsaKeyVal  *rsa.PrivateKey
)

// testRSAKey returns a process-wide 2048-bit RSA key, generated once, to keep the
// suite fast (RSA keygen dominates otherwise).
func testRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	rsaKeyOnce.Do(func() {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		rsaKeyVal = k
	})
	return rsaKeyVal
}

// TestVerifyECDSA is the regression test for the bug where EC keys could never be
// parsed from a JWKS (discoverKeysForIssuer's EC case had only a default error),
// even though verifySignature implements ES256/ES384/ES512.
func TestVerifyECDSA(t *testing.T) {
	cases := []struct {
		alg   string
		crv   string
		curve elliptic.Curve
	}{
		{"ES256", "P-256", elliptic.P256()},
		{"ES384", "P-384", elliptic.P384()},
		{"ES512", "P-521", elliptic.P521()},
	}
	for _, tc := range cases {
		t.Run(tc.alg, func(t *testing.T) {
			ti := newTestIssuer(t)
			key, err := ecdsa.GenerateKey(tc.curve, rand.Reader)
			if err != nil {
				t.Fatalf("GenerateKey: %v", err)
			}
			ti.addEC(t, "ec-1", tc.crv, &key.PublicKey)
			tok := mintJWT(t, tc.alg, "ec-1", key, validClaims(ti.issuer()))

			if _, err := NewVerifier(ti.issuer(), []string{testAudience}, ti.server.Client()).Verify(context.Background(), tok, time.Now()); err != nil {
				t.Fatalf("Verify(%s) = %v, want nil", tc.alg, err)
			}
		})
	}
}

// TestVerifyRejectsECKeyForRSAlg covers a path newly reachable now that EC keys
// load: an RS256 token whose kid names an EC key is rejected on the key-type
// mismatch, not verified.
func TestVerifyRejectsECKeyForRSAlg(t *testing.T) {
	ti := newTestIssuer(t)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	ti.addEC(t, "ec-1", "P-256", &ecKey.PublicKey)
	// RS256 header pointing at the EC key; the RSA signing key is irrelevant because
	// the key-type check fails before signature verification.
	tok := mintJWT(t, "RS256", "ec-1", testRSAKey(t), validClaims(ti.issuer()))
	if _, err := NewVerifier(ti.issuer(), []string{testAudience}, ti.server.Client()).Verify(context.Background(), tok, time.Now()); err == nil {
		t.Fatal("Verify accepted an RS256 token whose kid names an EC key")
	}
}

// TestVerifyMixedJWKS covers the more severe symptom of the same bug: before the
// fix, a single EC key anywhere in the JWKS made key discovery fail for the whole
// issuer, so even RS256 tokens from that issuer stopped verifying.
func TestVerifyMixedJWKS(t *testing.T) {
	ti := newTestIssuer(t)
	rsaKey := testRSAKey(t)
	ti.addRSA("rsa-1", &rsaKey.PublicKey)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	ti.addEC(t, "ec-1", "P-256", &ecKey.PublicKey)

	tok := mintJWT(t, "RS256", "rsa-1", rsaKey, validClaims(ti.issuer()))
	if _, err := NewVerifier(ti.issuer(), []string{testAudience}, ti.server.Client()).Verify(context.Background(), tok, time.Now()); err != nil {
		t.Fatalf("Verify with a mixed RSA+EC JWKS = %v, want nil", err)
	}
}

// TestVerifyUnusableJWKSKeySkipped pins the resilient discovery contract: a key the
// verifier cannot parse (here an unsupported P-192 curve) is skipped rather than
// failing discovery for the whole issuer, so a token signed by a supported key in
// the same JWKS still verifies — while a token that references the skipped key is
// still rejected (skipping is not accepting).
func TestVerifyUnusableJWKSKeySkipped(t *testing.T) {
	ti := newTestIssuer(t)
	rsaKey := testRSAKey(t)
	ti.addRSA("rsa-1", &rsaKey.PublicKey)
	ti.jwks.Keys = append(ti.jwks.Keys, jwkT{
		KeyType: "EC", KeyID: "ec-bad", EllipticCurve: "P-192", EllipticX: "AA", EllipticY: "AA",
	})

	good := mintJWT(t, "RS256", "rsa-1", rsaKey, validClaims(ti.issuer()))
	if _, err := NewVerifier(ti.issuer(), []string{testAudience}, ti.server.Client()).Verify(context.Background(), good, time.Now()); err != nil {
		t.Fatalf("Verify with an unusable key in the JWKS = %v, want nil (bad key should be skipped)", err)
	}

	referencesSkipped := mintJWT(t, "RS256", "ec-bad", rsaKey, validClaims(ti.issuer()))
	if _, err := NewVerifier(ti.issuer(), []string{testAudience}, ti.server.Client()).Verify(context.Background(), referencesSkipped, time.Now()); err == nil {
		t.Fatal("Verify accepted a token whose kid names a key that was skipped")
	}
}

// TestDiscoverKeysAllUnusable pins that an issuer whose JWKS contains only keys the
// verifier can't use fails at discovery (naming the cause) rather than returning an
// empty key set.
func TestDiscoverKeysAllUnusable(t *testing.T) {
	ti := newTestIssuer(t)
	ti.jwks.Keys = append(ti.jwks.Keys, jwkT{
		KeyType: "EC", KeyID: "ec-bad", EllipticCurve: "P-192", EllipticX: "AA", EllipticY: "AA",
	})
	_, err := discoverKeysForIssuer(context.Background(), ti.server.Client(), ti.issuer())
	if err == nil {
		t.Fatal("discoverKeysForIssuer returned nil error for an issuer with no usable keys")
	}
	if !strings.Contains(err.Error(), "no usable keys") {
		t.Errorf("error = %q, want it to name the cause (contain %q)", err, "no usable keys")
	}
}

// TestDiscoverKeysEmptyJWKS pins the distinct error for an issuer that publishes no
// keys at all, versus one whose keys are all unusable.
func TestDiscoverKeysEmptyJWKS(t *testing.T) {
	ti := newTestIssuer(t) // no keys registered
	_, err := discoverKeysForIssuer(context.Background(), ti.server.Client(), ti.issuer())
	if err == nil {
		t.Fatal("discoverKeysForIssuer returned nil error for an empty JWKS")
	}
	if !strings.Contains(err.Error(), "empty JWKS") {
		t.Errorf("error = %q, want it to mention %q", err, "empty JWKS")
	}
}

func TestParseJWKRejects(t *testing.T) {
	tests := []struct {
		name string
		jwk  jwkT
	}{
		{"no key ID", jwkT{KeyType: "RSA", KeyID: ""}},
		{"unknown key type", jwkT{KeyType: "OKP", KeyID: "k"}},
		{"unsupported EC curve", jwkT{KeyType: "EC", KeyID: "k", EllipticCurve: "P-192", EllipticX: "AA", EllipticY: "AA"}},
		{"EC missing coordinate", jwkT{KeyType: "EC", KeyID: "k", EllipticCurve: "P-256", EllipticX: "AA"}},
		{"EC malformed x", jwkT{KeyType: "EC", KeyID: "k", EllipticCurve: "P-256", EllipticX: "!!!", EllipticY: "AA"}},
		{"RSA malformed n", jwkT{KeyType: "RSA", KeyID: "k", RSAN: "!!!", RSAE: "AQAB"}},
		{"RSA oversized exponent", jwkT{KeyType: "RSA", KeyID: "k", RSAN: "Aw", RSAE: b64url(new(big.Int).Lsh(big.NewInt(1), 80).Bytes())}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseJWK(tc.jwk); err == nil {
				t.Errorf("parseJWK(%s) = nil, want error", tc.name)
			}
		})
	}
}

func TestEllipticCurveForJWK(t *testing.T) {
	for _, crv := range []string{"P-256", "P-384", "P-521"} {
		if _, err := ellipticCurveForJWK(crv); err != nil {
			t.Errorf("ellipticCurveForJWK(%q) = %v, want nil", crv, err)
		}
	}
	if _, err := ellipticCurveForJWK("P-192"); err == nil {
		t.Error("ellipticCurveForJWK(P-192) = nil, want error")
	}
}
