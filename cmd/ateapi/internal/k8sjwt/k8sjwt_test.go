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

package k8sjwt

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

	utilcache "k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/utils/clock"
)

const testAudience = "ate-api"

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// testIssuer serves the OIDC discovery document and a JWKS built from the keys
// registered on it, standing in for a Kubernetes API server's OIDC endpoints.
// The hit counters let tests observe how often Verify fetches from it.
type testIssuer struct {
	server        *httptest.Server
	jwks          jwkSetT
	discoveryHits atomic.Int64
	jwksHits      atomic.Int64
}

func newTestIssuer(t *testing.T) *testIssuer {
	t.Helper()
	ti := &testIssuer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		ti.discoveryHits.Add(1)
		writeJSON(t, w, oidcConfigT{JWKSURI: ti.server.URL + "/jwks"})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		ti.jwksHits.Add(1)
		writeJSON(t, w, ti.jwks)
	})
	ti.server = httptest.NewServer(mux)
	t.Cleanup(ti.server.Close)
	return ti
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

			if _, err := Verify(context.Background(), ti.server.Client(), nil, tok, ti.issuer(), testAudience, time.Now()); err != nil {
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
	if _, err := Verify(context.Background(), ti.server.Client(), nil, tok, ti.issuer(), testAudience, time.Now()); err == nil {
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
	if _, err := Verify(context.Background(), ti.server.Client(), nil, tok, ti.issuer(), testAudience, time.Now()); err != nil {
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
	if _, err := Verify(context.Background(), ti.server.Client(), nil, good, ti.issuer(), testAudience, time.Now()); err != nil {
		t.Fatalf("Verify with an unusable key in the JWKS = %v, want nil (bad key should be skipped)", err)
	}

	referencesSkipped := mintJWT(t, "RS256", "ec-bad", rsaKey, validClaims(ti.issuer()))
	if _, err := Verify(context.Background(), ti.server.Client(), nil, referencesSkipped, ti.issuer(), testAudience, time.Now()); err == nil {
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

// fakeClock embeds clock.RealClock so it satisfies clock.Clock (which requires
// After, NewTimer, Sleep, Tick in addition to Now/Since), while overriding
// Now/Since to a controllable value. utilcache.Expiring only ever calls
// Now(), so the promoted real methods are never invoked by the cache.
type fakeClock struct {
	clock.RealClock
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Since(ts time.Time) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now.Sub(ts)
}

func (f *fakeClock) step(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// newKeyCacheWithClock is the test-only constructor that injects a clock so
// cache expiry can be driven deterministically.
func newKeyCacheWithClock(ttl time.Duration, clk clock.Clock) *KeyCache {
	return &KeyCache{ttl: ttl, cache: utilcache.NewExpiringWithClock(clk)}
}

func TestVerifyKeyCacheHit(t *testing.T) {
	ti := newTestIssuer(t)
	key := testRSAKey(t)
	ti.addRSA("rsa-1", &key.PublicKey)
	tok := mintJWT(t, "RS256", "rsa-1", key, validClaims(ti.issuer()))
	cache := NewKeyCache(time.Hour)
	now := time.Now()

	for i := 0; i < 3; i++ {
		if _, err := Verify(context.Background(), ti.server.Client(), cache, tok, ti.issuer(), testAudience, now); err != nil {
			t.Fatalf("Verify #%d = %v, want nil", i, err)
		}
	}
	if got := ti.discoveryHits.Load(); got != 1 {
		t.Errorf("discovery hits = %d, want 1", got)
	}
	if got := ti.jwksHits.Load(); got != 1 {
		t.Errorf("jwks hits = %d, want 1", got)
	}
}

func TestVerifyKeyCacheExpiry(t *testing.T) {
	ti := newTestIssuer(t)
	key := testRSAKey(t)
	ti.addRSA("rsa-1", &key.PublicKey)
	tok := mintJWT(t, "RS256", "rsa-1", key, validClaims(ti.issuer()))
	fc := newFakeClock(time.Now())
	cache := newKeyCacheWithClock(time.Minute, fc)
	now := time.Now()

	if _, err := Verify(context.Background(), ti.server.Client(), cache, tok, ti.issuer(), testAudience, now); err != nil {
		t.Fatalf("Verify = %v, want nil", err)
	}
	// Advance the cache's clock past its 1-minute TTL so the entry expires;
	// the JWT itself is valid for an hour, so stepping a couple minutes is fine.
	fc.step(2 * time.Minute)
	if _, err := Verify(context.Background(), ti.server.Client(), cache, tok, ti.issuer(), testAudience, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("Verify after cache expiry = %v, want nil", err)
	}
	if got := ti.discoveryHits.Load(); got != 2 {
		t.Errorf("discovery hits = %d, want 2", got)
	}
	if got := ti.jwksHits.Load(); got != 2 {
		t.Errorf("jwks hits = %d, want 2", got)
	}
}

func TestVerifyKeyCacheRefreshesOnUnknownKeyID(t *testing.T) {
	ti := newTestIssuer(t)
	key := testRSAKey(t)
	ti.addRSA("k1", &key.PublicKey)
	cache := NewKeyCache(time.Hour)
	now := time.Now()

	tok1 := mintJWT(t, "RS256", "k1", key, validClaims(ti.issuer()))
	if _, err := Verify(context.Background(), ti.server.Client(), cache, tok1, ti.issuer(), testAudience, now); err != nil {
		t.Fatalf("Verify(k1) = %v, want nil", err)
	}

	// Simulate a rotation: the issuer publishes a new key after the cache
	// was populated. The unknown kid must trigger a refresh, not a rejection.
	ti.addRSA("k2", &key.PublicKey)
	tok2 := mintJWT(t, "RS256", "k2", key, validClaims(ti.issuer()))
	if _, err := Verify(context.Background(), ti.server.Client(), cache, tok2, ti.issuer(), testAudience, now); err != nil {
		t.Fatalf("Verify after key rotation = %v, want nil", err)
	}
	if got := ti.jwksHits.Load(); got != 2 {
		t.Errorf("jwks hits = %d, want 2 (one refresh on unknown kid)", got)
	}

	// The refreshed set is cached again: no further fetches.
	if _, err := Verify(context.Background(), ti.server.Client(), cache, tok2, ti.issuer(), testAudience, now); err != nil {
		t.Fatalf("Verify(tok2) after refresh = %v, want nil", err)
	}
	if got := ti.discoveryHits.Load(); got != 2 {
		t.Errorf("discovery hits = %d, want 2", got)
	}
	if got := ti.jwksHits.Load(); got != 2 {
		t.Errorf("jwks hits = %d, want 2", got)
	}
}

func TestVerifyNilCacheFetchesEveryTime(t *testing.T) {
	ti := newTestIssuer(t)
	key := testRSAKey(t)
	ti.addRSA("rsa-1", &key.PublicKey)
	tok := mintJWT(t, "RS256", "rsa-1", key, validClaims(ti.issuer()))

	for i := 0; i < 2; i++ {
		if _, err := Verify(context.Background(), ti.server.Client(), nil, tok, ti.issuer(), testAudience, time.Now()); err != nil {
			t.Fatalf("Verify #%d = %v, want nil", i, err)
		}
	}
	if got := ti.discoveryHits.Load(); got != 2 {
		t.Errorf("discovery hits = %d, want 2", got)
	}
	if got := ti.jwksHits.Load(); got != 2 {
		t.Errorf("jwks hits = %d, want 2", got)
	}
}

func TestVerifyKeyCacheNonPositiveTTLDisables(t *testing.T) {
	ti := newTestIssuer(t)
	key := testRSAKey(t)
	ti.addRSA("rsa-1", &key.PublicKey)
	tok := mintJWT(t, "RS256", "rsa-1", key, validClaims(ti.issuer()))
	cache := NewKeyCache(0) // non-positive TTL: every lookup misses

	for i := 0; i < 2; i++ {
		if _, err := Verify(context.Background(), ti.server.Client(), cache, tok, ti.issuer(), testAudience, time.Now()); err != nil {
			t.Fatalf("Verify #%d = %v, want nil", i, err)
		}
	}
	if got := ti.discoveryHits.Load(); got != 2 {
		t.Errorf("discovery hits = %d, want 2", got)
	}
	if got := ti.jwksHits.Load(); got != 2 {
		t.Errorf("jwks hits = %d, want 2", got)
	}
}
