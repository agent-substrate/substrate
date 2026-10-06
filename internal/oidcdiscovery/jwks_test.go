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

package oidcdiscovery

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestJWKS(t *testing.T) {
	got, err := JWKS([]PublicKey{
		{ID: "rsa", Algorithm: "RS256", Key: rfcRSAKey(t)},
		{ID: "ec", Algorithm: "ES256", Key: rfcECKey(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	var set struct {
		Keys []map[string]string `json:"keys"`
	}
	if err := json.Unmarshal(got, &set); err != nil {
		t.Fatal(err)
	}
	want := []map[string]string{
		{"kty": "EC", "kid": "ec", "use": "sig", "alg": "ES256", "crv": "P-256", "x": rfcECX, "y": rfcECY},
		{"kty": "RSA", "kid": "rsa", "use": "sig", "alg": "RS256", "n": rfcRSAN, "e": rfcRSAE},
	}
	if diff := cmp.Diff(want, set.Keys); diff != "" {
		t.Errorf("JWKS keys (-want +got):\n%s", diff)
	}
}

// testJWK decodes the published fields independently of jwk.
type testJWK struct {
	KeyType   string `json:"kty"`
	KeyID     string `json:"kid"`
	Use       string `json:"use"`
	Algorithm string `json:"alg"`
	X         string `json:"x"`
	Y         string `json:"y"`
	N         string `json:"n"`
	E         string `json:"e"`
}

func TestJWKSRoundTrip(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keys := []PublicKey{
		{ID: "a", Algorithm: "RS384", Key: &rsaKey.PublicKey},
		{ID: "b", Algorithm: "ES256", Key: &ecKey.PublicKey},
	}
	data, err := JWKS(keys)
	if err != nil {
		t.Fatal(err)
	}

	var set struct {
		Keys []testJWK `json:"keys"`
	}
	if err := json.Unmarshal(data, &set); err != nil {
		t.Fatal(err)
	}
	if len(set.Keys) != len(keys) {
		t.Fatalf("got %d keys, want %d", len(set.Keys), len(keys))
	}
	for i, key := range keys {
		j := set.Keys[i]
		if j.KeyID != key.ID || j.Algorithm != key.Algorithm || j.Use != "sig" {
			t.Errorf("key %d: kid %q alg %q use %q, want %q %q sig", i, j.KeyID, j.Algorithm, j.Use, key.ID, key.Algorithm)
		}
		var decoded crypto.PublicKey
		switch j.KeyType {
		case "RSA":
			decoded = &rsa.PublicKey{
				N: new(big.Int).SetBytes(mustDecode(t, j.N)),
				E: int(new(big.Int).SetBytes(mustDecode(t, j.E)).Int64()),
			}
		case "EC":
			point := append([]byte{0x04}, mustDecode(t, j.X)...)
			decoded, err = ecdsa.ParseUncompressedPublicKey(elliptic.P256(), append(point, mustDecode(t, j.Y)...))
			if err != nil {
				t.Fatal(err)
			}
		}
		if !key.Key.(interface{ Equal(crypto.PublicKey) bool }).Equal(decoded) {
			t.Errorf("key %q did not decode back to the original public key", key.ID)
		}
	}
}

func TestJWKSRejects(t *testing.T) {
	rsaKey := rfcRSAKey(t)
	ecKey := rfcECKey(t)
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	edKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string][]PublicKey{
		"no keys":              nil,
		"missing ID":           {{Algorithm: "RS256", Key: rsaKey}},
		"duplicate ID":         {{ID: "k", Algorithm: "RS256", Key: rsaKey}, {ID: "k", Algorithm: "ES256", Key: ecKey}},
		"missing algorithm":    {{ID: "k", Key: rsaKey}},
		"ES256 with RSA key":   {{ID: "k", Algorithm: "ES256", Key: rsaKey}},
		"RS256 with EC key":    {{ID: "k", Algorithm: "RS256", Key: ecKey}},
		"HMAC algorithm":       {{ID: "k", Algorithm: "HS256", Key: rsaKey}},
		"P-384 key":            {{ID: "k", Algorithm: "ES256", Key: &p384.PublicKey}},
		"unsupported key type": {{ID: "k", Algorithm: "EdDSA", Key: edKey}},
	}
	for name, keys := range tests {
		if got, err := JWKS(keys); err == nil {
			t.Errorf("%s: JWKS = %s, want error", name, got)
		}
	}
}

// RFC 7518 section 6.2.1.2 requires each EC coordinate at the full curve size,
// even when it starts with a zero byte.
func TestJWKSPadsECCoordinates(t *testing.T) {
	var key *ecdsa.PrivateKey
	for key == nil {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if point, _ := k.PublicKey.Bytes(); point[1] == 0 || point[33] == 0 {
			key = k
		}
	}
	data, err := JWKS([]PublicKey{{ID: "k", Algorithm: "ES256", Key: &key.PublicKey}})
	if err != nil {
		t.Fatal(err)
	}
	var set struct {
		Keys []testJWK `json:"keys"`
	}
	if err := json.Unmarshal(data, &set); err != nil {
		t.Fatal(err)
	}
	if x, y := mustDecode(t, set.Keys[0].X), mustDecode(t, set.Keys[0].Y); len(x) != 32 || len(y) != 32 {
		t.Errorf("x and y are %d and %d bytes, want 32 each", len(x), len(y))
	}
}
