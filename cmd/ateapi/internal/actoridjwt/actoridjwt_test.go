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

package actoridjwt

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"strings"
	"testing"
)

func testRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating RSA key: %v", err)
	}
	return key
}

func testECKey(t *testing.T, curve elliptic.Curve) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatalf("generating EC key: %v", err)
	}
	return key
}

// TestSignKeyAlgorithmMismatch covers every algorithm branch with a key of the
// wrong type. The algorithm and the key come from the same signing pool entry,
// which is operator-supplied, so a mismatched pair is a configuration error and
// must surface as an error rather than taking the process down.
func TestSignKeyAlgorithmMismatch(t *testing.T) {
	rsaKey := testRSAKey(t)
	ecKey := testECKey(t, elliptic.P256())

	tests := []struct {
		name      string
		algorithm string
		key       crypto.PrivateKey
	}{
		{"RS256 with EC key", "RS256", ecKey},
		{"RS384 with EC key", "RS384", ecKey},
		{"RS512 with EC key", "RS512", ecKey},
		{"ES256 with RSA key", "ES256", rsaKey},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Sign(&WireClaims{Issuer: "https://issuer.example"}, tc.key, tc.algorithm, "kid-1")
			if err == nil {
				t.Fatalf("Sign(%s) error = nil, want an error; token = %q", tc.algorithm, got)
			}
			if got != "" {
				t.Errorf("Sign(%s) returned token %q alongside an error, want empty", tc.algorithm, got)
			}
		})
	}
}

// TestSignUnimplementedAlgorithm pins the pre-existing behavior for an algorithm
// the signer does not implement, so the mismatch handling above cannot swallow it.
func TestSignUnimplementedAlgorithm(t *testing.T) {
	if _, err := Sign(&WireClaims{}, testRSAKey(t), "HS256", "kid-1"); err == nil {
		t.Fatal("Sign(HS256) error = nil, want an error")
	}
}

// TestSignMatchingKey checks the algorithms that do have a matching key still
// produce a three-segment compact JWT.
func TestSignMatchingKey(t *testing.T) {
	tests := []struct {
		algorithm string
		key       crypto.PrivateKey
	}{
		{"RS256", testRSAKey(t)},
		{"RS384", testRSAKey(t)},
		{"RS512", testRSAKey(t)},
		{"ES256", testECKey(t, elliptic.P256())},
	}
	for _, tc := range tests {
		t.Run(tc.algorithm, func(t *testing.T) {
			token, err := Sign(&WireClaims{Issuer: "https://issuer.example"}, tc.key, tc.algorithm, "kid-1")
			if err != nil {
				t.Fatalf("Sign(%s) error = %v, want nil", tc.algorithm, err)
			}
			if n := len(strings.Split(token, ".")); n != 3 {
				t.Errorf("Sign(%s) produced %d segments, want 3", tc.algorithm, n)
			}
		})
	}
}

// TestSignES256WrongCurve keeps the existing curve check covered: ES256 is
// defined over P-256 only, and a P-384 key must be rejected, not signed.
func TestSignES256WrongCurve(t *testing.T) {
	if _, err := Sign(&WireClaims{}, testECKey(t, elliptic.P384()), "ES256", "kid-1"); err == nil {
		t.Fatal("Sign(ES256) with a P-384 key error = nil, want an error")
	}
}
