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

package localjwtauthority

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"math/big"
	"testing"
)

func mustDecode(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("decoding %q: %v", s, err)
	}
	return b
}

// The example RSA key from RFC 7638 section 3.1.
const (
	rfcRSAN = "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw"
	rfcRSAE = "AQAB"
)

// The P-256 key from RFC 7517 appendix A.1.
const (
	rfcECX = "MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4"
	rfcECY = "4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM"
)

func rfcRSAKey(t *testing.T) *rsa.PublicKey {
	t.Helper()
	return &rsa.PublicKey{N: new(big.Int).SetBytes(mustDecode(t, rfcRSAN)), E: 65537}
}

func rfcECKey(t *testing.T) *ecdsa.PublicKey {
	t.Helper()
	point := append([]byte{0x04}, mustDecode(t, rfcECX)...)
	pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), append(point, mustDecode(t, rfcECY)...))
	if err != nil {
		t.Fatal(err)
	}
	return pub
}

// The expected values come from openssl:
// openssl pkey -pubin -outform DER | openssl dgst -sha256 -binary | basenc --base64url
func TestThumbprint(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  any
		want string
	}{
		{"RSA", rfcRSAKey(t), "rTIyDPbFltiEsFOBulc6uo3dV0m03o9KI6efmondrrI"},
		{"P-256", rfcECKey(t), "UblEzfpUTUwyc6pr81BiWn3VO7tqcXIydPU4sZogd2A"},
	} {
		got, err := Thumbprint(tc.key)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("%s: Thumbprint = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestThumbprintRejectsUnsupportedKeys(t *testing.T) {
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	edPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, pub := range []any{&p384.PublicKey, edPub} {
		if got, err := Thumbprint(pub); err == nil {
			t.Errorf("Thumbprint(%T) = %q, want error", pub, got)
		}
	}
}
