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

func TestThumbprintRFC7638Example(t *testing.T) {
	// The example key and result from RFC 7638 section 3.1.
	n := "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw"
	pub := &rsa.PublicKey{N: new(big.Int).SetBytes(mustDecode(t, n)), E: 65537}

	got, err := Thumbprint(pub)
	if err != nil {
		t.Fatal(err)
	}
	if want := "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs"; got != want {
		t.Errorf("Thumbprint = %q, want %q", got, want)
	}
}

func TestThumbprintEC(t *testing.T) {
	// The P-256 key from RFC 7517 appendix A.1. RFC 7638 has no EC example;
	// the expected value is go-jose's thumbprint of this key.
	point := append([]byte{0x04}, mustDecode(t, "MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4")...)
	point = append(point, mustDecode(t, "4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM")...)
	pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), point)
	if err != nil {
		t.Fatal(err)
	}

	got, err := Thumbprint(pub)
	if err != nil {
		t.Fatal(err)
	}
	if want := "cn-I_WNMClehiVp51i_0VpOENW1upEerA8sEam5hn-s"; got != want {
		t.Errorf("Thumbprint = %q, want %q", got, want)
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
