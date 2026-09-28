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
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/big"
)

// Thumbprint returns the RFC 7638 SHA-256 thumbprint of an RSA or P-256 EC
// public key, base64url-encoded without padding.
func Thumbprint(pub crypto.PublicKey) (string, error) {
	// RFC 7638 hashes the key's required JWK members, in lexicographic order,
	// with no whitespace.
	var canonical string
	switch k := pub.(type) {
	case *rsa.PublicKey:
		canonical = fmt.Sprintf(`{"e":"%s","kty":"RSA","n":"%s"}`,
			b64(big.NewInt(int64(k.E)).Bytes()), b64(k.N.Bytes()))
	case *ecdsa.PublicKey:
		if k.Curve != elliptic.P256() {
			return "", fmt.Errorf("unsupported EC curve %s", k.Curve.Params().Name)
		}
		ecdhKey, err := k.ECDH()
		if err != nil {
			return "", fmt.Errorf("while reading EC public key: %w", err)
		}
		// Uncompressed SEC1 point: 0x04 || X || Y, each 32 bytes.
		point := ecdhKey.Bytes()
		canonical = fmt.Sprintf(`{"crv":"P-256","kty":"EC","x":"%s","y":"%s"}`,
			b64(point[1:33]), b64(point[33:]))
	default:
		return "", fmt.Errorf("unsupported public key type %T", pub)
	}
	sum := sha256.Sum256([]byte(canonical))
	return b64(sum[:]), nil
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
