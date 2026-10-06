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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
)

// PublicKey is a verification key to publish in a JWK set.
type PublicKey struct {
	ID        string
	Algorithm string
	Key       crypto.PublicKey
}

// jwk is one key of a JWK set (RFC 7517), with the public parameters RFC 7518
// section 6 defines for RSA and EC keys.
type jwk struct {
	KeyType   string `json:"kty"`
	KeyID     string `json:"kid"`
	Use       string `json:"use"`
	Algorithm string `json:"alg"`
	Curve     string `json:"crv,omitempty"`
	X         string `json:"x,omitempty"`
	Y         string `json:"y,omitempty"`
	N         string `json:"n,omitempty"`
	E         string `json:"e,omitempty"`
}

// JWKS returns the JWK set, as JSON, that publishes keys for signature
// verification, sorted by key ID. Each key's algorithm must fit its type:
// ES256 for a P-256 EC key, or RS256, RS384, or RS512 for an RSA key.
func JWKS(keys []PublicKey) ([]byte, error) {
	if len(keys) == 0 {
		return nil, errors.New("no keys to publish")
	}
	var set struct {
		Keys []jwk `json:"keys"`
	}
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if key.ID == "" {
			return nil, errors.New("key has no ID")
		}
		if seen[key.ID] {
			return nil, fmt.Errorf("duplicate key ID %q", key.ID)
		}
		seen[key.ID] = true
		if !algorithmFits(key.Algorithm, key.Key) {
			return nil, fmt.Errorf("key %q: algorithm %q does not fit a %T", key.ID, key.Algorithm, key.Key)
		}
		j, err := toJWK(key)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", key.ID, err)
		}
		set.Keys = append(set.Keys, j)
	}
	slices.SortFunc(set.Keys, func(a, b jwk) int { return strings.Compare(a.KeyID, b.KeyID) })
	return json.Marshal(set)
}

// toJWK encodes a key that algorithmFits has accepted.
func toJWK(key PublicKey) (jwk, error) {
	b64 := base64.RawURLEncoding.EncodeToString
	j := jwk{KeyID: key.ID, Use: "sig", Algorithm: key.Algorithm}
	switch k := key.Key.(type) {
	case *rsa.PublicKey:
		j.KeyType = "RSA"
		j.N = b64(k.N.Bytes())
		j.E = b64(big.NewInt(int64(k.E)).Bytes())
	case *ecdsa.PublicKey:
		// The uncompressed point is 0x04 || x || y, each coordinate padded to
		// the curve size as RFC 7518 requires.
		point, err := k.Bytes()
		if err != nil {
			return jwk{}, err
		}
		size := (len(point) - 1) / 2
		j.KeyType, j.Curve = "EC", "P-256"
		j.X, j.Y = b64(point[1:1+size]), b64(point[1+size:])
	}
	return j, nil
}

func algorithmFits(algorithm string, pub crypto.PublicKey) bool {
	switch k := pub.(type) {
	case *ecdsa.PublicKey:
		return algorithm == "ES256" && k.Curve == elliptic.P256()
	case *rsa.PublicKey:
		return slices.Contains([]string{"RS256", "RS384", "RS512"}, algorithm)
	default:
		return false
	}
}
