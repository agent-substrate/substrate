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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	jose "github.com/go-jose/go-jose/v4"
)

// PublicKey is a verification key to publish in a JWK set.
type PublicKey struct {
	ID        string
	Algorithm string
	Key       crypto.PublicKey
}

// JWKS returns the JWK set, as JSON, that publishes keys for signature
// verification, sorted by key ID. Each key's algorithm must fit its type:
// ES256 for a P-256 EC key, or RS256, RS384, or RS512 for an RSA key.
func JWKS(keys []PublicKey) ([]byte, error) {
	if len(keys) == 0 {
		return nil, errors.New("no keys to publish")
	}
	var set jose.JSONWebKeySet
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
		set.Keys = append(set.Keys, jose.JSONWebKey{Key: key.Key, KeyID: key.ID, Algorithm: key.Algorithm, Use: "sig"})
	}
	slices.SortFunc(set.Keys, func(a, b jose.JSONWebKey) int { return strings.Compare(a.KeyID, b.KeyID) })
	return json.Marshal(set)
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
