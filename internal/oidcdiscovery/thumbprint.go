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
	"fmt"

	jose "github.com/go-jose/go-jose/v4"
)

// Thumbprint returns the RFC 7638 SHA-256 thumbprint of an RSA or P-256 EC
// public key, base64url-encoded without padding.
func Thumbprint(pub crypto.PublicKey) (string, error) {
	switch k := pub.(type) {
	case *rsa.PublicKey:
	case *ecdsa.PublicKey:
		if k.Curve != elliptic.P256() {
			return "", fmt.Errorf("unsupported EC curve %s", k.Curve.Params().Name)
		}
	default:
		return "", fmt.Errorf("unsupported public key type %T", pub)
	}
	sum, err := (&jose.JSONWebKey{Key: pub}).Thumbprint(crypto.SHA256)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(sum), nil
}
