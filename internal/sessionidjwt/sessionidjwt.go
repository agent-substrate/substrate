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

package sessionidjwt

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"time"
)

type Claims struct {
	// Claims from RFC7519
	Issuer     string
	Subject    string
	Audiences  []string
	Expiration time.Time
	NotBefore  time.Time
	IssuedAt   time.Time
	JTI        string

	// Claims from ADK's session model
	Substrate SubstrateClaims
}

type SubstrateClaims struct {
	AppID     string
	UserID    string
	SessionID string
}

type wireHeader struct {
	Type      string `json:"typ,omitempty"`
	Algorithm string `json:"alg,omitempty"`
	KeyID     string `json:"kid,omitempty"`
}

type WireClaims struct {
	// Claims from RFC7519
	Issuer     string          `json:"iss,omitempty"`
	Subject    string          `json:"sub,omitempty"`
	Audiences  json.RawMessage `json:"aud,omitempty"`
	Expiration float64         `json:"exp,omitempty"`
	NotBefore  float64         `json:"nbf,omitempty"`
	IssuedAt   float64         `json:"iat,omitempty"`
	JTI        string          `json:"jti,omitempty"`

	// Claims from ADK's session model.
	Substrate WireSubstrateClaims `json:"ate.dev,omitempty"`
}

type WireSubstrateClaims struct {
	AppID     string `json:"appID,omitempty"`
	UserID    string `json:"userID,omitempty"`
	SessionID string `json:"sessionID,omitempty"`
}

func ClaimsToWire(claims *Claims) (*WireClaims, error) {
	rawAudiences, err := json.Marshal(claims.Audiences)
	if err != nil {
		return nil, fmt.Errorf("while marshaling audience: %w", err)
	}

	wire := &WireClaims{
		Issuer:     claims.Issuer,
		Subject:    claims.Subject,
		Audiences:  rawAudiences,
		Expiration: float64(claims.Expiration.Unix()),
		NotBefore:  float64(claims.NotBefore.Unix()),
		IssuedAt:   float64(claims.IssuedAt.Unix()),
		JTI:        claims.JTI,
		Substrate: WireSubstrateClaims{
			claims.Substrate.AppID,
			claims.Substrate.UserID,
			claims.Substrate.SessionID,
		},
	}

	return wire, nil
}

// Sign
func Sign(wireClaims *WireClaims, signingKey crypto.PrivateKey, algorithm, keyID string) (string, error) {
	payloadBytes, err := json.Marshal(wireClaims)
	if err != nil {
		return "", fmt.Errorf("while marshaling payload: %w", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadBytes)

	rawHeader := wireHeader{
		Algorithm: algorithm,
		KeyID:     keyID,
	}
	headerBytes, err := json.Marshal(rawHeader)
	if err != nil {
		return "", fmt.Errorf("while marshaling header: %w", err)
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)

	toBeSigned := headerB64 + "." + payloadB64

	var sigBytes []byte
	switch algorithm {
	case "RS256":
		rsaKey := signingKey.(*rsa.PrivateKey)
		toBeSignedDigest := hashBytes(crypto.SHA256.New(), []byte(toBeSigned))
		sigBytes, err = rsa.SignPKCS1v15(rand.Reader, rsaKey, crypto.SHA256, toBeSignedDigest)
		if err != nil {
			return "", fmt.Errorf("while performing RSA PKCS1v15 signature: %w", err)
		}
	case "RS384":
		rsaKey := signingKey.(*rsa.PrivateKey)
		toBeSignedDigest := hashBytes(crypto.SHA384.New(), []byte(toBeSigned))
		sigBytes, err = rsa.SignPKCS1v15(rand.Reader, rsaKey, crypto.SHA384, toBeSignedDigest)
		if err != nil {
			return "", fmt.Errorf("while performing RSA PKCS1v15 signature: %w", err)
		}
	case "RS512":
		rsaKey := signingKey.(*rsa.PrivateKey)
		toBeSignedDigest := hashBytes(crypto.SHA512.New(), []byte(toBeSigned))
		sigBytes, err = rsa.SignPKCS1v15(rand.Reader, rsaKey, crypto.SHA512, toBeSignedDigest)
		if err != nil {
			return "", fmt.Errorf("while performing RSA PKCS1v15 signature: %w", err)
		}
	case "ES256":
		// JOSE ES256 defined at https://datatracker.ietf.org/doc/rfc7518/ section 3.4
		ecdsaKey := signingKey.(*ecdsa.PrivateKey)
		if ecdsaKey.Curve != elliptic.P256() {
			return "", fmt.Errorf("ES256 requires a P256 key")
		}
		toBeSignedDigest := hashBytes(crypto.SHA256.New(), []byte(toBeSigned))
		r, s, err := ecdsa.Sign(rand.Reader, ecdsaKey, toBeSignedDigest)
		if err != nil {
			return "", fmt.Errorf("while performing ecdsa signature: %w", err)
		}
		sigBytes = make([]byte, 2*32)
		r.FillBytes(sigBytes[:32])
		s.FillBytes(sigBytes[32:])
	default:
		return "", fmt.Errorf("unimplemented algorithm %q", algorithm)
	}

	sigB64 := base64.RawURLEncoding.EncodeToString(sigBytes)

	return toBeSigned + "." + sigB64, nil
}

func hashBytes(hasher hash.Hash, bytes []byte) []byte {
	hasher.Write(bytes)
	hash := hasher.Sum(nil)
	return hash[:]
}
