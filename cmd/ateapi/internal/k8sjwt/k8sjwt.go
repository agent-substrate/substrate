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

// Package k8sjwt provides a JWT verifier tailored to Kubernetes.
package k8sjwt

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"slices"
	"strings"
	"time"
)

// KeyAndID wraps a crypto.PublicKey along with the key ID that will identify it during
// the verification process.
//
// Use GKEKeyIDForLocallyStoredKey and GKEKeyIDForCloudKMSKey to get the correct key ID the way we
// calculate it in GKE.
type KeyAndID struct {
	KeyID     string
	PublicKey crypto.PublicKey
}

type parseHeader struct {
	Type      string `json:"typ,omitempty"`
	Algorithm string `json:"alg,omitempty"`
	KeyID     string `json:"kid,omitempty"`
}

type parseClaims struct {
	// Claims from RFC7519
	Issuer     string          `json:"iss,omitempty"`
	Subject    string          `json:"sub,omitempty"`
	Audiences  json.RawMessage `json:"aud,omitempty"`
	Expiration float64         `json:"exp,omitempty"`
	NotBefore  float64         `json:"nbf,omitempty"`
	IssuedAt   float64         `json:"iat,omitempty"`
	JTI        string          `json:"jti,omitempty"`

	// Kubernetes bound token claims.
	BoundClaims parseBoundClaims `json:"kubernetes.io,omitempty"`

	// Kubernetes legacy token claims.
	LegacyNamespace          string `json:"kubernetes.io/serviceaccount/namespace,omitempty"`
	LegacySecretName         string `json:"kubernetes.io/serviceaccount/secret.name,omitempty"`
	LegacyServiceAccountName string `json:"kubernetes.io/serviceaccount/service-account.name,omitempty"`
	LegacyServiceAccountUID  string `json:"kubernetes.io/serviceaccount/service-account.uid,omitempty"`
}

type parseBoundClaims struct {
	Namespace      string                    `json:"namespace,omitempty"`
	Pod            parseBoundObjectReference `json:"pod,omitempty"`
	ServiceAccount parseBoundObjectReference `json:"serviceaccount,omitempty"`
	Secret         parseBoundObjectReference `json:"secret,omitempty"`
	Node           parseBoundObjectReference `json:"node,omitempty"`
	WarnAfter      float64                   `json:"warnafter,omitempty"`
}

type parseBoundObjectReference struct {
	Name string `json:"name,omitempty"`
	UID  string `json:"uid,omitempty"`
}

// KubernetesClaims covers the claims that can be extracted from a newer Kubernetes bound service
// account JWT.
type KubernetesClaims struct {
	// Claims from RFC7519
	Issuer     string
	Subject    string
	Audiences  []string
	Expiration time.Time
	NotBefore  time.Time
	IssuedAt   time.Time
	JTI        string

	Namespace string

	ServiceAccountName string
	ServiceAccountUID  string
	PodName            string
	PodUID             string
	SecretName         string
	SecretUID          string
	NodeName           string
	NodeUID            string

	WarnAfter time.Time
}

var permittedSkew = 5 * time.Minute

// Verify verifies and extracts claims from a Kubernetes JWT.
//
// For bound service account tokens, this function performs cryptographic verification of the JWT,
// checks the issuer and audience claims, and checks the time-binding claims. It *does not* check
// the object binding claims. If needed for your use case, you will need check the object bindings
// by connecting to the cluster and seeing if the object(s) the bindings name still exist within the
// cluster.
func Verify(ctx context.Context, jwt string, expectedIssuer, expectedAudience string, now time.Time) (*KubernetesClaims, error) {
	segments := strings.Split(jwt, ".")
	if len(segments) != 3 {
		return nil, fmt.Errorf("malformed JWT")
	}
	headerB64String := segments[0]
	payloadB64String := segments[1]
	signatureB64String := segments[2]

	headerBytes, err := base64.RawURLEncoding.DecodeString(headerB64String)
	if err != nil {
		return nil, fmt.Errorf("while base64 decoding header: %w", err)
	}

	signatureBytes, err := base64.RawURLEncoding.DecodeString(signatureB64String)
	if err != nil {
		return nil, fmt.Errorf("while base64 decoding signature: %w", err)
	}

	var header parseHeader
	if err := json.Unmarshal([]byte(headerBytes), &header); err != nil {
		return nil, fmt.Errorf("while unmarshaling header: %w", err)
	}

	// K8s JWTs don't set the `typ` header field. They might in the future, so we should tolerate the
	// spec-recommended value.
	switch header.Type {
	case "", "JWT": // OK
	default:
		return nil, fmt.Errorf("unexpected value in type header")
	}

	// Parse the payload. The payload is not verified at this point, so the only safe thing to do with
	// it is extract the issuer, check the issuer, and fetch keys from the issuer.
	//
	// Don't consider any other data in the payload until the call to verifySignature() below.
	payloadBytes, err := base64.RawURLEncoding.DecodeString(payloadB64String)
	if err != nil {
		return nil, fmt.Errorf("while base64-decoding payload: %w", err)
	}
	var rawClaims parseClaims
	if err := json.Unmarshal(payloadBytes, &rawClaims); err != nil {
		return nil, fmt.Errorf("while unmarshaling payload: %w", err)
	}

	if rawClaims.Issuer != expectedIssuer {
		return nil, fmt.Errorf("unexpected issuer %q", rawClaims.Issuer)
	}

	// TODO: Cache keys, and only fetch new keys if the JWT's key ID is not in the cache.
	keys, err := discoverKeysForIssuer(ctx, rawClaims.Issuer)
	if err != nil {
		return nil, fmt.Errorf("while discovering keys from issuer: %w", err)
	}

	// Find the key we should use for verification based on the key ID in the JWT header.
	if header.KeyID == "" {
		return nil, fmt.Errorf("key ID is required")
	}
	selectedKeyIndex := slices.IndexFunc(keys, func(k *KeyAndID) bool {
		return k.KeyID == header.KeyID
	})
	if selectedKeyIndex == -1 {
		return nil, fmt.Errorf("unknown key ID %q", header.KeyID)
	}
	selectedKey := keys[selectedKeyIndex].PublicKey

	// Warning: don't ever refer to the payload data (except "iss") above this point. We need to
	// ensure that we _never_ consider the contents of the payload when deciding how to perform
	// signature verification.
	if err := verifySignature(header.Algorithm, selectedKey, []byte(headerB64String+"."+payloadB64String), signatureBytes); err != nil {
		return nil, fmt.Errorf("while verifying JWT signature: %w", err)
	}

	// It is now safe to consider arbitrary data from the payload.
	//
	// At this point, the payload is mostly trusted. We know that it was really issued by the selected
	// verification key, but we need to check the issuer, audience binding, and time bindings to be
	// sure that it's really valid.

	// Because the JWT spec authors wanted to be fancy, we need to try to deserialize
	// rawClaims.Audience both as a single string and as a slice of strings.
	var singleAudience string
	var audiences []string
	if err := json.Unmarshal(rawClaims.Audiences, &singleAudience); err == nil { // err EQUALS nil
		audiences = []string{singleAudience}
	} else if err := json.Unmarshal(rawClaims.Audiences, &audiences); err == nil { // err EQUALS nil
	} else {
		return nil, fmt.Errorf("unable to parse audiences")
	}

	// Check that our expected audience is one of the audiences in the token
	if !slices.Contains(audiences, expectedAudience) {
		return nil, fmt.Errorf("token is not issued for expected audience")
	}

	expiration := time.Unix(int64(rawClaims.Expiration), 0)
	notBefore := time.Unix(int64(rawClaims.NotBefore), 0)
	issuedAt := time.Unix(int64(rawClaims.IssuedAt), 0)

	if expiration.Before(now.Add(-permittedSkew)) {
		return nil, fmt.Errorf("jwt has expired")
	}

	if notBefore.After(now.Add(permittedSkew)) {
		return nil, fmt.Errorf("jwt is not valid yet")
	}

	if issuedAt.After(now.Add(permittedSkew)) {
		return nil, fmt.Errorf("jwt claims to have been issued in the future")
	}

	return &KubernetesClaims{
		Issuer:     rawClaims.Issuer,
		Audiences:  audiences,
		Subject:    rawClaims.Subject,
		Expiration: expiration,
		NotBefore:  notBefore,
		IssuedAt:   issuedAt,
		JTI:        rawClaims.JTI,

		Namespace:          rawClaims.BoundClaims.Namespace,
		ServiceAccountName: rawClaims.BoundClaims.ServiceAccount.Name,
		ServiceAccountUID:  rawClaims.BoundClaims.ServiceAccount.UID,
		PodName:            rawClaims.BoundClaims.Pod.Name,
		PodUID:             rawClaims.BoundClaims.Pod.UID,
		SecretName:         rawClaims.BoundClaims.Secret.Name,
		SecretUID:          rawClaims.BoundClaims.Secret.UID,
		NodeName:           rawClaims.BoundClaims.Node.Name,
		NodeUID:            rawClaims.BoundClaims.Node.UID,

		WarnAfter: time.Unix(int64(rawClaims.BoundClaims.WarnAfter), 0),
	}, nil
}

func verifySignature(algorithm string, selectedKey crypto.PublicKey, toBeSignedBytes, signatureBytes []byte) error {
	switch algorithm {
	case "RS256":
		rsaKey, ok := selectedKey.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("requested key ID is not an RSA key")
		}
		toBeSignedDigest := hashBytes(crypto.SHA256.New(), toBeSignedBytes)
		if err := rsa.VerifyPKCS1v15(rsaKey, crypto.SHA256, toBeSignedDigest, signatureBytes); err != nil {
			return fmt.Errorf("while validating RSA PKCS1v15 signature: %w", err)
		}
	case "RS384":
		rsaKey, ok := selectedKey.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("requested key ID is not an RSA key")
		}
		toBeSignedDigest := hashBytes(crypto.SHA384.New(), toBeSignedBytes)
		if err := rsa.VerifyPKCS1v15(rsaKey, crypto.SHA384, toBeSignedDigest, signatureBytes); err != nil {
			return fmt.Errorf("while validating RSA PKCS1v15 signature: %w", err)
		}
	case "RS512":
		rsaKey, ok := selectedKey.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("requested key ID is not an RSA key")
		}
		toBeSignedDigest := hashBytes(crypto.SHA512.New(), toBeSignedBytes)
		if err := rsa.VerifyPKCS1v15(rsaKey, crypto.SHA512, toBeSignedDigest, signatureBytes); err != nil {
			return fmt.Errorf("while validating RSA PKCS1v15 signature: %w", err)
		}
	case "ES256":
		ecdsaKey, ok := selectedKey.(*ecdsa.PublicKey)
		if !ok || ecdsaKey.Curve != elliptic.P256() {
			return fmt.Errorf("requested key ID is not an ECDSA P256 key")
		}
		toBeSignedDigest := hashBytes(crypto.SHA256.New(), toBeSignedBytes)
		if len(signatureBytes) != 2*32 {
			return fmt.Errorf("invalid ecdsa signature")
		}
		r := big.NewInt(0).SetBytes(signatureBytes[:32])
		s := big.NewInt(0).SetBytes(signatureBytes[32:])
		if !ecdsa.Verify(ecdsaKey, toBeSignedDigest, r, s) {
			return fmt.Errorf("invalid ecdsa signature")
		}
	case "ES384":
		ecdsaKey, ok := selectedKey.(*ecdsa.PublicKey)
		if !ok || ecdsaKey.Curve != elliptic.P384() {
			return fmt.Errorf("requested key ID is not an ECDSA P256 key")
		}
		toBeSignedDigest := hashBytes(crypto.SHA384.New(), toBeSignedBytes)
		if len(signatureBytes) != 2*48 {
			return fmt.Errorf("invalid ecdsa signature")
		}
		r := big.NewInt(0).SetBytes(signatureBytes[:48])
		s := big.NewInt(0).SetBytes(signatureBytes[48:])
		if !ecdsa.Verify(ecdsaKey, toBeSignedDigest, r, s) {
			return fmt.Errorf("invalid ecdsa signature")
		}
	case "ES512":
		ecdsaKey, ok := selectedKey.(*ecdsa.PublicKey)
		if !ok || ecdsaKey.Curve != elliptic.P521() {
			return fmt.Errorf("requested key ID is not an ECDSA P256 key")
		}
		toBeSignedDigest := hashBytes(crypto.SHA512.New(), toBeSignedBytes)
		if len(signatureBytes) != 2*66 {
			return fmt.Errorf("invalid ecdsa signature")
		}
		r := big.NewInt(0).SetBytes(signatureBytes[:66])
		s := big.NewInt(0).SetBytes(signatureBytes[66:])
		if !ecdsa.Verify(ecdsaKey, toBeSignedDigest, r, s) {
			return fmt.Errorf("invalid ecdsa signature")
		}
	default:
		return fmt.Errorf("unsupported algorithm %q", algorithm)
	}

	return nil
}

func hashBytes(hasher hash.Hash, bytes []byte) []byte {
	hasher.Write(bytes)
	hash := hasher.Sum(nil)
	return hash[:]
}

type oidcConfigT struct {
	JWKSURI string `json:"jwks_uri"`
}

type jwkSetT struct {
	Keys []jwkT `json:"keys"`
}

type jwkT struct {
	KeyType string `json:"kty"`
	KeyID   string `json:"kid,omitempty"`

	EllipticCurve string `json:"crv,omitempty"`
	EllipticX     string `json:"x,omitempty"`
	EllipticY     string `json:"y,omitempty"`

	RSAN string `json:"n"`
	RSAE string `json:"e"`
}

func discoverKeysForIssuer(ctx context.Context, issuer string) ([]*KeyAndID, error) {
	var discoveryDocURL string
	if strings.HasSuffix(issuer, "/") {
		discoveryDocURL = issuer + ".well-known/openid-configuration"
	} else {
		discoveryDocURL = issuer + "/.well-known/openid-configuration"
	}

	oidcConfig, err := fetchJSON[oidcConfigT](discoveryDocURL)
	if err != nil {
		return nil, fmt.Errorf("while fetching OIDC Discovery document: %w", err)
	}

	slog.InfoContext(ctx, "Fetched discovery doc", slog.Any("doc", oidcConfig))

	jwkSet, err := fetchJSON[jwkSetT](oidcConfig.JWKSURI)
	if err != nil {
		return nil, fmt.Errorf("while fetching JWKS: %w", err)
	}

	slog.InfoContext(ctx, "Fetched JWK set", slog.Any("jwkSet", jwkSet))

	var ret []*KeyAndID
	for _, jwk := range jwkSet.Keys {
		if jwk.KeyID == "" {
			return nil, fmt.Errorf("JWKs endpoint returned key without key ID")
		}

		switch jwk.KeyType {
		case "EC":
			switch jwk.EllipticCurve {
			default:
				return nil, fmt.Errorf("unhandled elliptic curve %q", jwk.EllipticCurve)
			}

		case "RSA":
			nBytes, err := base64.RawURLEncoding.DecodeString(jwk.RSAN)
			if err != nil {
				return nil, fmt.Errorf("while base64-decoding n: %w", err)
			}
			n := &big.Int{}
			n.SetBytes(nBytes)

			eBytes, err := base64.RawURLEncoding.DecodeString(jwk.RSAE)
			if err != nil {
				return nil, fmt.Errorf("while base64-decoding e: %w", err)
			}
			e := &big.Int{}
			e.SetBytes(eBytes)

			ret = append(ret, &KeyAndID{
				KeyID: jwk.KeyID,
				PublicKey: &rsa.PublicKey{
					N: n,
					E: int(e.Int64()),
				},
			})

		default:
			return nil, fmt.Errorf("unhandled key type %q", jwk.KeyType)
		}
	}

	return ret, nil
}

func fetchJSON[T any](url string) (T, error) {
	var parsedBody T

	resp, err := http.Get(url)
	if err != nil {
		return parsedBody, fmt.Errorf("while making HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return parsedBody, fmt.Errorf("non-200 response code %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return parsedBody, fmt.Errorf("while reading response body: %w", err)
	}

	if err := json.Unmarshal(bodyBytes, &parsedBody); err != nil {
		return parsedBody, fmt.Errorf("while parsing response body: %w", err)
	}

	return parsedBody, nil
}
