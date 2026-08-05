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

package oidcauth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// KeyAndID wraps a crypto.PublicKey along with the key ID that will identify it during
// the verification process.
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
	Email      string          `json:"email,omitempty"`

	// Kubernetes bound token claims.
	BoundClaims parseBoundClaims `json:"kubernetes.io,omitempty"`
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

// OIDCClaims covers standard RFC7519/OIDC claims as well as optional Kubernetes bound claims.
type OIDCClaims struct {
	// Mandatory OIDC Specification Claims (RFC 7519 / OIDC Core 1.0)
	Issuer     string
	Subject    string
	Audiences  []string
	Expiration time.Time
	IssuedAt   time.Time

	// Optional Standard Claims
	Email     string
	NotBefore time.Time
	JTI       string

	// Kubernetes contains structured bound token metadata when present (nil for non-Kubernetes tokens).
	Kubernetes *KubernetesBoundClaims
}

// KubernetesBoundClaims contains metadata from a Kubernetes ServiceAccount token ("kubernetes.io" claim).
type KubernetesBoundClaims struct {
	Namespace          string
	ServiceAccountName string
	ServiceAccountUID  string
	PodName            string
	PodUID             string
	SecretName         string
	SecretUID          string
	NodeName           string
	NodeUID            string
	WarnAfter          time.Time
}

var (
	permittedSkew     = 5 * time.Minute
	defaultHTTPClient = &http.Client{Timeout: 10 * time.Second}
)

// Verify verifies and extracts claims from a Kubernetes or external IDP OIDC JWT.
func Verify(ctx context.Context, httpClient *http.Client, jwt string, expectedIssuer string, expectedAudiences []string, now time.Time) (*OIDCClaims, error) {
	segments := strings.Split(jwt, ".")
	if len(segments) != 3 {
		return nil, fmt.Errorf("malformed JWT")
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(segments[0])
	if err != nil {
		return nil, fmt.Errorf("while base64 decoding header: %w", err)
	}

	signatureBytes, err := base64.RawURLEncoding.DecodeString(segments[2])
	if err != nil {
		return nil, fmt.Errorf("while base64 decoding signature: %w", err)
	}

	var header parseHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("while unmarshaling header: %w", err)
	}

	// RFC 7519 section 5.2 states that typ claims are case insensitive.
	// RFC 7519 section 5.2 states that the "JWT" typ claim MAY be omitted.
	// If present, it MUST be "JWT" or "application/jwt".
	typ := strings.ToLower(header.Type)
	if typ != "" && typ != "jwt" && typ != "application/jwt" {
		return nil, fmt.Errorf("unexpected value in type header")
	}

	if httpClient == nil {
		httpClient = defaultHTTPClient
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(segments[1])
	if err != nil {
		return nil, fmt.Errorf("while base64-decoding payload: %w", err)
	}

	var rawClaims parseClaims
	if err := json.Unmarshal(payloadBytes, &rawClaims); err != nil {
		return nil, fmt.Errorf("while unmarshaling payload: %w", err)
	}

	if !issuerMatches(rawClaims.Issuer, expectedIssuer) {
		return nil, fmt.Errorf("unexpected issuer %q", rawClaims.Issuer)
	}

	keys, err := discoverKeysForIssuer(ctx, httpClient, expectedIssuer)
	if err != nil {
		return nil, fmt.Errorf("while discovering keys from issuer: %w", err)
	}

	if header.KeyID == "" {
		return nil, fmt.Errorf("key ID is required")
	}

	selectedKey, ok := keys[header.KeyID]
	if !ok {
		return nil, fmt.Errorf("unknown key ID %q", header.KeyID)
	}

	toBeSignedBytes := []byte(fmt.Sprintf("%s.%s", segments[0], segments[1]))

	if err := verifySignature(header.Algorithm, selectedKey, toBeSignedBytes, signatureBytes); err != nil {
		return nil, fmt.Errorf("while verifying JWT signature: %w", err)
	}

	audiences, err := extractAudiences(rawClaims.Audiences)
	if err != nil {
		return nil, fmt.Errorf("unable to parse audiences")
	}

	if len(expectedAudiences) == 0 {
		return nil, fmt.Errorf("at least one expected audience is required")
	}

	matchedAudience := false
	for _, expectedAudience := range expectedAudiences {
		for _, audience := range audiences {
			if audience == expectedAudience {
				matchedAudience = true
				break
			}
		}
		if matchedAudience {
			break
		}
	}
	if !matchedAudience {
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

	var k8sClaims *KubernetesBoundClaims
	if rawClaims.BoundClaims.Namespace != "" || rawClaims.BoundClaims.ServiceAccount.Name != "" ||
		rawClaims.BoundClaims.Pod.Name != "" || rawClaims.BoundClaims.Node.Name != "" ||
		rawClaims.BoundClaims.Secret.Name != "" {
		k8sClaims = &KubernetesBoundClaims{
			Namespace:          rawClaims.BoundClaims.Namespace,
			ServiceAccountName: rawClaims.BoundClaims.ServiceAccount.Name,
			ServiceAccountUID:  rawClaims.BoundClaims.ServiceAccount.UID,
			PodName:            rawClaims.BoundClaims.Pod.Name,
			PodUID:             rawClaims.BoundClaims.Pod.UID,
			SecretName:         rawClaims.BoundClaims.Secret.Name,
			SecretUID:          rawClaims.BoundClaims.Secret.UID,
			NodeName:           rawClaims.BoundClaims.Node.Name,
			NodeUID:            rawClaims.BoundClaims.Node.UID,
			WarnAfter:          time.Unix(int64(rawClaims.BoundClaims.WarnAfter), 0),
		}
	}

	return &OIDCClaims{
		Issuer:     rawClaims.Issuer,
		Audiences:  audiences,
		Subject:    rawClaims.Subject,
		Email:      rawClaims.Email,
		Expiration: expiration,
		NotBefore:  notBefore,
		IssuedAt:   issuedAt,
		JTI:        rawClaims.JTI,
		Kubernetes: k8sClaims,
	}, nil
}

func verifySignature(algorithm string, selectedKey crypto.PublicKey, toBeSignedBytes, signatureBytes []byte) error {
	switch algorithm {
	case "RS256":
		rsaKey, ok := selectedKey.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("requested key ID is not an RSA key")
		}
		digest := crypto.SHA256.New()
		digest.Write(toBeSignedBytes)
		if err := rsa.VerifyPKCS1v15(rsaKey, crypto.SHA256, digest.Sum(nil), signatureBytes); err != nil {
			return fmt.Errorf("while validating RSA PKCS1v15 signature: %w", err)
		}
	case "RS384":
		rsaKey, ok := selectedKey.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("requested key ID is not an RSA key")
		}
		digest := crypto.SHA384.New()
		digest.Write(toBeSignedBytes)
		if err := rsa.VerifyPKCS1v15(rsaKey, crypto.SHA384, digest.Sum(nil), signatureBytes); err != nil {
			return fmt.Errorf("while validating RSA PKCS1v15 signature: %w", err)
		}
	case "RS512":
		rsaKey, ok := selectedKey.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("requested key ID is not an RSA key")
		}
		digest := crypto.SHA512.New()
		digest.Write(toBeSignedBytes)
		if err := rsa.VerifyPKCS1v15(rsaKey, crypto.SHA512, digest.Sum(nil), signatureBytes); err != nil {
			return fmt.Errorf("while validating RSA PKCS1v15 signature: %w", err)
		}
	case "ES256":
		ecKey, ok := selectedKey.(*ecdsa.PublicKey)
		if !ok {
			return fmt.Errorf("requested key ID is not an ECDSA P256 key")
		}
		r, s, err := parseECDSASignature(signatureBytes)
		if err != nil {
			return fmt.Errorf("invalid ecdsa signature")
		}
		digest := crypto.SHA256.New()
		digest.Write(toBeSignedBytes)
		if !ecdsa.Verify(ecKey, digest.Sum(nil), r, s) {
			return fmt.Errorf("invalid ecdsa signature")
		}
	case "ES384":
		ecKey, ok := selectedKey.(*ecdsa.PublicKey)
		if !ok {
			return fmt.Errorf("requested key ID is not an ECDSA P384 key")
		}
		r, s, err := parseECDSASignature(signatureBytes)
		if err != nil {
			return fmt.Errorf("invalid ecdsa signature")
		}
		digest := crypto.SHA384.New()
		digest.Write(toBeSignedBytes)
		if !ecdsa.Verify(ecKey, digest.Sum(nil), r, s) {
			return fmt.Errorf("invalid ecdsa signature")
		}
	case "ES512":
		ecKey, ok := selectedKey.(*ecdsa.PublicKey)
		if !ok {
			return fmt.Errorf("requested key ID is not an ECDSA P521 key")
		}
		r, s, err := parseECDSASignature(signatureBytes)
		if err != nil {
			return fmt.Errorf("invalid ecdsa signature")
		}
		digest := crypto.SHA512.New()
		digest.Write(toBeSignedBytes)
		if !ecdsa.Verify(ecKey, digest.Sum(nil), r, s) {
			return fmt.Errorf("invalid ecdsa signature")
		}
	default:
		return fmt.Errorf("unsupported algorithm %q", algorithm)
	}
	return nil
}

func parseECDSASignature(sig []byte) (*big.Int, *big.Int, error) {
	if len(sig)%2 != 0 {
		return nil, nil, fmt.Errorf("ECDSA signature length must be even")
	}
	half := len(sig) / 2
	r := new(big.Int).SetBytes(sig[:half])
	s := new(big.Int).SetBytes(sig[half:])
	return r, s, nil
}

func extractAudiences(rawAud json.RawMessage) ([]string, error) {
	if len(rawAud) == 0 {
		return nil, nil
	}
	var singleAud string
	if err := json.Unmarshal(rawAud, &singleAud); err == nil {
		return []string{singleAud}, nil
	}
	var multiAud []string
	if err := json.Unmarshal(rawAud, &multiAud); err == nil {
		return multiAud, nil
	}
	return nil, fmt.Errorf("invalid audience format")
}

var (
	keysCacheLock sync.RWMutex
	keysCache     = make(map[string]map[string]crypto.PublicKey)
)

func discoverKeysForIssuer(ctx context.Context, httpClient *http.Client, issuer string) (map[string]crypto.PublicKey, error) {
	keysCacheLock.RLock()
	keys, ok := keysCache[issuer]
	keysCacheLock.RUnlock()
	if ok {
		return keys, nil
	}

	keysCacheLock.Lock()
	defer keysCacheLock.Unlock()

	if keys, ok := keysCache[issuer]; ok {
		return keys, nil
	}

	issuerURL := issuer
	if !strings.HasPrefix(issuerURL, "http://") && !strings.HasPrefix(issuerURL, "https://") {
		issuerURL = "https://" + issuerURL
	}

	discURL, err := url.JoinPath(issuerURL, "/.well-known/openid-configuration")
	if err != nil {
		return nil, fmt.Errorf("invalid issuer URL: %w", err)
	}

	var discoveryDoc struct {
		JWKSURI string `json:"jwks_uri"`
	}
	if err := fetchJSON(ctx, httpClient, discURL, &discoveryDoc); err != nil {
		return nil, fmt.Errorf("while fetching OIDC Discovery document: %w", err)
	}
	slog.InfoContext(ctx, "Fetched discovery doc", slog.Any("doc", discoveryDoc))

	var jwkSet struct {
		Keys []struct {
			KeyType       string `json:"kty"`
			KeyID         string `json:"kid"`
			EllipticCurve string `json:"crv"`
			EllipticX     string `json:"x"`
			EllipticY     string `json:"y"`
			RSAN          string `json:"n"`
			RSAE          string `json:"e"`
		} `json:"keys"`
	}
	if err := fetchJSON(ctx, httpClient, discoveryDoc.JWKSURI, &jwkSet); err != nil {
		return nil, fmt.Errorf("while fetching JWKS: %w", err)
	}
	slog.InfoContext(ctx, "Fetched JWK set", slog.Any("jwkSet", fmt.Sprintf("%+v", jwkSet)))

	keys = make(map[string]crypto.PublicKey)
	var skipped int
	for _, jwk := range jwkSet.Keys {
		pubKey, err := parseJWK(jwk.KeyType, jwk.KeyID, jwk.EllipticCurve, jwk.EllipticX, jwk.EllipticY, jwk.RSAN, jwk.RSAE)
		if err != nil {
			skipped++
			slog.WarnContext(ctx, "Skipping unusable JWK", slog.String("kid", jwk.KeyID), slog.Any("err", err))
			continue
		}
		keys[jwk.KeyID] = pubKey
	}

	if len(keys) == 0 {
		if len(jwkSet.Keys) == 0 {
			return nil, fmt.Errorf("issuer %q published an empty JWKS", issuer)
		}
		return nil, fmt.Errorf("no usable keys in JWKS for issuer %q (%d skipped)", issuer, skipped)
	}

	keysCache[issuer] = keys
	return keys, nil
}

func parseJWK(kty, kid, crv, x, y, n, e string) (crypto.PublicKey, error) {
	if kid == "" {
		return nil, fmt.Errorf("JWK has no key ID")
	}

	switch kty {
	case "EC":
		curve, err := ellipticCurveForJWK(crv)
		if err != nil {
			return nil, err
		}
		if x == "" || y == "" {
			return nil, fmt.Errorf("EC JWK is missing the x or y coordinate")
		}
		xb, err := base64.RawURLEncoding.DecodeString(x)
		if err != nil {
			return nil, fmt.Errorf("while base64-decoding EC x coordinate: %w", err)
		}
		yb, err := base64.RawURLEncoding.DecodeString(y)
		if err != nil {
			return nil, fmt.Errorf("while base64-decoding EC y coordinate: %w", err)
		}
		xInt := new(big.Int).SetBytes(xb)
		yInt := new(big.Int).SetBytes(yb)

		if !curve.IsOnCurve(xInt, yInt) {
			return nil, fmt.Errorf("EC JWK coordinate is out of range for curve %q", crv)
		}
		return &ecdsa.PublicKey{Curve: curve, X: xInt, Y: yInt}, nil

	case "RSA":
		nb, err := base64.RawURLEncoding.DecodeString(n)
		if err != nil {
			return nil, fmt.Errorf("while base64-decoding n: %w", err)
		}
		eb, err := base64.RawURLEncoding.DecodeString(e)
		if err != nil {
			return nil, fmt.Errorf("while base64-decoding e: %w", err)
		}
		eInt := new(big.Int).SetBytes(eb)
		return &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: int(eInt.Int64())}, nil

	default:
		return nil, fmt.Errorf("unhandled key type %q", kty)
	}
}

func ellipticCurveForJWK(crv string) (elliptic.Curve, error) {
	switch crv {
	case "P-256":
		return elliptic.P256(), nil
	case "P-384":
		return elliptic.P384(), nil
	case "P-521":
		return elliptic.P521(), nil
	default:
		return nil, fmt.Errorf("unhandled elliptic curve %q", crv)
	}
}

func fetchJSON(ctx context.Context, httpClient *http.Client, urlStr string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return fmt.Errorf("while constructing HTTP request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("while making HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-200 response code %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("while reading response body: %w", err)
	}

	if err := json.Unmarshal(bodyBytes, target); err != nil {
		return fmt.Errorf("while unmarshaling response body: %w", err)
	}

	return nil
}

func issuerMatches(actual, expected string) bool {
	if actual == expected {
		return true
	}
	// Strip "https://" scheme for normalization comparisons (e.g. Google IDP)
	actNorm := strings.TrimPrefix(actual, "https://")
	expNorm := strings.TrimPrefix(expected, "https://")
	return actNorm == expNorm
}
