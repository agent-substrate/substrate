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
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
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

var (
	permittedSkew     = 5 * time.Minute
	defaultHTTPClient = &http.Client{Timeout: 10 * time.Second}
)

// keyRefetchInterval is the minimum time between key fetches for a single
// issuer. The keyID that triggers a refetch comes from an unverified token,
// so without this floor a client could force an OIDC discovery round-trip per
// request just by minting tokens with bogus keyIDs.
const keyRefetchInterval = 10 * time.Second

// keyMaxAge is how long cached keys are used before a background refresh.
// Unknown-keyID refetches only pick up keys ADDED to the issuer's JWKS;
// without a periodic refresh, a key REMOVED from the JWKS (revoked) would be
// trusted until process restart, because tokens signed by it keep hitting the
// cache. keyMaxAge bounds that exposure. Refreshes are asynchronous: requests
// are served from the current cache while the fetch runs, so this adds no
// latency to any request.
const keyMaxAge = 5 * time.Minute

// CachedVerifier verifies Kubernetes JWTs, caching each issuer's verification
// keys in memory. Keys are fetched on first use, refetched synchronously when
// a JWT presents an unknown keyID (i.e. on key rotation, rate-limited to one
// fetch per issuer per keyRefetchInterval), and refreshed in the background
// once cached keys are older than keyMaxAge (so revoked keys age out).
type CachedVerifier struct {
	httpClient *http.Client

	// flight coalesces concurrent key fetches for the same issuer.
	flight singleflight.Group

	mu sync.Mutex
	// issuers holds one entry per issuer that has been used for verification.
	// Its size is bounded by the number of distinct trusted issuers the
	// process is configured with (in practice one): Verify rejects tokens
	// whose issuer differs from expectedIssuer before consulting the cache,
	// so unverified traffic cannot create entries.
	issuers map[string]*issuerKeys
}

type issuerKeys struct {
	keys      []*KeyAndID
	lastFetch time.Time
	// refreshing is true while a background max-age refresh is in flight,
	// so cache hits don't spawn one goroutine each until it completes.
	refreshing bool
}

// NewCachedVerifier creates a CachedVerifier. httpClient is used for OIDC
// discovery and JWKS fetches; nil uses a default client with a whole-request
// timeout.
func NewCachedVerifier(httpClient *http.Client) *CachedVerifier {
	return &CachedVerifier{
		httpClient: httpClient,
		issuers:    make(map[string]*issuerKeys),
	}
}

// keyForIssuer returns the issuer's verification key with the given keyID,
// (re)fetching the issuer's keys if the keyID is not already cached.
func (v *CachedVerifier) keyForIssuer(ctx context.Context, issuer, keyID string, now time.Time) (crypto.PublicKey, error) {
	// Can't defer Unlock(): the lock must be dropped before the network fetch
	// below and re-taken afterwards, so it is managed manually at each return.
	v.mu.Lock()
	state, ok := v.issuers[issuer]
	if !ok {
		state = &issuerKeys{}
		v.issuers[issuer] = state
	}
	if key := findKey(state.keys, keyID); key != nil {
		// Serve from cache, but refresh in the background once the keys pass
		// keyMaxAge so removed (revoked) keys age out; see keyMaxAge.
		startRefresh := now.Sub(state.lastFetch) > keyMaxAge && !state.refreshing
		if startRefresh {
			state.refreshing = true
		}
		v.mu.Unlock()
		if startRefresh {
			go v.refreshIssuerKeys(issuer, state, now)
		}
		return key.PublicKey, nil
	}
	lastFetch := state.lastFetch
	v.mu.Unlock()
	if !lastFetch.IsZero() && now.Sub(lastFetch) < keyRefetchInterval {
		return nil, fmt.Errorf("keyID %q not found in cache, refetch throttled, last fetch was at %v", keyID, lastFetch)
	}

	_, err, _ := v.flight.Do(issuer, func() (any, error) {
		keys, err := discoverKeysForIssuer(ctx, v.httpClient, issuer)
		v.mu.Lock()
		defer v.mu.Unlock()
		// Whether or not the fetch succeeded, don't try again for another
		// interval, so a failing issuer isn't hammered.
		state.lastFetch = now
		if err != nil {
			return nil, err
		}
		state.keys = keys
		return nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("while discovering keys from issuer for keyID %q: %w", keyID, err)
	}

	v.mu.Lock()
	key := findKey(state.keys, keyID)
	v.mu.Unlock()
	if key == nil {
		return nil, fmt.Errorf("unknown keyID %q", keyID)
	}
	return key.PublicKey, nil
}

// refreshIssuerKeys refetches an issuer's keys, replacing the cached set on
// success and keeping the previous keys on failure (a transient discovery
// outage must not take down verification). Runs off the request path; the
// caller must have set state.refreshing under the lock.
func (v *CachedVerifier) refreshIssuerKeys(issuer string, state *issuerKeys, now time.Time) {
	_, err, _ := v.flight.Do(issuer, func() (any, error) {
		// context.Background(): this outlives the request that noticed the
		// stale keys; the HTTP client's own timeout bounds the fetch.
		keys, err := discoverKeysForIssuer(context.Background(), v.httpClient, issuer)
		v.mu.Lock()
		defer v.mu.Unlock()
		// Whether or not the fetch succeeded, don't try again for another
		// interval, so a failing issuer isn't hammered.
		state.lastFetch = now
		if err != nil {
			return nil, err
		}
		state.keys = keys
		return nil, nil
	})
	// Reset refreshing outside the closure: flight.Do coalesces with the
	// unknown-keyID fetch path, so our closure may never run — the flag must
	// clear whenever the shared flight completes.
	v.mu.Lock()
	state.refreshing = false
	v.mu.Unlock()
	if err != nil {
		slog.Warn("Background refresh of issuer keys failed; keeping previous keys",
			slog.String("issuer", issuer), slog.Any("err", err))
	}
}

func findKey(keys []*KeyAndID, keyID string) *KeyAndID {
	i := slices.IndexFunc(keys, func(k *KeyAndID) bool {
		return k.KeyID == keyID
	})
	if i == -1 {
		return nil
	}
	return keys[i]
}

// Verify is a convenience wrapper that performs a one-shot verification with
// no key caching: every call refetches the issuer's keys. Hot paths should
// share a CachedVerifier instead.
//
// httpClient is used for OIDC discovery and JWKS fetches; nil uses a default
// client with a whole-request timeout.
func Verify(ctx context.Context, httpClient *http.Client, jwt string, expectedIssuer, expectedAudience string, now time.Time) (*KubernetesClaims, error) {
	return NewCachedVerifier(httpClient).Verify(ctx, jwt, expectedIssuer, expectedAudience, now)
}

// Verify verifies and extracts claims from a Kubernetes JWT.
//
// For bound service account tokens, this function performs cryptographic verification of the JWT,
// checks the issuer and audience claims, and checks the time-binding claims. It *does not* check
// the object binding claims. If needed for your use case, you will need check the object bindings
// by connecting to the cluster and seeing if the object(s) the bindings name still exist within the
// cluster.
func (v *CachedVerifier) Verify(ctx context.Context, jwt string, expectedIssuer, expectedAudience string, now time.Time) (*KubernetesClaims, error) {
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

	// Find the key we should use for verification based on the keyID in the JWT header.
	if header.KeyID == "" {
		return nil, fmt.Errorf("key ID is required")
	}
	selectedKey, err := v.keyForIssuer(ctx, rawClaims.Issuer, header.KeyID, now)
	if err != nil {
		return nil, err
	}

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
			return fmt.Errorf("requested key ID is not an ECDSA P384 key")
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
			return fmt.Errorf("requested key ID is not an ECDSA P521 key")
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

// ellipticCurveForJWK maps a JWK "crv" value to its elliptic.Curve. Only the NIST
// curves that verifySignature supports (ES256/ES384/ES512) are accepted.
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

func discoverKeysForIssuer(ctx context.Context, httpClient *http.Client, issuer string) ([]*KeyAndID, error) {
	var discoveryDocURL string
	if strings.HasSuffix(issuer, "/") {
		discoveryDocURL = issuer + ".well-known/openid-configuration"
	} else {
		discoveryDocURL = issuer + "/.well-known/openid-configuration"
	}

	oidcConfig, err := fetchJSON[oidcConfigT](httpClient, discoveryDocURL)
	if err != nil {
		return nil, fmt.Errorf("while fetching OIDC Discovery document: %w", err)
	}

	slog.InfoContext(ctx, "Fetched discovery doc", slog.Any("doc", oidcConfig))

	jwkSet, err := fetchJSON[jwkSetT](httpClient, oidcConfig.JWKSURI)
	if err != nil {
		return nil, fmt.Errorf("while fetching JWKS: %w", err)
	}

	slog.InfoContext(ctx, "Fetched JWK set", slog.Any("jwkSet", jwkSet))

	var ret []*KeyAndID
	skipped := 0
	for _, jwk := range jwkSet.Keys {
		key, err := parseJWK(jwk)
		if err != nil {
			// Skip an unusable key instead of failing the whole issuer; it's safe because
			// keys are selected by kid and the signature is still verified, so a skipped
			// key can't be abused. Debug, not Warn: unsupported key types are a normal
			// config and discovery runs on every Verify (no cache yet), so Warn would spam.
			slog.DebugContext(ctx, "Skipping unusable JWK from issuer",
				slog.String("kid", jwk.KeyID), slog.String("kty", jwk.KeyType), slog.Any("err", err))
			skipped++
			continue
		}
		ret = append(ret, key)
	}

	// None usable: fail here (reasons logged above) rather than return an empty set
	// that fails later as a vaguer "unknown key ID".
	if len(ret) == 0 {
		if len(jwkSet.Keys) == 0 {
			return nil, fmt.Errorf("issuer %q published an empty JWKS", issuer)
		}
		return nil, fmt.Errorf("no usable keys in JWKS for issuer %q (%d skipped)", issuer, skipped)
	}

	if skipped > 0 {
		slog.DebugContext(ctx, "Skipped unusable JWKs from issuer",
			slog.String("issuer", issuer), slog.Int("skipped", skipped), slog.Int("usable", len(ret)))
	}

	return ret, nil
}

// parseJWK converts a single JWK into a verification key, returning an error for a key
// the verifier cannot use (missing key ID, unsupported key type or curve, or malformed
// parameters).
func parseJWK(jwk jwkT) (*KeyAndID, error) {
	if jwk.KeyID == "" {
		return nil, fmt.Errorf("JWK has no key ID")
	}
	switch jwk.KeyType {
	case "EC":
		curve, err := ellipticCurveForJWK(jwk.EllipticCurve)
		if err != nil {
			return nil, err
		}
		if jwk.EllipticX == "" || jwk.EllipticY == "" {
			return nil, fmt.Errorf("EC JWK is missing the x or y coordinate")
		}
		xBytes, err := base64.RawURLEncoding.DecodeString(jwk.EllipticX)
		if err != nil {
			return nil, fmt.Errorf("while base64-decoding EC x coordinate: %w", err)
		}
		yBytes, err := base64.RawURLEncoding.DecodeString(jwk.EllipticY)
		if err != nil {
			return nil, fmt.Errorf("while base64-decoding EC y coordinate: %w", err)
		}
		x := new(big.Int).SetBytes(xBytes)
		y := new(big.Int).SetBytes(yBytes)
		// Reject coordinates outside the field. This is a cheap sanity check; the
		// authoritative on-curve validation happens in ecdsa.Verify, which returns
		// false for a public key whose point is not on the curve.
		p := curve.Params().P
		if x.Cmp(p) >= 0 || y.Cmp(p) >= 0 {
			return nil, fmt.Errorf("EC JWK coordinate is out of range for curve %q", jwk.EllipticCurve)
		}
		return &KeyAndID{
			KeyID:     jwk.KeyID,
			PublicKey: &ecdsa.PublicKey{Curve: curve, X: x, Y: y},
		}, nil

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

		return &KeyAndID{
			KeyID: jwk.KeyID,
			PublicKey: &rsa.PublicKey{
				N: n,
				E: int(e.Int64()),
			},
		}, nil

	default:
		return nil, fmt.Errorf("unhandled key type %q", jwk.KeyType)
	}
}

func fetchJSON[T any](httpClient *http.Client, url string) (T, error) {
	var parsedBody T
	if httpClient == nil {
		httpClient = defaultHTTPClient
	}
	resp, err := httpClient.Get(url)
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
