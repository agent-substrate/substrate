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

// Package localjwtauthority implements a simple "CA" for JWTs.
package localjwtauthority

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/actoridjwt"
)

// Pool is the interface for a JWT signing pool.
//
// Logically, a Pool is a collection of multiple authorities.  One or more are
// designated as active for signing.  The rest are inactive, but are still
// trusted for verifying JWTs.
//
// The active/inactive designation allows a Pool to be seamlessly rotated.
//
// Normally, we let callers define their own compatibility interfaces.  But in
// most cases you'll want to either use a RefreshingPool (for controllers and
// servers), or a ConcretePool (for CLIs and tests).
type Pool interface {
	// SignJWT signs a JWT with the given claims.
	SignJWT(*actoridjwt.Claims) (string, error)

	// VerificationKeys returns the verification key set of this pool, for
	// exporting via OpenID Connect Discovery.
	VerificationKeys() ([]*VerificationKey, error)
}

type VerificationKey struct {
	KeyID     string
	Algorithm string
	PublicKey crypto.PublicKey
}

// RefreshingPool is a wrapper around ConcretePool that periodically reloads the
// state from disk.  This allows JWT signing and verification to continue
// working seamlessly even as an administrator rotates the pool, without
// requiring any components to restart.
type RefreshingPool struct {
	stateFile string

	// lock covers nextLoad and pool
	lock     sync.Mutex
	nextLoad time.Time
	pool     *ConcretePool
}

var _ Pool = (*RefreshingPool)(nil)

func NewRefreshingPool(stateFile string) (*RefreshingPool, error) {
	rp := &RefreshingPool{
		stateFile: stateFile,
	}
	rp.lock.Lock()
	defer rp.lock.Unlock()
	if err := rp.refreshIfNecessary(); err != nil {
		return nil, fmt.Errorf("while loading pool: %w", err)
	}
	return rp, nil
}

// refreshIfNecessary must be called under p.lock
func (p *RefreshingPool) refreshIfNecessary() error {
	if p.pool != nil && time.Now().Before(p.nextLoad) {
		return nil
	}

	poolBytes, err := os.ReadFile(p.stateFile)
	if err != nil {
		return fmt.Errorf("while reading pool state: %w", err)
	}

	pool, err := Unmarshal(poolBytes)
	if err != nil {
		return fmt.Errorf("while unmarshaling pool: %w", err)
	}

	p.pool = pool
	p.nextLoad = time.Now().Add(time.Minute)

	return nil
}

func (p *RefreshingPool) SignJWT(claims *actoridjwt.Claims) (string, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := p.refreshIfNecessary(); err != nil {
		return "", fmt.Errorf("while refreshing pool: %w", err)
	}
	return p.pool.SignJWT(claims)
}

func (p *RefreshingPool) VerificationKeys() ([]*VerificationKey, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := p.refreshIfNecessary(); err != nil {
		return nil, fmt.Errorf("while refreshing pool: %w", err)
	}
	return p.pool.VerificationKeys()
}

type ConcretePool struct {
	Authorities []*Authority
	// Which authority is active for signing?
	ActiveForSigning string
}

var _ Pool = (*ConcretePool)(nil)

func (p *ConcretePool) SignJWT(claims *actoridjwt.Claims) (string, error) {
	wireClaims, err := actoridjwt.ClaimsToWire(claims)
	if err != nil {
		return "", fmt.Errorf("while converting claims to wire model: %w", err)
	}

	payloadBytes, err := json.Marshal(wireClaims)
	if err != nil {
		return "", fmt.Errorf("while marshaling payload: %w", err)
	}

	// For backwards compatibility, pick the first authority if none is
	// designated.
	//
	// TODO(ahmedtd): Remove the fallback after a few weeks.  This is intended
	// to keep people from having to mess with the authorities in their existing
	// dev clusters.
	var selectedAuthority *Authority
	if p.ActiveForSigning != "" {
		for _, authority := range p.Authorities {
			if authority.ID == p.ActiveForSigning {
				selectedAuthority = authority
			}
		}
		if selectedAuthority == nil {
			return "", fmt.Errorf("selected authority %q not present", p.ActiveForSigning)
		}
	} else {
		if len(p.Authorities) == 0 {
			return "", fmt.Errorf("pool has no authorities defined")
		}
		selectedAuthority = p.Authorities[0]
	}

	jwt, err := sign(payloadBytes, selectedAuthority.SigningKey, selectedAuthority.Algorithm, selectedAuthority.ID)
	if err != nil {
		return "", fmt.Errorf("while signing JWT: %w", err)
	}

	return jwt, nil
}

func (p *ConcretePool) VerificationKeys() ([]*VerificationKey, error) {
	var keys []*VerificationKey
	for _, authority := range p.Authorities {
		vk := &VerificationKey{
			KeyID:     authority.ID,
			Algorithm: authority.Algorithm,
			PublicKey: authority.SigningKey.Public(),
		}
		keys = append(keys, vk)
	}
	return keys, nil
}

// AddAuthority adds authority to the pool without activating it, so relying
// parties can learn its key before it signs anything.
func (p *ConcretePool) AddAuthority(authority *Authority) error {
	if authority.ID == "" {
		return fmt.Errorf("authority has no ID")
	}
	if p.index(authority.ID) >= 0 {
		return fmt.Errorf("authority %q already present", authority.ID)
	}
	p.Authorities = append(p.Authorities, authority)
	return nil
}

// Activate makes the authority with id the one that signs.
func (p *ConcretePool) Activate(id string) error {
	if p.index(id) < 0 {
		return fmt.Errorf("authority %q not present", id)
	}
	p.ActiveForSigning = id
	return nil
}

// RemoveAuthority removes the authority with id, which must not be the one
// that signs.
func (p *ConcretePool) RemoveAuthority(id string) error {
	i := p.index(id)
	if i < 0 {
		return fmt.Errorf("authority %q not present", id)
	}
	if id == p.ActiveID() {
		return fmt.Errorf("authority %q is active for signing", id)
	}
	p.Authorities = slices.Delete(p.Authorities, i, i+1)
	return nil
}

// ActiveID returns the ID of the authority that signs: ActiveForSigning, or
// the first authority's when none is designated.
func (p *ConcretePool) ActiveID() string {
	if p.ActiveForSigning == "" && len(p.Authorities) > 0 {
		return p.Authorities[0].ID
	}
	return p.ActiveForSigning
}

// index returns the position of the authority with id, or -1.
func (p *ConcretePool) index(id string) int {
	return slices.IndexFunc(p.Authorities, func(a *Authority) bool { return a.ID == id })
}

type wireHeader struct {
	Type      string `json:"typ,omitempty"`
	Algorithm string `json:"alg,omitempty"`
	KeyID     string `json:"kid,omitempty"`
}

func sign(payloadBytes []byte, signingKey crypto.PrivateKey, algorithm, keyID string) (string, error) {
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadBytes)

	rawHeader := wireHeader{
		Type:      "JWT",
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

type Authority struct {
	ID         string
	Algorithm  string
	SigningKey crypto.Signer
}

type serializedPool struct {
	Authorities      []*serializedAuthority
	ActiveForSigning string
}

type serializedAuthority struct {
	ID              string
	Algorithm       string
	SigningKeyPKCS8 []byte
}

// Marshal serializes a Pool to JSON.
func Marshal(pool *ConcretePool) ([]byte, error) {
	wire := &serializedPool{}

	wire.ActiveForSigning = pool.ActiveForSigning
	for _, authority := range pool.Authorities {
		authorityWire := &serializedAuthority{}
		authorityWire.ID = authority.ID
		authorityWire.Algorithm = authority.Algorithm

		signingKeyPKCS8, err := x509.MarshalPKCS8PrivateKey(authority.SigningKey)
		if err != nil {
			return nil, fmt.Errorf("while serializing signing key to PKCS#8: %w", err)
		}
		authorityWire.SigningKeyPKCS8 = signingKeyPKCS8

		wire.Authorities = append(wire.Authorities, authorityWire)
	}

	wireBytes, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("while marshaling to JSON: %w", err)
	}

	return wireBytes, nil
}

// Unmarshal loads a Pool from JSON.
func Unmarshal(wireBytes []byte) (*ConcretePool, error) {
	wire := &serializedPool{}

	if err := json.Unmarshal(wireBytes, wire); err != nil {
		return nil, fmt.Errorf("while unmarshaling JSON: %w", err)
	}

	pool := &ConcretePool{
		ActiveForSigning: wire.ActiveForSigning,
	}
	for _, wireAuthority := range wire.Authorities {
		authority := &Authority{
			ID:        wireAuthority.ID,
			Algorithm: wireAuthority.Algorithm,
		}

		key, err := x509.ParsePKCS8PrivateKey(wireAuthority.SigningKeyPKCS8)
		if err != nil {
			return nil, fmt.Errorf("while parsing signing key: %w", err)
		}

		// All key types from ParsePKCS8PrivateKey implement Signer
		authority.SigningKey = key.(crypto.Signer)

		pool.Authorities = append(pool.Authorities, authority)
	}

	return pool, nil
}

// GenerateAuthority generates a JWT signing key for algorithm, which must be
// RS256 or ES256. An empty id defaults to the Thumbprint of the public key.
func GenerateAuthority(algorithm, id string) (*Authority, error) {
	var key crypto.Signer
	var err error
	switch algorithm {
	case "RS256":
		key, err = rsa.GenerateKey(rand.Reader, 2048)
	case "ES256":
		key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	default:
		return nil, fmt.Errorf("unsupported algorithm %q, want RS256 or ES256", algorithm)
	}
	if err != nil {
		return nil, fmt.Errorf("while generating key: %w", err)
	}
	if id == "" {
		id, err = Thumbprint(key.Public())
		if err != nil {
			return nil, fmt.Errorf("while computing key thumbprint: %w", err)
		}
	}
	return &Authority{
		ID:         id,
		Algorithm:  algorithm,
		SigningKey: key,
	}, nil
}

// GeneratePool generates a pool holding one authority, active for signing, and
// returns the serialized pool and the authority's ID. algorithm and id are as
// for GenerateAuthority.
func GeneratePool(algorithm, id string) ([]byte, string, error) {
	authority, err := GenerateAuthority(algorithm, id)
	if err != nil {
		return nil, "", err
	}
	wire, err := Marshal(&ConcretePool{
		Authorities:      []*Authority{authority},
		ActiveForSigning: authority.ID,
	})
	if err != nil {
		return nil, "", err
	}
	return wire, authority.ID, nil
}
