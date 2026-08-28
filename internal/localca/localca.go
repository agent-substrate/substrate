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

// Package localca implements a CA whose state can be stored in a local file or
// Kubernetes secret.
//
// In substrate's default setup, the CA pool state is kept in a Kubernetes
// secret, and administered with admin CLI commands.
//
// If you are writing an online signing component, use a projected volume to put
// the secret's content into your container's filesystem, and then point a
// RefreshingPool at the file.  Even if an administrator rotates the pool, your
// component will continue to work correctly with no restarts.
//
// If you are writing an admin command, read the secret from the Kubernetes API,
// use Unmarshal to parse it to a Pool, manipulate the Pool, and then use
// Marshal to serialize the state and write it back to the secret.
//
// For tests, generate an ephemeral ConcretePool.
package localca

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// Pool is the interface for a CA pool.
//
// Logically, a Pool is a collection of multiple CAs.  One or more are
// designated as active for signing.  The rest are inactive, but are still
// trusted for verifying certificates.
//
// The active/inactive designation allows a Pool to be seamlessly rotated.
//
//  1. (Steady State) The Pool has one CA, active for signing.
//  2. (Publish New Root) Add a new CA, inactive.
//  3. (Age In) Wait for trust in the new root to propagate throughout the system.
//  3. (Switch) Switch the new CA to be active, and the old CA to be inactive.
//  4. (Age Out) Wait for all certificates issued by the old CA to expire.
//  5. (Cleanup) Remove the old CA from the Pool.
//
// Normally, we let callers define their own compatibility interfaces.  But in
// most cases you'll want to either use a RefreshingPool (for controllers and
// servers), or a ConcretePool (for CLIs and tests).
type Pool interface {
	// CreateCertificate signs the given template certificate using one of the
	// Pool's currently-active CAs.
	CreateCertificate(template *x509.Certificate, subjectPublicKey crypto.PublicKey) ([][]byte, error)

	// TrustAnchors returns the root certificates for all of the pool's CAs.
	TrustAnchors() ([]*x509.Certificate, error)
}

// RefreshingPool is a wrapper around Pool that periodically reloads the CA
// state from disk.  This allows our various pieces that sign certificates
// (Actor identity broker, egress gateway) to properly continue signing even as
// an administrator rotates one of the CA pools, without requiring any
// components to restart.
type RefreshingPool struct {
	stateFile string
	clock     clock.PassiveClock

	// lock covers nextLoad and pool
	lock     sync.Mutex
	nextLoad time.Time
	pool     *ConcretePool
}

var _ Pool = (*RefreshingPool)(nil)

func NewRefreshingPool(stateFile string) (*RefreshingPool, error) {
	rp := &RefreshingPool{
		stateFile: stateFile,
		clock:     clock.RealClock{},
	}
	if err := rp.refreshIfNecessary(); err != nil {
		return nil, fmt.Errorf("while loading pool: %w", err)
	}
	return rp, nil
}

// refreshIfNecessary must be called while p.lock is held.
func (p *RefreshingPool) refreshIfNecessary() error {
	if p.pool != nil && p.clock.Now().Before(p.nextLoad) {
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
	p.nextLoad = p.clock.Now().Add(time.Minute)

	return nil
}

func (p *RefreshingPool) CreateCertificate(template *x509.Certificate, subjectPublicKey crypto.PublicKey) ([][]byte, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := p.refreshIfNecessary(); err != nil {
		return nil, fmt.Errorf("while refreshing pool: %w", err)
	}
	return p.pool.CreateCertificate(template, subjectPublicKey)
}

func (p *RefreshingPool) TrustAnchors() ([]*x509.Certificate, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := p.refreshIfNecessary(); err != nil {
		return nil, fmt.Errorf("while refreshing pool: %w", err)
	}
	return p.pool.TrustAnchors()
}

type ConcretePool struct {
	CAs []*CA

	// Which CA is active for signing operations?
	ActiveForSigning string
}

var _ Pool = (*ConcretePool)(nil)

func (p *ConcretePool) CreateCertificate(template *x509.Certificate, subjectPublicKey crypto.PublicKey) ([][]byte, error) {
	if len(p.CAs) == 0 {
		return nil, fmt.Errorf("pool has no CAs")
	}

	// For backwards compatibility, pick the first CA if none is designated.
	//
	// TODO(ahmedtd): Remove the fallback after a few weeks.  This is intended
	// to keep people from having to mess with the CAs in their existing dev
	// clusters.
	var selectedCA *CA
	if p.ActiveForSigning != "" {
		for _, ca := range p.CAs {
			if ca.ID == p.ActiveForSigning {
				selectedCA = ca
			}
		}
		if selectedCA == nil {
			return nil, fmt.Errorf("selected CA %q not in CA list", p.ActiveForSigning)
		}
	} else {
		selectedCA = p.CAs[0]
	}

	// TODO: Trim notAfter of the template so it doesn't outlast the selected
	// signing root.  I am not sure if this is necessary, and it may preclude
	// some operational patterns.  (Technically, the trust is established in the
	// CA's *key*, and nothing prevents a CA from rolling over to a new root
	// certificate that uses the same private key.)

	subjectCertDER, err := x509.CreateCertificate(rand.Reader, template, selectedCA.RootCertificate, subjectPublicKey, selectedCA.SigningKey)
	if err != nil {
		return nil, fmt.Errorf("while creating certificate: %w", err)
	}

	chain := [][]byte{subjectCertDER}

	return chain, nil
}

func (p *ConcretePool) TrustAnchors() ([]*x509.Certificate, error) {
	var anchors []*x509.Certificate
	for _, ca := range p.CAs {
		anchors = append(anchors, ca.RootCertificate)
	}
	return anchors, nil
}

// CA is a concrete certificate authority signing from a single root
// certificate.
//
// In most uses, you want to use a pool of CAs, in order to seamlessly handle CA
// rotation.
type CA struct {
	ID string

	// The private key to use for signing certificates.  Corresponds to
	// RootCertificate.
	SigningKey crypto.PrivateKey

	// The root certificate for this CA pool.
	RootCertificate *x509.Certificate
}

type serializedPool struct {
	CAs              []*serializedCA
	ActiveForSigning string
}
type serializedCA struct {
	ID                 string
	SigningKeyPKCS8    []byte
	RootCertificateDER []byte
}

func Marshal(pool *ConcretePool) ([]byte, error) {
	wire := &serializedPool{
		ActiveForSigning: pool.ActiveForSigning,
	}

	for _, ca := range pool.CAs {
		caWire := &serializedCA{}

		caWire.ID = ca.ID

		// An external signer has no exportable key material, so this is the
		// point where "the key lives in a KMS" stops being marshalable. Name
		// that case, because x509's own error ("unknown key type") reads like a
		// bug in this code rather than a deliberate property of the signer.
		signingKeyPKCS8, err := x509.MarshalPKCS8PrivateKey(ca.SigningKey)
		if err != nil {
			return nil, fmt.Errorf("while serializing signing key for CA %q to PKCS#8: %w "+
				"(a signer that holds no exportable key material, such as a KMS or HSM signer, "+
				"cannot be written to a pool file; keep it in its own store)", ca.ID, err)
		}

		caWire.SigningKeyPKCS8 = signingKeyPKCS8
		caWire.RootCertificateDER = ca.RootCertificate.Raw

		wire.CAs = append(wire.CAs, caWire)
	}

	wireBytes, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("while marshaling to JSON: %w", err)
	}

	return wireBytes, nil
}

func Unmarshal(wireBytes []byte) (*ConcretePool, error) {
	var err error
	wire := &serializedPool{}

	if err := json.Unmarshal(wireBytes, wire); err != nil {
		return nil, fmt.Errorf("while unmarshaling JSON: %w", err)
	}

	pool := &ConcretePool{}

	for _, wireCA := range wire.CAs {
		ca := &CA{
			ID: wireCA.ID,
		}

		ca.SigningKey, err = x509.ParsePKCS8PrivateKey(wireCA.SigningKeyPKCS8)
		if err != nil {
			return nil, fmt.Errorf("while parsing signing key: %w", err)
		}

		ca.RootCertificate, err = x509.ParseCertificate(wireCA.RootCertificateDER)
		if err != nil {
			return nil, fmt.Errorf("while parsing root certificate: %w", err)
		}

		pool.CAs = append(pool.CAs, ca)
	}

	pool.ActiveForSigning = wire.ActiveForSigning

	return pool, nil
}

type KeyType int

const (
	KeyTypeED25519 KeyType = iota
	KeyTypeECDSAP256
)

// GenerateCA creates a self-signed CA with its own freshly generated key.
func GenerateCA(id string, keyType KeyType, validity time.Duration) (*CA, error) {
	var rootPrivKey crypto.PrivateKey
	var rootPubKey crypto.PublicKey
	switch keyType {
	case KeyTypeED25519:
		pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("while generating root key: %w", err)
		}
		rootPrivKey = privKey
		rootPubKey = pubKey
	case KeyTypeECDSAP256:
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("while generating root key: %w", err)
		}
		rootPrivKey = key
		rootPubKey = &key.PublicKey
	default:
		return nil, fmt.Errorf("unsupported key type")
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(validity)

	rootTemplate := &x509.Certificate{
		// Some golang certificate handling code assumes that if the parent and
		// template Subject fields compare equal, we are doing a self-signing
		// operation [1].
		//
		// I'm not sure if this is correct, but for defense in depth include
		// some random content in the subject.
		//
		// [1] https://cs.opensource.google/go/go/+/refs/tags/go1.27.0:src/crypto/x509/x509.go;l=1871
		Subject: pkix.Name{
			CommonName: rand.Text(),
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}

	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, rootPubKey, rootPrivKey)
	if err != nil {
		return nil, fmt.Errorf("while generating root certificate: %w", err)
	}

	rootCert, err := x509.ParseCertificate(rootDER)
	if err != nil {
		return nil, fmt.Errorf("while parsing root certificate: %w", err)
	}

	return &CA{
		ID:              id,
		SigningKey:      rootPrivKey,
		RootCertificate: rootCert,
		// No intermediates.
	}, nil
}
