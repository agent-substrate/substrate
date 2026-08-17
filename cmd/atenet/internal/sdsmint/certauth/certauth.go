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

// Package certauth holds everything in sdsmint that touches the MITM signing
// key: the key itself, the leaf keypair certificates are bound to, and the one
// function that turns a hostname into a certificate for it.
package certauth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
)

// Signer issues leaves for arbitrary hostnames from the MITM CA.
type Signer struct {
	pool   *localca.Pool
	active *localca.CA

	// key is the keypair every leaf this Signer issues is bound to.
	key    crypto.Signer
	keyPEM []byte
}

// New builds a Signer over pool, signing with the CA named by id. An empty id
// takes the first entry.
//
// The whole pool is retained, not just the selected entry: see the Signer
// fields.
func New(pool *localca.Pool, id string) (*Signer, error) {
	active, err := selectCA(pool, id)
	if err != nil {
		return nil, err
	}
	// TODO(haiyanmeng): validate CA is not expired.
	if err := active.Validate(); err != nil {
		return nil, fmt.Errorf("ca pool: CA %q: %w", active.ID, err)
	}
	// Generated at startup rather than on first use, so that a mint never pays
	// for a keygen and one that cannot succeed fails before the server binds.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating leaf key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshalling leaf key: %w", err)
	}
	return &Signer{
		pool:   pool,
		active: active,
		key:    key,
		keyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}),
	}, nil
}

// selectCA picks the entry to sign with. An empty id takes the first.
// TODO(haiyanmeng): move this function to internal/localca.
func selectCA(pool *localca.Pool, id string) (*localca.CA, error) {
	if pool == nil || len(pool.CAs) == 0 {
		return nil, errors.New("ca pool: empty")
	}
	if id == "" {
		return pool.CAs[0], nil
	}
	ids := make([]string, 0, len(pool.CAs))
	for _, candidate := range pool.CAs {
		ids = append(ids, candidate.ID)
		if candidate.ID == id {
			return candidate, nil
		}
	}
	return nil, fmt.Errorf("ca pool: no CA with ID %q (have %q)", id, ids)
}

// Issuer returns the certificate this Signer's key belongs to, the one it puts
// directly above each leaf in the chain.
func (s *Signer) Issuer() *x509.Certificate {
	return s.active.RootCertificate
}

// Anchors returns every root in the pool: the full set a client has to trust
// for this Signer's leaves to verify across a rotation, not merely the one
// currently signing. This is what belongs in a trust store or a published
// bundle.
func (s *Signer) Anchors() []*x509.Certificate {
	roots := make([]*x509.Certificate, 0, len(s.pool.CAs))
	for _, ca := range s.pool.CAs {
		roots = append(roots, ca.RootCertificate)
	}
	return roots
}

// MintedCert is a freshly issued leaf plus its private key, in the PEM form
// Envoy's Secret proto expects.
type MintedCert struct {
	CertChainPEM  []byte // leaf, then the root, PEM
	PrivateKeyPEM []byte // leaf private key, PEM
	NotAfter      time.Time
	Serial        string // hex, for the issuance audit log
}

// Sign issues a leaf certificate for host, signed by the CA's root key and
// bound to the Signer's leaf keypair.
func (s *Signer) Sign(host string, ttl time.Duration) (*MintedCert, error) {
	if host == "" {
		return nil, errors.New("empty host")
	}

	ca := s.active
	now := time.Now()

	notAfter := now.Add(ttl)
	if notAfter.After(ca.RootCertificate.NotAfter) {
		notAfter = ca.RootCertificate.NotAfter
	}
	tmpl := &x509.Certificate{
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}
	// A literal IP in the SNI position has to land in IPAddresses, not
	// DNSNames, or clients will reject the leaf.
	// Envoy will hand us whatever was in the SNI, and although SNI is
	// not supposed to carry IP literals, some clients send them anyway.
	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
	} else {
		tmpl.DNSNames = []string{host}
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, tmpl, ca.RootCertificate, s.key.Public(), ca.SigningKey)
	if err != nil {
		return nil, fmt.Errorf("signing leaf for %q: %w", host, err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		return nil, fmt.Errorf("parsing the leaf just signed for %q: %w", host, err)
	}

	// Leaf first, then its issuer, then whatever climbs from the issuer toward
	// the certificate a client is actually configured to trust. RootCertificate
	// is the issuer and not necessarily the anchor: localca.Validate pins the
	// signing key to it, and IntermediateCertificates is what sits above.
	chain := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	chain = append(chain, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.RootCertificate.Raw})...)
	for _, intermediate := range ca.IntermediateCertificates {
		chain = append(chain, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: intermediate.Raw})...)
	}

	return &MintedCert{
		CertChainPEM: chain,
		// Shared with every other leaf this Signer issues, rendered once in
		// New. Read-only, like the rest of MintedCert.
		PrivateKeyPEM: s.keyPEM,
		NotAfter:      notAfter,
		Serial:        leaf.SerialNumber.Text(16),
	}, nil
}
