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
	"encoding/pem"
	"fmt"
	"time"
)

type Pool struct {
	CAs []*CA
}

type CA struct {
	ID                       string
	SigningKey               crypto.Signer
	RootCertificate          *x509.Certificate
	IntermediateCertificates []*x509.Certificate
}

// Validate reports whether the CA is well formed enough to sign with.
func (ca *CA) Validate() error {
	if ca == nil {
		return fmt.Errorf("ca: is nil")
	}
	if ca.RootCertificate == nil {
		return fmt.Errorf("ca cert: missing")
	}
	if ca.SigningKey == nil {
		return fmt.Errorf("ca key: missing")
	}
	if !ca.RootCertificate.IsCA {
		return fmt.Errorf("ca cert: %q is not a CA certificate", ca.RootCertificate.Subject)
	}
	if !keyMatchesCert(ca.RootCertificate, ca.SigningKey) {
		return fmt.Errorf("ca key: public key does not match the certificate for %q", ca.RootCertificate.Subject)
	}
	return nil
}

// keyMatchesCert catches a mismatched cert/key pair at load rather than at the
// first handshake.
func keyMatchesCert(cert *x509.Certificate, key crypto.Signer) bool {
	type equaler interface{ Equal(crypto.PublicKey) bool }
	pub, ok := cert.PublicKey.(equaler)
	if !ok {
		return true
	}
	return pub.Equal(key.Public())
}

type serializedPool struct {
	CAs []*serializedCA
}
type serializedCA struct {
	ID                          string
	SigningKeyPKCS8             []byte
	SigningKeyPEM               string
	RootCertificateDER          []byte
	RootCertificatePEM          string
	IntermediateCertificatesDER [][]byte
}

func Marshal(ca *Pool) ([]byte, error) {
	wire := &serializedPool{}

	for _, ca := range ca.CAs {
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
		for _, intermediate := range ca.IntermediateCertificates {
			caWire.IntermediateCertificatesDER = append(caWire.IntermediateCertificatesDER, intermediate.Raw)
		}

		wire.CAs = append(wire.CAs, caWire)
	}

	wireBytes, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("while marshaling to JSON: %w", err)
	}

	return wireBytes, nil
}

func Unmarshal(wireBytes []byte) (*Pool, error) {
	var err error
	wire := &serializedPool{}

	if err := json.Unmarshal(wireBytes, wire); err != nil {
		return nil, fmt.Errorf("while unmarshaling JSON: %w", err)
	}

	pool := &Pool{}

	for _, wireCA := range wire.CAs {
		ca := &CA{
			ID: wireCA.ID,
		}

		ca.SigningKey, err = parsePrivateKey(wireCA.SigningKeyPKCS8, wireCA.SigningKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("while parsing signing key: %w", err)
		}

		ca.RootCertificate, err = parseCertificate(wireCA.RootCertificateDER, wireCA.RootCertificatePEM)
		if err != nil {
			return nil, fmt.Errorf("while parsing root certificate: %w", err)
		}

		for _, intermediateDER := range wireCA.IntermediateCertificatesDER {
			intermediateCert, err := x509.ParseCertificate(intermediateDER)
			if err != nil {
				return nil, fmt.Errorf("while parsing intermediate certificate: %w", err)
			}
			ca.IntermediateCertificates = append(ca.IntermediateCertificates, intermediateCert)
		}

		pool.CAs = append(pool.CAs, ca)
	}

	return pool, nil
}

func parsePrivateKey(pkcs8 []byte, pemData string) (crypto.Signer, error) {
	if len(pkcs8) != 0 {
		key, err := x509.ParsePKCS8PrivateKey(pkcs8)
		if err != nil {
			return nil, err
		}
		return asSigner(key)
	}

	block, _ := pem.Decode([]byte(pemData))
	if block == nil {
		return nil, fmt.Errorf("missing PEM block")
	}

	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		return asSigner(key)
	}
	if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return asSigner(key)
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return asSigner(key)
	}
	return nil, fmt.Errorf("unsupported private key PEM type %q", block.Type)
}

// asSigner narrows a parsed key to the signing interface the CAs actually use.
// X25519 keys parse cleanly out of PKCS#8 and cannot sign, so this is a real
// case and not a defensive assertion.
func asSigner(key any) (crypto.Signer, error) {
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("private key of type %T cannot sign", key)
	}
	return signer, nil
}

func parseCertificate(der []byte, pemData string) (*x509.Certificate, error) {
	if len(der) != 0 {
		return x509.ParseCertificate(der)
	}

	block, _ := pem.Decode([]byte(pemData))
	if block == nil {
		return nil, fmt.Errorf("missing PEM block")
	}
	if block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("unsupported certificate PEM type %q", block.Type)
	}
	return x509.ParseCertificate(block.Bytes)
}

// KeyType selects the algorithm of a generated CA's signing key.
type KeyType string

const (
	// KeyTypeED25519 is the default. It is the smallest and fastest option and
	// is what substrate's internal CAs have always used.
	KeyTypeED25519 KeyType = "ed25519"
	// KeyTypeECDSAP256 exists for CAs whose certificates are validated by
	// clients outside substrate's control. Ed25519 in a chain needs OpenSSL
	// 1.1.1+ or Go 1.13+, which is fine for anything substrate ships and not
	// something to assume of an arbitrary process running inside an actor.
	KeyTypeECDSAP256 KeyType = "ecdsa-p256"
)

// GenerateOptions configures GenerateCA. The zero value, apart from ID,
// reproduces what GenerateED25519CA has always produced.
type GenerateOptions struct {
	// ID names the CA within its Pool.
	ID string
	// CommonName is the subject CN. Empty leaves the subject empty, which is
	// what the internal CAs do -- nothing authenticates on their name.
	CommonName string
	// KeyType defaults to KeyTypeED25519.
	KeyType KeyType
	// Lifetime defaults to 365 days.
	Lifetime time.Duration
}

// GenerateED25519CA creates an unconstrained 365-day Ed25519 CA. It is the
// long-standing shape of substrate's internal CAs, kept as its own function
// because every existing caller wants exactly this.
func GenerateED25519CA(id string) (*CA, error) {
	return GenerateCA(GenerateOptions{ID: id})
}

// GenerateCA creates a self-signed CA with its own freshly generated key.
func GenerateCA(opts GenerateOptions) (*CA, error) {
	if opts.Lifetime == 0 {
		opts.Lifetime = 365 * 24 * time.Hour
	}
	if opts.KeyType == "" {
		opts.KeyType = KeyTypeED25519
	}

	rootPrivKey, err := generateKey(opts.KeyType)
	if err != nil {
		return nil, err
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(opts.Lifetime)

	rootTemplate := &x509.Certificate{
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}
	if opts.CommonName != "" {
		rootTemplate.Subject = pkix.Name{CommonName: opts.CommonName}
	}

	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, rootPrivKey.Public(), rootPrivKey)
	if err != nil {
		return nil, fmt.Errorf("while generating root certificate: %w", err)
	}

	rootCert, err := x509.ParseCertificate(rootDER)
	if err != nil {
		return nil, fmt.Errorf("while parsing root certificate: %w", err)
	}

	return &CA{
		ID:              opts.ID,
		SigningKey:      rootPrivKey,
		RootCertificate: rootCert,
		// No intermediates.
	}, nil
}

func generateKey(kt KeyType) (crypto.Signer, error) {
	switch kt {
	case KeyTypeED25519:
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("while generating root key: %w", err)
		}
		return key, nil
	case KeyTypeECDSAP256:
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("while generating root key: %w", err)
		}
		return key, nil
	default:
		return nil, fmt.Errorf("unsupported key type %q, want one of %q or %q", kt, KeyTypeED25519, KeyTypeECDSAP256)
	}
}
