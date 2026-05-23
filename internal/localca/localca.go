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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"time"
)

type Pool struct {
	CAs []*CA
}

type CA struct {
	ID                       string
	SigningKey               crypto.PrivateKey
	RootCertificate          *x509.Certificate
	IntermediateCertificates []*x509.Certificate
}

type serializedPool struct {
	CAs []*serializedCA
}
type serializedCA struct {
	ID                          string
	SigningKeyPKCS8             []byte
	RootCertificateDER          []byte
	IntermediateCertificatesDER [][]byte
}

func Marshal(ca *Pool) ([]byte, error) {
	wire := &serializedPool{}

	for _, ca := range ca.CAs {
		caWire := &serializedCA{}

		caWire.ID = ca.ID

		signingKeyPKCS8, err := x509.MarshalPKCS8PrivateKey(ca.SigningKey)
		if err != nil {
			return nil, fmt.Errorf("while serializing signing key to PKCS#8: %w", err)
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

		ca.SigningKey, err = x509.ParsePKCS8PrivateKey(wireCA.SigningKeyPKCS8)
		if err != nil {
			return nil, fmt.Errorf("while parsing signing key: %w", err)
		}

		ca.RootCertificate, err = x509.ParseCertificate(wireCA.RootCertificateDER)
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

func GenerateED25519CA(id string) (*CA, error) {
	rootPubKey, rootPrivKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("while generating root key: %w", err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour)

	rootTemplate := &x509.Certificate{
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
