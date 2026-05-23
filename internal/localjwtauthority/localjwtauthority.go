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
	"crypto/x509"
	"encoding/json"
	"fmt"
)

type Pool struct {
	Authorities []*Authority
}

type Authority struct {
	ID         string
	Algorithm  string
	SigningKey crypto.PrivateKey
}

type serializedPool struct {
	Authorities []*serializedAuthority
}

type serializedAuthority struct {
	ID              string
	Algorithm       string
	SigningKeyPKCS8 []byte
}

// Marshal serializes a Pool to JSON.
func Marshal(pool *Pool) ([]byte, error) {
	wire := &serializedPool{}

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
func Unmarshal(wireBytes []byte) (*Pool, error) {
	wire := &serializedPool{}

	if err := json.Unmarshal(wireBytes, wire); err != nil {
		return nil, fmt.Errorf("while unmarshaling JSON: %w", err)
	}

	pool := &Pool{}
	for _, wireAuthority := range wire.Authorities {
		authority := &Authority{
			ID:        wireAuthority.ID,
			Algorithm: wireAuthority.Algorithm,
		}

		signingKey, err := x509.ParsePKCS8PrivateKey(wireAuthority.SigningKeyPKCS8)
		if err != nil {
			return nil, fmt.Errorf("while parsing signing key: %w", err)
		}
		authority.SigningKey = signingKey

		pool.Authorities = append(pool.Authorities, authority)
	}

	return pool, nil
}

// GenerateECDSAP256Authority generates an ECDSA P256 JWT signing key.
func GenerateECDSAP256Authority(id string) (*Authority, error) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("while generating key: %w", err)
	}

	return &Authority{
		ID:         id,
		Algorithm:  "ES256",
		SigningKey: privKey,
	}, nil
}
