// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aead

import (
	"fmt"
	"slices"

	"github.com/tink-crypto/tink-go/v2/internal/factoryutil"
	"github.com/tink-crypto/tink-go/v2/internal/prefixmap"
	"github.com/tink-crypto/tink-go/v2/internal/registryconfig"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/monitoring"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// New returns an AEAD primitive from the given keyset handle.
func New(handle *keyset.Handle) (tink.AEAD, error) {
	return NewWithConfig(handle, &registryconfig.RegistryConfig{})
}

// NewWithConfig creates an AEAD primitive from the given [keyset.Handle] using
// the provided [Config].
func NewWithConfig(handle *keyset.Handle, config keyset.Config) (tink.AEAD, error) {
	if handle.Len() == 0 {
		return nil, fmt.Errorf("aead_factory: empty or nil keyset handle")
	}
	primitives := prefixmap.New[aeadAndKeyID]()
	var primary aeadAndKeyID
	for entry := range factoryutil.EnabledUnmonitoredEntries(handle) {
		p, isLegacyPrimitive, err := factoryutil.PrimitiveFromKey[tink.AEAD](entry.Key(), config)
		if err != nil {
			return nil, err
		}
		outputPrefix, err := factoryutil.OutputPrefix(entry.Key())
		if err != nil {
			return nil, err
		}
		if isLegacyPrimitive {
			p = &fullAEADPrimitiveAdapter{primitive: p, prefix: outputPrefix}
		}
		a := aeadAndKeyID{primitive: p, keyID: entry.KeyID()}
		if entry.IsPrimary() {
			primary = a
		}
		primitives.Insert(string(outputPrefix), a)
	}

	encLogger, decLogger, err := createLoggers(handle)
	if err != nil {
		return nil, err
	}
	return &wrappedAead{
		primary:    primary,
		primitives: primitives,
		encLogger:  encLogger,
		decLogger:  decLogger,
	}, nil
}

// wrappedAead is an AEAD implementation that uses the underlying primitive set for encryption
// and decryption.
type wrappedAead struct {
	primary    aeadAndKeyID
	primitives *prefixmap.PrefixMap[aeadAndKeyID]

	encLogger monitoring.Logger
	decLogger monitoring.Logger
}

type aeadAndKeyID struct {
	primitive tink.AEAD
	keyID     uint32
}

func (a *aeadAndKeyID) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	return a.primitive.Encrypt(plaintext, associatedData)
}

func (a *aeadAndKeyID) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	return a.primitive.Decrypt(ciphertext, associatedData)
}

// aeadPrimitiveAdapter is an adapter that turns a non-full [tink.AEAD]
// primitive into a full [tink.AEAD] primitive.
type fullAEADPrimitiveAdapter struct {
	primitive tink.AEAD
	prefix    []byte
}

func (a *fullAEADPrimitiveAdapter) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	ct, err := a.primitive.Encrypt(plaintext, associatedData)
	if err != nil {
		return nil, err
	}
	return slices.Concat(a.prefix, ct), nil
}

func (a *fullAEADPrimitiveAdapter) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	return a.primitive.Decrypt(ciphertext[len(a.prefix):], associatedData)
}

func createLoggers(kh *keyset.Handle) (monitoring.Logger, monitoring.Logger, error) {
	factory, err := factoryutil.NewLoggerFactory(kh)
	if err != nil {
		return nil, nil, err
	}
	encLogger, err := factory.CreateFor("aead", "encrypt")
	if err != nil {
		return nil, nil, err
	}
	decLogger, err := factory.CreateFor("aead", "decrypt")
	if err != nil {
		return nil, nil, err
	}
	return encLogger, decLogger, nil
}

// Encrypt encrypts the given plaintext with the given associatedData.
// It returns the concatenation of the primary's identifier and the ciphertext.
func (a *wrappedAead) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	ct, err := a.primary.Encrypt(plaintext, associatedData)
	if err != nil {
		a.encLogger.LogFailure()
		return nil, err
	}
	a.encLogger.Log(a.primary.keyID, len(plaintext))
	return ct, nil
}

// Decrypt decrypts the given ciphertext and authenticates it with the given
// associatedData. It returns the corresponding plaintext if the
// ciphertext is authenticated.
func (a *wrappedAead) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	it := a.primitives.PrimitivesMatchingPrefix(ciphertext)
	for primitive, ok := it.Next(); ok; primitive, ok = it.Next() {
		pt, err := primitive.Decrypt(ciphertext, associatedData)
		if err != nil {
			continue
		}
		a.decLogger.Log(primitive.keyID, len(ciphertext))
		return pt, nil
	}
	// Nothing worked.
	a.decLogger.LogFailure()
	return nil, fmt.Errorf("aead_factory: decryption failed")
}
