// Copyright 2024 Google LLC
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

package chacha20poly1305

import (
	"bytes"
	"crypto/cipher"
	"fmt"
	"math"

	"golang.org/x/crypto/chacha20poly1305"
	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/random"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

type fullAEAD struct {
	rawAEAD cipher.AEAD
	prefix  []byte
	variant Variant
}

var _ tink.AEAD = (*fullAEAD)(nil)

func newAEAD(key *Key) (tink.AEAD, error) {
	if want, got := key.KeyBytes().Len(), chacha20poly1305.KeySize; want != got {
		return nil, fmt.Errorf("chacha20_poly1305: bad key length: got %d, want %d", got, want)
	}
	rawAEAD, err := chacha20poly1305.New(key.KeyBytes().Data(insecuresecretdataaccess.Token{}))
	if err != nil {
		return nil, fmt.Errorf("chacha20_poly1305: failed to create AEAD, error: %v", err)
	}
	return &fullAEAD{
		rawAEAD: rawAEAD,
		prefix:  key.OutputPrefix(),
		variant: key.parameters.Variant(),
	}, nil
}

// Encrypt encrypts plaintext with associatedData.
func (ca *fullAEAD) Encrypt(plaintext []byte, associatedData []byte) ([]byte, error) {
	maxPlaintextLength := math.MaxInt - len(ca.prefix) - chacha20poly1305.NonceSize - chacha20poly1305.Overhead
	if len(plaintext) > maxPlaintextLength {
		return nil, fmt.Errorf("chacha20_poly1305: plaintext too long: got %d, want at most %d", len(plaintext), maxPlaintextLength)
	}
	ciphertext := make([]byte, len(ca.prefix)+chacha20poly1305.NonceSize, len(ca.prefix)+chacha20poly1305.NonceSize+len(plaintext)+chacha20poly1305.Overhead)
	copy(ciphertext, ca.prefix)
	nonce := ciphertext[len(ca.prefix):]
	random.MustRand(nonce)
	return ca.rawAEAD.Seal(ciphertext, nonce, plaintext, associatedData), nil
}

// Decrypt decrypts ciphertext with associatedData.
func (ca *fullAEAD) Decrypt(ciphertext []byte, associatedData []byte) ([]byte, error) {
	if !bytes.HasPrefix(ciphertext, ca.prefix) {
		return nil, fmt.Errorf("chacha20_poly1305: ciphertext has invalid prefix")
	}
	ciphertextNoPrefix := ciphertext[len(ca.prefix):]
	minCiphertextLength := chacha20poly1305.NonceSize + chacha20poly1305.Overhead
	if len(ciphertextNoPrefix) < minCiphertextLength {
		return nil, fmt.Errorf("chacha20_poly1305: ciphertext is too short: got %d, want at least %d", len(ciphertextNoPrefix), minCiphertextLength)
	}
	nonce := ciphertextNoPrefix[:chacha20poly1305.NonceSize]
	ciphertextAndTag := ciphertextNoPrefix[chacha20poly1305.NonceSize:]
	plaintext, err := ca.rawAEAD.Open(nil, nonce, ciphertextAndTag, associatedData)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}

func primitiveConstructor(key key.Key) (any, error) {
	that, ok := key.(*Key)
	if !ok {
		return nil, fmt.Errorf("key is not a *chacha20poly1305.Key")
	}
	return newAEAD(that)
}
