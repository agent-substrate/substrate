// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aesgcm

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/internal/aead"
	"github.com/tink-crypto/tink-go/v2/internal/random"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/tink"
)

const (
	// ivSize is the acceptable IV size defined by RFC 5116.
	ivSize = 12
	// tagSize is the acceptable tag size defined by RFC 5116.
	tagSize = 16
)

// fullAEAD is an implementation of the [tink.AEAD] interface with AES-GCM.
//
// It implements RFC 5116 Section 5.1 and 5.2 and adds a prefix to the
// ciphertext.
type fullAEAD struct {
	cipher cipher.AEAD
	prefix []byte
}

var _ tink.AEAD = (*fullAEAD)(nil)

// Encrypt encrypts plaintext with associatedData.
//
// The returned ciphertext is of the form:
//
//	prefix || iv || ciphertext || tag
//
// where prefix is the key's output prefix, iv is a random 12-byte IV,
// ciphertext is the encrypted plaintext, and tag is a 16-byte tag.
func (a *fullAEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	if err := aead.CheckAESGCMPlaintextSize(uint64(len(plaintext))); err != nil {
		return nil, fmt.Errorf("aesgcm.Encrypt: %v", err)
	}
	dst := make([]byte, len(a.prefix)+ivSize, len(a.prefix)+ivSize+len(plaintext)+tagSize)
	copy(dst, a.prefix)
	iv := dst[len(a.prefix):]
	random.MustRand(iv)
	return a.cipher.Seal(dst, iv, plaintext, associatedData), nil
}

// Decrypt decrypts ciphertext with associatedData.
//
// The ciphertext is assumed to be of the form:
//
//	<prefix> || iv || ciphertext || tag
//
// where prefix is the key's output prefix, iv is the 12-byte IV, ciphertext is
// the encrypted plaintext, and tag is the 16-byte tag.
// prefix must match the key's output prefix. The prefix may be empty.
func (a *fullAEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	if len(ciphertext) < len(a.prefix)+ivSize+tagSize {
		return nil, fmt.Errorf("aesgcm.Decrypt: ciphertext with size %d is too short", len(ciphertext))
	}
	prefix := ciphertext[:len(a.prefix)]
	if !bytes.Equal(prefix, a.prefix) {
		return nil, fmt.Errorf("aesgcm.Decrypt: ciphertext prefix does not match")
	}
	iv := ciphertext[len(a.prefix) : len(a.prefix)+ivSize]
	ciphertextWithTag := ciphertext[len(a.prefix)+ivSize:]
	plaintextLen := len(ciphertextWithTag) - tagSize
	output := make([]byte, 0, plaintextLen)
	pt, err := a.cipher.Open(output, iv, ciphertextWithTag, associatedData)
	if err != nil {
		return nil, fmt.Errorf("aesgcm.Decrypt: %v", err)
	}
	return pt, nil
}

// NewAEAD creates a [tink.AEAD] from a [Key].
func NewAEAD(k *Key) (tink.AEAD, error) {
	if err := aead.ValidateAESKeySize(uint32(k.parameters.KeySizeInBytes())); err != nil {
		return nil, fmt.Errorf("aesgcm.NewAEAD: %v", err)
	}
	if k.parameters.IVSizeInBytes() != ivSize {
		return nil, fmt.Errorf("aesgcm.NewAEAD: unsupported IV size: got %v, want %v", k.parameters.IVSizeInBytes(), ivSize)
	}
	if k.parameters.TagSizeInBytes() != tagSize {
		return nil, fmt.Errorf("aesgcm.NewAEAD: unsupported tag size: got %v, want %v", k.parameters.TagSizeInBytes(), tagSize)
	}
	c, err := aes.NewCipher(k.KeyBytes().Data(insecuresecretdataaccess.Token{}))
	if err != nil {
		return nil, fmt.Errorf("aesgcm.NewAEAD: failed to initialize cipher")
	}
	aeadCipher, err := cipher.NewGCM(c)
	if err != nil {
		return nil, fmt.Errorf("aesgcm.NewAEAD: failed to create cipher.AEAD")
	}
	return &fullAEAD{
		cipher: aeadCipher,
		prefix: k.OutputPrefix(),
	}, nil
}

// primitiveConstructor creates a [fullAEAD] from a [key.Key].
//
// The key must be of type [aesgcm.Key].
func primitiveConstructor(k key.Key) (any, error) {
	that, ok := k.(*Key)
	if !ok {
		return nil, fmt.Errorf("key is of type %T; needed *aesgcm.Key", k)
	}
	return NewAEAD(that)
}
