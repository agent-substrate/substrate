// Copyright 2020 Google LLC
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
	"crypto/aes"
	"crypto/cipher"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"math"

	// Placeholder for internal crypto/cipher allowlist, please ignore.
	// Placeholder for internal crypto/subtle allowlist, please ignore. // to allow import of "crypto/subte"
	"github.com/tink-crypto/tink-go/v2/internal/random"
)

const (
	// AESGCMSIVNonceSize is the acceptable IV size defined by RFC 8452.
	AESGCMSIVNonceSize = 12

	// aesgcmsivBlockSize is the block size that AES-GCM-SIV uses. This is the
	// size for the tag, the KDF etc.
	// Note: this value is the same as AES block size.
	aesgcmsivBlockSize = 16

	// AESGCMSIVTagSize is the byte-length of the authentication tag produced by
	// AES-GCM-SIV.
	AESGCMSIVTagSize = aesgcmsivBlockSize

	// aesgcmsivPolyvalSize is the byte-length of result produced by the
	// POLYVAL function.
	aesgcmsivPolyvalSize = aesgcmsivBlockSize

	maxAESGCMSIVKeySize = 32
)

// AESGCMSIV is an implementation of AEAD interface.
type AESGCMSIV struct {
	block   cipher.Block
	keySize int
}

// NewAESGCMSIV returns an AESGCMSIV instance.
// The key argument should be the AES key, either 16 or 32 bytes to select
// AES-128 or AES-256.
func NewAESGCMSIV(key []byte) (*AESGCMSIV, error) {
	keySize := uint32(len(key))
	if err := ValidateAESKeySize(keySize); err != nil {
		return nil, fmt.Errorf("aes_gcm_siv: %s", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes_gcm_siv: failed to create block cipher, error: %v", err)
	}
	return &AESGCMSIV{block: block, keySize: len(key)}, nil
}

// Encrypt encrypts plaintext with associatedData and appends the result to dst.
//
// The resulting ciphertext consists of three parts:
// (1) the Nonce used for encryption
// (2) the actual ciphertext
// (3) the authentication tag.
//
// Note: dst must not overlap with plaintext or associatedData (in-place
// encryption is not supported). Additionally, the capacity of dst must be
// large enough to hold the prefix, nonce, ciphertext, and tag
// (cap(dst) >= len(dst) + AESGCMSIVNonceSize + AESGCMSIVTagSize + len(plaintext)).
func (a *AESGCMSIV) Encrypt(dst, plaintext, associatedData []byte) ([]byte, error) {
	if len(plaintext) > math.MaxInt32-AESGCMSIVNonceSize-AESGCMSIVTagSize {
		return nil, fmt.Errorf("aes_gcm_siv: plaintext too long")
	}
	if len(associatedData) > math.MaxInt32 {
		return nil, fmt.Errorf("aes_gcm_siv: associatedData too long")
	}

	retLen := len(dst) + AESGCMSIVNonceSize + AESGCMSIVTagSize + len(plaintext)
	if cap(dst) < retLen {
		return nil, fmt.Errorf("aes_gcm_siv: dst capacity is too small; got %d, want >= %d", cap(dst), retLen)
	}
	ret := dst[:retLen]

	nonce := ret[len(dst) : len(dst)+AESGCMSIVNonceSize]
	random.MustRand(nonce)

	var authKeyData [aesgcmsivBlockSize]byte
	var encKeyData [maxAESGCMSIVKeySize]byte
	authKey := authKeyData[:]
	encKey := encKeyData[:a.keySize]
	if err := a.deriveKeys(nonce, authKey, encKey); err != nil {
		return nil, err
	}

	polyval, err := a.computePolyval(authKey, plaintext, associatedData)
	if err != nil {
		return nil, err
	}

	tag := ret[retLen-AESGCMSIVTagSize:]
	if err := a.computeTag(polyval, nonce, encKey, tag); err != nil {
		return nil, err
	}

	ct := ret[len(dst)+AESGCMSIVNonceSize : retLen-AESGCMSIVTagSize]
	if err := aesCTR(encKey, tag, plaintext, ct); err != nil {
		return nil, err
	}

	return ret, nil
}

// Decrypt decrypts ciphertext with associatedData.
func (a *AESGCMSIV) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	if len(ciphertext) < AESGCMSIVNonceSize+AESGCMSIVTagSize {
		return nil, fmt.Errorf("aes_gcm_siv: ciphertext too short")
	}
	if len(ciphertext) > math.MaxInt32 {
		return nil, fmt.Errorf("aes_gcm_siv: ciphertext too long")
	}
	if len(associatedData) > math.MaxInt32 {
		return nil, fmt.Errorf("aes_gcm_siv: associatedData too long")
	}

	nonce := ciphertext[:AESGCMSIVNonceSize]
	tag := ciphertext[len(ciphertext)-AESGCMSIVTagSize:]
	ciphertext = ciphertext[AESGCMSIVNonceSize : len(ciphertext)-AESGCMSIVTagSize]

	var authKeyData [aesgcmsivBlockSize]byte
	var encKeyData [maxAESGCMSIVKeySize]byte
	authKey := authKeyData[:]
	encKey := encKeyData[:a.keySize]
	if err := a.deriveKeys(nonce, authKey, encKey); err != nil {
		return nil, err
	}

	pt := make([]byte, len(ciphertext))
	if err := aesCTR(encKey, tag, ciphertext, pt); err != nil {
		return nil, err
	}

	polyval, err := a.computePolyval(authKey, pt, associatedData)
	if err != nil {
		return nil, err
	}

	var tagOut [AESGCMSIVTagSize]byte
	if err := a.computeTag(polyval, nonce, encKey, tagOut[:]); err != nil {
		return nil, err
	}

	if subtle.ConstantTimeCompare(tagOut[:], tag) != 1 {
		return nil, fmt.Errorf("aes_gcm_siv: message authentication failure")
	}

	return pt, nil
}

// deriveKeys implements the `derive_keys` function described by RFC 8452.
//
// It uses the key and nonce to derive authentication key and encryption key,
// which are written to authKey and encKey respectively. authKey and encKey must
// be of length aesgcmsivBlockSize and maxAESGCMSIVKeySize respectively.
func (a *AESGCMSIV) deriveKeys(nonce, authKey, encKey []byte) error {
	if len(nonce) != AESGCMSIVNonceSize {
		return fmt.Errorf("aes_gcm_siv: invalid nonce size")
	}
	if len(authKey) != aesgcmsivBlockSize {
		return fmt.Errorf("aes_gcm_siv: invalid authKey size")
	}
	if len(encKey) != a.keySize {
		return fmt.Errorf("aes_gcm_siv: invalid encKey size")
	}
	var nonceBlock [aesgcmsivBlockSize]byte
	copy(nonceBlock[aesgcmsivBlockSize-AESGCMSIVNonceSize:], nonce)

	const counterSize = 4 // aesgcmsivBlockSize - AESGCMSIVNonceSize

	var encBlock [aesgcmsivBlockSize]byte
	kdfAes := func(counter uint32, dst []byte) {
		binary.LittleEndian.PutUint32(nonceBlock[:counterSize], counter)
		a.block.Encrypt(encBlock[:], nonceBlock[:])
		copy(dst, encBlock[0:8])
	}

	kdfAes(0, authKey[0:8])
	kdfAes(1, authKey[8:16])
	kdfAes(2, encKey[0:8])
	kdfAes(3, encKey[8:16])
	if a.keySize == 32 {
		kdfAes(4, encKey[16:24])
		kdfAes(5, encKey[24:32])
	}
	return nil
}

func (a *AESGCMSIV) computePolyval(authKey, pt, ad []byte) ([]byte, error) {
	var lengthBlock [aesgcmsivBlockSize]byte
	binary.LittleEndian.PutUint64(lengthBlock[:8], uint64(len(ad))*8)
	binary.LittleEndian.PutUint64(lengthBlock[8:], uint64(len(pt))*8)

	p, err := NewPolyval(authKey)
	if err != nil {
		return nil, fmt.Errorf("aes_gcm_siv: failed to create polyval, error: %v", err)
	}

	p.Update(ad)
	p.Update(pt)
	p.Update(lengthBlock[:])
	polyval := p.Finish()

	return polyval[:], nil
}

func (a *AESGCMSIV) computeTag(polyval, nonce, encKey, out []byte) error {
	if len(polyval) != aesgcmsivPolyvalSize {
		return fmt.Errorf("aes_gcm_siv: polyval returned invalid sized response")
	}
	if len(out) != AESGCMSIVTagSize {
		return fmt.Errorf("aes_gcm_siv: tag buffer should have the same length as tag size")
	}

	subtle.XORBytes(polyval, polyval, nonce)
	polyval[aesgcmsivPolyvalSize-1] &= 0x7f

	block, err := aes.NewCipher(encKey)
	if err != nil {
		return fmt.Errorf("aes_gcm_siv: failed to create block cipher, error: %v", err)
	}

	block.Encrypt(out, polyval)
	return nil
}

// aesCTR implements the AES-CTR operation in AES-GCM-SIV, writing the result to
// out.
//
// NOTE: This is from RFC 8452. The counter incrementation is different from
// standard AES-CTR. Arguments in and out must have the same length.
func aesCTR(key, tag, in, out []byte) error {
	if len(out) != len(in) {
		return fmt.Errorf("aes_gcm_siv: output buffer should have the same length as input buffer; got %d, want %d", len(out), len(in))
	}
	if len(tag) != AESGCMSIVTagSize {
		return fmt.Errorf("aes_gcm_siv: incorrect IV size for stream cipher")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return fmt.Errorf(
			"aes_gcm_siv: failed to create block cipher, error: %v", err)
	}

	var counter [aesgcmsivBlockSize]byte
	copy(counter[:], tag)
	counter[aesgcmsivBlockSize-1] |= 0x80
	counterInc := binary.LittleEndian.Uint32(counter[0:4])

	outputIdx := 0
	var keystreamBlock [aesgcmsivBlockSize]byte
	for len(in) > 0 {
		block.Encrypt(keystreamBlock[:], counter[:])
		counterInc++
		binary.LittleEndian.PutUint32(counter[0:4], counterInc)
		n := subtle.XORBytes(out[outputIdx:], in, keystreamBlock[:])
		outputIdx += n
		in = in[n:]
	}
	return nil
}
