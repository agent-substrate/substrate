// Copyright 2026 Google LLC
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
	"math"

	"golang.org/x/crypto/chacha20poly1305"
)

// CheckChaCha20Poly1305PlaintextSize checks if the given plaintext size is
// valid for ChaCha20Poly1305.
func CheckChaCha20Poly1305PlaintextSize(plaintextLength int) error {
	maxSize := math.MaxInt - chacha20poly1305.NonceSize - chacha20poly1305.Overhead
	if plaintextLength > maxSize {
		return fmt.Errorf("plaintext too long: got %d, want %d", plaintextLength, maxSize)
	}
	return nil
}
