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

package resources

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// NetworkPolicyName returns the deterministic Kubernetes NetworkPolicy name
// generated for a given WorkerPool name.
func NetworkPolicyName(wpName string) string {
	sum := sha256.Sum256([]byte(wpName))
	hash := hex.EncodeToString(sum[:])
	if len(hash) > 5 {
		hash = hash[:5]
	}
	truncated := wpName
	if len(truncated) > 30 {
		truncated = truncated[:30]
	}
	// Trim trailing hyphens to prevent double hyphens when appending "-<hash>".
	truncated = strings.TrimRight(truncated, "-")
	return fmt.Sprintf("substrate-%s-%s", truncated, hash)
}
