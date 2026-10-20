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

import "testing"

func TestNetworkPolicyName(t *testing.T) {
	tests := []struct {
		wpName string
		want   string
	}{
		{"my-wp", "substrate-my-wp"},
		{"a-very-long-workerpool-name-that-exceeds-30-chars", "substrate-a-very-long-workerpool-name-that-exceeds-30-chars"},
		{"my.pool", "substrate-my.pool"},
	}
	for _, tt := range tests {
		if got := NetworkPolicyName(tt.wpName); got != tt.want {
			t.Errorf("NetworkPolicyName(%q) = %q, want %q", tt.wpName, got, tt.want)
		}
	}
}

// These names share their first 30 characters and the first 5 hex digits of
// their SHA-256, so a name built from those collided.
func TestNetworkPolicyNameDiffersForFormerlyCollidingPools(t *testing.T) {
	a := NetworkPolicyName("a-very-long-workerpool-name-th-323")
	b := NetworkPolicyName("a-very-long-workerpool-name-th-423")
	if a == b {
		t.Errorf("NetworkPolicyName gave %q for both pools", a)
	}
}
