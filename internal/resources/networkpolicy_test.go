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
	"strings"
	"testing"
)

func TestNetworkPolicyName(t *testing.T) {
	tests := []struct {
		name       string
		wpName     string
		wantPrefix string
	}{
		{
			name:       "short workerpool name",
			wpName:     "my-wp",
			wantPrefix: "substrate-my-wp-",
		},
		{
			name:       "long workerpool name truncated at 30 chars",
			wpName:     "a-very-long-workerpool-name-that-exceeds-30-chars",
			wantPrefix: "substrate-a-very-long-workerpool-name-th-",
		},
		{
			name:   "workerpool name truncated ending with a hyphen",
			wpName: "this-is-a-long-worker-pool-n-with-hyphen-at-30",
			// "this-is-a-long-worker-pool-n-" is 30 chars long, ending in a hyphen.
			// Trimming trailing hyphens turns it into "this-is-a-long-worker-pool-n".
			wantPrefix: "substrate-this-is-a-long-worker-pool-n-",
		},
		{
			name:       "workerpool name ending with hyphens",
			wpName:     "workerpool-name---",
			wantPrefix: "substrate-workerpool-name-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NetworkPolicyName(tt.wpName)

			if !strings.HasPrefix(got, tt.wantPrefix) {
				t.Errorf("NetworkPolicyName(%q) = %q, want prefix %q", tt.wpName, got, tt.wantPrefix)
			}

			// Ensure no double hyphens before the hash
			// The format is substrate-<truncated>-<hash>
			parts := strings.Split(got, "-")
			for i, part := range parts {
				if part == "" && i > 0 && i < len(parts)-1 {
					t.Errorf("NetworkPolicyName(%q) = %q contains empty part (double hyphen)", tt.wpName, got)
				}
			}
		})
	}
}
