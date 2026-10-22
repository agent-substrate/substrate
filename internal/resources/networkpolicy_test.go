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
		name   string
		wpName string
		want   string
	}{
		{
			name:   "short name",
			wpName: "my-wp",
			want:   "substrate-my-wp",
		},
		{
			name:   "dotted name",
			wpName: "my.pool",
			want:   "substrate-my.pool",
		},
		// The next two share their first 30 characters and the first 5 hex
		// digits of their SHA-256.
		{
			name:   "long name ending 323",
			wpName: "a-very-long-workerpool-name-th-323",
			want:   "substrate-a-very-long-workerpool-name-th-323",
		},
		{
			name:   "long name ending 423",
			wpName: "a-very-long-workerpool-name-th-423",
			want:   "substrate-a-very-long-workerpool-name-th-423",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NetworkPolicyName(tt.wpName); got != tt.want {
				t.Errorf("NetworkPolicyName(%q) = %q, want %q", tt.wpName, got, tt.want)
			}
		})
	}
}
