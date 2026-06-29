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

package controlapi

import (
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

func TestToAteletSnapshotScope(t *testing.T) {
	tests := []struct {
		name     string
		in       atev1alpha1.SnapshotScope
		expected ateletpb.SnapshotScope
	}{
		{
			name:     "Full scope",
			in:       atev1alpha1.SnapshotScopeFull,
			expected: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			name:     "Data scope",
			in:       atev1alpha1.SnapshotScopeData,
			expected: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		},
		{
			name:     "Default scope (empty)",
			in:       "",
			expected: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
		{
			name:     "Default scope (unknown)",
			in:       "unknown",
			expected: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toAteletSnapshotScope(tt.in)
			if result != tt.expected {
				t.Errorf("toAteletSnapshotScope(%q) = %v, want %v", tt.in, result, tt.expected)
			}
		})
	}
}
