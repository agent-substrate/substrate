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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestFidelityToAtelet pins the level-by-level mapping onto the atelet enum:
// nothing is promoted, and an unset fidelity stays unset so atelet rejects it.
func TestFidelityToAtelet(t *testing.T) {
	tests := []struct {
		name     string
		in       ateapipb.SnapshotFidelity
		expected ateletpb.SnapshotFidelity
	}{
		{
			name:     "memory",
			in:       ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			expected: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
		{
			name:     "rootfs",
			in:       ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			expected: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
		},
		{
			name:     "volumes",
			in:       ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			expected: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		},
		{
			name:     "unspecified stays unspecified",
			in:       ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED,
			expected: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED,
		},
		{
			name:     "value outside the enum",
			in:       ateapipb.SnapshotFidelity(99),
			expected: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fidelityToAtelet(tt.in)
			if result != tt.expected {
				t.Errorf("fidelityToAtelet(%v) = %v, want %v", tt.in, result, tt.expected)
			}
		})
	}
}

// TestSandboxClassString pins the label values the scheduler and metrics
// share with the CRD's lower-case enum.
func TestSandboxClassString(t *testing.T) {
	tests := []struct {
		in       ateapipb.SandboxClass
		expected string
	}{
		{ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, "gvisor"},
		{ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, "microvm"},
		{ateapipb.SandboxClass_SANDBOX_CLASS_UNSPECIFIED, ""},
	}
	for _, tt := range tests {
		if got := sandboxClassString(tt.in); got != tt.expected {
			t.Errorf("sandboxClassString(%v) = %q, want %q", tt.in, got, tt.expected)
		}
	}
}
