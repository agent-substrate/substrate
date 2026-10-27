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
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// TestFidelityToAtelet pins the level-by-level mapping onto the atelet enum:
// nothing is promoted, and an unset fidelity stays unset so atelet rejects it.
func TestFidelityToAtelet(t *testing.T) {
	tests := []struct {
		name     string
		in       ateapi.SnapshotFidelity
		expected ateletpb.SnapshotFidelity
	}{
		{
			name:     "memory",
			in:       ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			expected: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
		{
			name:     "rootfs",
			in:       ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			expected: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
		},
		{
			name:     "volumes",
			in:       ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			expected: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		},
		{
			name:     "unspecified stays unspecified",
			in:       ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED,
			expected: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED,
		},
		{
			name:     "value outside the enum",
			in:       ateapi.SnapshotFidelity(99),
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

// TestFidelityFromAtelet verifies each atelet fidelity maps back to the same
// public level, and anything else to UNSPECIFIED.
func TestFidelityFromAtelet(t *testing.T) {
	tests := []struct {
		name string
		in   ateletpb.SnapshotFidelity
		want ateapi.SnapshotFidelity
	}{
		{"memory", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
		{"rootfs", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS},
		{"volumes", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES},
		{"unspecified stays unspecified", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED},
		{"value outside the enum", ateletpb.SnapshotFidelity(99), ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fidelityFromAtelet(tt.in); got != tt.want {
				t.Errorf("fidelityFromAtelet(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestSnapshotReportFromAtelet(t *testing.T) {
	committed := timestamppb.New(time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC))
	uploaded := timestamppb.New(time.Date(2026, 10, 14, 12, 5, 0, 0, time.UTC))
	tests := []struct {
		name string
		in   *ateletpb.SnapshotReport
		want *ateapi.SnapshotReport
	}{{
		name: "every field",
		in: &ateletpb.SnapshotReport{
			Snapshot: &ateletpb.Snapshot{
				SnapshotUuid: "snap-2",
				Generation:   2,
				Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
				Object:       &ateletpb.ObjectStorage{SnapshotUri: "gs://snapshots/snap-2"},
			},
			LocalCommitTime:     committed,
			DurableCompleteTime: uploaded,
		},
		want: &ateapi.SnapshotReport{
			Uuid:                "snap-2",
			Generation:          2,
			Fidelity:            ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			Object:              &ateapi.ObjectSnapshot{SnapshotUri: "gs://snapshots/snap-2"},
			LocalCommitTime:     committed,
			DurableCompleteTime: uploaded,
		},
	}, {
		name: "a local snapshot has no object",
		in: &ateletpb.SnapshotReport{
			Snapshot:        &ateletpb.Snapshot{SnapshotUuid: "snap-3", Generation: 3, Fidelity: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
			LocalCommitTime: committed,
		},
		want: &ateapi.SnapshotReport{
			Uuid:            "snap-3",
			Generation:      3,
			Fidelity:        ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			LocalCommitTime: committed,
		},
	}, {
		name: "a report that names no snapshot converts to nil",
		in:   &ateletpb.SnapshotReport{LocalCommitTime: committed},
	}, {
		name: "nil stays nil",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, snapshotReportFromAtelet(tt.in), protocmp.Transform()); diff != "" {
				t.Errorf("snapshotReportFromAtelet mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
