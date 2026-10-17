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

package snapshotstate

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// TestFidelityToAPI verifies each atelet fidelity maps to the same public
// level, and anything else to UNSPECIFIED.
func TestFidelityToAPI(t *testing.T) {
	tests := []struct {
		name string
		in   ateletpb.SnapshotFidelity
		want ateapipb.SnapshotFidelity
	}{
		{"memory", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
		{"rootfs", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS},
		{"volumes", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES},
		{"unspecified stays unspecified", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED},
		{"value outside the enum", ateletpb.SnapshotFidelity(99), ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FidelityToAPI(tt.in); got != tt.want {
				t.Errorf("FidelityToAPI(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestReportToAPI(t *testing.T) {
	committed := timestamppb.New(time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC))
	uploaded := timestamppb.New(time.Date(2026, 10, 14, 12, 5, 0, 0, time.UTC))
	tests := []struct {
		name string
		in   *ateletpb.SnapshotReport
		want *ateapipb.SnapshotReport
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
		want: &ateapipb.SnapshotReport{
			Uuid:                "snap-2",
			Generation:          2,
			Fidelity:            ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			Object:              &ateapipb.ObjectSnapshot{SnapshotUri: "gs://snapshots/snap-2"},
			LocalCommitTime:     committed,
			DurableCompleteTime: uploaded,
		},
	}, {
		name: "a local snapshot has no object",
		in: &ateletpb.SnapshotReport{
			Snapshot:        &ateletpb.Snapshot{SnapshotUuid: "snap-3", Generation: 3, Fidelity: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
			LocalCommitTime: committed,
		},
		want: &ateapipb.SnapshotReport{
			Uuid:            "snap-3",
			Generation:      3,
			Fidelity:        ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
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
			if diff := cmp.Diff(tt.want, ReportToAPI(tt.in), protocmp.Transform()); diff != "" {
				t.Errorf("ReportToAPI mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestToAPI(t *testing.T) {
	due := timestamppb.New(time.Date(2026, 10, 14, 12, 10, 0, 0, time.UTC))
	snap := func(uuid string, gen int32) *ateletpb.SnapshotReport {
		return &ateletpb.SnapshotReport{Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: uuid,
			Generation:   gen,
			Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			Object:       &ateletpb.ObjectStorage{SnapshotUri: "gs://snapshots/" + uuid},
		}}
	}
	want := func(uuid string, gen int32) *ateapipb.SnapshotReport {
		return &ateapipb.SnapshotReport{
			Uuid:       uuid,
			Generation: gen,
			Fidelity:   ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			Object:     &ateapipb.ObjectSnapshot{SnapshotUri: "gs://snapshots/" + uuid},
		}
	}
	tests := []struct {
		name string
		in   *ateletpb.ActorSnapshotState
		want *ateapipb.ActorSnapshotState
	}{{
		name: "every field",
		in: &ateletpb.ActorSnapshotState{
			LatestLocal:   snap("snap-3", 3),
			Uploading:     snap("snap-2", 2),
			LatestDurable: snap("snap-1", 1),
			UploadDueTime: due,
		},
		want: &ateapipb.ActorSnapshotState{
			LatestLocal:   want("snap-3", 3),
			Uploading:     want("snap-2", 2),
			LatestDurable: want("snap-1", 1),
			UploadDueTime: due,
		},
	}, {
		name: "a detach leaves only the latest durable snapshot",
		in:   &ateletpb.ActorSnapshotState{LatestDurable: snap("snap-3", 3)},
		want: &ateapipb.ActorSnapshotState{LatestDurable: want("snap-3", 3)},
	}, {
		name: "empty state",
		in:   &ateletpb.ActorSnapshotState{},
		want: &ateapipb.ActorSnapshotState{},
	}, {
		name: "nil stays nil",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, ToAPI(tt.in), protocmp.Transform()); diff != "" {
				t.Errorf("ToAPI mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
