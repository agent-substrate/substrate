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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestFindSnapshotByGenerationAndSnapshotAtLatestGeneration(t *testing.T) {
	if got := snapshotAtLatestGeneration(nil); got != nil {
		t.Errorf("snapshotAtLatestGeneration(nil) = %v, want nil", got)
	}
	if got := findSnapshotByGeneration(nil, 1); got != nil {
		t.Errorf("findSnapshotByGeneration(nil, 1) = %v, want nil", got)
	}

	s1 := newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", "node-1", "gs://b/snap-1")
	s2 := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-2", "gs://b/snap-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	status := &ateapipb.ActorStatus{
		LastAssignedGeneration: 2,
		Snapshots:              []*ateapipb.Snapshot{s1, s2},
	}

	if got := findSnapshotByGeneration(status.GetSnapshots(), 1); got != s1 {
		t.Errorf("findSnapshotByGeneration(1) = %v, want %v", got, s1)
	}
	if got := findSnapshotByGeneration(status.GetSnapshots(), 99); got != nil {
		t.Errorf("findSnapshotByGeneration(99) = %v, want nil", got)
	}
	if got := snapshotAtLatestGeneration(status); got != s2 {
		t.Errorf("snapshotAtLatestGeneration() = %v, want %v", got, s2)
	}
}

func TestFindLatestDurableAndLocalSnapshots(t *testing.T) {
	if snap := findLatestDurableSnapshot(nil, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); snap != nil {
		t.Errorf("findLatestDurableSnapshot(nil) = %v, want nil", snap)
	}
	if snap := findLatestLocalSnapshot(nil); snap != nil {
		t.Errorf("findLatestLocalSnapshot(nil) = %v, want nil", snap)
	}

	s1 := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	s2 := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-2", "gs://b/snap-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	s3 := newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	s4 := newLocalSnapshot(4, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-4", "node-1", "gs://b/snap-4")

	// Place s2 before s1 to verify generation comparison rather than slice order.
	status := &ateapipb.ActorStatus{
		Snapshots: []*ateapipb.Snapshot{s2, s1, s3, s4},
	}

	gotSnap := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if gotSnap != s2 || gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != "gs://b/snap-2" {
		t.Errorf("findLatestDurableSnapshot(COMPLETED) = %v, want gen 2 (gs://b/snap-2)", gotSnap)
	}

	gotSnap = findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	if gotSnap != s4 || gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != "gs://b/snap-4" {
		t.Errorf("findLatestDurableSnapshot(PENDING) = %v, want gen 4 (gs://b/snap-4)", gotSnap)
	}

	gotLocal := findLatestLocalSnapshot(status)
	if gotLocal != s4 || gotLocal.GetUuid() != "local-4" || gotLocal.GetLocality() != "node-1" {
		t.Errorf("findLatestLocalSnapshot() = %v, want %v", gotLocal, s4)
	}
}

func TestSnapshotPruningHelpers(t *testing.T) {
	// Should not panic on nil status.
	clearLocalSnapshots(nil)
	pruneSnapshots(nil)

	t.Run("clearLocalSnapshots clears locality without dropping snapshots", func(t *testing.T) {
		localGen1 := newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", "node-1", "gs://b/snap-1")
		localGen2 := newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2", "gs://b/snap-2")
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{localGen1, localGen2},
		}

		clearLocalSnapshots(status)

		want := []*ateapipb.Snapshot{
			newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", "", "gs://b/snap-1"),
			newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "", "gs://b/snap-2"),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("pause flow: clearLocalSnapshots + set locality + pruneSnapshots preserves latest COMPLETED durable and current local snapshot", func(t *testing.T) {
		completedGen1 := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		completedGen1.Locality = "node-1"
		localGen2 := newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2", "gs://b/snap-2")
		localGen3 := newLocalSnapshot(3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "", "gs://b/snap-3")

		status := &ateapipb.ActorStatus{
			LastAssignedGeneration: 3,
			Snapshots:              []*ateapipb.Snapshot{completedGen1, localGen2, localGen3},
		}

		clearLocalSnapshots(status)
		localGen3.Locality = "node-3"
		pruneSnapshots(status)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newLocalSnapshot(3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "node-3", "gs://b/snap-3"),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
		if status.GetLastAssignedGeneration() != 3 {
			t.Errorf("LastAssignedGeneration = %d, want 3", status.GetLastAssignedGeneration())
		}
	})

	t.Run("revert flow: clearLocalSnapshots + pruneSnapshots drops all PENDING snapshots and retains latest COMPLETED", func(t *testing.T) {
		completedWithLocality := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		completedWithLocality.Locality = "node-1"
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				completedWithLocality,
				newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2", "gs://b/snap-2"),
				newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		}

		clearLocalSnapshots(status)
		pruneSnapshots(status)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("suspend flow: clearLocalSnapshots + pruneSnapshots drops older COMPLETED and PENDING snapshots", func(t *testing.T) {
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-1", "gs://b/snap-2"),
				newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			},
		}
		status.Snapshots[2].Locality = "node-1"

		clearLocalSnapshots(status)
		pruneSnapshots(status)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestTagDurableSnapshotURI(t *testing.T) {
	if got := tagDurableSnapshotURI(nil, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED); got != "" {
		t.Errorf("tagDurableSnapshotURI(nil) = %q, want empty", got)
	}

	pendingTag := &ateapipb.Tag{
		Status: &ateapipb.TagStatus{
			Snapshot: newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/tag-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
		},
	}
	if got := tagDurableSnapshotURI(pendingTag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); got != "" {
		t.Errorf("tagDurableSnapshotURI(pendingTag, COMPLETED) = %q, want empty", got)
	}
	if got := tagDurableSnapshotURI(pendingTag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); got != "gs://b/tag-1" {
		t.Errorf("tagDurableSnapshotURI(pendingTag, PENDING) = %q, want gs://b/tag-1", got)
	}
	if got := tagDurableSnapshotURI(pendingTag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED); got != "gs://b/tag-1" {
		t.Errorf("tagDurableSnapshotURI(pendingTag, UNSPECIFIED) = %q, want gs://b/tag-1", got)
	}
}
