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

	s1 := newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	s2 := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/snap-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
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

func TestSnapshotStorageAccessors(t *testing.T) {
	if got := findSnapshotStorage(nil, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL); got != nil {
		t.Errorf("findSnapshotStorage(nil) = %v, want nil", got)
	}
	// Should not panic on nil snapshot.
	removeSnapshotStorage(nil, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL)

	snap := newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-in-progress", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS)

	// Replace existing LOCAL entry.
	completedLocal := &ateapipb.SnapshotStorage{
		Durability: ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL,
		Status:     ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
		Local:      &ateapipb.LocalSnapshot{SnapshotName: "local-completed"},
	}
	setSnapshotStorage(snap, completedLocal)
	if len(snap.GetStorage()) != 1 {
		t.Fatalf("len(snap.Storage) = %d, want 1", len(snap.GetStorage()))
	}
	if got := findSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL); got != completedLocal {
		t.Errorf("findSnapshotStorage(LOCAL) = %v, want %v", got, completedLocal)
	}

	// Append DURABLE entry alongside LOCAL.
	durableEntry := &ateapipb.SnapshotStorage{
		Durability: ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE,
		Status:     ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
		Object:     &ateapipb.ObjectSnapshot{SnapshotUri: "gs://b/snap-1"},
	}
	setSnapshotStorage(snap, durableEntry)
	if len(snap.GetStorage()) != 2 {
		t.Fatalf("len(snap.Storage) = %d, want 2", len(snap.GetStorage()))
	}
	if got := findSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE); got != durableEntry {
		t.Errorf("findSnapshotStorage(DURABLE) = %v, want %v", got, durableEntry)
	}

	// Remove LOCAL and verify DURABLE remains.
	removeSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL)
	if got := findSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL); got != nil {
		t.Errorf("findSnapshotStorage(LOCAL) after remove = %v, want nil", got)
	}
	if got := findSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE); got != durableEntry {
		t.Errorf("findSnapshotStorage(DURABLE) after removing LOCAL = %v, want %v", got, durableEntry)
	}
}

func TestFindLatestSnapshotStorage(t *testing.T) {
	if snap, st := findLatestSnapshotStorage(nil, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); snap != nil || st != nil {
		t.Errorf("findLatestSnapshotStorage(nil) = (%v, %v), want (nil, nil)", snap, st)
	}

	s1 := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	s2 := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/snap-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	s3 := newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS)
	s4 := newLocalSnapshot(4, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-4", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)

	// Place s2 before s1 to verify generation comparison rather than slice order.
	status := &ateapipb.ActorStatus{
		Snapshots: []*ateapipb.Snapshot{s2, s1, s3, s4},
	}

	gotSnap, gotSt := findLatestSnapshotStorage(status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if gotSnap != s2 || gotSt.GetObject().GetSnapshotUri() != "gs://b/snap-2" {
		t.Errorf("findLatestSnapshotStorage(DURABLE, COMPLETED) = (gen %d, %v), want (gen 2, gs://b/snap-2)", gotSnap.GetGeneration(), gotSt)
	}

	gotSnap, gotSt = findLatestSnapshotStorage(status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS)
	if gotSnap != s3 || gotSt.GetObject().GetSnapshotUri() != "gs://b/snap-3" {
		t.Errorf("findLatestSnapshotStorage(DURABLE, IN_PROGRESS) = (gen %d, %v), want (gen 3, gs://b/snap-3)", gotSnap.GetGeneration(), gotSt)
	}

	gotSnap, gotSt = findLatestSnapshotStorage(status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if gotSnap != s4 || gotSt.GetLocal().GetSnapshotName() != "local-4" {
		t.Errorf("findLatestSnapshotStorage(LOCAL, COMPLETED) = (gen %d, %v), want (gen 4, local-4)", gotSnap.GetGeneration(), gotSt)
	}

	gotSnap, gotSt = findLatestSnapshotStorage(status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS)
	if gotSnap != nil || gotSt != nil {
		t.Errorf("findLatestSnapshotStorage(LOCAL, IN_PROGRESS) = (%v, %v), want (nil, nil)", gotSnap, gotSt)
	}
}

func TestRemoveSnapshotStorageEntries(t *testing.T) {
	// Should not panic on nil status.
	removeSnapshotStorageEntries(nil, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, nil)
	removeOlderSnapshotStorageEntries(nil, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, nil, 3)

	t.Run("removes older local snapshots while preserving keepGen, newer generations, and durable storage", func(t *testing.T) {
		bothGen1 := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		setSnapshotStorage(bothGen1, &ateapipb.SnapshotStorage{
			Durability: ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL,
			Status:     ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
			Local:      &ateapipb.LocalSnapshot{SnapshotName: "local-1"},
		})
		localGen2 := newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		localGen3 := newLocalSnapshot(3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		localGen4 := newLocalSnapshot(4, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-4", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS)

		status := &ateapipb.ActorStatus{
			LastAssignedGeneration: 4,
			Snapshots:              []*ateapipb.Snapshot{bothGen1, localGen2, localGen3, localGen4},
		}

		removeOlderSnapshotStorageEntries(status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, nil, 3)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newLocalSnapshot(3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newLocalSnapshot(4, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-4", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
		if status.GetLastAssignedGeneration() != 4 {
			t.Errorf("LastAssignedGeneration = %d, want 4", status.GetLastAssignedGeneration())
		}
	})

	t.Run("removes all matching snapshot storage entries across all generations", func(t *testing.T) {
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				newLocalSnapshot(3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS),
			},
		}

		removeSnapshotStorageEntries(status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, nil)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("filters by specific storageStatus", func(t *testing.T) {
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS),
				newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/snap-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			},
		}

		removeSnapshotStorageEntries(status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, new(ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS))

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/snap-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
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

	inProgressTag := &ateapipb.Tag{
		Status: &ateapipb.TagStatus{
			Snapshot: newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "gs://b/tag-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS),
		},
	}
	if got := tagDurableSnapshotURI(inProgressTag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); got != "" {
		t.Errorf("tagDurableSnapshotURI(inProgressTag, COMPLETED) = %q, want empty", got)
	}
	if got := tagDurableSnapshotURI(inProgressTag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS); got != "gs://b/tag-1" {
		t.Errorf("tagDurableSnapshotURI(inProgressTag, IN_PROGRESS) = %q, want gs://b/tag-1", got)
	}
	if got := tagDurableSnapshotURI(inProgressTag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED); got != "gs://b/tag-1" {
		t.Errorf("tagDurableSnapshotURI(inProgressTag, UNSPECIFIED) = %q, want gs://b/tag-1", got)
	}
}
