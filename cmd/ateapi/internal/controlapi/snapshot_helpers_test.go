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

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestFindSnapshotByGenerationAndSnapshotAtLatestGeneration(t *testing.T) {
	if got := snapshotAtLatestGeneration(nil); got != nil {
		t.Errorf("snapshotAtLatestGeneration(nil) = %v, want nil", got)
	}
	if got := findSnapshotByGeneration(nil, 1); got != nil {
		t.Errorf("findSnapshotByGeneration(nil, 1) = %v, want nil", got)
	}

	s1 := newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", "node-1")
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
	s4 := newLocalSnapshot(4, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-4", "node-1")

	// Place s2 before s1 to verify generation comparison rather than slice order.
	status := &ateapipb.ActorStatus{
		Snapshots: []*ateapipb.Snapshot{s2, s1, s3, s4},
	}

	gotSnap := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if gotSnap != s2 || gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != "gs://b/snap-2" {
		t.Errorf("findLatestDurableSnapshot(COMPLETED) = %v, want gen 2 (gs://b/snap-2)", gotSnap)
	}

	gotSnap = findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	if gotSnap != s3 || gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != "gs://b/snap-3" {
		t.Errorf("findLatestDurableSnapshot(PENDING) = %v, want gen 3 (gs://b/snap-3)", gotSnap)
	}

	gotLocal := findLatestLocalSnapshot(status)
	if gotLocal != s4 || gotLocal.GetUuid() != "local-4" || gotLocal.GetLocality() != "node-1" {
		t.Errorf("findLatestLocalSnapshot() = %v, want %v", gotLocal, s4)
	}
}

func TestSnapshotPruningHelpers(t *testing.T) {
	// Should not panic on nil status.
	clearLocalSnapshots(nil)
	pruneSnapshots(nil, nil)

	t.Run("clearLocalSnapshots clears locality without dropping snapshots", func(t *testing.T) {
		localGen1 := newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", "node-1")
		localGen2 := newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2")
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{localGen1, localGen2},
		}

		clearLocalSnapshots(status)

		want := []*ateapipb.Snapshot{
			newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", ""),
			newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", ""),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("pause flow: pruneSnapshots keeps the latest COMPLETED durable and the new local snapshot", func(t *testing.T) {
		completedGen1 := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		completedGen1.Locality = "node-1"
		localGen2 := newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2")
		localGen3 := newLocalSnapshot(3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "")

		status := &ateapipb.ActorStatus{
			LastAssignedGeneration: 3,
			Snapshots:              []*ateapipb.Snapshot{completedGen1, localGen2, localGen3},
		}

		localGen3.Locality = "node-3"
		pruneSnapshots(status, localGen3)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newLocalSnapshot(3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "node-3"),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
		if status.GetLastAssignedGeneration() != 3 {
			t.Errorf("LastAssignedGeneration = %d, want 3", status.GetLastAssignedGeneration())
		}
	})

	t.Run("pruneSnapshots drops a PENDING snapshot that still names a node", func(t *testing.T) {
		pendingWithLocality := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-2", "gs://b/snap-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
		pendingWithLocality.Locality = "node-1"
		local := newLocalSnapshot(3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "node-1")
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				pendingWithLocality,
				local,
			},
		}

		pruneSnapshots(status, local)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newLocalSnapshot(3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "node-1"),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
		if pendingWithLocality.GetLocality() != "" {
			t.Errorf("dropped PENDING snapshot Locality = %q, want cleared", pendingWithLocality.GetLocality())
		}
	})

	t.Run("revert flow: pruneSnapshots drops all PENDING snapshots and retains latest COMPLETED", func(t *testing.T) {
		completedWithLocality := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		completedWithLocality.Locality = "node-1"
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				completedWithLocality,
				newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2"),
				newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		}

		pruneSnapshots(status, nil)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("suspend flow: pruneSnapshots drops older COMPLETED and PENDING snapshots", func(t *testing.T) {
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-1"),
				newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			},
		}
		status.Snapshots[2].Locality = "node-1"

		pruneSnapshots(status, nil)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
	})
}

// TestLatestSnapshotIsLocalOnly pins the shape a pause leaves behind: the
// latest snapshot is node-local with no durable copy. A durable latest
// snapshot, a pending upload of the local one, or a local snapshot that is
// not captured yet do not count; a RUNNING actor that kept its local snapshot
// across a resume does.
func TestLatestSnapshotIsLocalOnly(t *testing.T) {
	local := func(gen int32) *ateapipb.Snapshot {
		return newLocalSnapshot(gen, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "node-1")
	}
	durable := func(gen int32, st ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
		return newDurableSnapshot(gen, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "durable", "gs://b/durable", st)
	}
	uploading := local(2)
	uploading.DurableSnapshot = &ateapipb.SnapshotStorage{Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING, Object: &ateapipb.ObjectSnapshot{SnapshotUri: "gs://b/up"}}

	tests := []struct {
		name   string
		status *ateapipb.ActorStatus
		want   bool
	}{
		{"nil status", nil, false},
		{"no snapshots", &ateapipb.ActorStatus{}, false},
		{"local only above a durable one", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{durable(1, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED), local(2)}}, true},
		{"local only alone", &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{local(1)}}, true},
		{"durable only", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{durable(2, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)}}, false},
		{"local with an upload pending", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{uploading}}, false},
		{"local not yet captured", &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "")}}, false},
		{"older local only under a pause in progress", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{local(1), newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "")}}, false},
		{"latest generation dropped by a revert", &ateapipb.ActorStatus{LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{local(2)}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := latestSnapshotIsLocalOnly(tc.status); got != tc.want {
				t.Errorf("latestSnapshotIsLocalOnly = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestLatestSnapshotIsPendingLocal pins what tells a SUSPENDING pause from a
// SUSPENDING suspend: the latest snapshot is node-local with no locality yet
// and no durable copy. A running-origin suspend has a pending durable copy, a
// paused-origin suspend has both locality and a pending durable copy, and an
// actor with no snapshot at all has nothing pending.
func TestLatestSnapshotIsPendingLocal(t *testing.T) {
	pendingLocal := newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "")
	capturedLocal := newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "node-1")
	pendingDurable := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "durable", "gs://b/d", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	uploadingLocal := newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "node-1")
	uploadingLocal.DurableSnapshot = &ateapipb.SnapshotStorage{Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING, Object: &ateapipb.ObjectSnapshot{SnapshotUri: "gs://b/up"}}

	tests := []struct {
		name   string
		status *ateapipb.ActorStatus
		want   bool
	}{
		{"nil status", nil, false},
		{"no snapshots", &ateapipb.ActorStatus{}, false},
		{"local not yet captured", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{pendingLocal}}, true},
		{"durable upload pending", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{pendingDurable}}, false},
		{"local with an upload pending", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{uploadingLocal}}, false},
		{"local only", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{capturedLocal}}, false},
		{"latest generation dropped by a revert", &ateapipb.ActorStatus{LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{pendingLocal}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := latestSnapshotIsPendingLocal(tc.status); got != tc.want {
				t.Errorf("latestSnapshotIsPendingLocal = %t, want %t", got, tc.want)
			}
		})
	}
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
