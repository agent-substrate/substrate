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
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestFindSnapshotByGenerationAndSnapshotAtLatestGeneration(t *testing.T) {
	if got := snapshotAtLatestGeneration(nil); got != nil {
		t.Errorf("snapshotAtLatestGeneration(nil) = %v, want nil", got)
	}
	if got := findSnapshotByGeneration(nil, 1); got != nil {
		t.Errorf("findSnapshotByGeneration(nil, 1) = %v, want nil", got)
	}

	s1 := newPausedSnapshot(t, 1, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", "node-1")
	s2 := newDurableSnapshot(2, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-2", "gs://b/snap-2", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	status := &ateapi.ActorStatus{
		LastAssignedGeneration: 2,
		Snapshots:              []*ateapi.Snapshot{s1, s2},
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
	if snap := findLatestDurableSnapshot(nil, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); snap != nil {
		t.Errorf("findLatestDurableSnapshot(nil) = %v, want nil", snap)
	}
	if snap := findLatestLocalSnapshot(nil); snap != nil {
		t.Errorf("findLatestLocalSnapshot(nil) = %v, want nil", snap)
	}

	s1 := newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	s2 := newDurableSnapshot(2, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-2", "gs://b/snap-2", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	s3 := newDurableSnapshot(3, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	s4 := newPausedSnapshot(t, 4, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-4", "node-1")

	// Place s2 before s1 to verify generation comparison rather than slice order.
	status := &ateapi.ActorStatus{
		Snapshots: []*ateapi.Snapshot{s2, s1, s3, s4},
	}

	gotSnap := findLatestDurableSnapshot(status, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if gotSnap != s2 || gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != "gs://b/snap-2" {
		t.Errorf("findLatestDurableSnapshot(COMPLETED) = %v, want gen 2 (gs://b/snap-2)", gotSnap)
	}

	// An async suspend snapshot's durable copy is PENDING until atelet uploads it.
	gotSnap = findLatestDurableSnapshot(status, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	if gotSnap != s4 {
		t.Errorf("findLatestDurableSnapshot(PENDING) = %v, want gen 4", gotSnap)
	}
	gotSnap = findLatestDurableSnapshot(&ateapi.ActorStatus{Snapshots: []*ateapi.Snapshot{s2, s1, s3}}, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	if gotSnap != s3 || gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != "gs://b/snap-3" {
		t.Errorf("findLatestDurableSnapshot(PENDING) without the paused snapshot = %v, want gen 3 (gs://b/snap-3)", gotSnap)
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
		localGen1 := newPausedSnapshot(t, 1, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", "node-1")
		localGen2 := newPausedSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2")
		status := &ateapi.ActorStatus{
			Snapshots: []*ateapi.Snapshot{localGen1, localGen2},
		}

		clearLocalSnapshots(status)

		want := []*ateapi.Snapshot{
			newPausedSnapshot(t, 1, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", ""),
			newPausedSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", ""),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("pause flow: pruneSnapshots keeps the latest COMPLETED durable and the new local snapshot", func(t *testing.T) {
		completedGen1 := newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		completedGen1.Locality = "node-1"
		localGen2 := newPausedSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2")
		localGen3 := newPausedSnapshot(t, 3, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "")

		status := &ateapi.ActorStatus{
			LastAssignedGeneration: 3,
			Snapshots:              []*ateapi.Snapshot{completedGen1, localGen2, localGen3},
		}

		localGen3.Locality = "node-3"
		pruneSnapshots(status, localGen3)

		want := []*ateapi.Snapshot{
			newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newPausedSnapshot(t, 3, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "node-3"),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
		if status.GetLastAssignedGeneration() != 3 {
			t.Errorf("LastAssignedGeneration = %d, want 3", status.GetLastAssignedGeneration())
		}
	})

	t.Run("pruneSnapshots drops a PENDING snapshot that still names a node", func(t *testing.T) {
		pendingWithLocality := newDurableSnapshot(2, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-2", "gs://b/snap-2", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
		pendingWithLocality.Locality = "node-1"
		local := newPausedSnapshot(t, 3, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "node-1")
		status := &ateapi.ActorStatus{
			Snapshots: []*ateapi.Snapshot{
				newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				pendingWithLocality,
				local,
			},
		}

		pruneSnapshots(status, local)

		want := []*ateapi.Snapshot{
			newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newPausedSnapshot(t, 3, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "node-1"),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
		if pendingWithLocality.GetLocality() != "" {
			t.Errorf("dropped PENDING snapshot Locality = %q, want cleared", pendingWithLocality.GetLocality())
		}
	})

	t.Run("revert flow: pruneSnapshots drops all PENDING snapshots and retains latest COMPLETED", func(t *testing.T) {
		completedWithLocality := newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		completedWithLocality.Locality = "node-1"
		status := &ateapi.ActorStatus{
			Snapshots: []*ateapi.Snapshot{
				completedWithLocality,
				newPausedSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2"),
				newDurableSnapshot(3, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		}

		pruneSnapshots(status, nil)

		want := []*ateapi.Snapshot{
			newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("suspend flow: pruneSnapshots drops older COMPLETED and PENDING snapshots", func(t *testing.T) {
		status := &ateapi.ActorStatus{
			Snapshots: []*ateapi.Snapshot{
				newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				newPausedSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-1"),
				newDurableSnapshot(3, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			},
		}
		status.Snapshots[2].Locality = "node-1"

		pruneSnapshots(status, nil)

		want := []*ateapi.Snapshot{
			newDurableSnapshot(3, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
	})
}

// TestLatestSnapshotIsLocal pins when the actor's latest snapshot is
// node-local, whether or not atelet has uploaded it. A latest snapshot with
// only a durable copy, or a local one not captured yet, does not count.
func TestLatestSnapshotIsLocal(t *testing.T) {
	local := func(gen int32) *ateapi.Snapshot {
		return newPausedSnapshot(t, gen, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "node-1")
	}
	withCopy := func(st ateapi.SnapshotStorageStatus) *ateapi.Snapshot {
		snap := local(1)
		snap.DurableSnapshot = &ateapi.SnapshotStorage{Status: st, Object: &ateapi.ObjectSnapshot{SnapshotUri: "gs://b/up"}}
		return snap
	}
	durable := newDurableSnapshot(2, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "durable", "gs://b/durable", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	notCaptured := newPausedSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "")

	tests := []struct {
		name   string
		status *ateapi.ActorStatus
		want   bool
	}{
		{"nil status", nil, false},
		{"no snapshots", &ateapi.ActorStatus{}, false},
		{"local only", &ateapi.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapi.Snapshot{local(1)}}, true},
		{"local and uploaded", &ateapi.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapi.Snapshot{withCopy(ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)}}, true},
		{"local with an upload pending", &ateapi.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapi.Snapshot{withCopy(ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)}}, true},
		{"durable only", &ateapi.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{durable}}, false},
		{"older local under a pause in progress", &ateapi.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{local(1), notCaptured}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := latestSnapshotIsLocal(tc.status); got != tc.want {
				t.Errorf("latestSnapshotIsLocal = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestTagDurableSnapshotURI(t *testing.T) {
	if got := tagDurableSnapshotURI(nil, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED); got != "" {
		t.Errorf("tagDurableSnapshotURI(nil) = %q, want empty", got)
	}

	pendingTag := &ateapi.Tag{
		Status: &ateapi.TagStatus{
			Snapshot: newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/tag-1", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
		},
	}
	if got := tagDurableSnapshotURI(pendingTag, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); got != "" {
		t.Errorf("tagDurableSnapshotURI(pendingTag, COMPLETED) = %q, want empty", got)
	}
	if got := tagDurableSnapshotURI(pendingTag, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); got != "gs://b/tag-1" {
		t.Errorf("tagDurableSnapshotURI(pendingTag, PENDING) = %q, want gs://b/tag-1", got)
	}
	if got := tagDurableSnapshotURI(pendingTag, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED); got != "gs://b/tag-1" {
		t.Errorf("tagDurableSnapshotURI(pendingTag, UNSPECIFIED) = %q, want gs://b/tag-1", got)
	}
}

func TestResolveSnapshotLocation(t *testing.T) {
	const (
		templateLocation = "gs://template-bucket/root"
		bucketPrefix     = "gs://flag-bucket/prefix/"
	)
	tests := []struct {
		name             string
		gateEnabled      bool
		bucketPrefix     string
		templateLocation string
		want             string
		wantErr          bool
	}{
		{
			name:             "gate disabled uses the flag and ignores the template location",
			bucketPrefix:     bucketPrefix,
			templateLocation: templateLocation,
			want:             bucketPrefix,
		},
		{
			name:         "gate disabled uses the flag when the template sets none",
			bucketPrefix: bucketPrefix,
			want:         bucketPrefix,
		},
		{
			name:             "gate disabled fails without the flag even with a template location",
			templateLocation: templateLocation,
			wantErr:          true,
		},
		{
			name:             "gate enabled prefers the template location",
			gateEnabled:      true,
			bucketPrefix:     bucketPrefix,
			templateLocation: templateLocation,
			want:             templateLocation,
		},
		{
			name:         "gate enabled falls back to the flag",
			gateEnabled:  true,
			bucketPrefix: bucketPrefix,
			want:         bucketPrefix,
		},
		{
			name:             "gate enabled without the flag uses the template location",
			gateEnabled:      true,
			templateLocation: templateLocation,
			want:             templateLocation,
		},
		{
			name:        "gate enabled with neither fails",
			gateEnabled: true,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gates []string
			if tt.gateEnabled {
				gates = []string{string(preview.GatePerTemplateSnapshotLocation)}
			}
			preview.InitForTest(t, gates...)
			tmpl := &ateapi.ActorTemplate{
				Metadata:       &ateapi.ResourceMetadata{Atespace: "team-a", Name: "tmpl"},
				SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: tt.templateLocation},
			}
			got, err := resolveSnapshotLocation(tt.bucketPrefix, tmpl)
			if tt.wantErr {
				if apierror.Code(err) != codes.FailedPrecondition {
					t.Fatalf("resolveSnapshotLocation() err = %v, want FailedPrecondition", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveSnapshotLocation() err = %v", err)
			}
			if got != tt.want {
				t.Errorf("resolveSnapshotLocation() = %q, want %q", got, tt.want)
			}
		})
	}
}
