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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// findSnapshotByGeneration returns the Snapshot in snapshots with the given
// generation, or nil if none exists.
func findSnapshotByGeneration(snapshots []*ateapipb.Snapshot, gen int32) *ateapipb.Snapshot {
	for _, snap := range snapshots {
		if snap.GetGeneration() == gen {
			return snap
		}
	}
	return nil
}

// snapshotAtLatestGeneration returns the Snapshot at status.last_assigned_generation, or
// nil if none exists.
func snapshotAtLatestGeneration(status *ateapipb.ActorStatus) *ateapipb.Snapshot {
	if status == nil {
		return nil
	}
	return findSnapshotByGeneration(status.GetSnapshots(), status.GetLastAssignedGeneration())
}

// findLatestDurableSnapshot returns the highest-generation Snapshot on status
// whose DurableSnapshot matches storageStatus, or nil if none exists.
func findLatestDurableSnapshot(status *ateapipb.ActorStatus, storageStatus ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
	var bestSnap *ateapipb.Snapshot
	for _, snap := range status.GetSnapshots() {
		if snap.GetDurableSnapshot().GetStatus() != storageStatus {
			continue
		}
		if bestSnap == nil || snap.GetGeneration() >= bestSnap.GetGeneration() {
			bestSnap = snap
		}
	}
	return bestSnap
}

// findLatestLocalSnapshot returns the highest-generation Snapshot on status
// that holds a completed local checkpoint (Locality != ""), or nil if none
// exists.
func findLatestLocalSnapshot(status *ateapipb.ActorStatus) *ateapipb.Snapshot {
	var bestSnap *ateapipb.Snapshot
	for _, snap := range status.GetSnapshots() {
		if snap.GetLocality() == "" {
			continue
		}
		if bestSnap == nil || snap.GetGeneration() >= bestSnap.GetGeneration() {
			bestSnap = snap
		}
	}
	return bestSnap
}

// clearLocalSnapshots clears Locality across all Snapshots in status.Snapshots
// without dropping PENDING durable snapshots (so DeleteActor can still collect
// an in-flight durable snapshot after releasing the worker).
func clearLocalSnapshots(status *ateapipb.ActorStatus) {
	for _, snap := range status.GetSnapshots() {
		snap.Locality = ""
	}
}

// pruneSnapshots retains only Snapshots in status.Snapshots that are the
// latest COMPLETED durable snapshot or still hold a local checkpoint
// (Locality != ""), dropping all other (stale PENDING or superseded COMPLETED)
// Snapshots. Any older COMPLETED durable snapshots are expected to have
// already been garbage collected (or captured for cleanup) beforehand.
//
// Dropping PENDING durable snapshots that have no local checkpoint is safe
// because:
//   - On pause finalization, pause is only reachable from RUNNING (a failed
//     suspend cannot resume without reverting first, which discards any
//     in-flight upload), so any older PENDING entry was only pre-populated by
//     an earlier pause that was resumed rather than suspended and never wrote
//     to object storage.
//   - On revert finalization, ensureInProgressSnapshotDiscarded has already
//     deleted any in-flight durable snapshot objects.
//   - On suspend finalization, the latest snapshot has already been marked
//     COMPLETED.
//
// status.LastAssignedGeneration is not decreased so that we can keep track of
// stale snapshots that need to be cleaned up and discarded generation numbers
// are never reused.
func pruneSnapshots(status *ateapipb.ActorStatus) {
	if status == nil {
		return
	}
	latestDurable := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	kept := status.Snapshots[:0]
	for _, snap := range status.Snapshots {
		if snap == latestDurable || snap.GetLocality() != "" {
			kept = append(kept, snap)
		}
	}
	status.Snapshots = kept
}

// newDurableSnapshot constructs a Snapshot with DurableSnapshot populated.
func newDurableSnapshot(gen int32, owner ateapipb.SnapshotOwner, fidelity ateapipb.SnapshotFidelity, templateUID, uuid, uri string, storageStatus ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
	return &ateapipb.Snapshot{
		Uuid:             uuid,
		Generation:       gen,
		Owner:            owner,
		ActorTemplateUid: templateUID,
		DurableSnapshot: &ateapipb.SnapshotStorage{
			Status:   storageStatus,
			Fidelity: fidelity,
			Object:   &ateapipb.ObjectSnapshot{SnapshotUri: uri},
		},
	}
}

// newLocalSnapshot constructs an actor Snapshot for a local pause checkpoint
// with Uuid, Locality, and a prepopulated PENDING DurableSnapshot entry.
func newLocalSnapshot(gen int32, fidelity ateapipb.SnapshotFidelity, templateUID, uuid, locality, uri string) *ateapipb.Snapshot {
	return &ateapipb.Snapshot{
		Uuid:             uuid,
		Generation:       gen,
		Owner:            ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
		ActorTemplateUid: templateUID,
		Locality:         locality,
		DurableSnapshot: &ateapipb.SnapshotStorage{
			Status:   ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING,
			Fidelity: fidelity,
			Object:   &ateapipb.ObjectSnapshot{SnapshotUri: uri},
		},
	}
}

// tagDurableSnapshotURI returns the durable snapshot URI on tag matching
// storageStatus (or any status if storageStatus is UNSPECIFIED), or "" if none
// exists.
func tagDurableSnapshotURI(tag *ateapipb.Tag, storageStatus ateapipb.SnapshotStorageStatus) string {
	st := tag.GetStatus().GetSnapshot().GetDurableSnapshot()
	if storageStatus != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED && st.GetStatus() != storageStatus {
		return ""
	}
	return st.GetObject().GetSnapshotUri()
}
