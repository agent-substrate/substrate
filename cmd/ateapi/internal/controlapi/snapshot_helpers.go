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
	"math"

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

// findSnapshotStorage returns the SnapshotStorage entry on snap with the given
// durability, or nil if none exists.
func findSnapshotStorage(snap *ateapipb.Snapshot, durability ateapipb.SnapshotDurability) *ateapipb.SnapshotStorage {
	for _, st := range snap.GetStorage() {
		if st.GetDurability() == durability {
			return st
		}
	}
	return nil
}

// setSnapshotStorage replaces an existing SnapshotStorage entry on snap with
// the same durability, or appends entry if none exists.
func setSnapshotStorage(snap *ateapipb.Snapshot, entry *ateapipb.SnapshotStorage) {
	for i, st := range snap.Storage {
		if st.GetDurability() == entry.GetDurability() {
			snap.Storage[i] = entry
			return
		}
	}
	snap.Storage = append(snap.Storage, entry)
}

// removeSnapshotStorage removes any SnapshotStorage entry on snap with the
// given durability.
func removeSnapshotStorage(snap *ateapipb.Snapshot, durability ateapipb.SnapshotDurability) {
	if snap == nil {
		return
	}
	filtered := snap.Storage[:0]
	for _, st := range snap.Storage {
		if st.GetDurability() != durability {
			filtered = append(filtered, st)
		}
	}
	snap.Storage = filtered
}

// findLatestSnapshotStorage returns the highest-generation Snapshot on status
// that holds a SnapshotStorage entry matching durability and storageStatus,
// along with that SnapshotStorage entry, or (nil, nil) if none exists.
func findLatestSnapshotStorage(status *ateapipb.ActorStatus, durability ateapipb.SnapshotDurability, storageStatus ateapipb.SnapshotStorageStatus) (*ateapipb.Snapshot, *ateapipb.SnapshotStorage) {
	var bestSnap *ateapipb.Snapshot
	var bestStorage *ateapipb.SnapshotStorage
	for _, snap := range status.GetSnapshots() {
		st := findSnapshotStorage(snap, durability)
		if st.GetStatus() != storageStatus {
			continue
		}
		if bestSnap == nil || snap.GetGeneration() >= bestSnap.GetGeneration() {
			bestSnap = snap
			bestStorage = st
		}
	}
	return bestSnap, bestStorage
}

// removeSnapshotStorageEntries removes SnapshotStorage entries matching
// durability (and *storageStatus, if non-nil) from status.Snapshots, dropping
// any Snapshot left with no storage entries.
//
// Called from ensureWorkerReleased during DeleteActor, ensureRevertedFinalized
// during revert, and ensureSuspendedFinalized during suspend.
func removeSnapshotStorageEntries(status *ateapipb.ActorStatus, durability ateapipb.SnapshotDurability, storageStatus *ateapipb.SnapshotStorageStatus) {
	removeOlderSnapshotStorageEntries(status, durability, storageStatus, math.MaxInt32)
}

// removeOlderSnapshotStorageEntries removes SnapshotStorage entries matching
// durability (and *storageStatus, if non-nil) from Snapshots in
// status.Snapshots with generation older than keepGen, dropping any Snapshot
// left with no storage entries.
//
// status.LastAssignedGeneration is not decreased so that we can keep track of
// stale snapshots that need to be cleaned up and discarded generation numbers
// are never reused.
//
// Called from ensurePausedFinalized on pause success and
// ensureSuspendedFinalized during suspend.
func removeOlderSnapshotStorageEntries(status *ateapipb.ActorStatus, durability ateapipb.SnapshotDurability, storageStatus *ateapipb.SnapshotStorageStatus, keepGen int32) {
	if status == nil {
		return
	}
	kept := status.Snapshots[:0]
	for _, snap := range status.Snapshots {
		if snap.GetGeneration() < keepGen {
			storage := snap.Storage[:0]
			for _, st := range snap.Storage {
				if st.GetDurability() == durability &&
					(storageStatus == nil || st.GetStatus() == *storageStatus) {
					continue
				}
				storage = append(storage, st)
			}
			snap.Storage = storage
		}
		if len(snap.GetStorage()) > 0 {
			kept = append(kept, snap)
		}
	}
	status.Snapshots = kept
}

// newDurableSnapshot constructs a Snapshot with a single DURABLE ObjectSnapshot
// storage entry.
func newDurableSnapshot(gen int32, owner ateapipb.SnapshotOwner, fidelity ateapipb.SnapshotFidelity, templateUID, uri string, storageStatus ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
	return &ateapipb.Snapshot{
		Generation:       gen,
		Owner:            owner,
		ActorTemplateUid: templateUID,
		Storage: []*ateapipb.SnapshotStorage{{
			Durability: ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE,
			Status:     storageStatus,
			Fidelity:   fidelity,
			Object:     &ateapipb.ObjectSnapshot{SnapshotUri: uri},
		}},
	}
}

// newLocalSnapshot constructs an actor Snapshot with a single LOCAL
// LocalSnapshot storage entry.
func newLocalSnapshot(gen int32, fidelity ateapipb.SnapshotFidelity, templateUID, snapshotName string, storageStatus ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
	return &ateapipb.Snapshot{
		Generation:       gen,
		Owner:            ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
		ActorTemplateUid: templateUID,
		Storage: []*ateapipb.SnapshotStorage{{
			Durability: ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL,
			Status:     storageStatus,
			Fidelity:   fidelity,
			Local:      &ateapipb.LocalSnapshot{SnapshotName: snapshotName},
		}},
	}
}

// tagDurableSnapshotURI returns the durable snapshot URI on tag matching
// storageStatus (or any status if storageStatus is UNSPECIFIED), or "" if none
// exists.
func tagDurableSnapshotURI(tag *ateapipb.Tag, storageStatus ateapipb.SnapshotStorageStatus) string {
	st := findSnapshotStorage(tag.GetStatus().GetSnapshot(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE)
	if storageStatus != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED && st.GetStatus() != storageStatus {
		return ""
	}
	return st.GetObject().GetSnapshotUri()
}
