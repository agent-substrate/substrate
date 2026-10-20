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
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
)

// findSnapshotByGeneration returns the Snapshot in snapshots with the given
// generation, or nil if none exists.
func findSnapshotByGeneration(snapshots []*ateapi.Snapshot, gen int32) *ateapi.Snapshot {
	for _, snap := range snapshots {
		if snap.GetGeneration() == gen {
			return snap
		}
	}
	return nil
}

// snapshotAtLatestGeneration returns the Snapshot at status.last_assigned_generation, or
// nil if none exists.
func snapshotAtLatestGeneration(status *ateapi.ActorStatus) *ateapi.Snapshot {
	if status == nil {
		return nil
	}
	return findSnapshotByGeneration(status.GetSnapshots(), status.GetLastAssignedGeneration())
}

// latestSnapshot returns the highest-generation Snapshot on status that
// matches, or nil if none does.
func latestSnapshot(status *ateapi.ActorStatus, matches func(*ateapi.Snapshot) bool) *ateapi.Snapshot {
	var latest *ateapi.Snapshot
	for _, snap := range status.GetSnapshots() {
		if matches(snap) && (latest == nil || snap.GetGeneration() >= latest.GetGeneration()) {
			latest = snap
		}
	}
	return latest
}

// findLatestDurableSnapshot returns the highest-generation Snapshot on status
// whose DurableSnapshot matches storageStatus, or nil if none exists.
func findLatestDurableSnapshot(status *ateapi.ActorStatus, storageStatus ateapi.SnapshotStorageStatus) *ateapi.Snapshot {
	return latestSnapshot(status, func(snap *ateapi.Snapshot) bool {
		return snap.GetDurableSnapshot().GetStatus() == storageStatus
	})
}

// findLatestLocalSnapshot returns the highest-generation snapshot with a local copy.
func findLatestLocalSnapshot(status *ateapi.ActorStatus) *ateapi.Snapshot {
	return latestSnapshot(status, func(snap *ateapi.Snapshot) bool {
		return snap.GetLocality() != ""
	})
}

// latestSnapshotIsLocal reports whether the latest generation has a local copy.
func latestSnapshotIsLocal(status *ateapi.ActorStatus) bool {
	return snapshotAtLatestGeneration(status).GetLocality() != ""
}

// clearLocalSnapshots clears Locality across all Snapshots in status.Snapshots
// without dropping PENDING durable snapshots (so DeleteActor can still collect
// an in-flight durable snapshot after releasing the worker).
func clearLocalSnapshots(status *ateapi.ActorStatus) {
	for _, snap := range status.GetSnapshots() {
		snap.Locality = ""
	}
}

// pruneSnapshots retains only local, the Snapshot whose local checkpoint the
// actor keeps (nil if it keeps none), and the latest COMPLETED durable
// snapshot, dropping all other (stale PENDING or superseded COMPLETED)
// Snapshots. It clears Locality on every Snapshot other than local: it runs
// in finalize steps that follow an atelet call that deleted the actor's other
// local checkpoints from the node. Any older COMPLETED durable snapshots are
// expected to have already been garbage collected (or captured for cleanup)
// beforehand.
//
// Dropping PENDING durable snapshots is safe because:
//   - On asynchronous suspend finalization, the new snapshot is kept with its
//     PENDING copy. An earlier asynchronous suspend's PENDING copy is dropped:
//     whatever atelet uploaded of it stays under the actor's prefix until
//     DeleteActor collects it.
//   - On revert finalization, ensureInProgressSnapshotDiscarded has already
//     deleted the objects of every PENDING durable snapshot.
//   - On synchronous suspend finalization, the latest snapshot has already been
//     marked COMPLETED.
//
// status.LastAssignedGeneration is not decreased so that we can keep track of
// stale snapshots that need to be cleaned up and discarded generation numbers
// are never reused.
func pruneSnapshots(status *ateapi.ActorStatus, local *ateapi.Snapshot) {
	if status == nil {
		return
	}
	latestDurable := findLatestDurableSnapshot(status, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	kept := status.Snapshots[:0]
	for _, snap := range status.Snapshots {
		if snap != local {
			snap.Locality = ""
		}
		if snap == local || snap == latestDurable {
			kept = append(kept, snap)
		}
	}
	status.Snapshots = kept
}

// newDurableSnapshot constructs a Snapshot with DurableSnapshot populated.
func newDurableSnapshot(gen int32, owner ateapi.SnapshotOwner, fidelity ateapi.SnapshotFidelity, templateUID, uuid, uri string, storageStatus ateapi.SnapshotStorageStatus) *ateapi.Snapshot {
	return &ateapi.Snapshot{
		Uuid:             uuid,
		Generation:       gen,
		Owner:            owner,
		Fidelity:         fidelity,
		ActorTemplateUid: templateUID,
		DurableSnapshot: &ateapi.SnapshotStorage{
			Status: storageStatus,
			Object: &ateapi.ObjectSnapshot{SnapshotUri: uri},
		},
	}
}

// tagDurableSnapshotURI returns the durable snapshot URI on tag matching
// storageStatus (or any status if storageStatus is UNSPECIFIED), or "" if none
// exists.
func tagDurableSnapshotURI(tag *ateapi.Tag, storageStatus ateapi.SnapshotStorageStatus) string {
	st := tag.GetStatus().GetSnapshot().GetDurableSnapshot()
	if storageStatus != ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED && st.GetStatus() != storageStatus {
		return ""
	}
	return st.GetObject().GetSnapshotUri()
}

// resolveSnapshotLocation returns the base object-storage URI actorTemplate's
// snapshots are stored under: its snapshotConfig.storage_location when the
// PerTemplateSnapshotLocation preview gate is enabled and the field is set,
// otherwise snapshotLocation, ate-api-server's cluster-wide
// --snapshot-location. It fails with FailedPrecondition when neither is set.
//
// It reads the process-wide gate rather than preview.IsEnabledFor, so a
// request's preview opt-out cannot move where an actor's snapshots live.
func resolveSnapshotLocation(snapshotLocation string, actorTemplate *ateapi.ActorTemplate) (string, error) {
	if preview.IsEnabled(preview.GatePerTemplateSnapshotLocation) {
		if location := actorTemplate.GetSnapshotConfig().GetStorageLocation(); location != "" {
			return location, nil
		}
	}
	if snapshotLocation != "" {
		return snapshotLocation, nil
	}
	return "", apierror.FailedPrecondition(
		"actor template %s/%s has no snapshot storage location: set snapshotConfig.storageLocation, or set ate-api-server's --snapshot-location",
		actorTemplate.GetMetadata().GetAtespace(), actorTemplate.GetMetadata().GetName())
}
