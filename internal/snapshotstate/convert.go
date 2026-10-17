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

// Package snapshotstate converts the snapshot state atelet holds for an actor
// into the form the control plane's API takes. atelet uses it to report that
// state, and ate-api-server to apply the state atelet returns from its RPCs.
package snapshotstate

import (
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// FidelityToAPI maps atelet's fidelity enum onto the identical public one.
// Anything else comes back UNSPECIFIED.
func FidelityToAPI(in ateletpb.SnapshotFidelity) ateapipb.SnapshotFidelity {
	switch in {
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		return ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		return ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		return ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	default:
		return ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
	}
}

// ReportToAPI converts a snapshot atelet reports into the form
// ReportActorSnapshotState takes, which carries the snapshot's fields inline.
// A report that names no snapshot converts to nil.
func ReportToAPI(r *ateletpb.SnapshotReport) *ateapipb.SnapshotReport {
	if r.GetSnapshot() == nil {
		return nil
	}
	snap := r.GetSnapshot()
	out := &ateapipb.SnapshotReport{
		Uuid:                snap.GetSnapshotUuid(),
		Generation:          snap.GetGeneration(),
		Fidelity:            FidelityToAPI(snap.GetFidelity()),
		LocalCommitTime:     r.GetLocalCommitTime(),
		DurableCompleteTime: r.GetDurableCompleteTime(),
	}
	if obj := snap.GetObject(); obj != nil {
		out.Object = &ateapipb.ObjectSnapshot{SnapshotUri: obj.GetSnapshotUri()}
	}
	return out
}

// ToAPI converts the snapshot state atelet holds for an actor into the form
// ReportActorSnapshotState takes. nil converts to nil.
func ToAPI(st *ateletpb.ActorSnapshotState) *ateapipb.ActorSnapshotState {
	if st == nil {
		return nil
	}
	return &ateapipb.ActorSnapshotState{
		LatestLocal:   ReportToAPI(st.GetLatestLocal()),
		Uploading:     ReportToAPI(st.GetUploading()),
		LatestDurable: ReportToAPI(st.GetLatestDurable()),
		UploadDueTime: st.GetUploadDueTime(),
	}
}
