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

package snapshottracker

import (
	"encoding/json"
	"time"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SnapshotRef is one snapshot of an actor that atelet holds or has uploaded.
type SnapshotRef struct {
	// Snapshot is the snapshot as the control plane named it.
	Snapshot *ateletpb.Snapshot
	// CommitTime is when the checkpoint that produced the snapshot committed
	// on this node. Zero when this atelet did not take it.
	CommitTime time.Time
	// UploadTime is when this atelet committed the snapshot's manifest to
	// object storage. Zero unless it did.
	UploadTime time.Time
}

// snapshotRefJSON is a SnapshotRef as the snapshot state file stores it.
// Snapshot is protojson, so the file keeps every field of ateletpb.Snapshot.
type snapshotRefJSON struct {
	Snapshot   json.RawMessage `json:"snapshot"`
	CommitTime time.Time       `json:"commitTime,omitzero"`
	UploadTime time.Time       `json:"uploadTime,omitzero"`
}

// MarshalJSON encodes r as a snapshotRefJSON.
func (r SnapshotRef) MarshalJSON() ([]byte, error) {
	snap, err := protojson.Marshal(r.Snapshot)
	if err != nil {
		return nil, err
	}
	return json.Marshal(snapshotRefJSON{Snapshot: snap, CommitTime: r.CommitTime, UploadTime: r.UploadTime})
}

// UnmarshalJSON decodes a snapshotRefJSON into r.
func (r *SnapshotRef) UnmarshalJSON(data []byte) error {
	var rec snapshotRefJSON
	if err := json.Unmarshal(data, &rec); err != nil {
		return err
	}
	snap := &ateletpb.Snapshot{}
	if err := protojson.Unmarshal(rec.Snapshot, snap); err != nil {
		return err
	}
	*r = SnapshotRef{Snapshot: snap, CommitTime: rec.CommitTime, UploadTime: rec.UploadTime}
	return nil
}

// clone returns a copy of r that shares no memory with it.
func (r SnapshotRef) clone() *SnapshotRef {
	r.Snapshot = proto.CloneOf(r.Snapshot)
	return &r
}

// report converts the SnapshotRef for a field of ActorSnapshotState.
func (r *SnapshotRef) report() *ateletpb.SnapshotReport {
	if r == nil {
		return nil
	}
	rep := &ateletpb.SnapshotReport{Snapshot: proto.CloneOf(r.Snapshot)}
	if !r.CommitTime.IsZero() {
		rep.LocalCommitTime = timestamppb.New(r.CommitTime)
	}
	if !r.UploadTime.IsZero() {
		rep.DurableCompleteTime = timestamppb.New(r.UploadTime)
	}
	return rep
}

// actorSnapshotsRecord is the part of actorSnapshots saved to the actor's
// snapshot state file.
type actorSnapshotsRecord struct {
	// LatestLocal is the newest snapshot currently stored in the actor's
	// node-local checkpoint directory. Nil after an UPLOAD_MODE_IMMEDIATE
	// checkpoint.
	LatestLocal *SnapshotRef `json:"latestLocal,omitempty"`
	// LatestDurable is the newest snapshot confirmed in object storage.
	LatestDurable *SnapshotRef `json:"latestDurable,omitempty"`
}

// actorSnapshots is what atelet tracks of one actor's snapshots on this node.
// Its fields are guarded by the actor's lock in Tracker.locks.
type actorSnapshots struct {
	actorUID string

	// actorSnapshotsRecord holds what the snapshot state file keeps.
	actorSnapshotsRecord

	// op is the actor's background operation in progress, or nil. An actor
	// has at most one. The entry stays in memory while op is set.
	op *actorOp
	// unsaved is set while the snapshot state file lacks a change to the
	// entry because writing the file failed. The entry stays in memory while
	// unsaved is set.
	unsaved bool
}
