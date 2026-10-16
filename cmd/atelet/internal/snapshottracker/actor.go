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

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
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
	// object storage. Zero unless it did. It is only valid for durable
	// snapshots, so LatestLocal never carries it.
	UploadTime time.Time
	// Actor is the actor the snapshot belongs to, so a background upload of
	// the snapshot can act for the actor the way an RPC for it would. The
	// Checkpoint or PromoteSnapshot that commits the snapshot sets it.
	Actor resources.ActorRef
}

// snapshotRefJSON is a SnapshotRef as the snapshot state file stores it.
// Snapshot is protojson, so the file keeps every field of ateletpb.Snapshot.
type snapshotRefJSON struct {
	Snapshot   json.RawMessage `json:"snapshot"`
	CommitTime time.Time       `json:"commitTime,omitzero"`
	UploadTime time.Time       `json:"uploadTime,omitzero"`
	Atespace   string          `json:"atespace,omitempty"`
	ActorName  string          `json:"actorName,omitempty"`
}

// MarshalJSON encodes r as a snapshotRefJSON.
func (r SnapshotRef) MarshalJSON() ([]byte, error) {
	snap, err := protojson.Marshal(r.Snapshot)
	if err != nil {
		return nil, err
	}
	return json.Marshal(snapshotRefJSON{Snapshot: snap, CommitTime: r.CommitTime, UploadTime: r.UploadTime, Atespace: r.Actor.Atespace, ActorName: r.Actor.Name})
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
	*r = SnapshotRef{Snapshot: snap, CommitTime: rec.CommitTime, UploadTime: rec.UploadTime, Actor: resources.ActorRef{Atespace: rec.Atespace, Name: rec.ActorName}}
	return nil
}

// clone returns a copy of r that shares no memory with it.
func (r SnapshotRef) clone() *SnapshotRef {
	r.Snapshot = proto.CloneOf(r.Snapshot)
	return &r
}

// uuid returns the snapshot's UUID.
func (r *SnapshotRef) uuid() string {
	return r.Snapshot.GetSnapshotUuid()
}

// uri returns the snapshot's object storage URI.
func (r *SnapshotRef) uri() string {
	return r.Snapshot.GetObject().GetSnapshotUri()
}

// generation returns the snapshot's generation.
func (r *SnapshotRef) generation() int32 {
	return r.Snapshot.GetGeneration()
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
	// node-local checkpoint directory. Nil once detached.
	LatestLocal *SnapshotRef `json:"latestLocal,omitempty"`
	// LatestDurable is the newest snapshot confirmed in object storage.
	LatestDurable *SnapshotRef `json:"latestDurable,omitempty"`

	// DetachAt is when atelet next checks whether to detach the actor. Zero
	// means no detach is scheduled (see detachScheduled).
	//
	// It is set or updated:
	//   - After committing a local snapshot (CommitLocal).
	//   - After a failed detach.
	//
	// It is cleared:
	//   - When a Run or Restore of the actor begins (markStarted), whether or
	//     not it succeeds.
	//   - When the latest local snapshot is detached or deleted
	//     (clearLatestLocal).
	//   - When Terminate forgets the actor.
	//
	// It is not updated, but the actor is queued again, when a worker pulls
	// the actor out of the workqueue and:
	//   - A Run, Restore, or Checkpoint of the actor is in progress
	//     (BeginStart or BeginCheckpoint). Its end queues the actor again.
	//   - Another operation on the actor, such as a Detach call, is in
	//     progress. Its end queues the actor again.
	//   - The detach is not due yet. The worker queues the actor again for
	//     when it is.
	//
	// atelet never detaches a snapshot that no detach can make durable (see
	// detachTarget).
	DetachAt time.Time `json:"detachAt,omitzero"`
}

// actorSnapshots is what atelet tracks of one actor's snapshots on this node.
// Its fields are guarded by the actor's lock in Tracker.locks.
type actorSnapshots struct {
	actorUID string

	// actorSnapshotsRecord holds what the snapshot state file keeps. The
	// fields after it live only in memory.
	actorSnapshotsRecord

	// op is the actor's operation in progress, such as a detach, or nil. A
	// background worker or an RPC call such as Detach runs it; an actor has at
	// most one. The entry stays in memory while op is set.
	op *actorOp
	// holds counts the Runs, Restores, and Checkpoints of the actor in
	// progress on this node. While it is nonzero, atelet does not detach the
	// actor, and the entry stays in memory.
	holds int
	// holdDone is closed once holds drops to zero.
	holdDone chan struct{}
	// retryAt holds the actor's next background detach back after one failed.
	// The entry stays in memory while retryAt is set.
	retryAt time.Time
	// unsaved is set while the snapshot state file lacks a change to the
	// entry because writing the file failed. The entry stays in memory while
	// unsaved is set.
	unsaved bool
}

// isLatestDurable reports whether ref is LatestDurable. It fails with
// FailedPrecondition when LatestDurable has ref's UUID but a different
// generation or URI: the control plane named one snapshot two ways, so refuse
// rather than guess which is right. A later checkpoint clears the condition.
func (a *actorSnapshots) isLatestDurable(ref SnapshotRef) (bool, error) {
	d := a.LatestDurable
	if d == nil || d.uuid() != ref.uuid() {
		return false, nil
	}
	if d.generation() != ref.generation() || d.uri() != ref.uri() {
		return false, apierror.FailedPrecondition("snapshot %s is durable as generation %d at %q, but local as generation %d at %q",
			ref.uuid(), d.generation(), d.uri(), ref.generation(), ref.uri())
	}
	return true, nil
}

// recordDurable records that d is durable, unless a newer snapshot already is.
func (a *actorSnapshots) recordDurable(d *SnapshotRef) {
	if a.LatestDurable == nil || d.generation() >= a.LatestDurable.generation() {
		a.LatestDurable = d
	}
}

// setLatestLocal records snap as the actor's latest local snapshot and
// schedules its detach at detachAt, or none when detachAt is zero. LatestLocal
// keeps snap's Snapshot and CommitTime only: UploadTime is only valid for
// durable snapshots.
func (a *actorSnapshots) setLatestLocal(snap SnapshotRef, detachAt time.Time) {
	a.LatestLocal = &SnapshotRef{Snapshot: proto.CloneOf(snap.Snapshot), CommitTime: snap.CommitTime, Actor: snap.Actor}
	a.DetachAt = detachAt
}

// clearLatestLocal records that atelet holds no local snapshot of the actor,
// and zeroes DetachAt to match: with nothing left to detach, no detach is
// scheduled.
func (a *actorSnapshots) clearLatestLocal() {
	a.LatestLocal = nil
	a.DetachAt = time.Time{}
}

// markStarted unschedules the actor's detach as a Run or Restore of the actor
// begins on this node: atelet does not detach the actor again until its next
// checkpoint schedules a detach. It reports whether a detach was scheduled.
func (a *actorSnapshots) markStarted() bool {
	if a.DetachAt.IsZero() {
		return false
	}
	a.DetachAt = time.Time{}
	return true
}

// detachScheduled reports whether a detach of the actor is scheduled: atelet
// holds a local snapshot of the actor, and no Run or Restore of the actor has
// begun since that snapshot was committed. A scheduled detach runs once it is
// due (see detachDue) and no Run, Restore, or Checkpoint of the actor is in
// progress.
func (a *actorSnapshots) detachScheduled() bool {
	return a.LatestLocal != nil && !a.DetachAt.IsZero()
}

// detachTarget returns LatestLocal, for a detach to make durable, and whether
// it is durable already. It fails with FailedPrecondition when no detach can
// make it durable until a commit replaces it: LatestDurable names the same
// snapshot differently, or LatestLocal is not durable and names no object
// storage location to upload it to, as after a pause today.
func (a *actorSnapshots) detachTarget() (target SnapshotRef, alreadyDurable bool, err error) {
	target = *a.LatestLocal
	alreadyDurable, err = a.isLatestDurable(target)
	if err == nil && !alreadyDurable && target.uri() == "" {
		err = apierror.FailedPrecondition("local snapshot %s names no object storage location to upload it to", target.uuid())
	}
	return target, alreadyDurable, err
}

// detachDue returns when atelet next checks whether to detach the actor: at
// DetachAt, or at retryAt if that is later. ok is false when there is nothing
// a detach can do: no detach is scheduled (see detachScheduled), or
// detachTarget fails, which only a commit can change.
func (a *actorSnapshots) detachDue() (due time.Time, ok bool) {
	if !a.detachScheduled() {
		return time.Time{}, false
	}
	if _, _, err := a.detachTarget(); err != nil {
		return time.Time{}, false
	}
	return later(a.DetachAt, a.retryAt), true
}

func later(t, u time.Time) time.Time {
	if u.After(t) {
		return u
	}
	return t
}
