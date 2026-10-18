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

// sameUUID reports whether a and b are non-nil and have the same UUID.
func sameUUID(a, b *SnapshotRef) bool {
	return a != nil && b != nil && a.uuid() == b.uuid()
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
	// Uploading is the snapshot a background upload has begun making durable.
	// It is saved in the state file and stays set across checkpoints, retry
	// backoffs, and atelet restarts until the upload succeeds, fails and a
	// newer local snapshot replaces it, or a newer snapshot becomes durable,
	// unlike runningUpload, which is only set while an upload goroutine is
	// running.
	Uploading *SnapshotRef `json:"uploading,omitempty"`
	// LatestDurable is the newest snapshot confirmed in object storage.
	LatestDurable *SnapshotRef `json:"latestDurable,omitempty"`

	// UndurableSince is what uploadDelay is measured from: the CommitTime of
	// the oldest local checkpoint taken since the last durable snapshot, or,
	// for one taken while an upload ran, when that upload succeeded. Zero
	// once the latest local checkpoint is durable, or an immediate
	// checkpoint has superseded it.
	//
	// Measuring from UndurableSince rather than LatestLocal.CommitTime
	// ensures an actor that pauses and resumes repeatedly still uploads
	// within uploadDelay instead of postponing durability indefinitely. An
	// upload changes UndurableSince only once it succeeds, and a failed or
	// interrupted one leaves it as it was.
	UndurableSince time.Time `json:"undurableSince,omitzero"`
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
	//   - When the latest local snapshot is detached, or a newer durable
	//     snapshot supersedes it (clearLatestLocal).
	//   - When Terminate forgets the actor.
	//
	// It is not updated, but the actor is queued again, when a worker pulls
	// the actor out of the workqueue and:
	//   - A Run, Restore, Checkpoint, or PromoteSnapshot of the actor is in
	//     progress (BeginStart, BeginCheckpoint, or BeginPromote). Its end
	//     queues the actor again.
	//   - Another operation on the actor, such as a Detach call, is in
	//     progress. Its end queues the actor again.
	//   - The detach is not due yet. The worker queues the actor again for
	//     when it is.
	//
	// atelet never detaches a snapshot that no detach can make durable (see
	// detachTarget).
	DetachAt time.Time `json:"detachAt,omitzero"`
	// ReportDueSince is when the latest background change atelet owes a
	// report of (a new LatestDurable, or local snapshots cleared by a
	// detach) finished. Zero once no report is due: the control plane
	// acknowledged or refused it, a commit or Detach call returned the
	// state, or atelet gave up after reportQueue.giveUpAfter.
	ReportDueSince time.Time `json:"reportDueSince,omitzero"`
}

// actorSnapshots is what atelet tracks of one actor's snapshots on this node.
// Its fields are guarded by the actor's lock in Tracker.locks.
type actorSnapshots struct {
	actorUID string

	// actorSnapshotsRecord holds what the snapshot state file keeps. The
	// fields after it live only in memory.
	actorSnapshotsRecord

	// op is the actor's operation in progress, an upload or a detach, or nil.
	// A background worker or an RPC call such as Detach runs it; an actor has
	// at most one. The entry stays in memory while op is set.
	op *actorOp
	// holds counts the Runs, Restores, Checkpoints, and PromoteSnapshots of
	// the actor in progress on this node. While it is nonzero, atelet does not
	// detach the actor or report its state, and the entry stays in memory.
	holds int
	// inFlight holds the snapshots of Checkpoints and PromoteSnapshots of the
	// actor currently in progress on this node. newest includes them, so an
	// older generation cannot start while a newer one is in flight.
	inFlight []*SnapshotRef
	// promotes counts the PromoteSnapshots of the actor in progress on this
	// node. While it is nonzero, atelet does not start a background upload of
	// the actor.
	promotes int
	// reportDeferred is set when a report of the actor's state is held back
	// while holds is nonzero, so holdLocked queues it once holds reaches zero.
	reportDeferred bool
	// retryAt holds the actor's next background upload or detach back after
	// one failed. The entry stays in memory while retryAt is set.
	retryAt time.Time
	// uploadingFailed is set after a background upload of Uploading failed and
	// Uploading was kept to retry. The entry stays in memory while it is set.
	uploadingFailed bool
	// unsaved is set while the snapshot state file lacks a change to the
	// entry because writing the file failed. The entry stays in memory while
	// unsaved is set.
	unsaved bool
}

// localNeeded reports whether CommitLocal should record snap as LatestLocal:
// LatestLocal does not already hold snap or a newer snapshot, and no other
// committed snapshot has a higher generation or the same generation under
// another UUID.
func (a *actorSnapshots) localNeeded(snap SnapshotRef) bool {
	if a.LatestLocal != nil && a.LatestLocal.generation() >= snap.generation() {
		return false
	}
	for _, r := range []*SnapshotRef{a.Uploading, a.LatestDurable} {
		if r == nil {
			continue
		}
		if r.generation() > snap.generation() {
			return false
		}
		if r.generation() == snap.generation() && !sameUUID(r, &snap) {
			return false
		}
	}
	return true
}

// reportDue reports whether atelet owes the control plane a report of the
// actor's state.
func (a *actorSnapshots) reportDue() bool {
	return !a.ReportDueSince.IsZero()
}

// latestDurableActor returns the actor LatestDurable belongs to, as its commit
// named it. A report is due only once LatestDurable is set.
func (a *actorSnapshots) latestDurableActor() resources.ActorRef {
	if a.LatestDurable == nil {
		return resources.ActorRef{}
	}
	return a.LatestDurable.Actor
}

// uploadNeeded reports whether ref still needs uploading: neither it nor a
// newer snapshot is durable.
func (a *actorSnapshots) uploadNeeded(ref SnapshotRef) bool {
	return a.LatestDurable == nil || a.LatestDurable.generation() < ref.generation()
}

// canUpload reports whether ref is non-nil, still needs uploading, and names
// an object storage location where atelet can upload it.
func (a *actorSnapshots) canUpload(ref *SnapshotRef) bool {
	if ref == nil || !a.uploadNeeded(*ref) || ref.uri() == "" {
		return false
	}
	_, err := a.isLatestDurable(*ref)
	return err == nil
}

// localSuperseded reports whether ref is no longer kept as LatestLocal:
// LatestLocal was cleared or has moved on to a higher generation.
func (a *actorSnapshots) localSuperseded(ref *SnapshotRef) bool {
	if sameUUID(a.LatestLocal, ref) {
		return false
	}
	return a.LatestLocal == nil || a.LatestLocal.generation() > ref.generation()
}

// keepUploading reports whether Uploading is still in use: an upload is
// running it, or it can still be uploaded and has not failed with a newer
// uploadable local snapshot ready to replace it.
func (a *actorSnapshots) keepUploading() bool {
	if a.Uploading == nil {
		return false
	}
	if a.runningUpload() != nil {
		return true
	}
	if !a.canUpload(a.Uploading) {
		return false
	}
	if a.uploadingFailed && a.localSuperseded(a.Uploading) && a.canUpload(a.LatestLocal) {
		return false
	}
	return true
}

// clearSupersededUploading clears Uploading when keepUploading reports it is
// no longer kept. It returns the local copy to delete, or nil, and whether
// Uploading was cleared.
func (a *actorSnapshots) clearSupersededUploading() (removed *SnapshotRef, cleared bool) {
	if a.Uploading == nil || a.keepUploading() {
		return nil, false
	}
	old := a.Uploading
	a.Uploading = nil
	a.uploadingFailed = false
	if !a.localSuperseded(old) {
		return nil, true
	}
	return old, true
}

// nextUploadTarget returns the snapshot the next background upload makes
// durable. An earlier upload recorded in Uploading is retried or resumed when
// no upload goroutine is running it (runningUpload is nil) before moving on to
// LatestLocal, unless that upload failed and LatestLocal can replace it, or a
// newer snapshot is already durable. It returns nil when nothing is waiting to
// upload or that snapshot is already uploading.
func (a *actorSnapshots) nextUploadTarget() *SnapshotRef {
	running := a.runningUpload()
	if a.keepUploading() && running == nil {
		return a.Uploading
	}
	if !sameUUID(running, a.LatestLocal) && a.canUpload(a.LatestLocal) {
		return a.LatestLocal
	}
	return nil
}

// isLatestDurable reports whether ref is LatestDurable. It fails with
// FailedPrecondition when LatestDurable has ref's UUID but a different
// generation, fidelity, or URI: the control plane named one snapshot two
// ways, so refuse rather than guess which is right. A later checkpoint clears
// the condition.
func (a *actorSnapshots) isLatestDurable(ref SnapshotRef) (bool, error) {
	d := a.LatestDurable
	if !sameUUID(d, &ref) {
		return false, nil
	}
	if d.generation() != ref.generation() || d.uri() != ref.uri() {
		return false, apierror.FailedPrecondition("snapshot %s is durable as generation %d at %q, but requested as generation %d at %q",
			ref.uuid(), d.generation(), d.uri(), ref.generation(), ref.uri())
	}
	if d.Snapshot.GetFidelity() != ref.Snapshot.GetFidelity() {
		return false, apierror.FailedPrecondition("snapshot %s is durable with fidelity %s, but requested with fidelity %s",
			ref.uuid(), d.Snapshot.GetFidelity(), ref.Snapshot.GetFidelity())
	}
	return true, nil
}

// recordDurable records that d is durable, unless that generation or a newer
// snapshot already is, and reports whether it recorded d.
func (a *actorSnapshots) recordDurable(d *SnapshotRef) bool {
	if !a.uploadNeeded(*d) {
		return false
	}
	a.LatestDurable = d
	return true
}

// runningUpload returns the snapshot an opUpload goroutine is currently
// uploading, or nil when no upload is running right now (such as while
// Uploading waits in retry backoff or after an atelet restart).
func (a *actorSnapshots) runningUpload() *SnapshotRef {
	if a.op == nil || a.op.kind != opUpload {
		return nil
	}
	return &a.op.target
}

// runningDetach returns the snapshot an opDetach goroutine is currently
// detaching, or nil.
func (a *actorSnapshots) runningDetach() *SnapshotRef {
	if a.op == nil || a.op.kind != opDetach {
		return nil
	}
	return &a.op.target
}

// opTarget returns the snapshot an operation in progress acts on, or nil.
func (a *actorSnapshots) opTarget() *SnapshotRef {
	if a.op == nil {
		return nil
	}
	return &a.op.target
}

// newest returns the actor's snapshot with the highest generation, or nil
// when atelet holds no snapshot for the actor. It includes snapshots whose
// Checkpoint or PromoteSnapshot is in flight.
func (a *actorSnapshots) newest() *SnapshotRef {
	var newest *SnapshotRef
	consider := func(r *SnapshotRef) {
		if r != nil && (newest == nil || r.generation() > newest.generation()) {
			newest = r
		}
	}
	consider(a.LatestLocal)
	consider(a.Uploading)
	consider(a.LatestDurable)
	consider(a.opTarget())
	for _, r := range a.inFlight {
		consider(r)
	}
	return newest
}

// checkGeneration returns a FailedPrecondition error if snap has a stale
// generation for the actor: below the actor's newest snapshot, or equal to
// it under another snapshot_uuid.
func (a *actorSnapshots) checkGeneration(snap *ateletpb.Snapshot) error {
	newest := a.newest()
	if newest == nil {
		return nil
	}
	switch {
	case snap.GetGeneration() < newest.generation():
		return apierror.FailedPrecondition("snapshot %q has generation %d, below the actor's newest snapshot %q at generation %d", snap.GetSnapshotUuid(), snap.GetGeneration(), newest.uuid(), newest.generation())
	case snap.GetGeneration() == newest.generation() && snap.GetSnapshotUuid() != newest.uuid():
		return apierror.FailedPrecondition("snapshot %q has generation %d, which the actor's newest snapshot %q already holds", snap.GetSnapshotUuid(), snap.GetGeneration(), newest.uuid())
	default:
		return nil
	}
}

// uploadDue returns when atelet next runs a background upload of the actor:
// uploadDelay after UndurableSince, or at retryAt if that is later. An upload
// or promote in progress can delay it. ok is false when there is nothing
// waiting to upload (see nextUploadTarget).
func (a *actorSnapshots) uploadDue(uploadDelay time.Duration) (due time.Time, ok bool) {
	target := a.nextUploadTarget()
	if target == nil {
		return time.Time{}, false
	}
	since := a.UndurableSince
	if since.IsZero() {
		since = target.CommitTime
	}
	return later(since.Add(uploadDelay), a.retryAt), true
}

// uploadSucceeded records that an upload succeeded at now. UndurableSince
// becomes zero, or now if a checkpoint committed while the upload ran is not
// durable yet.
func (a *actorSnapshots) uploadSucceeded(now time.Time) {
	a.UndurableSince = time.Time{}
	if a.LatestLocal != nil && a.uploadNeeded(*a.LatestLocal) {
		a.UndurableSince = now
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
// due (see detachDue) and no Run, Restore, Checkpoint, or PromoteSnapshot of
// the actor is in progress.
func (a *actorSnapshots) detachScheduled() bool {
	return a.LatestLocal != nil && !a.DetachAt.IsZero()
}

// detachTarget returns LatestLocal, the snapshot a detach makes durable, and
// whether it is durable already. It fails with FailedPrecondition when no
// detach can make it durable until a commit replaces it: LatestDurable names
// the same snapshot differently, or LatestLocal is not durable and names no
// object storage location to upload it to, as after a pause today.
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
