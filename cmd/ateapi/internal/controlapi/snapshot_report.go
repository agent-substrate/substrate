// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controlapi

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// ApplyActorSnapshotState records the snapshot state that the atelet on node
// reports for the Actor. The caller has authenticated that atelet.
func (s *RPCService) ApplyActorSnapshotState(ctx context.Context, actorRef resources.ActorRef, actorUID, node string, state *ateapipb.ActorSnapshotState) error {
	setSpanActorRefAttributes(ctx, actorRef)
	return s.actorWorkflow.ApplyReportedSnapshotState(ctx, actorRef, actorUID, node, state)
}

// ApplyReportedSnapshotState applies a report from the atelet on node to the
// Actor. It holds the Actor's lease and applies reports only to a SUSPENDED or
// RUNNING Actor, so a report never interleaves with a workflow. A report that
// changes nothing writes nothing.
func (w *ActorWorkflow) ApplyReportedSnapshotState(ctx context.Context, actorRef resources.ActorRef, actorUID, node string, state *ateapipb.ActorSnapshotState) error {
	ctx = preview.ContextWithOptIn(ctx)
	// Check before taking the lease, so an atelet can't hold the lease of an
	// Actor that isn't on its node. Check again under the lease.
	if _, err := w.loadReportedActor(ctx, actorRef, actorUID, node); err != nil {
		return err
	}
	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return err
	}
	defer lease.Close()
	before, err := w.loadReportedActor(leaseCtx, actorRef, actorUID, node)
	if err != nil {
		return err
	}

	// A workflow owns a transitioning Actor's snapshots, even after its lease
	// lapses. A crashed Actor's snapshots wait for a revert.
	switch st := before.GetStatus().GetState(); st {
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_RUNNING:
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return apierror.FailedPrecondition("actor %s is %s; revert it first", actorRef, st)
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING, ateapipb.ActorState_ACTOR_STATE_RESUMING,
		ateapipb.ActorState_ACTOR_STATE_DELETING, ateapipb.ActorState_ACTOR_STATE_REVERTING:
		return apierror.Aborted("actor %s is %s; retry once that finishes", actorRef, st)
	default:
		return apierror.Internal("actor %s has unexpected state %s", actorRef, st)
	}

	applied := proto.Clone(before).(*ateapipb.Actor)
	if err := applyActorSnapshotState(leaseCtx, applied.GetStatus(), node, state); err != nil {
		return err
	}
	if proto.Equal(applied.GetStatus(), before.GetStatus()) {
		return nil
	}
	if _, err := w.store.UpdateActor(leaseCtx, actorRef, store.PreconditionFrom(before), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status = applied.GetStatus()
		return nil
	}); err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return apierror.Aborted("concurrent update conflict, please retry")
		}
		return err
	}
	return nil
}

// loadReportedActor returns the Actor if it is the incarnation actorUID names
// and is on node. Otherwise it returns NotFound, so an atelet learns nothing
// about Actors on other nodes.
func (w *ActorWorkflow) loadReportedActor(ctx context.Context, actorRef resources.ActorRef, actorUID, node string) (*ateapipb.Actor, error) {
	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, err
	}
	if got := actor.GetMetadata().GetUid(); got != actorUID {
		slog.WarnContext(ctx, "Refusing a snapshot state report naming a stale actor incarnation",
			slog.Any("actor", actorRef),
			slog.String("requested_uid", actorUID),
			slog.String("actual_uid", got),
			slog.String("caller_node", node))
		return nil, apierror.NotFound("Actor %s not found", actorRef)
	}
	if !actorOnNode(actor, node) {
		slog.WarnContext(ctx, "Refusing a snapshot state report for an actor not on the caller's node",
			slog.Any("actor", actorRef),
			slog.String("caller_node", node),
			slog.String("assigned_node", actor.GetStatus().GetAssignedNode()))
		return nil, apierror.NotFound("Actor %s not found", actorRef)
	}
	return actor, nil
}

// actorOnNode reports whether the Actor is placed on node or has a local
// snapshot there. Unlike HostingNode, it includes a node that holds only
// older snapshots of an Actor placed elsewhere, since that node's atelet
// reports them.
func actorOnNode(actor *ateapipb.Actor, node string) bool {
	if actor.GetStatus().GetAssignedNode() == node {
		return true
	}
	for _, snap := range actor.GetStatus().GetSnapshots() {
		if snap.GetLocality() == node {
			return true
		}
	}
	return false
}

// Messages logged for a reported durable snapshot that changes nothing.
const (
	msgDurableOlder       = "Ignoring a durable snapshot older than the latest completed one"
	msgDurableNotRecorded = "Ignoring a durable snapshot at a generation the actor no longer records"
)

// applyActorSnapshotState applies the two facts a report carries: an upload
// finished, and the atelet on node detached the Actor. A report with neither
// latest_local nor uploading is a detach.
func applyActorSnapshotState(ctx context.Context, status *ateapipb.ActorStatus, node string, state *ateapipb.ActorSnapshotState) error {
	if err := applyLatestDurable(ctx, status, state.GetLatestDurable()); err != nil {
		return err
	}
	if state.GetLatestLocal() == nil && state.GetUploading() == nil {
		return applyDetach(status, node, state.GetLatestDurable().GetGeneration())
	}
	return nil
}

// applyCheckpointState records an upload that atelet's state, as returned by
// Checkpoint, says finished, with the same rules as a report. It records the
// fidelity atelet reports, which for Checkpoint is the one requested. Unlike a
// report, a state with no local snapshot is not a detach. The workflow that
// made the call records where its snapshot lives.
func applyCheckpointState(ctx context.Context, status *ateapipb.ActorStatus, state *ateletpb.ActorSnapshotState) error {
	return applyLatestDurable(ctx, status, snapshotReportFromAtelet(state.GetLatestDurable()))
}

// applyLatestDurable marks the reported durable snapshot COMPLETED, and records
// its fidelity. Its uuid and upload URI must match the recorded ones, and its
// fidelity must be set. The snapshot keeps its locality, so a paused Actor
// stays on its node.
//
// A late report changes nothing and is logged at Info. A report is late when
// its generation is older than the latest COMPLETED one, or is no longer in
// status.snapshots, for example because a revert removed it. A repeat of the
// latest COMPLETED generation also changes nothing, and isn't logged.
func applyLatestDurable(ctx context.Context, status *ateapipb.ActorStatus, reported *ateapipb.SnapshotReport) error {
	if reported == nil {
		return nil
	}
	if latest := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); latest.GetGeneration() >= reported.GetGeneration() {
		if latest.GetGeneration() > reported.GetGeneration() {
			slog.InfoContext(ctx, msgDurableOlder,
				slog.Int("generation", int(reported.GetGeneration())),
				slog.String("uuid", reported.GetUuid()),
				slog.Int("latest_completed_generation", int(latest.GetGeneration())))
		}
		return nil
	}
	snap := findSnapshotByGeneration(status.GetSnapshots(), reported.GetGeneration())
	if snap == nil {
		if reported.GetGeneration() <= status.GetLastAssignedGeneration() {
			slog.InfoContext(ctx, msgDurableNotRecorded,
				slog.Int("generation", int(reported.GetGeneration())),
				slog.String("uuid", reported.GetUuid()))
			return nil
		}
		return apierror.FailedPrecondition("snapshot generation %d (%s) is not recorded for this actor", reported.GetGeneration(), reported.GetUuid())
	}
	if snap.GetUuid() != reported.GetUuid() {
		return apierror.FailedPrecondition("snapshot generation %d is recorded as %s, not %s", reported.GetGeneration(), snap.GetUuid(), reported.GetUuid())
	}
	if snap.GetDurableSnapshot() == nil {
		return apierror.FailedPrecondition("snapshot generation %d records no upload", reported.GetGeneration())
	}
	want := snap.GetDurableSnapshot().GetObject().GetSnapshotUri()
	if got := reported.GetObject().GetSnapshotUri(); got != want {
		return apierror.InvalidArgument("snapshot generation %d was uploaded to %q, want %q", reported.GetGeneration(), got, want)
	}
	if reported.GetFidelity() == ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
		return apierror.InvalidArgument("snapshot generation %d reports no fidelity", reported.GetGeneration())
	}
	snap.Fidelity = reported.GetFidelity()
	snap.DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	return nil
}

// applyDetach records that the atelet on node deleted its copies of the
// Actor's snapshots up to generation upTo, the latest snapshot it reported
// durable. A newer snapshot on node postdates the detach, so it stays. A
// SUSPENDED Actor is released from node once none of its snapshots are there.
// It refuses if the latest deleted snapshot isn't durable, so the Actor can't
// resume from an older one.
func applyDetach(status *ateapipb.ActorStatus, node string, upTo int32) error {
	deleted := func(s *ateapipb.Snapshot) bool { return s.GetLocality() == node && s.GetGeneration() <= upTo }
	if latest := latestSnapshot(status, deleted); latest != nil &&
		latest.GetDurableSnapshot().GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
		return apierror.FailedPrecondition("snapshot generation %d on node %s is not durable", latest.GetGeneration(), node)
	}
	for _, snap := range status.GetSnapshots() {
		if deleted(snap) {
			snap.Locality = ""
		}
	}
	onNode := latestSnapshot(status, func(s *ateapipb.Snapshot) bool { return s.GetLocality() == node })
	if status.GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED && status.GetAssignedNode() == node && onNode == nil {
		status.AssignedNode = ""
	}
	return nil
}
