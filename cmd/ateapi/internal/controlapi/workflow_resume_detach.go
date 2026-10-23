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
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// shouldDetachFromNode reports whether a SUSPENDED actor's assignment failed
// for lack of room on the node it is pinned to.
func shouldDetachFromNode(actor *ateapi.Actor, assignErr error) bool {
	return errors.Is(assignErr, errNoWorkerRoom) &&
		actor.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_SUSPENDED &&
		actor.GetStatus().GetAssignedNode() != ""
}

// detachFromFullNode releases the node binding so scheduling can retry.
// Unreachable nodes can be forgotten when the latest local snapshot is durable.
// A deleted node is released even when that snapshot is not durable, so resume
// tries its upload.
func (w *ActorWorkflow) detachFromFullNode(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, actorTemplate *ateapi.ActorTemplate, assignErr error) (_ *ateapi.Actor, err error) {
	ctx, done := stepSpan(ctx, "DetachFromFullNode")
	defer func() { err = done(err) }()

	node := actor.GetStatus().GetAssignedNode()
	// Assignment may have retried on a newer version than actor.
	if actor, err = w.loadPinnedActor(ctx, actorRef, actor.GetMetadata().GetUid(), node); err != nil {
		return nil, err
	}
	constraints := schedulingConstraints(actor, actorTemplate)
	constraints.RequiredNodes = nil
	constraints.RequiredRuntime = hardware.Requirement{}
	if _, err := w.scheduler.Schedule(ctx, constraints); err != nil {
		if errors.Is(err, scheduling.ErrNoCapacity) {
			return nil, assignErr
		}
		return nil, err
	}

	// The place is not claimed first, so two actors detached for it race in
	// their assignments; the loser is left durable-only.
	ateletConn, err := w.dialer.DialForAteletOnNode(node)
	if err != nil {
		if canDetachWithoutAtelet(ctx, actor, err) {
			return w.recordDetach(ctx, actorRef, actor, node, nil)
		}
		if errors.Is(err, ErrNoAteletOnNode) && latestLocalNotDurable(actor) {
			gone, nodeErr := w.dialer.NodeGone(ctx, node)
			if nodeErr != nil {
				// The caller went away or the lease was lost: report that.
				if ctx.Err() != nil {
					return nil, fmt.Errorf("while checking node %q: %w: %w", node, nodeErr, ctx.Err())
				}
				return nil, apierror.Unavailable("while getting atelet conn for node %q: %w (and while checking the node: %w)", node, err, nodeErr)
			}
			if gone {
				return w.recordGoneNode(ctx, actorRef, actor, node)
			}
		}
		return nil, apierror.Unavailable("while getting atelet conn for node %q: %w", node, err)
	}
	resp, err := ateletpb.NewAteletClient(ateletConn).DetachActor(ctx, &ateletpb.DetachActorRequest{
		Atespace:  actor.GetMetadata().GetAtespace(),
		ActorName: actor.GetMetadata().GetName(),
		ActorUid:  actor.GetMetadata().GetUid(),
	})
	if err != nil {
		if canDetachWithoutAtelet(ctx, actor, err) {
			return w.recordDetach(ctx, actorRef, actor, node, nil)
		}
		return nil, detachActorError(ctx, err)
	}
	state := resp.GetState()
	if state.GetLatestLocal() != nil || state.GetUploading() != nil {
		return nil, apierror.Internal("atelet on node %q detached Actor %s but still reports a local snapshot", node, actorRef)
	}

	// A conflict fails the resume with Aborted. atelet's report of the detach
	// records it, and so does the next resume, which detaches again.
	stored, err := w.recordDetach(ctx, actorRef, actor, node, state)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Detached actor from its full node to resume it on another node",
		slog.Any("actor", actorRef),
		slog.String("node", node))
	return stored, nil
}

// loadPinnedActor reads the actor, and fails with Aborted unless it is still
// the SUSPENDED incarnation uid, pinned to node.
func (w *ActorWorkflow) loadPinnedActor(ctx context.Context, actorRef resources.ActorRef, uid, node string) (*ateapi.Actor, error) {
	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, fmt.Errorf("while reading Actor %s: %w", actorRef, err)
	}
	if actor.GetMetadata().GetUid() != uid ||
		actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED ||
		actor.GetStatus().GetAssignedNode() != node {
		return nil, apierror.Aborted("Actor %s changed during the resume, please retry", actorRef)
	}
	return actor, nil
}

// recordDetach forgets local copies after the latest local generation is durable.
// state may be nil when atelet is unreachable.
func (w *ActorWorkflow) recordDetach(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, node string, state *ateletpb.ActorSnapshotState) (*ateapi.Actor, error) {
	applied := proto.CloneOf(actor.GetStatus())
	if err := applyLatestDurable(ctx, applied, snapshotReportFromAtelet(state.GetLatestDurable())); err != nil {
		return nil, apierror.Internal("atelet on node %q reported a state for Actor %s that conflicts with its record: %w", node, actorRef, err)
	}
	// Forget local copies only after the latest recorded generation is durable.
	if err := applyDetach(applied, node, applied.GetLastAssignedGeneration()); err != nil {
		return nil, err
	}
	if applied.GetState() == ateapi.ActorState_ACTOR_STATE_REVERTING && applied.GetWorkerAssignment() == nil {
		applied.AssignedNode = ""
	}
	stored, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapi.Actor) error {
		// The precondition makes toUpdate the version applied was built from.
		toUpdate.Status.Snapshots = applied.GetSnapshots()
		toUpdate.Status.AssignedNode = applied.GetAssignedNode()
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while recording the detach of Actor %s from node %q: %w", actorRef, node, err)
	}
	return stored, nil
}

// recordGoneNode releases an actor from its deleted node when its latest
// snapshot is not recorded durable. atelet may have uploaded it without
// reporting, so the snapshot keeps its locality and resume restores it from
// the upload on another node. It reads the actor again first, so an upload
// recorded during the node lookup detaches it instead.
func (w *ActorWorkflow) recordGoneNode(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, node string) (*ateapi.Actor, error) {
	actor, err := w.loadPinnedActor(ctx, actorRef, actor.GetMetadata().GetUid(), node)
	if err != nil {
		return nil, err
	}
	if !latestLocalNotDurable(actor) {
		return w.recordDetach(ctx, actorRef, actor, node, nil)
	}
	stored, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapi.Actor) error {
		toUpdate.Status.AssignedNode = ""
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("Actor %s changed during the resume, please retry", actorRef)
		}
		return nil, fmt.Errorf("while releasing Actor %s from deleted node %q: %w", actorRef, node, err)
	}
	slog.InfoContext(ctx, "Released actor from its deleted node to restore its snapshot's upload on another node",
		slog.Any("actor", actorRef),
		slog.String("node", node),
		slog.Int("generation", int(findLatestLocalSnapshot(stored.GetStatus()).GetGeneration())))
	return stored, nil
}

// localElsewhere reports whether snap is recorded local on a node other than
// node. Resume restores such a snapshot from its upload after recordGoneNode
// released its deleted node.
func localElsewhere(snap *ateapi.Snapshot, node string) bool {
	return snap.GetLocality() != "" && snap.GetLocality() != node
}

// latestLocalNotDurable reports whether the actor's latest local snapshot has
// no completed durable copy.
func latestLocalNotDurable(actor *ateapi.Actor) bool {
	local := findLatestLocalSnapshot(actor.GetStatus())
	return local != nil && local.GetDurableSnapshot().GetStatus() != ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
}

func canDetachWithoutAtelet(ctx context.Context, actor *ateapi.Actor, err error) bool {
	if ctx.Err() != nil || actor.GetStatus().GetWorkerAssignment() != nil {
		return false
	}
	// A failed restore can leave a live workload after its assignment is cleared.
	if actor.GetStatus().GetCrash() != nil {
		return false
	}
	code := status.Code(err)
	if code == codes.Unknown {
		code = apierror.Code(err)
	}
	if !errors.Is(err, ErrNoAteletOnNode) && code != codes.Unavailable && code != codes.DeadlineExceeded {
		return false
	}
	local := findLatestLocalSnapshot(actor.GetStatus())
	return local != nil && local.GetDurableSnapshot().GetStatus() == ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
}

// detachActorError maps a DetachActor error to the one the resume fails with.
func detachActorError(ctx context.Context, err error) error {
	// The caller went away or the lease was lost: report that, not atelet's error.
	if ctx.Err() != nil {
		return fmt.Errorf("while calling atelet DetachActor: %w: %w", err, ctx.Err())
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
		return apierror.Unavailable("while calling atelet DetachActor: %w", err)
	case codes.Aborted:
		return apierror.Aborted("while calling atelet DetachActor: %w", err)
	// NotFound: the local snapshot is gone and no uploaded copy exists.
	case codes.FailedPrecondition, codes.NotFound:
		return apierror.FailedPrecondition("while calling atelet DetachActor: %w", err)
	}
	return apierror.Internal("while calling atelet DetachActor: %w", err)
}
