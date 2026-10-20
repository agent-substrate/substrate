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

	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// shouldDetachFromNode reports whether a SUSPENDED actor's assignment failed
// for lack of room on the node it is pinned to.
func shouldDetachFromNode(actor *ateapipb.Actor, assignErr error) bool {
	return errors.Is(assignErr, errNoWorkerRoom) &&
		actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED &&
		actor.GetStatus().GetAssignedNode() != ""
}

// detachFromFullNode detaches a SUSPENDED actor from its full node when
// another node has room, so the resume restores it there from the durable
// snapshot, and returns the actor as stored. A worker of another runtime
// counts: it restores the snapshot at ROOTFS, without its process memory, and
// assignment still prefers one that keeps it. With no room anywhere it returns
// assignErr. A failed detach leaves the actor on its node.
func (w *ActorWorkflow) detachFromFullNode(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, assignErr error) (_ *ateapipb.Actor, err error) {
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
		return nil, apierror.Unavailable("while getting atelet conn for node %q: %w", node, err)
	}
	resp, err := ateletpb.NewAteletClient(ateletConn).DetachActor(ctx, &ateletpb.DetachActorRequest{
		Atespace:  actor.GetMetadata().GetAtespace(),
		ActorName: actor.GetMetadata().GetName(),
		ActorUid:  actor.GetMetadata().GetUid(),
	})
	if err != nil {
		return nil, detachActorError(ctx, err)
	}
	state := resp.GetState()
	if state.GetLatestLocal() != nil || state.GetUploading() != nil {
		return nil, apierror.Internal("atelet on node %q detached Actor %s but still reports a local snapshot", node, actorRef)
	}

	// A conflict fails the resume with Aborted. The next resume detaches
	// again, and atelet returns the same state.
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
func (w *ActorWorkflow) loadPinnedActor(ctx context.Context, actorRef resources.ActorRef, uid, node string) (*ateapipb.Actor, error) {
	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, fmt.Errorf("while reading Actor %s: %w", actorRef, err)
	}
	if actor.GetMetadata().GetUid() != uid ||
		actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED ||
		actor.GetStatus().GetAssignedNode() != node {
		return nil, apierror.Aborted("Actor %s changed during the resume, please retry", actorRef)
	}
	return actor, nil
}

// recordDetach records the state atelet on node returned from DetachActor on
// the actor as read, and returns it as stored. It fails with
// FailedPrecondition if the latest copy on node was not durable.
func (w *ActorWorkflow) recordDetach(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, node string, state *ateletpb.ActorSnapshotState) (*ateapipb.Actor, error) {
	applied := proto.CloneOf(actor.GetStatus())
	if err := applyLatestDurable(ctx, applied, snapshotReportFromAtelet(state.GetLatestDurable())); err != nil {
		return nil, apierror.Internal("atelet on node %q reported a state for Actor %s that conflicts with its record: %w", node, actorRef, err)
	}
	// Under the lease, atelet deleted every copy on node, including any newer
	// than the snapshot it reports durable, and reports none when it holds no
	// entry for the actor.
	if err := applyDetach(applied, node, applied.GetLastAssignedGeneration()); err != nil {
		return nil, err
	}
	stored, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
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
