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
	"slices"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/status"
)

// rollBackAbortedCheckpoint handles a Checkpoint that atelet aborted
// (checkpointabort.Is(cause)): the workload still runs on its worker, its
// tunnel still serves it, and nothing was written to the in-progress snapshot.
// It moves the actor from the SUSPENDING state the suspend committed, in
// either mode, back to RUNNING, drops the in-progress snapshot that mark
// appended, and fails the suspend with a FailedPrecondition that
// isCheckpointRolledBack reports. SuspendActor returns it without retrying,
// and the golden-template reconciler fails the golden snapshot on it rather
// than retrying.
// last_assigned_generation keeps its value: generations are never reused, and
// the golden-template reconciler reads a RUNNING golden actor whose latest
// generation has no snapshot as this rollback (goldenSuspendRolledBack).
//
// It re-reads the actor and changes nothing unless it is still in marked's
// state at marked's generation, so an out-of-band transition, such as the
// syncer crashing the actor, is not overwritten.
func (w *ActorWorkflow) rollBackAbortedCheckpoint(ctx context.Context, actorRef resources.ActorRef, marked *ateapipb.Actor, cause error) error {
	markedState := marked.GetStatus().GetState()
	gen := marked.GetStatus().GetLastAssignedGeneration()
	latest, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return fmt.Errorf("while loading actor to roll back an aborted checkpoint: %w", err)
	}
	if got := latest.GetStatus().GetState(); got != markedState || latest.GetStatus().GetLastAssignedGeneration() != gen {
		return apierror.FailedPrecondition("checkpoint of actor %s was aborted, and the actor is now %v rather than %v: %s",
			actorRef, got, markedState, status.Convert(cause).Message())
	}
	stored, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(latest), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		toUpdate.Status.Snapshots = slices.DeleteFunc(toUpdate.Status.Snapshots, func(snap *ateapipb.Snapshot) bool {
			return snap.GetGeneration() == gen
		})
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return apierror.Aborted("concurrent update conflict, please retry")
		}
		return fmt.Errorf("while rolling back an aborted checkpoint: %w", err)
	}
	logActorStateChanged(ctx, stored, ateattr.OperationSuspend)
	slog.LogAttrs(ctx, slog.LevelWarn, "Checkpoint aborted with the workload still running; actor is RUNNING again",
		append(ateattr.ActorRefLogAttrs(actorRef),
			slog.String(string(ateattr.ActorOperationNameKey), ateattr.NormalizeOperationName(ateattr.OperationSuspend)),
			slog.Any("err", cause))...)
	return &checkpointRolledBackError{err: apierror.FailedPrecondition("suspend of actor %s was aborted and the actor is still RUNNING: %s",
		actorRef, status.Convert(cause).Message())}
}

// checkpointRolledBackError is the error of a checkpoint that was aborted and
// rolled back, leaving the actor RUNNING with its snapshots as they were. It
// wraps the FailedPrecondition the RPC returns, so an in-process caller can
// tell it apart from other FailedPrecondition errors.
type checkpointRolledBackError struct {
	err error
}

func (e *checkpointRolledBackError) Error() string { return e.err.Error() }

func (e *checkpointRolledBackError) Unwrap() error { return e.err }

// isCheckpointRolledBack reports whether err is, or wraps, the error of a
// checkpoint that rollBackAbortedCheckpoint moved back to RUNNING.
func isCheckpointRolledBack(err error) bool {
	var rolledBack *checkpointRolledBackError
	return errors.As(err, &rolledBack)
}
