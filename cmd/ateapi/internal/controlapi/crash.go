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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maybeCrashActor inspects err returned by an atelet RPC and crashes the actor
// if err carries the actorCrashed=true metadata directive.
func maybeCrashActor(ctx context.Context, st crashActorStore, actorRef resources.ActorRef, err error, wrapMsg, opName string) error {
	if err == nil {
		return nil
	}

	if ateerrors.ActorCrashRequested(err) {
		slog.ErrorContext(ctx, "Setting Actor to crashed due to error", slog.Any("error", err))
		// Extract AIP-193 ErrorInfo reason enum from the RPC error detail. If unclassified
		// or unlisted, default to ateattr.ReasonUnknown to protect metric low-cardinality.
		reason := ateerrors.ExtractReason(err)

		if cerr := crashActor(ctx, st, actorRef, opName, reason); cerr != nil {
			slog.ErrorContext(ctx, "Failed to crash actor", slog.Any("cerr", cerr))
			return cerr
		}
		return status.Errorf(codes.DataLoss, "actor %s crashed", actorRef)
	}
	return fmt.Errorf("%s: %w", wrapMsg, err)
}

// crashActor moves the actor to CRASHED state and frees the worker it was
// assigned to, if any, so the worker can host other actors.
func crashActor(ctx context.Context, st crashActorStore, actorRef resources.ActorRef, opName, reason string) error {
	actor, err := st.GetActor(ctx, actorRef)
	if err != nil {
		return fmt.Errorf("while loading actor to crash: %w", err)
	}

	wasAlreadyCrashed := actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED
	opName = ateattr.NormalizeOperationName(opName)
	if reason == "" {
		reason = ateattr.ReasonUnknown
	}

	// Release the worker before moving the actor to the terminal CRASHED state.
	// If the release fails we must not clear the actor's worker assignment or
	// mark it CRASHED: doing so would strand the still-assigned worker with no
	// actor referencing it, so nothing would ever retry the release and the
	// worker slot would be consumed until its pod dies. Returning the error
	// instead leaves the actor (and its assignment) intact so the caller retries
	// crashActor, which re-attempts the release. releaseWorker is idempotent, so
	// a retry after a release that already succeeded is a no-op.
	sandboxClass, err := releaseWorker(ctx, st, actor)
	if err != nil {
		return fmt.Errorf("while releasing worker to crash actor: %w", err)
	}

	// Snapshot crash attributes before pod and pool pointers are cleared below;
	// the counter itself is emitted only after the transition commits.
	crashAttrs := ateattr.ActorMetricAttributes(actor, sandboxClass, opName, reason)

	_, err = st.UpdateActor(ctx, actorRef, store.WithPrecondition(actor, func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED

		// InProgressSnapshotName and InProgressLocalSnapshotName are kept for
		// debugging; failed workflow steps must never promote either of them to an
		// ActorSnapshot or to LocalSnapshotInfo.
		toUpdate.Status.WorkerAssignment = nil
		return nil
	}))
	if err != nil {
		return fmt.Errorf("while marking actor crashed: %w", err)
	}

	// Increment metric only after a successful UpdateActor, and only if the actor was not already crashed.
	if !wasAlreadyCrashed {
		recordActorCrash(ctx, crashAttrs)
	}

	return nil
}

// crashActorStore encapsulates the subset of store operations needed to crash
// an actor.
type crashActorStore interface {
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	UpdateActor(ctx context.Context, actorRef resources.ActorRef, mutate func(toUpdate *ateapipb.Actor) error) (*ateapipb.Actor, error)
	GetWorker(ctx context.Context, namespace, pool, pod string) (*ateapipb.Worker, error)
	UpdateWorker(ctx context.Context, worker *ateapipb.Worker, expectedVersion int64) error
}

// releaseWorker clears the worker's assignment if it still points at the given
// actor. A missing worker or an already-cleared assignment is not an error.
// It returns the worker's sandboxClass if found.
func releaseWorker(ctx context.Context, st crashActorStore, actor *ateapipb.Actor) (string, error) {
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		slog.WarnContext(ctx, "Actor's worker assignment is already cleared")
		return "", nil
	}
	podUid := assignment.GetWorkerPodUid()

	worker, err := st.GetWorker(ctx, assignment.GetWorkerNamespace(), assignment.GetWorkerPool(), assignment.GetWorkerPod())
	if errors.Is(err, store.ErrNotFound) {
		// No need to release if the worker is not found.
		slog.WarnContext(ctx, "Worker already gone while crashing actor, skipping release", slog.String("worker", podUid))
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("while getting worker to release: %w", err)
	}

	sandboxClass := worker.GetSandboxClass()
	wass := worker.GetAssignment()
	if wass == nil {
		slog.WarnContext(ctx, "Worker's assignment is already nil, skipping release", slog.String("worker", podUid))
		return sandboxClass, nil
	}
	// Only free it if it still belongs to us
	if wass.GetActorUid() != actor.GetMetadata().GetUid() {
		slog.WarnContext(ctx, "Worker already assigned to another Actor", slog.String("worker", podUid))
		return sandboxClass, nil
	}

	worker.Assignment = nil
	if err := st.UpdateWorker(ctx, worker, worker.GetVersion()); err != nil {
		return sandboxClass, fmt.Errorf("while releasing worker: %w", err)
	}
	return sandboxClass, nil
}
