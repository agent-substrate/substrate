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
func maybeCrashActor(ctx context.Context, st store.Interface, actorRef resources.ActorRef, err error, wrapMsg, opName string) error {
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
func crashActor(ctx context.Context, st store.Interface, actorRef resources.ActorRef, opName, reason string) error {
	actor, err := st.GetActor(ctx, actorRef)
	if err != nil {
		return fmt.Errorf("while loading actor to crash: %w", err)
	}

	wasAlreadyCrashed := actor.GetStatus() == ateapipb.Actor_STATUS_CRASHED
	opName = ateattr.NormalizeOperationName(opName)
	if reason == "" {
		reason = ateattr.ReasonUnknown
	}

	var errCollected []error
	sandboxClass, err := releaseWorker(ctx, st, actor)
	if err != nil {
		errCollected = append(errCollected, err)
	}

	// Snapshot crash attributes before pod and pool pointers are cleared below;
	// the counter itself is emitted only after the transition commits.
	crashAttrs := ateattr.ActorMetricAttributes(actor, sandboxClass, opName, reason)

	_, err = st.UpdateActor(ctx, actorRef, store.WithPrecondition(actor, func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status = ateapipb.Actor_STATUS_CRASHED

		// InProgressSnapshotName and InProgressLocalSnapshotName are kept for
		// debugging; failed workflow steps must never promote either of them to an
		// ActorSnapshot or to LocalSnapshotInfo.
		toUpdate.WorkerAssignment = nil
		return nil
	}))
	if err != nil {
		errCollected = append(errCollected, fmt.Errorf("while marking actor crashed: %w", err))
		return errors.Join(errCollected...)
	}

	// Increment metric only after a successful UpdateActor, and only if the actor was not already crashed.
	if !wasAlreadyCrashed {
		recordActorCrash(ctx, crashAttrs)
	}

	return errors.Join(errCollected...)
}

// releaseWorker clears the worker's assignment if it still points at the given
// actor. A missing worker or an already-cleared assignment is not an error.
// It returns the worker's sandboxClass if found.
func releaseWorker(ctx context.Context, st store.Interface, actor *ateapipb.Actor) (string, error) {
	assignment := actor.GetWorkerAssignment()
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
