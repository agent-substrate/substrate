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
//
// opNames is variadic (...string) to provide backward compatibility for generic callers
// where the operation context is not explicitly supplied. If provided, opNames[0] is
// normalized against the bounded operations {resume, suspend, pause, unknown}.
func maybeCrashActor(ctx context.Context, st store.Interface, actorRef resources.ActorRef, err error, wrapMsg string, opNames ...string) error {
	if err == nil {
		return nil
	}

	if ateerrors.ActorCrashRequested(err) {
		slog.ErrorContext(ctx, "Setting Actor to crashed due to error", slog.Any("error", err))
		// Extract AIP-193 ErrorInfo reason enum from the RPC error detail. If unclassified
		// or unlisted, default to ateattr.ReasonUnknown to protect metric low-cardinality.
		reason := ateerrors.ExtractReason(err)
		if reason == "" {
			reason = ateattr.ReasonUnknown
		}

		opName := ateattr.OperationNameUnknown
		if len(opNames) > 0 {
			opName = ateattr.NormalizeOperationName(opNames[0])
		}

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
//
// args is variadic to support optional telemetry metadata:
//   - 0 args: defaults opName and reason to OperationNameUnknown and ReasonUnknown
//   - 1 arg:  (reason)
//   - 2 args: (opName, reason)
func crashActor(ctx context.Context, st store.Interface, actorRef resources.ActorRef, args ...string) error {
	actor, err := st.GetActor(ctx, actorRef)
	if err != nil {
		return fmt.Errorf("while loading actor to crash: %w", err)
	}

	opName := ateattr.OperationNameUnknown
	reason := ateattr.ReasonUnknown

	if len(args) == 1 {
		reason = args[0]
	} else if len(args) >= 2 {
		opName = args[0]
		reason = args[1]
	}

	opName = ateattr.NormalizeOperationName(opName)

	var sandboxClass string
	if actor.AteomPodNamespace != "" && actor.WorkerPoolName != "" && actor.AteomPodName != "" {
		if worker, werr := st.GetWorker(ctx, actor.AteomPodNamespace, actor.WorkerPoolName, actor.AteomPodName); werr == nil && worker != nil {
			sandboxClass = worker.GetSandboxClass()
		}
	}
	recordActorCrash(ctx, actor, sandboxClass, opName, reason)

	var errCollected []error
	if err := releaseWorker(ctx, st, actor); err != nil {
		errCollected = append(errCollected, err)
	}

	actor.Status = ateapipb.Actor_STATUS_CRASHED

	// InProgressSnapshot is kept for debugging; failed workflow
	// steps must never promote it to an ActorSnapshot.
	actor.AteomPodNamespace = ""
	actor.AteomPodName = ""
	actor.AteomPodIp = ""
	actor.AteomPodUid = ""
	actor.WorkerPoolName = ""

	if _, err := st.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion()); err != nil {
		errCollected = append(errCollected, fmt.Errorf("while marking actor crashed: %w", err))
	}
	return errors.Join(errCollected...)
}

// releaseWorker clears the worker's assignment if it still points at the given
// actor. A missing worker or an already-cleared assignment is not an error.
func releaseWorker(ctx context.Context, st store.Interface, actor *ateapipb.Actor) error {
	podNamespace := actor.GetAteomPodNamespace()
	podName := actor.GetAteomPodName()
	podUid := actor.GetAteomPodUid()
	poolName := actor.GetWorkerPoolName()

	if podNamespace == "" || podName == "" || poolName == "" {
		slog.WarnContext(ctx, "Actor's worker assignment is already cleared")
		return nil
	}

	worker, err := st.GetWorker(ctx, podNamespace, poolName, podName)
	if errors.Is(err, store.ErrNotFound) {
		// No need to release if the worker is not found.
		slog.WarnContext(ctx, "Worker already gone while crashing actor, skipping release", slog.String("worker", podUid))
		return nil
	}
	if err != nil {
		return fmt.Errorf("while getting worker to release: %w", err)
	}
	wass := worker.GetAssignment()
	if wass == nil {
		slog.WarnContext(ctx, "Worker's assignment is already nil, skipping release", slog.String("worker", podUid))
		return nil
	}
	// Only free it if it still belongs to us
	if resources.ActorRefFromObjectRef(wass.GetActor()) != resources.ActorRefFromActor(actor) {
		slog.WarnContext(ctx, "Worker already assigned to another Actor", slog.String("worker", podUid))
		return nil
	}

	worker.Assignment = nil
	if err := st.UpdateWorker(ctx, worker, worker.GetVersion()); err != nil {
		return fmt.Errorf("while releasing worker: %w", err)
	}
	return nil
}
