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
	"strings"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/actorevent"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Fixed messages recorded in ActorCrash.message for crashes due to non-atelet
// reasons. A failed atelet call builds its message with ateletCrashMessage instead.
const (
	crashMessageWorkerAssignmentMissing  = "actor has no worker assignment in a state that requires one"
	crashMessageLocalSnapshotNodeUnknown = "node holding the actor's local snapshot is unknown"
	crashMessageWorkerGone               = "assigned worker no longer exists"
	crashMessageWorkerDraining           = "assigned worker is draining"
	crashMessageWorkerReassigned         = "assigned worker no longer hosts the actor"
	crashMessageWorkerIneligible         = "assigned worker no longer satisfies the actor's placement constraints"
	crashMessageWorkerPodGone            = "worker pod went away while hosting the actor"
	crashMessageAteomRestarted           = "ateom restarted while hosting the actor"
)

// maxCrashMessageBytes matches the maxLength on ActorCrash.message.
const maxCrashMessageBytes = 4096

// ateletCrashMessage describes a failed atelet call with the error text atelet
// returned, since its gRPC code is Unknown for most failures.
// TODO: consider sanizing the error message returned by atelet.
func ateletCrashMessage(rpc string, err error) string {
	return fmt.Sprintf("atelet %s: %s", rpc, status.Convert(err).Message())
}

// ateletErrorCrashesActor reports whether a failed call to atelet's rpc should
// crash the actor.
func ateletErrorCrashesActor(ctx context.Context, isTerminateRPC bool, err error) bool {
	// atelet.Terminate runs while the actor is being crashed, reverted or
	// deleted. Each of those handles a failed Terminate itself and keeps the
	// worker assignment so the teardown can be retried. Crashing from here
	// would re-enter crashActor, or pull a REVERTING or DELETING actor back to
	// CRASHED.
	if isTerminateRPC {
		return false
	}

	// Workflow's own context ended: the caller went away or the actor lease
	// was lost.
	if ctx.Err() != nil {
		return false
	}

	switch status.Code(err) {
	// Usually transport errors or atelet retriable errors.
	case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
		return false
	default:
		return true
	}
}

// handleAteletError handles an error from the atelet call rpc made for opName.
// It crashes the actor if ateletErrorCrashesActor says so, and otherwise
// returns the error for the operation to be retried.
func (w *ActorWorkflow) handleAteletError(ctx context.Context, actorRef resources.ActorRef, actorTemplate *ateapipb.ActorTemplate, opName, rpc string, isTerminateRPC bool, err error) error {
	if ateletErrorCrashesActor(ctx, isTerminateRPC, err) {
		slog.LogAttrs(ctx, slog.LevelError, "Setting Actor to crashed due to Atelet error",
			append(ateattr.ActorRefLogAttrs(actorRef), slog.Any("err", err))...)
		if cerr := w.crashActor(ctx, actorRef, actorTemplate, opName, ateletCrashMessage(rpc, err)); cerr != nil {
			return cerr
		}
		return fmt.Errorf("actor %s crashed: %w", actorRef, err)
	}

	// The caller went away or its deadline passed: report that, not atelet's
	// error. The actor keeps its assignment, so a retry can finish the operation.
	if ctx.Err() != nil {
		return fmt.Errorf("while calling atelet %s: %w: %w", rpc, err, ctx.Err())
	}
	if status.Code(err) == codes.Unavailable {
		return apierror.Unavailable("while calling atelet %s: %w", rpc, err)
	}
	return apierror.Internal("while calling atelet %s: %w", rpc, err)
}

// crashActor moves the actor to CRASHED state, recording message in its
// status. It first tears down what the actor holds on its worker, and frees
// the worker only if the teardown succeeds. Otherwise the actor keeps its
// worker assignment, so the worker is not handed to another actor while a
// sandbox or a mount may still be live on it, and a later RevertActor or
// DeleteActor finishes the teardown and frees it. actorTemplate may be nil, in
// which case every external volume the actor records is unmounted and
// detached.
func (w *ActorWorkflow) crashActor(ctx context.Context, actorRef resources.ActorRef, actorTemplate *ateapipb.ActorTemplate, opName, message string) error {
	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return fmt.Errorf("while loading actor to crash: %w", err)
	}

	wasAlreadyCrashed := actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED
	opName = ateattr.NormalizeOperationName(opName)

	sandboxClass, teardownErr := w.tearDownForCrash(ctx, actorRef, actor, actorTemplate, opName)
	if teardownErr != nil {
		slog.LogAttrs(ctx, slog.LevelWarn, "Crashing actor without freeing its worker",
			append(ateattr.ActorRefLogAttrs(actorRef), slog.Any("err", teardownErr))...)
	}

	// Snapshot crash attributes before pod and pool pointers are cleared below;
	// the counter itself is emitted only after the transition commits.
	crashAttrs := ateattr.ActorMetricAttributes(actor, sandboxClass, opName)

	_, err = w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		// An actor crashed concurrently, e.g. by worker deletion, keeps its first crash.
		if !wasAlreadyCrashed {
			toUpdate.Status.Crash = newActorCrash(opName, message)
		}

		// In-progress snapshot entries in Status.Snapshots are kept so a later
		// DeleteActor or RevertActor can delete what they name: each is the only
		// pointer to it, so clearing them here would leak the objects for good;
		// failed workflow steps must never promote them to COMPLETED.
		if teardownErr == nil {
			// Similarly, we keep WorkerAssignment if we failed to tear down
			// the actor, so DeleteActor or RevertActor can retry.
			toUpdate.Status.WorkerAssignment = nil
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("while marking actor crashed: %w", err)
	}

	// Increment metric only after a successful UpdateActor, and only if the actor was not already crashed.
	if !wasAlreadyCrashed {
		logActorCrashed(ctx, actor, opName)
		recordActorCrash(ctx, crashAttrs)
	}

	return nil
}

// tearDownForCrash terminates the actor's sandbox, detaches its volumes, and
// releases its worker, in that order, so a worker is never released while a
// sandbox or a mount may still be live on it. It returns the worker's
// sandboxClass if the worker was found. Every step is idempotent.
func (w *ActorWorkflow) tearDownForCrash(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, opName string) (string, error) {
	if err := w.ensureAteletTerminated(ctx, actorRef, actor, actorTemplate, opName); err != nil {
		return "", fmt.Errorf("while terminating atelet: %w", err)
	}
	if err := w.ensureVolumesDetached(ctx, actor, actorTemplate, "DetachVolumesForCrash", opName); err != nil {
		return "", fmt.Errorf("while detaching volumes: %w", err)
	}
	sandboxClass, _, err := releaseWorker(ctx, w.store, actor)
	if err != nil {
		return "", fmt.Errorf("while releasing worker: %w", err)
	}
	return sandboxClass, nil
}

// newActorCrash records a crash that happens now, prefixing message with the
// operation that failed when it is known.
func newActorCrash(opName, message string) *ateapipb.ActorCrash {
	if opName = ateattr.NormalizeOperationName(opName); opName != ateattr.OperationUnknown {
		message = opName + " failed: " + message
	}
	return &ateapipb.ActorCrash{
		Message:   truncateUTF8(strings.ToValidUTF8(message, "�"), maxCrashMessageBytes),
		CrashTime: timestamppb.Now(),
	}
}

// logActorCrashed carries the identity ate.actor.crashes cannot: actor identity
// is barred from metric labels, so this record is the only way to attribute a
// crash to one agent. Call it beside recordActorCrash, under the same guard.
//
// It names ate.actor.state for the same reason ateom's lifecycle records do: a
// crash is the one transition ateom never observes, so a consumer taking the
// last state an actor reached has to see this record to reach "crashed" at all.
func logActorCrashed(ctx context.Context, actor *ateapipb.Actor, opName string) {
	attrs := ateattr.ActorLogAttrs(resources.ActorAttributionFromActor(actor))
	attrs = append(attrs, slog.String(string(ateattr.ActorOperationNameKey), ateattr.NormalizeOperationName(opName)))
	attrs = append(attrs, slog.String(string(ateattr.ActorStateKey), ateattr.ActorStateCrashed))
	actorevent.Log(ctx, actorevent.Crashed, attrs)
}

// releaseWorkerStore encapsulates the subset of store operations needed to
// release an actor's worker.
type releaseWorkerStore interface {
	GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error)
	ReleaseActorFromWorker(ctx context.Context, workerName string, actorUID string) (*ateapipb.Worker, error)
}

// releaseWorker clears the worker's assignment if it still points at the given
// actor. A missing worker or an already-cleared assignment is not an error.
// It returns the worker's sandboxClass if found, and the worker as it stands
// after the release, which callers holding a cache of workers hand to it.
func releaseWorker(ctx context.Context, st releaseWorkerStore, actor *ateapipb.Actor) (string, *ateapipb.Worker, error) {
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		slog.WarnContext(ctx, "Actor's worker assignment is already cleared")
		return "", nil, nil
	}
	workerName := assignment.GetWorker().GetName()

	worker, err := st.GetWorker(ctx, workerName)
	if errors.Is(err, store.ErrNotFound) {
		// No need to release if the worker is not found.
		slog.WarnContext(ctx, "Worker already gone while crashing actor, skipping release", slog.String("worker", workerName))
		return "", nil, nil
	}
	if err != nil {
		return "", nil, fmt.Errorf("while getting worker to release: %w", err)
	}

	sandboxClass := worker.GetSandboxClass()
	// Release only this actor's assignment; the worker may be hosting others,
	// and they are unaffected by this one crashing. A worker that is no longer
	// hosting it has already been released.
	released, err := st.ReleaseActorFromWorker(ctx, workerName, actor.GetMetadata().GetUid())
	if err != nil {
		return sandboxClass, nil, fmt.Errorf("while releasing worker: %w", err)
	}
	if released == nil {
		slog.WarnContext(ctx, "Worker is not hosting this Actor, skipping release",
			slog.String("worker", workerName))
	}
	return sandboxClass, released, nil
}
