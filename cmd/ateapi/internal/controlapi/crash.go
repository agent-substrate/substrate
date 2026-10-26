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
	"fmt"
	"log/slog"
	"strings"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/actorevent"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Fixed messages recorded in ActorCrash.message for crashes due to non-atelet
// reasons. A failed atelet call builds its message with ateletCrashMessage instead.
const (
	crashMessageLocalSnapshotNodeGone = "node holding the actor's latest snapshot no longer exists"
	crashMessageSnapshotStateConflict = "atelet's snapshot state conflicts with the actor's record"
	crashMessageWorkerDraining        = "assigned worker is draining"
	crashMessageWorkerReassigned      = "assigned worker no longer hosts the actor"
	crashMessageWorkerIneligible      = "assigned worker no longer satisfies the actor's placement constraints"
	crashMessageWorkerPodGone         = "worker pod went away while hosting the actor"
	crashMessageWorkerRestarted       = "worker restarted while hosting the actor"
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
	// atelet.Terminate runs while the actor is being reverted or deleted. Crashing
	// would clear the worker assignment the retry needs to terminate a sandbox
	// that may still be running and to detach its volumes.
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
func handleAteletError(ctx context.Context, st crashActorStore, actorRef resources.ActorRef, opName, rpc string, isTerminateRPC bool, err error) error {
	if ateletErrorCrashesActor(ctx, isTerminateRPC, err) {
		slog.LogAttrs(ctx, slog.LevelError, "Setting Actor to crashed due to Atelet error",
			append(ateattr.ActorRefLogAttrs(actorRef), slog.Any("err", err))...)
		if cerr := crashActor(ctx, st, actorRef, opName, ateletCrashMessage(rpc, err)); cerr != nil {
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

// crashActorAndFail crashes the actor for a workflow step that found its
// record in a state no retry can recover from, and returns the error the step
// fails with: FailedPrecondition once the crash is committed, since retrying
// the operation is pointless, or crashActor's own error if the crash did not
// commit, so the caller retries and the retry crashes the actor again.
func crashActorAndFail(ctx context.Context, st crashActorStore, actorRef resources.ActorRef, opName, message string) error {
	if err := crashActor(ctx, st, actorRef, opName, message); err != nil {
		return err
	}
	return apierror.FailedPrecondition("actor %s is CRASHED: %s", actorRef, message)
}

// crashActor moves the actor to CRASHED state and frees the worker it was
// assigned to, if any, so the worker can host other actors. message is
// recorded in the actor's status.
func crashActor(ctx context.Context, st crashActorStore, actorRef resources.ActorRef, opName, message string) error {
	actor, err := st.GetActor(ctx, actorRef)
	if err != nil {
		return fmt.Errorf("while loading actor to crash: %w", err)
	}

	// An actor crashed by a writer that took over after this workflow lost its
	// lease keeps that first crash, and is not counted again.
	wasAlreadyCrashed := actor.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_CRASHED
	opName = ateattr.NormalizeOperationName(opName)

	// Snapshot crash attributes before pod and pool pointers are cleared below;
	// the counter itself is emitted only after the transition commits.
	sandboxClass := actor.GetStatus().GetWorkerAssignment().GetSandboxRuntime().GetSandboxClass()
	crashAttrs := ateattr.ActorMetricAttributes(actor, sandboxClass, opName)

	// The worker is released in the same write, so a failure leaves both the
	// actor and its assignment for the caller's retry.
	if _, err := st.UpdateActorAndRelease(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapi.Actor) error {
		toUpdate.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
		if !wasAlreadyCrashed {
			toUpdate.Status.Crash = newActorCrash(opName, message)
		}

		// In-progress snapshot entries in Status.Snapshots are kept so a later
		// DeleteActor or RevertActor can delete what they name: each is the only
		// pointer to it, so clearing them here would leak the objects for good;
		// failed workflow steps must never promote them to COMPLETED.
		toUpdate.Status.WorkerAssignment = nil
		return nil
	}); err != nil {
		return fmt.Errorf("while marking actor crashed: %w", actorWriteError(actorRef, err))
	}

	// Count the crash only once it has committed.
	if !wasAlreadyCrashed {
		logActorCrashed(ctx, actor, opName)
		recordActorCrash(ctx, crashAttrs)
	}
	return nil
}

// newActorCrash records a crash that happens now, prefixing message with the
// operation that failed when it is known.
func newActorCrash(opName, message string) *ateapi.ActorCrash {
	if opName = ateattr.NormalizeOperationName(opName); opName != ateattr.OperationUnknown {
		message = opName + " failed: " + message
	}
	return &ateapi.ActorCrash{
		Message:   truncateUTF8(strings.ToValidUTF8(message, "�"), maxCrashMessageBytes),
		CrashTime: timestamppb.Now(),
	}
}

// logActorCrashed carries the identity ate.actor.crashes cannot: actor identity
// is barred from metric labels, so this record is the only way to attribute a
// crash to one agent. Call it beside recordActorCrash, under the same guard.
//
// It names ate.actor.state for the same reason worker's lifecycle records do: a
// crash is the one transition worker never observes, so a consumer taking the
// last state an actor reached has to see this record to reach "crashed" at all.
func logActorCrashed(ctx context.Context, actor *ateapi.Actor, opName string) {
	attrs := ateattr.ActorLogAttrs(resources.ActorAttributionFromActor(actor))
	attrs = append(attrs, slog.String(string(ateattr.ActorOperationNameKey), ateattr.NormalizeOperationName(opName)))
	attrs = append(attrs, slog.String(string(ateattr.ActorStateKey), ateattr.ActorStateCrashed))
	actorevent.Log(ctx, actorevent.Crashed, attrs)
}

// crashActorStore encapsulates the subset of store operations needed to crash
// an actor.
type crashActorStore interface {
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapi.Actor, error)
	UpdateActorAndRelease(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(toUpdate *ateapi.Actor) error) (*ateapi.Actor, error)
}
