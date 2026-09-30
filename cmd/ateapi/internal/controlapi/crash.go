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
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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
)

// maxCrashMessageBytes matches the maxLength on ActorCrash.message.
const maxCrashMessageBytes = 4096

// ateletCrashMessage describes a failed atelet call with the error text atelet
// returned, since its gRPC code is Unknown for most failures.
// TODO: consider sanizing the error message returned by atelet.
func ateletCrashMessage(rpc string, err error) string {
	return fmt.Sprintf("atelet %s: %s", rpc, status.Convert(err).Message())
}

// crashAndTearDownActor moves the actor to CRASHED state, asks atelet to
// terminate the actor workload, detaches its volumes, and frees the assigned
// worker. The crash message is recorded in the actor's status.
//
// Call it when a workflow step fails while the actor may still be running on a
// worker. A caller that already attempted the teardown itself calls
// markActorCrashed instead.
//
// If terminate, detach, or release fails, the actor is still marked CRASHED
// but keeps its worker assignment and the worker stays booked, so the worker is
// not overcommitted while the sandbox or a mount may still be live on it.
//
// From CRASHED, an actor can only be deleted or reverted. DeleteActor and
// RevertActor retry terminate and detach before freeing the worker, and
// DeleteWorker frees it once its pod is gone.
func (w *ActorWorkflow) crashAndTearDownActor(ctx context.Context, actorRef resources.ActorRef, actorTemplate *ateapipb.ActorTemplate, opName, message string) error {
	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return fmt.Errorf("while loading actor to crash: %w", err)
	}
	freeWorkerAssignment := true
	// Terminate workload and detach volumes before freeing the worker assignment. Otherwise
	// the control plane will no longer know the node where the volumes/actor workload are and
	// may leak those resources for the lifetime of the node.
	if err := w.ensureAteletTerminated(ctx, actorRef, actor, actorTemplate); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "Keeping the worker assigned for a crashed actor whose workload could not be terminated",
			append(ateattr.ActorRefLogAttrs(actorRef), slog.Any("err", err))...)
		freeWorkerAssignment = false
	} else if err := w.ensureVolumesDetached(ctx, actor, actorTemplate, "DetachVolumesForCrash", opName); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "Keeping the worker assigned for a crashed actor whose volumes could not be detached",
			append(ateattr.ActorRefLogAttrs(actorRef), slog.Any("err", err))...)
		freeWorkerAssignment = false
	}
	return markActorCrashed(ctx, w.store, actor, opName, message, freeWorkerAssignment)
}

// markActorCrashed moves actor to CRASHED state and records the crash message to its status.
// When freeWorkerAssignment is set, it releases the worker the actor is assigned to and clears the
// worker assignment from the actor. Otherwise, or if the worker release fails, both are left for a later
// DeleteActor, RevertActor, or DeleteWorker to clean it up.
//
// It never terminates the workload or detaches volumes. Call it directly only when the caller has
// already attempted that teardown, as revert does after a failed terminate. Otherwise call
// crashAndTearDownActor.
func markActorCrashed(ctx context.Context, st crashActorStore, actor *ateapipb.Actor, opName, message string, freeWorkerAssignment bool) error {
	actorRef := resources.ActorRefFromActor(actor)
	wasAlreadyCrashed := actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED
	opName = ateattr.NormalizeOperationName(opName)

	var sandboxClass string
	if freeWorkerAssignment {
		// Release the worker before clearing the assignment that points at it.
		// A failed release keeps the assignment, as a failed terminate does: the
		// actor still names the worker, so a later cleanup finds and releases
		// it. releaseWorker is idempotent, so a release that committed despite
		// the error is a no-op when retried.
		class, _, err := releaseWorker(ctx, st, actor)
		if err != nil {
			slog.LogAttrs(ctx, slog.LevelError, "Keeping the worker booked for a crashed actor whose worker could not be released",
				append(ateattr.ActorRefLogAttrs(actorRef), slog.Any("err", err))...)
			freeWorkerAssignment = false
		} else {
			sandboxClass = class
		}
	} else if workerName := actor.GetStatus().GetWorkerAssignment().GetWorker().GetName(); workerName != "" {
		if worker, err := st.GetWorker(ctx, workerName); err == nil {
			sandboxClass = worker.GetSandboxClass()
		}
	}

	// Snapshot crash attributes before pod and pool pointers are cleared below;
	// the counter itself is emitted only after the transition commits.
	crashAttrs := ateattr.ActorMetricAttributes(actor, sandboxClass, opName)

	_, err := st.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		// An actor crashed concurrently, e.g. by worker deletion, keeps its first crash.
		if !wasAlreadyCrashed {
			toUpdate.Status.Crash = newActorCrash(opName, message)
		}

		// InProgressSnapshotUri and InProgressLocalSnapshotName are kept so a
		// later DeleteActor or RevertActor can delete what they name: each is
		// the only pointer to it, so clearing them here would leak the objects
		// for good; failed workflow steps must never promote either of them to an
		// ExternalSnapshot or to LocalSnapshot.
		if freeWorkerAssignment {
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

// crashActorStore encapsulates the subset of store operations needed to crash
// an actor.
type crashActorStore interface {
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.Actor) error) (*ateapipb.Actor, error)
	GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error)
	ReleaseActorFromWorker(ctx context.Context, workerName string, actorUID string) (*ateapipb.Worker, error)
}

// releaseWorker clears the worker's assignment if it still points at the given
// actor. A missing worker or an already-cleared assignment is not an error.
// It returns the worker's sandboxClass if found, and the worker as it stands
// after the release, which callers holding a cache of workers hand to it.
func releaseWorker(ctx context.Context, st crashActorStore, actor *ateapipb.Actor) (string, *ateapipb.Worker, error) {
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
