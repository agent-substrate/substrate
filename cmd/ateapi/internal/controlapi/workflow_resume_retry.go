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
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

// Retry policy of a resume that a rolling worker pool interrupts, within one
// ResumeActor call.
const (
	// transientResumeAttempts bounds the resumes one call makes. A call that
	// uses them up leaves the actor at rest for the caller's retry.
	transientResumeAttempts = 6
	// transientResumeRetryDelay is the pause before the second resume; it
	// doubles after each one, up to transientResumeRetryMaxDelay.
	transientResumeRetryDelay    = time.Second
	transientResumeRetryMaxDelay = 15 * time.Second
	// transientResumeCrashReadTimeout bounds the read of a resume's crash,
	// which outlives the call's context.
	transientResumeCrashReadTimeout = 30 * time.Second
)

// ResumeActor resumes a suspended or paused actor (see resumeActorOnce). A
// resume that a rolling worker pool interrupts (the worker drained or went
// away, atelet or ateom on it not reachable yet) crashes the actor only for
// the moment: the actor is returned to the snapshot it was resuming from, its
// worker freed, and the resume tried again with backoff on another worker,
// waiting for one while the rolling pool has none free. An actor a suspend
// or a pause left crashed for such a cause (transientCheckpointCrash) is
// returned to its latest durable snapshot and resumed from it the same way. A
// call whose attempts or context run out answers Unavailable naming the crash,
// also while the actor's recovery is unfinished: the caller's retry returns it
// to rest and resumes it. Any other failure is returned as it is.
func (w *ActorWorkflow) ResumeActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, bool, error) {
	delay := transientResumeRetryDelay
	// crash is the last transient crash this call met: once one is known,
	// the pool is rolling and a resume that finds no free worker waits for
	// one with the same backoff.
	var crash string
	for attempt := 1; ; attempt++ {
		actor, resumed, err := w.resumeActorOnce(ctx, actorRef)
		if err == nil {
			return actor, resumed, nil
		}
		transientCrash, _, rerr := w.recoverTransientCrash(ctx, actorRef)
		switch {
		case transientCrash != "":
			crash = transientCrash
			if rerr != nil {
				// The worker going away is still being torn down; the next
				// attempt recovers the actor once it is released.
				slog.LogAttrs(ctx, slog.LevelWarn, "Failed to recover an actor from a transient crash; retrying",
					append(ateattr.ActorRefLogAttrs(actorRef), slog.Any("err", rerr))...)
			}
		case crash != "" && apierror.Code(err) == codes.ResourceExhausted:
		default:
			return nil, false, err
		}
		retryable := apierror.Unavailable("resume of actor %s was interrupted by its worker (%s); the actor returns to its snapshot, retry the resume", actorRef, crash)
		if attempt >= transientResumeAttempts {
			return nil, false, retryable
		}
		slog.LogAttrs(ctx, slog.LevelWarn, "Resume interrupted by a transient worker failure; retrying",
			append(ateattr.ActorRefLogAttrs(actorRef),
				slog.Int("attempt", attempt),
				slog.Duration("retry_in", delay),
				slog.String("crash", crash))...)
		select {
		case <-ctx.Done():
			return nil, false, retryable
		case <-time.After(delay):
		}
		delay = min(2*delay, transientResumeRetryMaxDelay)
	}
}

// recoverTransientCrash returns an actor that a rolling worker pool crashed
// (transientCrash) to a snapshot, with its worker freed and its crash cleared:
//
//   - one a resume crashed (transientResumeCrash) to the state it was resumed
//     from: PAUSED while it holds its pause snapshot, SUSPENDED otherwise.
//     Unlike RevertActor it keeps the pause snapshot, which the resume was
//     restoring;
//   - one a suspend or a pause crashed (transientCheckpointCrash) to its
//     latest durable snapshot, SUSPENDED, as RevertActor does: the
//     half-written snapshot is discarded and the local snapshots dropped.
//
// It reports the crash message whenever the actor was found crashed for a
// transient cause, even when the recovery then fails or finds it recovered
// already, and whether this call recovered it; an actor in any other state is
// left alone. Idempotent: the teardown steps are, and the state change is one
// update under the actor lease.
func (w *ActorWorkflow) recoverTransientCrash(ctx context.Context, actorRef resources.ActorRef) (crash string, recovered bool, err error) {
	// Read beyond the caller's context, as the crash itself is recorded, so a
	// call that ends under its crash still answers for it.
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), transientResumeCrashReadTimeout)
	actor, err := w.store.GetActor(readCtx, actorRef)
	cancel()
	if err != nil || !transientCrash(actor) {
		// A missing actor or a failed read leaves the call's own error to
		// the caller.
		return "", false, nil
	}
	crash = actor.GetStatus().GetCrash().GetMessage()

	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return crash, false, err
	}
	defer lease.Close()

	actor, err = w.store.GetActor(leaseCtx, actorRef)
	if err != nil {
		return crash, false, fmt.Errorf("while loading actor to recover: %w", err)
	}
	if !transientCrash(actor) {
		return crash, false, nil
	}
	crash = actor.GetStatus().GetCrash().GetMessage()
	fromCheckpoint := transientCheckpointCrash(actor)
	op := ateattr.OperationResume
	if fromCheckpoint {
		op = ateattr.OperationRevert
	}

	actorTemplate, err := resolveActorTemplate(leaseCtx, w.store, actor)
	if err != nil && !errors.Is(err, errActorTemplateNotFound) {
		return crash, false, err
	}
	// A paused actor a resume crashed keeps its local snapshot and the node
	// holding it, which confines its next resume there: only its worker claim
	// is released. Any other has what the failed operation left torn down, as
	// a revert does, and goes back unplaced.
	rest := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	if _, local := findLatestSnapshotStorage(actor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); local != nil && !fromCheckpoint {
		rest = ateapipb.ActorState_ACTOR_STATE_PAUSED
	}
	if rest == ateapipb.ActorState_ACTOR_STATE_PAUSED {
		err = w.releaseAssignmentWithoutBacklink(leaseCtx, actor)
	} else {
		err = w.ensureWorkerDiscarded(leaseCtx, actorRef, actor, actorTemplate)
	}
	if err != nil {
		return crash, false, fmt.Errorf("while freeing the worker of actor %s: %w", actorRef, err)
	}
	if fromCheckpoint {
		if err := w.ensureInProgressSnapshotDiscarded(leaseCtx, actor); err != nil {
			return crash, false, fmt.Errorf("while discarding the snapshot actor %s was taking: %w", actorRef, err)
		}
	}

	stored, err := w.store.UpdateActor(leaseCtx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = rest
		toUpdate.Status.WorkerAssignment = nil
		if rest == ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
			toUpdate.Status.AssignedNode = ""
		}
		if fromCheckpoint {
			// Back to the latest durable snapshot: the local snapshots and the
			// discarded in-progress durable one go, as in ensureRevertedFinalized.
			removeSnapshotStorageEntries(toUpdate.Status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, nil)
			removeSnapshotStorageEntries(toUpdate.Status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, new(ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS))
		}
		toUpdate.Status.Crash = nil
		return nil
	})
	if err != nil {
		return crash, false, fmt.Errorf("while returning actor %s to %s: %w", actorRef, rest, err)
	}
	slog.LogAttrs(leaseCtx, slog.LevelInfo, "Recovered actor from a transient crash",
		append(ateattr.ActorRefLogAttrs(actorRef),
			slog.String(string(ateattr.ActorStateKey), ateattr.ActorStateValue(rest)),
			slog.String("crash", crash))...)
	logActorStateChanged(leaseCtx, stored, op)
	return crash, true, nil
}

// checkpointInterrupted answers a suspend or a pause that failed with err: a
// checkpoint a rolling worker pool interrupted (transientCheckpointCrash)
// returns the actor to its latest durable snapshot and answers Unavailable
// naming the crash, also while the recovery is unfinished (the next resume
// finishes it). Any other failure is returned as it is.
func (w *ActorWorkflow) checkpointInterrupted(ctx context.Context, actorRef resources.ActorRef, op string, err error) error {
	crash, _, rerr := w.recoverTransientCrash(ctx, actorRef)
	if crash == "" {
		return err
	}
	if rerr != nil {
		slog.LogAttrs(ctx, slog.LevelWarn, "Failed to recover an actor from a transient checkpoint crash; the next resume recovers it",
			append(ateattr.ActorRefLogAttrs(actorRef), slog.Any("err", rerr))...)
	}
	return apierror.Unavailable("%s of actor %s was interrupted by its worker (%s); the actor returns to its last durable snapshot, suspended", op, actorRef, crash)
}
