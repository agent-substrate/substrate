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
)

// ResumeActor resumes a suspended or paused actor (see resumeActorOnce). A
// resume that a rolling worker pool interrupts (the worker drained or went
// away, atelet or ateom on it not reachable yet) crashes the actor only for
// the moment: the actor is returned to the snapshot it was resuming from, its
// worker freed, and the resume tried again with backoff on another worker,
// waiting for one while the rolling pool has none free. A
// call whose attempts or context run out answers Unavailable with the actor
// at rest, so the caller's retry resumes it. Any other failure is returned as
// it is.
func (w *ActorWorkflow) ResumeActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, bool, error) {
	delay := transientResumeRetryDelay
	// crash is the last transient crash this call recovered from: once one
	// is known, the pool is rolling and a resume that finds no free worker
	// waits for one with the same backoff.
	var crash string
	for attempt := 1; ; attempt++ {
		actor, resumed, err := w.resumeActorOnce(ctx, actorRef)
		if err == nil {
			return actor, resumed, nil
		}
		recoveredCrash, recovered, rerr := w.recoverTransientResumeCrash(ctx, actorRef)
		switch {
		case rerr != nil:
			// The worker going away is still being torn down; the next
			// attempt recovers the actor once it is released.
			slog.LogAttrs(ctx, slog.LevelWarn, "Failed to recover an actor from a transient resume crash; retrying",
				append(ateattr.ActorRefLogAttrs(actorRef), slog.Any("err", rerr))...)
		case recovered:
			crash = recoveredCrash
		case crash != "" && apierror.Code(err) == codes.ResourceExhausted:
		default:
			return nil, false, err
		}
		retryable := apierror.Unavailable("resume of actor %s was interrupted by its worker (%s); the actor is back at its snapshot, retry the resume", actorRef, crash)
		if rerr != nil {
			retryable = err
		}
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

// recoverTransientResumeCrash returns an actor that a resume crashed for a
// transient cause (transientResumeCrash) to the state it was resumed from:
// PAUSED while it holds its pause snapshot, SUSPENDED otherwise, with its
// worker freed and its crash cleared. Unlike RevertActor it keeps
// the pause snapshot, which the resume was restoring. It reports the crash
// message and whether the actor was recovered; an actor in any other state is
// left alone. Idempotent: the teardown steps are, and the state change is one
// update under the actor lease.
func (w *ActorWorkflow) recoverTransientResumeCrash(ctx context.Context, actorRef resources.ActorRef) (crash string, recovered bool, err error) {
	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil || !transientResumeCrash(actor) {
		// A missing actor or a failed read leaves the resume's own error to
		// the caller.
		return "", false, nil
	}

	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return "", false, err
	}
	defer lease.Close()

	actor, err = w.store.GetActor(leaseCtx, actorRef)
	if err != nil {
		return "", false, fmt.Errorf("while loading actor to recover: %w", err)
	}
	if !transientResumeCrash(actor) {
		return "", false, nil
	}
	crash = actor.GetStatus().GetCrash().GetMessage()

	actorTemplate, err := resolveActorTemplate(leaseCtx, w.store, actor)
	if err != nil && !errors.Is(err, errActorTemplateNotFound) {
		return "", false, err
	}
	// A paused actor keeps its local snapshot and the node holding it, which
	// confines its next resume there: only its worker claim is released. A
	// suspended one has what the failed resume left torn down, as a revert
	// does, and goes back unplaced.
	rest := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	if _, local := findLatestSnapshotStorage(actor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); local != nil {
		rest = ateapipb.ActorState_ACTOR_STATE_PAUSED
	}
	if rest == ateapipb.ActorState_ACTOR_STATE_PAUSED {
		err = w.releaseAssignmentWithoutBacklink(leaseCtx, actor)
	} else {
		err = w.ensureWorkerDiscarded(leaseCtx, actorRef, actor, actorTemplate)
	}
	if err != nil {
		return "", false, fmt.Errorf("while freeing the worker of actor %s: %w", actorRef, err)
	}

	stored, err := w.store.UpdateActor(leaseCtx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = rest
		toUpdate.Status.WorkerAssignment = nil
		if rest == ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
			toUpdate.Status.AssignedNode = ""
		}
		toUpdate.Status.Crash = nil
		return nil
	})
	if err != nil {
		return "", false, fmt.Errorf("while returning actor %s to %s: %w", actorRef, rest, err)
	}
	slog.LogAttrs(leaseCtx, slog.LevelInfo, "Recovered actor from a transient resume crash",
		append(ateattr.ActorRefLogAttrs(actorRef),
			slog.String(string(ateattr.ActorStateKey), ateattr.ActorStateValue(rest)),
			slog.String("crash", crash))...)
	logActorStateChanged(leaseCtx, stored, ateattr.OperationResume)
	return crash, true, nil
}
