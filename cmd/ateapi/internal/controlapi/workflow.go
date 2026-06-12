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
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// actorLockTTL is the Redis TTL on the per-actor workflow lock. It bounds how
// long a peer must wait to retry an actor after this process crashes mid-workflow.
const actorLockTTL = 30 * time.Second

// actorLockHeartbeatInterval is how often the heartbeat refreshes the lock.
// Chosen so we get ~3 attempts before the TTL would otherwise lapse.
const actorLockHeartbeatInterval = actorLockTTL / 3

// errLostActorLock is the context cause set when the heartbeat can no longer
// keep the actor lock alive (peer stole it, or Redis returned an error).
var errLostActorLock = errors.New("lost actor lock during workflow")

// WorkflowStep represents a single, idempotent operation in a workflow graph.
// Params is the immutable parameters used to start the workflow.
// Context is the mutable context fetched or modified during execution.
type WorkflowStep[Params any, Context any] interface {
	// Name returns the identifier for this step (useful for logging and debugging).
	Name() string

	// IsComplete checks if this step's work has already been completed.
	// If it returns true, the engine skips Execute() and fast-forwards to the next step.
	IsComplete(ctx context.Context, params Params, wCtx Context) (bool, error)

	// Execute performs the step's business logic and persists any state changes.
	// If an error is returned, the workflow stops and relies on the client to retry.
	Execute(ctx context.Context, params Params, wCtx Context) error

	// RetryBackoff returns an optional backoff configuration for this step.
	// If non-nil, the workflow orchestrator automatically retries Execute() on persistence conflicts.
	RetryBackoff() *wait.Backoff
}

// RunWorkflow is a synchronous executor that iterates through a sequence of generic steps.
// It implements the Client-Driven Forward Recovery pattern.
func RunWorkflow[Params any, Context any](ctx context.Context, params Params, wCtx Context, steps []WorkflowStep[Params, Context]) error {
	tracer := otel.Tracer("controlapi")

	for _, step := range steps {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("workflow cancelled: %w", err)
		}

		ctx, span := tracer.Start(ctx, "step."+step.Name())

		done, err := step.IsComplete(ctx, params, wCtx)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return fmt.Errorf("failed checking status of step %s: %w", step.Name(), err)
		}

		if done {
			span.End()
			// Fast-forward past this step
			continue
		}

		err = runStep(ctx, params, wCtx, step)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return fmt.Errorf("workflow failed at step %s: %w", step.Name(), err)
		}
		span.End()
	}

	return nil
}

func runStep[Params any, Context any](ctx context.Context, params Params, wCtx Context, step WorkflowStep[Params, Context]) error {
	backoff := step.RetryBackoff()
	if backoff == nil {
		return step.Execute(ctx, params, wCtx)
	}

	return wait.ExponentialBackoff(*backoff, func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		execErr := step.Execute(ctx, params, wCtx)
		if execErr == nil {
			return true, nil
		}
		if errors.Is(execErr, store.ErrPersistenceRetry) {
			return false, nil // retryable
		}
		return false, execErr // fatal
	})
}

// ActorWorkflow handles the workflows for actor's resume / suspend operations.
type ActorWorkflow struct {
	store               store.Interface
	dialer              *AteletDialer
	actorTemplateLister listersv1alpha1.ActorTemplateLister
	kubeClient          kubernetes.Interface
	secretCache         *envSecretCache
	// workflowDeadline is the maximum duration of a single Resume/Suspend
	// workflow. The lock is kept alive across this duration by a heartbeat,
	// independent of actorLockTTL.
	workflowDeadline time.Duration
}

// NewActorWorkflow creates a new ActorWorkflow. workflowDeadline bounds how
// long a single Resume/Suspend can run end-to-end.
func NewActorWorkflow(store store.Interface, dialer *AteletDialer, actorTemplateLister listersv1alpha1.ActorTemplateLister, kubeClient kubernetes.Interface, workflowDeadline time.Duration) *ActorWorkflow {
	return &ActorWorkflow{
		store:               store,
		dialer:              dialer,
		actorTemplateLister: actorTemplateLister,
		kubeClient:          kubeClient,
		secretCache:         newEnvSecretCache(envSecretCacheTTL),
		workflowDeadline:    workflowDeadline,
	}
}

// ResumeActor executes the workflow to resume a suspended actor. Idempotent.
func (w *ActorWorkflow) ResumeActor(ctx context.Context, id string, boot bool) (*ateapipb.Actor, error) {
	input := &ResumeInput{
		ActorID: id,
		Boot:    boot,
	}
	state := &ResumeState{}

	ctx, releaseLock, err := w.acquireActorLock(ctx, id, actorLockTTL, actorLockHeartbeatInterval)
	if err != nil {
		return nil, err
	}
	defer releaseLock()

	steps := []WorkflowStep[*ResumeInput, *ResumeState]{
		&LoadActorForResumeStep{store: w.store, actorTemplateLister: w.actorTemplateLister},
		&AssignWorkerStep{store: w.store},
		&CallAteletRestoreStep{dialer: w.dialer, kubeClient: w.kubeClient, secretCache: w.secretCache},
		&FinalizeRunningStep{store: w.store},
	}

	if err := RunWorkflow(ctx, input, state, steps); err != nil {
		return nil, err
	}

	return state.Actor, nil
}

// SuspendActor executes the workflow to suspend a running actor. Idempotent.
func (w *ActorWorkflow) SuspendActor(ctx context.Context, id string) (*ateapipb.Actor, error) {
	input := &SuspendInput{
		ActorID: id,
	}
	state := &SuspendState{}

	ctx, releaseLock, err := w.acquireActorLock(ctx, id, actorLockTTL, actorLockHeartbeatInterval)
	if err != nil {
		return nil, err
	}
	defer releaseLock()

	steps := []WorkflowStep[*SuspendInput, *SuspendState]{
		&LoadActorForSuspendStep{store: w.store, actorTemplateLister: w.actorTemplateLister},
		&MarkSuspendingStep{store: w.store},
		&CallAteletSuspendStep{dialer: w.dialer},
		&FinalizeSuspendedStep{store: w.store},
	}

	if err := RunWorkflow(ctx, input, state, steps); err != nil {
		return nil, err
	}

	return state.Actor, nil
}

// acquireActorLock takes the per-actor workflow lock and returns a workflow
// context bounded by w.workflowDeadline. A background heartbeat keeps the lock
// alive — independent of lockTTL — for as long as the workflow runs. If the
// heartbeat fails (Redis error or another peer stole the lock) the returned
// context is cancelled with errLostActorLock as the cause, and in-flight steps
// will see ctx.Err() and unwind. The returned release function stops the
// heartbeat, waits for it to exit, then best-effort releases the lock.
func (w *ActorWorkflow) acquireActorLock(ctx context.Context, id string, lockTTL, heartbeatInterval time.Duration) (context.Context, func(), error) {
	lockKey := "lock:actor:" + id
	lockValue := uuid.New().String()

	acquired, err := w.store.AcquireLock(ctx, lockKey, lockValue, lockTTL)
	if err != nil {
		return nil, nil, fmt.Errorf("while acquiring lock: %w", err)
	}
	if !acquired {
		return nil, nil, status.Error(grpcCodes.Aborted, "another operation is in progress for this actor")
	}

	cancellableCtx, cancelCause := context.WithCancelCause(ctx)
	workflowCtx, cancelDeadline := context.WithTimeout(cancellableCtx, w.workflowDeadline)

	heartbeatDone := make(chan struct{})
	go w.runLockHeartbeat(workflowCtx, lockKey, lockValue, id, lockTTL, heartbeatInterval, cancelCause, heartbeatDone)

	release := func() {
		cancelDeadline()
		cancelCause(context.Canceled)
		<-heartbeatDone
		// Use context.Background() to ensure the lock is released even if the workflow context was canceled.
		w.store.ReleaseLock(context.Background(), lockKey, lockValue) //nolint:errcheck // best-effort release; the lock TTL is the safety net.
	}
	return workflowCtx, release, nil
}

// runLockHeartbeat refreshes the actor lock on a ticker until ctx is done. If
// a refresh fails or returns false (we no longer own the lock), it cancels the
// workflow context with errLostActorLock so workflow steps tear down promptly.
func (w *ActorWorkflow) runLockHeartbeat(ctx context.Context, lockKey, lockValue, actorID string, lockTTL, heartbeatInterval time.Duration, cancelCause context.CancelCauseFunc, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := w.store.RefreshLock(ctx, lockKey, lockValue, lockTTL)
			if err != nil {
				// If ctx was cancelled out from under us we're already tearing
				// down — no need to set a misleading cause.
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					slog.WarnContext(ctx, "Lock heartbeat failed; cancelling workflow",
						slog.String("actor_id", actorID),
						slog.String("err", err.Error()))
					cancelCause(fmt.Errorf("%w: %w", errLostActorLock, err))
				}
				return
			}
			if !ok {
				slog.WarnContext(ctx, "Actor lock no longer owned; cancelling workflow",
					slog.String("actor_id", actorID))
				cancelCause(errLostActorLock)
				return
			}
		}
	}
}
