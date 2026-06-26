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
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
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
	workerCache         *workercache.Cache
	dialer              *AteletDialer
	actorTemplateLister listersv1alpha1.ActorTemplateLister
	workerPoolLister    listersv1alpha1.WorkerPoolLister
	sandboxConfigLister listersv1alpha1.SandboxConfigLister
	kubeClient          kubernetes.Interface
	secretCache         *envSecretCache
}

// NewActorWorkflow creates a new ActorWorkflow.
func NewActorWorkflow(
	store store.Interface,
	workerCache *workercache.Cache,
	dialer *AteletDialer,
	actorTemplateLister listersv1alpha1.ActorTemplateLister,
	workerPoolLister listersv1alpha1.WorkerPoolLister,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	kubeClient kubernetes.Interface,
) *ActorWorkflow {
	return &ActorWorkflow{
		store:               store,
		workerCache:         workerCache,
		dialer:              dialer,
		actorTemplateLister: actorTemplateLister,
		workerPoolLister:    workerPoolLister,
		sandboxConfigLister: sandboxConfigLister,
		kubeClient:          kubeClient,
		secretCache:         newEnvSecretCache(envSecretCacheTTL),
	}
}

// ResumeActor executes the workflow to resume a suspended actor. Idempotent.
func (w *ActorWorkflow) ResumeActor(ctx context.Context, id string, boot bool) (*ateapipb.Actor, error) {
	input := &ResumeInput{
		ActorID: id,
		Boot:    boot,
	}
	state := &ResumeState{}

	// Acquire lock and get the timeout context for the workflow
	// Lock TTL is 7 seconds, with 2 seconds padding for workflow timeout
	ctx, releaseLock, err := w.acquireActorLock(ctx, id, 30*time.Second, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer releaseLock()

	steps := []WorkflowStep[*ResumeInput, *ResumeState]{
		&LoadActorForResumeStep{store: w.store, actorTemplateLister: w.actorTemplateLister},
		&AssignWorkerStep{store: w.store, workerCache: w.workerCache, workerPoolLister: w.workerPoolLister},
		&CallAteletRestoreStep{dialer: w.dialer, kubeClient: w.kubeClient, secretCache: w.secretCache, workerPoolLister: w.workerPoolLister, sandboxConfigLister: w.sandboxConfigLister},
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

	// Acquire lock and get the timeout context for the workflow
	// Lock TTL is 30 seconds, with 2 seconds padding for workflow timeout
	ctx, releaseLock, err := w.acquireActorLock(ctx, id, 30*time.Second, 2*time.Second)
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

// PauseActor executes the workflow to pause a running actor. Idempotent.
func (w *ActorWorkflow) PauseActor(ctx context.Context, id string) (*ateapipb.Actor, error) {
	input := &PauseInput{
		ActorID: id,
	}
	state := &PauseState{}

	// Acquire lock and get the timeout context for the workflow
	// Lock TTL is 30 seconds, with 2 seconds padding for workflow timeout
	ctx, releaseLock, err := w.acquireActorLock(ctx, id, 30*time.Second, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer releaseLock()

	steps := []WorkflowStep[*PauseInput, *PauseState]{
		&LoadActorForPauseStep{store: w.store, actorTemplateLister: w.actorTemplateLister},
		&MarkPausingStep{store: w.store},
		&CallAteletPauseStep{dialer: w.dialer},
		&FinalizePausedStep{store: w.store},
	}

	if err := RunWorkflow(ctx, input, state, steps); err != nil {
		return nil, err
	}

	return state.Actor, nil
}

func (w *ActorWorkflow) acquireActorLock(ctx context.Context, id string, ttl time.Duration, padding time.Duration) (context.Context, func(), error) {
	lockKey := "lock:actor:" + id
	lockValue := uuid.New().String()

	// Create a child context for the workflow that expires BEFORE the lock
	workflowTimeout := ttl - padding
	workflowCtx, cancel := context.WithTimeout(ctx, workflowTimeout)

	acquired, err := w.store.AcquireLock(workflowCtx, lockKey, lockValue, ttl)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("while acquiring lock: %w", err)
	}
	if !acquired {
		cancel()
		return nil, nil, status.Error(grpcCodes.Aborted, "another operation is in progress for this actor")
	}

	return workflowCtx, func() {
		cancel()
		// Use context.Background() to ensure the lock is released even if the workflow context was canceled.
		w.store.ReleaseLock(context.Background(), lockKey, lockValue) //nolint:errcheck // best-effort release; the lock TTL is the safety net.
	}, nil
}
