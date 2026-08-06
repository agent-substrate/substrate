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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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

	// CheckPrerequisite validates that the current state permits executing this
	// step (e.g. the actor's status allows this state-machine edge). The engine
	// calls it only when IsComplete returned false, immediately before Execute,
	// so completed steps of a retried workflow fast-forward without
	// re-validation. Return a gRPC status error with
	// codes.FailedPrecondition to abort the workflow if prereqs are not met.
	CheckPrerequisite(ctx context.Context, params Params, wCtx Context) error

	// Execute performs the step's business logic and persists any state changes.
	// If an error is returned, the workflow stops and relies on the client to retry.
	Execute(ctx context.Context, params Params, wCtx Context) error

	// RetryBackoff returns an optional backoff configuration for this step.
	// If non-nil, the workflow orchestrator automatically retries Execute() on version conflicts.
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

		if err := step.CheckPrerequisite(ctx, params, wCtx); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return fmt.Errorf("prerequisite not met at step %s: %w", step.Name(), err)
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
		if errors.Is(execErr, store.ErrVersionConflict) {
			return false, nil // retryable
		}
		return false, execErr // fatal
	})
}

// ActorWorkflow handles the workflows for actor's resume / suspend operations.
type ActorWorkflow struct {
	store               store.Interface
	workerCache         *workercache.Cache
	scheduler           scheduling.Scheduler
	dialer              *AteletDialer
	actorTemplateLister listersv1alpha1.ActorTemplateLister
	workerPoolLister    listersv1alpha1.WorkerPoolLister
	sandboxConfigLister listersv1alpha1.SandboxConfigLister
	kubeClient          kubernetes.Interface
	secretCache         *envSecretCache
	instruments         *Instruments
}

// NewActorWorkflow creates a new ActorWorkflow. instruments may be nil.
func NewActorWorkflow(
	store store.Interface,
	workerCache *workercache.Cache,
	dialer *AteletDialer,
	actorTemplateLister listersv1alpha1.ActorTemplateLister,
	workerPoolLister listersv1alpha1.WorkerPoolLister,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	kubeClient kubernetes.Interface,
	instruments *Instruments,
) *ActorWorkflow {
	return &ActorWorkflow{
		store:               store,
		workerCache:         workerCache,
		scheduler:           scheduling.New(workerCache, scheduling.WithMeter(otel.Meter("ateapi"))),
		dialer:              dialer,
		actorTemplateLister: actorTemplateLister,
		workerPoolLister:    workerPoolLister,
		sandboxConfigLister: sandboxConfigLister,
		kubeClient:          kubeClient,
		secretCache:         newEnvSecretCache(envSecretCacheTTL),
		instruments:         instruments,
	}
}

// ResumeActor executes the workflow to resume a suspended actor. Idempotent.
func (w *ActorWorkflow) ResumeActor(ctx context.Context, actorRef resources.ActorRef, boot bool) (actor *ateapipb.Actor, resumed bool, err error) {
	start := time.Now()
	input := &ResumeInput{
		ActorRef: actorRef,
		Boot:     boot,
	}
	state := &ResumeState{}

	// Recorded before the lock so lock contention still counts as an attempt.
	// Clean already-running no-ops are skipped: the router resumes per routed
	// request, and recording those would sample at router QPS and bury
	// cold-resume latency.
	defer func() {
		if err == nil && state.WasRunning {
			return
		}
		w.instruments.recordLifecycleOp(ctx, ateattr.OperationResume, start, err,
			lifecycleOpAttrs(state.Actor, state.ActorTemplate, state.SnapshotKind, state.WireSnapshotScope)...)
	}()

	lockCtx, lock, err := w.acquireActorLock(ctx, actorRef)
	if err != nil {
		return nil, false, err
	}
	defer lock.Close()

	steps := []WorkflowStep[*ResumeInput, *ResumeState]{
		&LoadActorForResumeStep{store: w.store, actorTemplateLister: w.actorTemplateLister},
		&CreateVolumesStep{store: w.store},
		&AssignWorkerStep{store: w.store, workerCache: w.workerCache, scheduler: w.scheduler, instruments: w.instruments},
		&AttachVolumesStep{store: w.store},
		&CallAteletRestoreStep{store: w.store, dialer: w.dialer, kubeClient: w.kubeClient, secretCache: w.secretCache, workerPoolLister: w.workerPoolLister, sandboxConfigLister: w.sandboxConfigLister, scheduler: w.scheduler},
		&FinalizeRunningStep{store: w.store},
	}

	if err = RunWorkflow(lockCtx, input, state, steps); err != nil {
		return nil, false, err
	}

	return state.Actor, !state.WasRunning, nil
}

// SuspendActor executes the workflow to suspend a running actor. Idempotent.
func (w *ActorWorkflow) SuspendActor(ctx context.Context, actorRef resources.ActorRef) (actor *ateapipb.Actor, err error) {
	start := time.Now()
	input := &SuspendInput{
		ActorRef: actorRef,
	}
	state := &SuspendState{}

	defer func() {
		w.instruments.recordLifecycleOp(ctx, ateattr.OperationSuspend, start, err,
			lifecycleOpAttrs(state.Actor, state.ActorTemplate, "", state.WireSnapshotScope)...)
	}()

	lockCtx, lock, err := w.acquireActorLock(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lock.Close()

	steps := []WorkflowStep[*SuspendInput, *SuspendState]{
		&LoadActorForSuspendStep{store: w.store, actorTemplateLister: w.actorTemplateLister},
		&MarkSuspendingStep{store: w.store},
		&CallAteletSuspendStep{store: w.store, dialer: w.dialer},
		&DetachVolumesStep{store: w.store},
		&FinalizeSuspendedStep{store: w.store},
	}

	if err = RunWorkflow(lockCtx, input, state, steps); err != nil {
		return nil, err
	}

	return state.Actor, nil
}

// PauseActor executes the workflow to pause a running actor. Idempotent.
func (w *ActorWorkflow) PauseActor(ctx context.Context, actorRef resources.ActorRef) (actor *ateapipb.Actor, err error) {
	start := time.Now()
	input := &PauseInput{
		ActorRef: actorRef,
	}
	state := &PauseState{}

	defer func() {
		w.instruments.recordLifecycleOp(ctx, ateattr.OperationPause, start, err,
			lifecycleOpAttrs(state.Actor, state.ActorTemplate, "", state.WireSnapshotScope)...)
	}()

	lockCtx, lock, err := w.acquireActorLock(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lock.Close()

	steps := []WorkflowStep[*PauseInput, *PauseState]{
		&LoadActorForPauseStep{store: w.store, actorTemplateLister: w.actorTemplateLister},
		&MarkPausingStep{store: w.store},
		&CallAteletPauseStep{store: w.store, dialer: w.dialer},
		&DetachVolumesForPauseStep{store: w.store},
		&FinalizePausedStep{store: w.store},
	}

	if err = RunWorkflow(lockCtx, input, state, steps); err != nil {
		return nil, err
	}

	return state.Actor, nil
}

// DeleteActor executes the workflow to delete an actor. Idempotent.
func (w *ActorWorkflow) DeleteActor(ctx context.Context, atespace, name string) (*ateapipb.Actor, error) {
	actorRef := resources.ActorRef{Atespace: atespace, Name: name}
	input := &DeleteInput{
		ActorRef: actorRef,
	}
	state := &DeleteState{}

	ctx, lock, err := w.acquireActorLock(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lock.Close()

	steps := []WorkflowStep[*DeleteInput, *DeleteState]{
		&LoadActorForDeleteStep{store: w.store},
		&MarkDeletingStep{store: w.store},
		&DeleteVolumesStep{store: w.store},
		&FinalizeDeletedStep{store: w.store},
	}

	if err := RunWorkflow(ctx, input, state, steps); err != nil {
		return nil, err
	}

	return state.DeletedActor, nil
}

func (w *ActorWorkflow) acquireActorLock(ctx context.Context, actorRef resources.ActorRef) (context.Context, *store.Lock, error) {
	lockKey := "lock:actor:" + actorRef.Atespace + ":" + actorRef.Name

	lock, err := w.store.AcquireLock(ctx, lockKey)
	if err != nil {
		if errors.Is(err, store.ErrLockConflict) {
			return nil, nil, status.Error(grpcCodes.Aborted, "another operation is in progress for this actor")
		}
		return nil, nil, fmt.Errorf("while acquiring lock: %w", err)
	}

	return lock.Context(), lock, nil
}
