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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/resources"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// stepSpan opens the per-step trace span ("step.<name>" on the controlapi
// tracer) and returns the step context plus a finish func the step defers:
// it records a non-nil error on the span and wraps it with the step name.
//
// Workflow steps follow the ensure pattern: each step derives whether its
// work is already done from persisted state alone (calling markSkipped when
// so), validates the state-machine edge it is about to take, and persists
// what it changed before returning — so a re-entered workflow fast-forwards
// to wherever the previous attempt stopped.
func stepSpan(ctx context.Context, name string) (context.Context, func(error) error) {
	ctx, span := otel.Tracer("controlapi").Start(ctx, "step."+name)
	return ctx, func(err error) error {
		defer span.End()
		if err == nil {
			return nil
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("workflow failed at step %s: %w", name, err)
	}
}

// markSkipped annotates the current step's span when its postcondition
// already holds, so a re-entered workflow's trace shows which steps
// fast-forwarded and where real work restarted.
func markSkipped(ctx context.Context, reason string) {
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.Bool("step.skipped", true),
		attribute.String("step.skip_reason", reason),
	)
}

// ActorWorkflow handles the workflows for actor's resume / suspend operations.
type ActorWorkflow struct {
	store                actorWorkflowStore
	workerCache          *workercache.Cache
	scheduler            scheduling.Scheduler
	dialer               *AteletDialer
	workerPoolLister     listersv1alpha1.WorkerPoolLister
	sandboxConfigLister  listersv1alpha1.SandboxConfigLister
	storageClassLister   storagev1listers.StorageClassLister
	instruments          *Instruments
	egressGatewayAddress string
	pluginRegistry       VolumePluginRegistry
}

// NewActorWorkflow creates a new ActorWorkflow. instruments may be nil.
func NewActorWorkflow(
	store actorWorkflowStore,
	workerCache *workercache.Cache,
	dialer *AteletDialer,
	workerPoolLister listersv1alpha1.WorkerPoolLister,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	storageClassLister storagev1listers.StorageClassLister,
	instruments *Instruments,
	egressGatewayAddress string,
	pluginRegistry VolumePluginRegistry,
) *ActorWorkflow {
	return &ActorWorkflow{
		store:                store,
		workerCache:          workerCache,
		scheduler:            scheduling.New(workerCache, scheduling.WithMeter(otel.Meter("ateapi"))),
		dialer:               dialer,
		workerPoolLister:     workerPoolLister,
		sandboxConfigLister:  sandboxConfigLister,
		storageClassLister:   storageClassLister,
		instruments:          instruments,
		egressGatewayAddress: egressGatewayAddress,
		pluginRegistry:       pluginRegistry,
	}
}

// actorWorkflowStore enumerates the exact storage methods needed by
// ActorWorkflow and nothing more.
type actorWorkflowStore interface {
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.Actor) error) (*ateapipb.Actor, error)
	DeleteActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error)
	UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapipb.Worker) error) (*ateapipb.Worker, error)
	GetActorSnapshot(ctx context.Context, snapshotRef resources.ActorSnapshotRef) (*ateapipb.ActorSnapshot, error)
	CreateActorSnapshot(ctx context.Context, snapshot *ateapipb.ActorSnapshot) (*ateapipb.ActorSnapshot, error)
	GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error)
	AcquireLease(ctx context.Context, key string) (*store.Lease, error)
}

// WorkerWorkflow handles the multi-step operations on a Worker.
//
// Its steps reach across the Actor↔Worker binding, which an ActorWorkflow step
// does from the other side: releasing the Actor bound to a Worker stays
// in-process because there is no bind/release RPC.
type WorkerWorkflow struct {
	store workerWorkflowStore
}

// NewWorkerWorkflow creates a new WorkerWorkflow.
func NewWorkerWorkflow(store workerWorkflowStore) *WorkerWorkflow {
	return &WorkerWorkflow{store: store}
}

// workerWorkflowStore enumerates the exact storage methods needed by
// WorkerWorkflow and nothing more.
type workerWorkflowStore interface {
	GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error)
	DeleteWorker(ctx context.Context, name string, pre store.DeletePreconditions) (*ateapipb.Worker, error)
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.Actor) error) (*ateapipb.Actor, error)
}

func (w *ActorWorkflow) acquireActorLease(ctx context.Context, actorRef resources.ActorRef) (context.Context, *store.Lease, error) {
	leaseKey := "lease:actor:" + actorRef.Atespace + ":" + actorRef.Name

	lease, err := w.store.AcquireLease(ctx, leaseKey)
	if err != nil {
		if errors.Is(err, store.ErrLeaseConflict) {
			return nil, nil, status.Error(grpcCodes.Aborted, "another operation is in progress for this actor")
		}
		return nil, nil, fmt.Errorf("while acquiring lease: %w", err)
	}

	return lease.Context(), lease, nil
}
