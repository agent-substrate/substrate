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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/admission"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/actorevent"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/resources"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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

// logActorStateChanged records an actor state change. The last record for an
// actor's uid is the state it is in now.
//
// The state is read off the committed record, never passed in, so a record
// cannot claim a state the store did not hold. Pass the actor the store returned
// and call it after UpdateActor returns, not inside the mutate closure, which
// can be retried. Every state commit carries a version precondition, so a call
// that returns is the one that made the change.
//
// Crashes go through logActorCrashed instead, so read the state off
// ate.actor.state rather than off the message.
func logActorStateChanged(ctx context.Context, actor *ateapipb.Actor, opName string) {
	logActorState(ctx, actor, opName, ateattr.ActorStateValue(actor.GetStatus().GetState()))
}

// logActorDeleted records the terminal transition. The row is gone, so there is
// no committed state left to read and this is the one state named by hand.
func logActorDeleted(ctx context.Context, actor *ateapipb.Actor, opName string) {
	logActorState(ctx, actor, opName, ateattr.ActorStateDeleted)
}

func logActorState(ctx context.Context, actor *ateapipb.Actor, opName, state string) {
	attrs := ateattr.ActorLogAttrs(resources.ActorAttributionFromActor(actor))
	attrs = append(attrs,
		slog.String(string(ateattr.ActorOperationNameKey), ateattr.NormalizeOperationName(opName)),
		slog.String(string(ateattr.ActorStateKey), state))
	actorevent.Log(ctx, actorevent.StateChanged, attrs)
}

// ActorWorkflow handles the workflows for actor's resume / suspend operations.
type ActorWorkflow struct {
	admission            *admission.Admission
	workerCache          *workercache.Cache
	scheduler            scheduling.Scheduler
	dialer               *AteletDialer
	sandboxConfigLister  listersv1alpha1.SandboxConfigLister
	storageClassLister   storagev1listers.StorageClassLister
	instruments          *Instruments
	egressGatewayAddress string
	pluginRegistry       VolumePluginRegistry
	objectStore          objectstore.Store
}

// NewActorWorkflow creates a new ActorWorkflow. instruments may be nil.
//
// objectStore may be nil, which leaves external snapshots in place instead of
// copying and releasing them. Only tests that never reach those steps pass nil;
// ate-api always builds one.
func NewActorWorkflow(
	admission *admission.Admission,
	workerCache *workercache.Cache,
	dialer *AteletDialer,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	storageClassLister storagev1listers.StorageClassLister,
	instruments *Instruments,
	egressGatewayAddress string,
	pluginRegistry VolumePluginRegistry,
	objectStore objectstore.Store,
) *ActorWorkflow {
	return &ActorWorkflow{
		admission:            admission,
		workerCache:          workerCache,
		scheduler:            scheduling.New(workerCache),
		dialer:               dialer,
		sandboxConfigLister:  sandboxConfigLister,
		storageClassLister:   storageClassLister,
		instruments:          instruments,
		egressGatewayAddress: egressGatewayAddress,
		pluginRegistry:       pluginRegistry,
		objectStore:          objectStore,
	}
}

// WorkerWorkflow handles the multi-step operations on a Worker.
//
// Its steps reach across the Actor↔Worker binding, which an ActorWorkflow step
// does from the other side: releasing the Actor bound to a Worker stays
// in-process because there is no bind/release RPC.
type WorkerWorkflow struct {
	admission *admission.Admission
}

// NewWorkerWorkflow creates a new WorkerWorkflow.
func NewWorkerWorkflow(admission *admission.Admission) *WorkerWorkflow {
	return &WorkerWorkflow{admission: admission}
}

// acquireLease takes the lease named by key and returns the context to run
// under: it is cancelled if the lease is lost. subject names what the lease
// covers, for the message a caller that loses the race gets.
func acquireLease(ctx context.Context, admission *admission.Admission, key, subject string) (context.Context, *store.Lease, error) {
	lease, err := admission.AcquireLease(ctx, key)
	if err != nil {
		if errors.Is(err, store.ErrLeaseConflict) {
			return nil, nil, apierror.Aborted("another operation is in progress for this %s", subject)
		}
		return nil, nil, fmt.Errorf("while acquiring lease: %w", err)
	}

	return lease.Context(), lease, nil
}

// actorLeaseKey names the lease that serializes the operations on an Actor.
func actorLeaseKey(actorRef resources.ActorRef) string {
	return "lease:actor:" + actorRef.Atespace + ":" + actorRef.Name
}

func (w *ActorWorkflow) acquireActorLease(ctx context.Context, actorRef resources.ActorRef) (context.Context, *store.Lease, error) {
	return acquireLease(ctx, w.admission, actorLeaseKey(actorRef), "actor")
}

func acquireTagLease(ctx context.Context, admission *admission.Admission, tagRef resources.TagRef) (context.Context, *store.Lease, error) {
	return acquireLease(ctx, admission, "lease:tag:"+tagRef.Atespace+":"+tagRef.Name, "Tag")
}
