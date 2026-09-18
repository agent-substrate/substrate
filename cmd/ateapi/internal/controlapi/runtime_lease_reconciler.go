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
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	runtimeLeaseHeartbeatInterval = 30 * time.Second
	runtimeLeaseReconcileLimit    = 100
)

type runtimeLeaseReconcilerStore interface {
	actorWorkflowStore
	ListExpiredActorRuntimeLeases(ctx context.Context, limit int) ([]store.ActorRuntimeLease, error)
}

type runtimeLeaseReconciler struct {
	store     runtimeLeaseReconcilerStore
	terminate func(context.Context, *ateapipb.Actor) error
}

func newRuntimeLeaseReconciler(st runtimeLeaseReconcilerStore, terminate func(context.Context, *ateapipb.Actor) error) *runtimeLeaseReconciler {
	return &runtimeLeaseReconciler{store: st, terminate: terminate}
}

func (r *runtimeLeaseReconciler) Start(ctx context.Context) {
	go func() {
		r.reconcile(ctx)
		ticker := time.NewTicker(runtimeLeaseHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.reconcile(ctx)
			}
		}
	}()
}

func (r *runtimeLeaseReconciler) reconcile(ctx context.Context) {
	leases, err := r.store.ListExpiredActorRuntimeLeases(ctx, runtimeLeaseReconcileLimit)
	if err != nil {
		slog.WarnContext(ctx, "failed to list expired actor runtime leases", "error", err)
		return
	}
	for _, lease := range leases {
		if err := r.reconcileOne(ctx, lease); err != nil {
			slog.WarnContext(ctx, "failed to reclaim expired actor runtime lease",
				"actor_uid", lease.ActorUID, "actor", lease.ActorRef, "error", err)
		}
	}
}

func (r *runtimeLeaseReconciler) reconcileOne(ctx context.Context, expired store.ActorRuntimeLease) error {
	leaseCtx, operationLease, err := acquireLease(ctx, r.store, "lease:actor:"+expired.ActorRef.Atespace+":"+expired.ActorRef.Name, "actor")
	if err != nil {
		return err
	}
	defer operationLease.Close()

	current, err := r.store.GetActorRuntimeLease(leaseCtx, expired.ActorUID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !runtimeLeaseEqual(current, &expired) || current.ExpiresAt.After(time.Now()) {
		return nil
	}
	if err := r.store.ClaimExpiredActorRuntimeLease(leaseCtx, expired.ActorUID, expired.Token, expired.Generation); err != nil {
		if errors.Is(err, store.ErrRuntimeLeaseInvalid) {
			return nil
		}
		return err
	}

	actor, err := r.store.GetActor(leaseCtx, expired.ActorRef)
	if errors.Is(err, store.ErrNotFound) {
		return r.store.DeleteActorRuntimeLease(leaseCtx, expired.ActorUID, expired.Token, expired.Generation)
	}
	if err != nil {
		return err
	}
	if actor.GetMetadata().GetUid() != expired.ActorUID {
		// The name now addresses a replacement actor. The old UID's lease can
		// be removed, but the replacement is never touched by this reclaim.
		return r.store.DeleteActorRuntimeLease(leaseCtx, expired.ActorUID, expired.Token, expired.Generation)
	}

	state := actor.GetStatus().GetState()
	terminalWithoutWorkload := state == ateapipb.ActorState_ACTOR_STATE_SUSPENDED ||
		state == ateapipb.ActorState_ACTOR_STATE_PAUSED ||
		state == ateapipb.ActorState_ACTOR_STATE_CRASHED
	if actor.GetStatus().GetWorkerAssignment() != nil {
		if err := r.terminate(leaseCtx, actor); err != nil {
			return err
		}
	}
	if !terminalWithoutWorkload || actor.GetStatus().GetWorkerAssignment() != nil {
		// crashActor releases the worker before publishing CRASHED. It does
		// not checkpoint or alter the actor's durable snapshot fields.
		if err := crashActor(leaseCtx, r.store, expired.ActorRef, ateattr.OperationResume, ateattr.ReasonUnknown); err != nil {
			return err
		}
	}

	// Re-check the exact row after termination/release. The reclaim claim fences
	// renewal; this keeps deletion fail-closed if a future caller changes that
	// ordering.
	current, err = r.store.GetActorRuntimeLease(leaseCtx, expired.ActorUID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !runtimeLeaseEqual(current, &expired) {
		return nil
	}
	return r.store.DeleteActorRuntimeLease(leaseCtx, expired.ActorUID, expired.Token, expired.Generation)
}

func runtimeLeaseEqual(a, b *store.ActorRuntimeLease) bool {
	return a != nil && b != nil && a.ActorUID == b.ActorUID &&
		a.ActorRef == b.ActorRef && a.Token == b.Token && a.Generation == b.Generation
}

func (w *ActorWorkflow) terminateActorWorkload(ctx context.Context, actor *ateapipb.Actor) error {
	if w.terminateWorkload != nil {
		return w.terminateWorkload(ctx, actor)
	}
	return w.terminateActorWorkloadNative(ctx, actor)
}

func (w *ActorWorkflow) terminateActorWorkloadNative(ctx context.Context, actor *ateapipb.Actor) error {
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		return nil
	}
	actorTemplate, err := resolveActorTemplate(ctx, w.store, actor)
	if err != nil {
		return fmt.Errorf("while resolving actor template for termination: %w", err)
	}
	spec, err := workloadSpecFromActorTemplate(actorTemplate, actor)
	if err != nil {
		return fmt.Errorf("while resolving workload for termination: %w", err)
	}
	conn, err := w.dialer.DialForWorker(assignment.GetWorkerNamespace(), assignment.GetWorkerPod())
	if errors.Is(err, ErrWorkerPodNotFound) {
		// The workload is already gone with its worker pod.
		return nil
	}
	if err != nil {
		return fmt.Errorf("while getting atelet conn for expired actor: %w", err)
	}
	_, err = ateletpb.NewAteomHerderClient(conn).Terminate(ctx, &ateletpb.TerminateRequest{
		TargetAteomUid:        assignment.GetWorkerPodUid(),
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorUid:              actor.GetMetadata().GetUid(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		Spec:                  spec,
	})
	if err != nil {
		return fmt.Errorf("while terminating expired actor workload: %w", err)
	}
	return nil
}
