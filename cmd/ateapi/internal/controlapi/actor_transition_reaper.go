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

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	// transitionReapInterval is how often a replica tries to sweep the Actors
	// for abandoned transitions.
	transitionReapInterval = time.Minute

	// transitionGracePeriod is how long an Actor may sit in a transitional
	// state, unleased and unwritten, before the reaper resolves it. A workflow
	// renews its lease every third of the lease TTL (30s in atepg), so the
	// lease of a dead workflow lapses well inside this. The rest is a margin
	// for clock skew between replicas, which stamp update_time, and for an
	// atelet call that outlived its workflow to settle.
	transitionGracePeriod = 2 * time.Minute

	// transitionReaperLeaseKey is the lease one replica holds while it sweeps.
	transitionReaperLeaseKey = "lease:actor-transition-reaper"

	crashMessageTransitionAbandoned = "workflow stopped before the transition finished"
)

// transitionReaperStore is the store methods the reaper uses.
type transitionReaperStore interface {
	crashActorStore
	actorTemplateGetter
	ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.Actor], error)
	AcquireLease(ctx context.Context, key string) (*store.Lease, error)
}

// ActorTransitionReaper resolves Actors whose transition workflow died, for
// example with the ate-api-server replica running it. Such an Actor stays
// RESUMING, SUSPENDING, DELETING or REVERTING, keeps its worker assignment,
// and no RPC but DeleteActor accepts it, so its share of the worker is lost
// until someone deletes it.
//
// A workflow holds the Actor's lease from before it writes the transitional
// state until it leaves it, so an Actor found in one with no lease held has
// been abandoned. The reaper crashes an abandoned SUSPENDING, RESUMING or
// REVERTING Actor, which releases its worker and lets RevertActor return it to
// its last snapshot, and resumes an abandoned delete, which is re-drivable.
//
// One replica sweeps per interval: each takes transitionReaperLeaseKey and
// the others skip. The TODO on WorkerAssignmentReconciler describes the proper
// fix, one leader for loops like this. The per-Actor lease keeps a workflow
// that is still running, or a second sweeper, off an Actor.
type ActorTransitionReaper struct {
	persistence transitionReaperStore
	control     *RPCService
	now         func() time.Time
	// reapTimeout bounds the work on one Actor, as MaxDeadlineUnaryInterceptor
	// bounds an RPC, so an atelet call that hangs does not stall the sweep.
	reapTimeout time.Duration
}

func NewActorTransitionReaper(persistence transitionReaperStore, control *RPCService) *ActorTransitionReaper {
	return &ActorTransitionReaper{
		persistence: persistence,
		control:     control,
		now:         time.Now,
		reapTimeout: MaxRPCDeadline,
	}
}

// Start sweeps every transitionReapInterval until ctx ends.
func (r *ActorTransitionReaper) Start(ctx context.Context) {
	go wait.UntilWithContext(ctx, r.sweep, transitionReapInterval)
}

// sweep lists every Actor and resolves the abandoned ones. A failure on one
// Actor is logged and the rest are swept; the next sweep retries it.
func (r *ActorTransitionReaper) sweep(ctx context.Context) {
	lease, err := r.persistence.AcquireLease(ctx, transitionReaperLeaseKey)
	if errors.Is(err, store.ErrLeaseConflict) {
		slog.DebugContext(ctx, "Another replica is sweeping for abandoned transitions")
		return
	}
	if err != nil {
		slog.ErrorContext(ctx, "Failed to acquire the lease to sweep for abandoned transitions", slog.Any("err", err))
		return
	}
	defer lease.Close()
	ctx = lease.Context()

	for token := ""; ; {
		page, err := r.persistence.ListActors(ctx, "", store.ListOptions{PageToken: token})
		if err != nil {
			slog.ErrorContext(ctx, "Failed to list actors for abandoned transitions", slog.Any("err", err))
			return
		}
		for _, actor := range page.Items {
			if !r.abandoned(actor) {
				continue
			}
			attrs := append(ateattr.ActorRefLogAttrs(resources.ActorRefFromActor(actor)),
				slog.String("state", actor.GetStatus().GetState().String()))
			reapCtx, cancel := context.WithTimeout(ctx, r.reapTimeout)
			err := r.reap(reapCtx, actor)
			cancel()
			switch {
			case errors.Is(err, errActorBusy):
				slog.LogAttrs(ctx, slog.LevelDebug, "Actor in a transitional state has an operation in progress, leaving it", attrs...)
			case err != nil:
				slog.LogAttrs(ctx, slog.LevelError, "Failed to resolve an actor whose transition workflow died", append(attrs, slog.Any("err", err))...)
			}
		}
		if !page.HasNextPage() {
			return
		}
		token = page.NextPageToken
	}
}

// abandoned reports whether actor is in a transitional state and has not been
// written for transitionGracePeriod.
func (r *ActorTransitionReaper) abandoned(actor *ateapipb.Actor) bool {
	if transitionOperation(actor.GetStatus().GetState()) == ateattr.OperationUnknown {
		return false
	}
	return r.now().Sub(actor.GetMetadata().GetUpdateTime().AsTime()) >= transitionGracePeriod
}

// transitionOperation names the operation that drives an Actor through state,
// or OperationUnknown if state is not transitional.
func transitionOperation(state ateapipb.ActorState) string {
	switch state {
	case ateapipb.ActorState_ACTOR_STATE_RESUMING:
		return ateattr.OperationResume
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		return ateattr.OperationSuspend
	case ateapipb.ActorState_ACTOR_STATE_DELETING:
		return ateattr.OperationDelete
	case ateapipb.ActorState_ACTOR_STATE_REVERTING:
		return ateattr.OperationRevert
	default:
		return ateattr.OperationUnknown
	}
}

// reap resolves one abandoned Actor as listed. An Actor whose lease is held
// is reported as errActorBusy. An Actor that changed since the listing, or is
// gone, is left alone.
func (r *ActorTransitionReaper) reap(ctx context.Context, listed *ateapipb.Actor) error {
	actorRef := resources.ActorRefFromActor(listed)
	if listed.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_DELETING {
		// DeleteActor takes the lease itself and fast-forwards past the steps
		// the dead delete finished. The uid keeps it off a new incarnation.
		_, err := r.control.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor:   actorRef.ToObjectRef(),
			Options: &ateapipb.DeleteOptions{Uid: listed.GetMetadata().GetUid()},
		})
		switch apierror.Code(err) {
		case codes.OK:
			slog.LogAttrs(ctx, slog.LevelInfo, "Finished the delete of an actor whose delete workflow died", ateattr.ActorRefLogAttrs(actorRef)...)
			return nil
		case codes.NotFound:
			return nil
		case codes.Aborted:
			// A held lease and a uid that no longer matches both come back
			// Aborted. Tell them apart: a new incarnation is not abandoned.
			current, getErr := r.persistence.GetActor(ctx, actorRef)
			if errors.Is(getErr, store.ErrNotFound) || (getErr == nil && current.GetMetadata().GetUid() != listed.GetMetadata().GetUid()) {
				return nil
			}
			return fmt.Errorf("%w: %w", errActorBusy, err)
		default:
			return err
		}
	}

	lease, err := r.persistence.AcquireLease(ctx, actorLeaseKey(actorRef))
	if errors.Is(err, store.ErrLeaseConflict) {
		return errActorBusy
	}
	if err != nil {
		return fmt.Errorf("while acquiring the lease of actor %s: %w", actorRef, err)
	}
	defer lease.Close()
	ctx = lease.Context()

	actor, err := r.persistence.GetActor(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("while getting actor %s: %w", actorRef, err)
	}
	if actor.GetMetadata().GetUid() != listed.GetMetadata().GetUid() || !r.abandoned(actor) {
		return nil
	}

	opName := transitionOperation(actor.GetStatus().GetState())
	slog.LogAttrs(ctx, slog.LevelWarn, "Crashing an actor whose transition workflow died",
		append(ateattr.ActorRefLogAttrs(actorRef),
			slog.String("state", actor.GetStatus().GetState().String()),
			slog.Time("last_write", actor.GetMetadata().GetUpdateTime().AsTime()))...)
	// Terminate the sandbox before the crash releases the worker: atelet
	// leaves the workload alone when the request names no worker pod, and
	// ensureAteletTerminated names none once the worker no longer hosts the
	// actor. The crash goes ahead if this fails. The actor keeps its node, so
	// RevertActor or DeleteActor terminates the sandbox later.
	if err := r.terminateSandbox(ctx, actorRef, actor, opName); err != nil {
		slog.LogAttrs(ctx, slog.LevelWarn, "Failed to terminate the sandbox of an actor being crashed, leaving it to RevertActor or DeleteActor",
			append(ateattr.ActorRefLogAttrs(actorRef), slog.Any("err", err))...)
	}
	return crashActorVersion(ctx, r.persistence, actorRef, actor, opName, crashMessageTransitionAbandoned)
}

// terminateSandbox asks atelet to terminate the actor's sandbox, the way
// RevertActor and DeleteActor do. A missing template is terminated from the
// volumes the actor records, like DeleteActor.
func (r *ActorTransitionReaper) terminateSandbox(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, opName string) error {
	actorTemplate, err := resolveActorTemplate(ctx, r.persistence, actor)
	if errors.Is(err, errActorTemplateNotFound) {
		actorTemplate, err = nil, nil
	}
	if err != nil {
		return err
	}
	return r.control.actorWorkflow.ensureAteletTerminated(ctx, actorRef, actor, actorTemplate, opName)
}
