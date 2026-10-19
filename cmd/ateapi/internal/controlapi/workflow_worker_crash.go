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

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// CrashActorReportedByWorker crashes a RUNNING actor whose sandbox the worker
// hosting it saw die. It holds the actor's lease, so an operation in flight
// keeps the actor and fails on its own if the sandbox is gone.
func (w *ActorWorkflow) CrashActorReportedByWorker(ctx context.Context, actorRef resources.ActorRef, actorUID, workerName, message string) (*ateapipb.Actor, error) {
	ctx = preview.ContextWithOptIn(ctx)
	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	actor, err := w.store.GetActor(leaseCtx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, fmt.Errorf("while loading actor %s: %w", actorRef, err)
	}
	// rechecked under the lease, the caller's checks raced other operations
	if actor.GetMetadata().GetUid() != actorUID || actor.GetStatus().GetWorkerAssignment().GetWorker().GetName() != workerName {
		return nil, apierror.NotFound("Actor %s not found", actorRef)
	}
	if state := actor.GetStatus().GetState(); state != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return nil, apierror.FailedPrecondition("actor %s is %s, not RUNNING", actorRef, state)
	}
	if err := crashActor(leaseCtx, w.store, actorRef, ateattr.OperationUnknown, message); err != nil {
		return nil, err
	}
	return w.store.GetActor(leaseCtx, actorRef)
}
