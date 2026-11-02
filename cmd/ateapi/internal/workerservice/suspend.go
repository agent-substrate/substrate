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

package workerservice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
)

// RequestActorSuspend suspends an Actor on behalf of the Worker hosting it. The
// caller must be an atelet running on the Worker's node.
func (s *Server) RequestActorSuspend(ctx context.Context, req *ateapi.RequestActorSuspendRequest) (*ateapi.RequestActorSuspendResponse, error) {
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateRequestActorSuspendRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	workerName := req.GetWorker().GetName()
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	if err := s.authorizeHostedActor(ctx, caller, workerName, actorRef, req.GetActorUid()); err != nil {
		return nil, err
	}

	// From here it is an ordinary suspend. Running the same workflow Control
	// runs is the point: its preconditions already refuse an Actor that is not
	// suspendable, and a proposal that races a resume, suspend, or delete loses
	// to whichever holds the Actor's lease.
	//
	// TODO: the checks above run outside the Actor's lease, so there is a small
	// race with the suspend that follows: within the one round trip it takes
	// the workflow to acquire the lease, the Actor can be resumed onto another
	// Worker, or deleted and recreated under the same name, and the workflow
	// suspends whatever the reference resolves to by then. See #1773.
	resp, err := s.control.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: req.GetActor()})
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Worker requested an actor suspend",
		slog.String("worker", workerName),
		slog.String("actor", actorRef.String()),
		slog.String("actor_uid", req.GetActorUid()),
		slog.String("node", caller.NodeName))
	return &ateapi.RequestActorSuspendResponse{Actor: resp.GetActor()}, nil
}

// authorizeHostedActor checks that the calling atelet may speak for actorRef:
// workerName must be on the caller's node, and the actor must be assigned to it.
func (s *Server) authorizeHostedActor(ctx context.Context, caller *ateletauth.Caller, workerName string, actorRef resources.ActorRef, actorUID string) error {
	// Use authoritative state to authorize the request, never the request
	// itself: the caller proves which node it is on, and nothing else.
	worker, err := s.store.GetWorker(ctx, workerName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return apierror.NotFound("Worker %s not found", workerName)
		}
		return fmt.Errorf("while fetching worker %s: %w", workerName, err)
	}
	if worker.GetNodeName() != caller.NodeName {
		// Do not disclose Workers on other nodes.
		slog.WarnContext(ctx, "Refusing a request for a worker on another node",
			slog.String("worker", workerName),
			slog.String("worker_node", worker.GetNodeName()),
			slog.String("caller_node", caller.NodeName),
			slog.String("caller_pod", caller.PodName))
		return apierror.NotFound("Worker %s not found", workerName)
	}

	actor, err := s.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return apierror.NotFound("Actor %s not found", actorRef)
		}
		return fmt.Errorf("while fetching actor %s: %w", actorRef, err)
	}
	return checkActorHostedBy(ctx, actor, workerName, actorUID)
}

// checkActorHostedBy reports whether the Worker may speak for this Actor: the
// Actor must be assigned to it, and must be the incarnation the Worker thinks
// it is hosting.
//
// Both failures are NotFound rather than PermissionDenied, as for a Worker on
// another node: an atelet learns nothing about Actors it does not host, not
// even that they exist.
func checkActorHostedBy(ctx context.Context, actor *ateapi.Actor, workerName, actorUID string) error {
	actorRef := resources.ActorRefFromActor(actor)
	if got := actor.GetMetadata().GetUid(); got != actorUID {
		// The named Actor was deleted and recreated, and the caller never
		// hosted the recreation.
		slog.WarnContext(ctx, "Refusing a request naming a stale actor incarnation",
			slog.String("worker", workerName),
			slog.String("actor", actorRef.String()),
			slog.String("requested_uid", actorUID),
			slog.String("actual_uid", got))
		return apierror.NotFound("Actor %s not found", actorRef)
	}
	// An Actor with no assignment has no worker to speak for it: it is
	// SUSPENDED (with or without a local snapshot) or CRASHED, and nothing
	// hosts it.
	if assigned := actor.GetStatus().GetWorkerAssignment().GetWorker().GetName(); assigned != workerName {
		slog.WarnContext(ctx, "Refusing a request for an actor hosted elsewhere",
			slog.String("worker", workerName),
			slog.String("actor", actorRef.String()),
			slog.String("assigned_worker", assigned))
		return apierror.NotFound("Actor %s not found", actorRef)
	}
	return nil
}
