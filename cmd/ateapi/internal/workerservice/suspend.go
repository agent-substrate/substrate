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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// RequestActorSuspend suspends an Actor on behalf of the Worker hosting it. The
// caller must be an atelet running on the Worker's node.
func (s *Server) RequestActorSuspend(ctx context.Context, req *ateapipb.RequestActorSuspendRequest) (*ateapipb.RequestActorSuspendResponse, error) {
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	// TODO: replace the three checks below with the generated
	// Validate_RequestActorSuspendRequest, which already enforces all of them
	// from the tags on the message. It lives in controlapi today, because that
	// package holds the only +k8s:validation-gen marker, and this package must
	// not depend on the Control service. Once the marker moves to a neutral
	// package both can import, call the generated validator here and drop
	// validateActorRef with it.
	//
	// Workers are global-scoped, so the reference carries no atespace.
	if errs := resources.ValidateGlobalObjectRef(req.GetWorker(), field.NewPath("worker")); len(errs) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid worker: %v", errs.ToAggregate())
	}
	if errs := validateActorRef(req.GetActor(), field.NewPath("actor")); len(errs) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid actor: %v", errs.ToAggregate())
	}
	if req.GetActorUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_uid is required")
	}

	// A golden actor may be suspended, but only by the template controller:
	// the suspend is what commits its snapshot, and the controller runs it once
	// the warmup window it set has elapsed. A golden actor boots and waits, so
	// it looks idle from the moment it starts, and honoring its own request
	// would commit the snapshot before that window ends -- leaving every actor
	// restored from the template to start from a workload that never warmed up.
	if req.GetActor().GetAtespace() == resources.GoldenActorAtespace {
		return nil, status.Errorf(codes.FailedPrecondition, "actors in atespace %q are golden actors, which cannot request their own suspend", resources.GoldenActorAtespace)
	}

	workerName := req.GetWorker().GetName()
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())

	// Use authoritative state to authorize the request, never the request
	// itself: the caller proves which node it is on, and nothing else.
	worker, err := s.store.GetWorker(ctx, workerName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Worker %s not found", workerName)
		}
		return nil, fmt.Errorf("while fetching worker %s: %w", workerName, err)
	}
	if worker.GetNodeName() != caller.NodeName {
		// Do not disclose Workers on other nodes.
		slog.WarnContext(ctx, "Refusing a suspend request for a worker on another node",
			slog.String("worker", workerName),
			slog.String("worker_node", worker.GetNodeName()),
			slog.String("caller_node", caller.NodeName),
			slog.String("caller_pod", caller.PodName))
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", workerName)
	}

	actor, err := s.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
		}
		return nil, fmt.Errorf("while fetching actor %s: %w", actorRef, err)
	}
	if err := checkActorHostedBy(ctx, actor, workerName, req.GetActorUid()); err != nil {
		return nil, err
	}

	// From here it is an ordinary suspend. Running the same workflow Control
	// runs is the point: its preconditions already refuse an Actor that is not
	// suspendable, and a proposal that races a resume, pause, or delete loses
	// to whichever holds the Actor's lease.
	//
	// TODO: the checks above run outside the Actor's lease, so there is a small
	// race with the suspend that follows: within the one round trip it takes
	// the workflow to acquire the lease, the Actor can be resumed onto another
	// Worker, or deleted and recreated under the same name, and the workflow
	// suspends whatever the reference resolves to by then. See #1773.
	resp, err := s.suspender.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: req.GetActor()})
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Worker requested an actor suspend",
		slog.String("worker", workerName),
		slog.String("actor", actorRef.String()),
		slog.String("actor_uid", req.GetActorUid()),
		slog.String("node", caller.NodeName))
	return &ateapipb.RequestActorSuspendResponse{Actor: resp.GetActor()}, nil
}

// checkActorHostedBy reports whether the Worker may speak for this Actor: the
// Actor must be assigned to it, and must be the incarnation the Worker thinks
// it is hosting.
//
// Both failures are NotFound rather than PermissionDenied, as for a Worker on
// another node: an atelet learns nothing about Actors it does not host, not
// even that they exist.
func checkActorHostedBy(ctx context.Context, actor *ateapipb.Actor, workerName, actorUID string) error {
	actorRef := resources.ActorRefFromActor(actor)
	if got := actor.GetMetadata().GetUid(); got != actorUID {
		// The named Actor was deleted and recreated. Suspending the recreation
		// would checkpoint a workload the caller never hosted.
		slog.WarnContext(ctx, "Refusing a suspend request naming a stale actor incarnation",
			slog.String("worker", workerName),
			slog.String("actor", actorRef.String()),
			slog.String("requested_uid", actorUID),
			slog.String("actual_uid", got))
		return status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
	}
	// An Actor with no assignment has no worker to speak for it: it is
	// SUSPENDED, PAUSED, or CRASHED, and nothing hosts it.
	if assigned := actor.GetStatus().GetWorkerAssignment().GetWorker().GetName(); assigned != workerName {
		slog.WarnContext(ctx, "Refusing a suspend request for an actor hosted elsewhere",
			slog.String("worker", workerName),
			slog.String("actor", actorRef.String()),
			slog.String("assigned_worker", assigned))
		return status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
	}
	return nil
}

// validateActorRef checks that a reference to an atespace-scoped resource is
// well-formed. Unlike resources.ValidateGlobalObjectRef, which forbids the
// atespace, an Actor is always in one and must name it.
func validateActorRef(ref *ateapipb.ObjectRef, fldPath *field.Path) field.ErrorList {
	if ref == nil {
		return field.ErrorList{field.Required(fldPath, "")}
	}
	var errs field.ErrorList
	for _, part := range []struct {
		value   string
		fldPath *field.Path
	}{
		{ref.GetAtespace(), fldPath.Child("atespace")},
		{ref.GetName(), fldPath.Child("name")},
	} {
		if part.value == "" {
			errs = append(errs, field.Required(part.fldPath, ""))
			continue
		}
		errs = append(errs, resources.ValidateResourceName(part.value, part.fldPath)...)
	}
	return errs
}
