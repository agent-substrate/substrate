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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// GetActorEgressPolicy returns the egress policy of an Actor hosted on the
// caller's node, for the Worker hosting it to enforce. The caller must be an
// atelet.
func (s *Server) GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateGetActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	if err := s.checkActorHostedOnNode(ctx, actorRef, caller.NodeName); err != nil {
		return nil, err
	}

	// Policies are found by the Actor's name, not its UID, as on Control.
	policy, err := s.store.GetEgressPolicy(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, egressPolicyNotFound(actorRef)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting egress policy of actor %s: %w", actorRef, err)
	}
	return policy, nil
}

// egressPolicyNotFound is the answer both for a hosted Actor with no policy and
// for an Actor the caller's node does not host, so the two are
// indistinguishable to the caller.
func egressPolicyNotFound(actorRef resources.ActorRef) error {
	return apierror.NotFound("EgressPolicy for actor %s not found", actorRef)
}

// checkActorHostedOnNode refuses an atelet whose node does not host the Actor.
// Authoritative state decides, never the request. The refusal is NotFound, the
// same as for an Actor with no policy, so a node learns nothing about Actors it
// does not host, not even that they exist.
func (s *Server) checkActorHostedOnNode(ctx context.Context, actorRef resources.ActorRef, callerNode string) error {
	actor, err := s.store.GetActor(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return egressPolicyNotFound(actorRef)
	}
	if err != nil {
		return fmt.Errorf("while fetching actor %s: %w", actorRef, err)
	}
	hostingNode, err := controlapi.HostingNode(ctx, s.store, actor)
	if err != nil {
		return err
	}
	if hostingNode == "" || hostingNode != callerNode {
		slog.WarnContext(ctx, "Refusing an egress policy read for an actor the caller's node does not host",
			slog.String("actor", actorRef.String()),
			slog.String("hosting_node", hostingNode),
			slog.String("caller_node", callerNode))
		return egressPolicyNotFound(actorRef)
	}
	return nil
}
