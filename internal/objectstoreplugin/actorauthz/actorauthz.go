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

// Package actorauthz holds the checks every provider's credential exchange
// must apply to a worker-for-actor JWT before it issues any credential. It
// verifies the JWT, looks the actor up, checks the actor is still on the
// token's node, and derives the actor's storage grants. It is provider-neutral
// and imports no cloud SDK.
package actorauthz

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorlookup"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/oidcjwt"
	"go.ate.dev/substrate/internal/resources"
)

// Audience is the audience of the objectstore plugin API. atelet requests it
// on the worker-for-actor JWT it hands to whichever node plugin is installed,
// and every exchange serving this API requires it. A token minted for another
// audience is refused.
const Audience = "objectstore-plugin.ate.dev"

// Lookup resolves the actor a verified JWT names. actorlookup implements it.
type Lookup interface {
	Actor(ctx context.Context, claims *actoridjwt.WireSubstrateClaims) (*ateapi.Actor, error)
}

// Verifier checks a JWT and returns its claims. oidcjwt.Verifier implements
// it, built with WithSubstrateClaims so the token's kind is known.
type Verifier interface {
	Verify(ctx context.Context, jwt string, now time.Time) (*oidcjwt.Claims, error)
}

// NewVerifier returns the verifier every exchange checks JWTs with: tokens
// from issuer for Audience, with Substrate's claims, verified with the keys
// in the JWK set file jwksFile. It reads the file once, so a bad path fails
// here rather than on the first exchange.
func NewVerifier(ctx context.Context, issuer, jwksFile string) (*oidcjwt.Verifier, error) {
	keys := oidcjwt.FileKeySource(jwksFile)
	if _, err := keys.Keys(ctx); err != nil {
		return nil, fmt.Errorf("while reading the actor JWT key set: %w", err)
	}
	return oidcjwt.NewVerifier(issuer, []string{Audience}, keys, oidcjwt.WithSubstrateClaims()), nil
}

// Decision is what Authorize allowed: the UID of the actor a credential may
// be issued for, and the storage grants the credential must be limited to.
type Decision struct {
	ActorUID string
	Grants   []actorscope.Grant
}

// Authorizer runs the checks.
type Authorizer struct {
	verifier Verifier
	lookup   Lookup
	now      func() time.Time
}

// New returns an Authorizer. The verifier must accept only Audience, as one
// from NewVerifier does.
func New(v Verifier, l Lookup) *Authorizer {
	return &Authorizer{verifier: v, lookup: l, now: time.Now}
}

// Authorize checks jwt and returns what a credential may be issued for. Only
// a worker-for-actor token is accepted: an actor's own token has the same
// shape and can be minted with the same audience, and actor code must not
// obtain credentials for its own snapshots. Every error is a gRPC status
// error the exchange can return as is.
func (a *Authorizer) Authorize(ctx context.Context, jwt string) (*Decision, error) {
	if jwt == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_jwt is required")
	}
	claims, err := a.verifier.Verify(ctx, jwt, a.now())
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "actor_jwt is not valid: %v", err)
	}
	if claims.Kind != actoridjwt.KindWorkerForActor {
		return nil, status.Error(codes.PermissionDenied, "only a worker-for-actor token may be exchanged")
	}
	if sub := claims.Substrate; sub == nil || sub.Atespace == "" || sub.ActorName == "" || sub.ActorUID == "" || sub.Node == "" {
		return nil, status.Error(codes.Unauthenticated, "actor_jwt must carry the actor's atespace, name and UID and the minting node, and one of them is empty")
	}
	actor, err := a.lookup.Actor(ctx, claims.Substrate)
	switch {
	case errors.Is(err, actorlookup.ErrStale), status.Code(err) == codes.NotFound:
		return nil, status.Error(codes.NotFound, "actor not found")
	case status.Code(err) == codes.InvalidArgument:
		// ate-api-server refused the actor reference the token carries.
		return nil, status.Errorf(codes.Unauthenticated, "actor_jwt names an invalid actor: %v", err)
	case status.Code(err) == codes.PermissionDenied, status.Code(err) == codes.Unauthenticated:
		// The exchange's own identity lacks access. Retrying cannot help.
		return nil, status.Errorf(codes.FailedPrecondition, "the exchange may not read actors: %v", err)
	case status.Code(err) == codes.Canceled, status.Code(err) == codes.DeadlineExceeded:
		// The caller gave up, which says nothing about ate-api-server.
		return nil, err
	case err != nil:
		return nil, status.Errorf(codes.Unavailable, "while looking up the actor: %v", err)
	}
	if err := actorStillOnNode(actor, claims.Substrate.Node); err != nil {
		return nil, err
	}
	grants, err := actorscope.FromActor(actor)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "while deriving the actor's storage grants: %v", err)
	}
	if len(grants) == 0 {
		return nil, status.Error(codes.PermissionDenied, "actor has no snapshot location to grant")
	}
	return &Decision{ActorUID: actor.GetMetadata().GetUid(), Grants: grants}, nil
}

// actorStillOnNode refuses a token minted for a node the actor has since
// left, or for an actor in a state with no transfer. ate-api-server checked
// the node when it minted, and the token outlives that check by up to an
// hour. status.assigned_node names the node holding the actor: a resume sets
// it to the worker's node, it stays set while the actor is SUSPENDED and its
// local snapshot remains on that node, and it is cleared when the node
// detaches the actor, by a revert and by a delete, or replaced by a resume
// onto another node. So a token from a node the actor moved away from, or
// one kept past the actor's last transfer there, no longer matches.
func actorStillOnNode(actor *ateapi.Actor, node string) error {
	st := actor.GetStatus()
	if !slices.Contains(resources.TransferStates, st.GetState()) {
		return status.Errorf(codes.PermissionDenied, "actor is %s, no transfer is due", st.GetState())
	}
	if st.GetAssignedNode() != node {
		return status.Error(codes.PermissionDenied, "actor is no longer on the token's node")
	}
	return nil
}
