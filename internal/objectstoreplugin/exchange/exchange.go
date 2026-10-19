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

// Package exchange serves this plugin's credential exchange. The node plugin
// presents the worker-for-actor JWT atelet gave it and gets back a GCS access
// token scoped to that actor's snapshot prefixes. The handler verifies the
// JWT with oidcjwt, looks the actor up with actorlookup, derives the grants
// with actorscope, and hands them to an Issuer. Those three packages are
// provider-neutral and are what another provider's plugin reuses. This
// package and its proto are not: they are shaped for this binary's
// credential, and the Issuer interface is the seam between its backends.
package exchange

import (
	"context"
	"errors"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorlookup"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/objectstoreplugin/credential"
	"go.ate.dev/substrate/internal/oidcjwt"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// ActorJWTAudience is the audience of the objectstore plugin API. atelet
// requests it on the worker-for-actor JWT it hands to whichever node plugin is
// installed, and every exchange serving this API requires it. A token minted
// for another audience is refused.
const ActorJWTAudience = "objectstore-plugin.ate.dev"

// Issuer mints a credential limited to grants for the actor. force skips any
// cache the issuer keeps. An error carrying a gRPC status code other than
// Unavailable is passed to the caller unchanged, for failures the same request
// would hit again, and one wrapping the context's error keeps the caller's
// code. Any other error is reported as Unavailable. The GCS issuer is
// gcsissuer.
type Issuer interface {
	Issue(ctx context.Context, actorUID string, grants []actorscope.Grant, force bool) (credential.Token, error)
}

// Lookup resolves the actor a verified JWT names. actorlookup implements it.
type Lookup interface {
	Actor(ctx context.Context, claims *actoridjwt.WireSubstrateClaims) (*ateapipb.Actor, error)
}

// Verifier checks a JWT and returns its claims. oidcjwt.Verifier implements
// it, built with WithSubstrateClaims so the token's kind is known.
type Verifier interface {
	Verify(ctx context.Context, jwt string, now time.Time) (*oidcjwt.Claims, error)
}

// Server implements CredentialExchange.
type Server struct {
	exchangepb.UnimplementedCredentialExchangeServer
	verifier Verifier
	lookup   Lookup
	issuer   Issuer
	now      func() time.Time
}

// New returns a Server. The verifier must accept ActorJWTAudience.
func New(verifier Verifier, lookup Lookup, issuer Issuer) *Server {
	return &Server{verifier: verifier, lookup: lookup, issuer: issuer, now: time.Now}
}

// ExchangeActorJwt is the exchange. Only a worker-for-actor token is accepted:
// an actor's own token has the same shape and can be minted with this
// audience, and actor code must not obtain credentials for its own snapshots.
func (s *Server) ExchangeActorJwt(ctx context.Context, req *exchangepb.ExchangeActorJwtRequest) (*exchangepb.ExchangeActorJwtResponse, error) {
	if req.GetActorJwt() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_jwt is required")
	}
	claims, err := s.verifier.Verify(ctx, req.GetActorJwt(), s.now())
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "actor_jwt is not valid: %v", err)
	}
	if claims.Kind != actoridjwt.KindWorkerForActor {
		return nil, status.Error(codes.PermissionDenied, "only a worker-for-actor token may be exchanged")
	}
	if sub := claims.Substrate; sub == nil || sub.Atespace == "" || sub.ActorName == "" || sub.ActorUID == "" || sub.Node == "" {
		return nil, status.Error(codes.Unauthenticated, "actor_jwt must carry the actor's atespace, name and UID and the minting node, and one of them is empty")
	}
	actor, err := s.lookup.Actor(ctx, claims.Substrate)
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
	tok, err := s.issuer.Issue(ctx, actor.GetMetadata().GetUid(), grants, req.GetForce())
	if err != nil {
		return nil, status.Errorf(issueCode(err), "while issuing the credential: %v", err)
	}
	return &exchangepb.ExchangeActorJwtResponse{AccessToken: tok.AccessToken, ExpireTime: timestamppb.New(tok.Expiry)}, nil
}

// issueCode is the code the caller sees for an issuer error. A context error
// keeps the caller's code, a gRPC status keeps its own, and anything else is
// a backend failure worth retrying.
func issueCode(err error) codes.Code {
	switch {
	case errors.Is(err, context.Canceled):
		return codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded
	case status.Code(err) == codes.Unknown:
		return codes.Unavailable
	}
	return status.Code(err)
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
func actorStillOnNode(actor *ateapipb.Actor, node string) error {
	st := actor.GetStatus()
	if !slices.Contains(resources.TransferStates, st.GetState()) {
		return status.Errorf(codes.PermissionDenied, "actor is %s, no transfer is due", st.GetState())
	}
	if st.GetAssignedNode() != node {
		return status.Error(codes.PermissionDenied, "actor is no longer on the token's node")
	}
	return nil
}
